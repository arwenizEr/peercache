package actor_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/actor"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

func newRuntime(t *testing.T, opts ...func(*actor.Options)) (*actor.Runtime, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	o := actor.Options{Clock: fc}
	for _, opt := range opts {
		opt(&o)
	}
	rt, err := actor.New(o)
	if err != nil {
		t.Fatalf("actor.New: %v", err)
	}
	t.Cleanup(func() {
		if err := rt.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
		if pending := fc.Pending(); pending != 0 {
			t.Errorf("timer leak: %d timer(s) armed after Close", pending)
		}
	})
	return rt, fc
}

// counterFunc increments a big-endian counter held under "count" and returns it.
func counterFunc(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
	var n uint64
	if cur, ok := s.Get("count"); ok {
		n = binary.BigEndian.Uint64(cur)
	}
	n++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	s.Set("count", buf)
	return buf, nil
}

func TestCallRunsRegisteredFunction(t *testing.T) {
	rt, _ := newRuntime(t)
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for i := 1; i <= 3; i++ {
		got, err := rt.Call(context.Background(), "g", "increment", nil)
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if n := binary.BigEndian.Uint64(got); n != uint64(i) {
			t.Fatalf("Call %d returned %d", i, n)
		}
	}
}

func TestCallOfAnUnregisteredFunction(t *testing.T) {
	rt, _ := newRuntime(t)
	if _, err := rt.Call(context.Background(), "g", "missing", nil); !errors.Is(err, core.ErrFuncNotRegistered) {
		t.Fatalf("Call = %v, want ErrFuncNotRegistered", err)
	}
}

func TestRegisterRejectsDuplicatesAndEmptyNames(t *testing.T) {
	rt, _ := newRuntime(t)
	if err := rt.Register("f", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("f", counterFunc); !errors.Is(err, core.ErrDuplicateRegistration) {
		t.Fatalf("duplicate = %v, want ErrDuplicateRegistration", err)
	}
	if err := rt.Register("", counterFunc); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("empty name = %v, want ErrInvalidOption", err)
	}
	if err := rt.Register("nil", nil); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("nil function = %v, want ErrInvalidOption", err)
	}
	if !rt.Registered("f") || rt.Registered("nope") {
		t.Fatal("Registered disagrees with Register")
	}
}

// TestTenThousandCallsAcrossOneThousandGroups is the M2 acceptance criterion:
// 10k concurrent calls across 1k groups produce serialized, correct results
// under -race.
//
// Correct arithmetic *is* the proof of serialization. Every call increments a
// counter held in group state with no lock of any kind; if two turns for one
// group ever overlapped, the read-modify-write would lose increments and the
// totals would come out short. A test that merely checked for the absence of a
// race report would prove far less, because the detector only sees the
// interleavings that actually happen.
func TestTenThousandCallsAcrossOneThousandGroups(t *testing.T) {
	const groups = 1000
	const callsPerGroup = 10
	const total = groups * callsPerGroup

	rt, _ := newRuntime(t, func(o *actor.Options) {
		// Generous, so that this measures serialization rather than
		// backpressure. Rejection has its own test.
		o.MailboxSize = 256
		o.IdleTimeout = time.Hour
	})
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("g%d", i%groups)
			if _, err := rt.Call(context.Background(), id, "increment", nil); err != nil {
				errs <- fmt.Errorf("group %s: %w", id, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Call: %v", err)
	}

	// Every group must show exactly the number of calls it received.
	for g := 0; g < groups; g++ {
		id := fmt.Sprintf("g%d", g)
		got, err := rt.Do(context.Background(), id, func(s core.GroupState) (any, error) {
			cur, ok := s.Get("count")
			if !ok {
				return uint64(0), nil
			}
			return binary.BigEndian.Uint64(cur), nil
		})
		if err != nil {
			t.Fatalf("reading %s: %v", id, err)
		}
		if n := got.(uint64); n != callsPerGroup {
			t.Fatalf("group %s counted %d calls, want %d: turns overlapped and lost increments",
				id, n, callsPerGroup)
		}
	}

	stats := rt.Stats()
	if stats.Escapes != 0 {
		t.Fatalf("Stats().Escapes = %d, want 0", stats.Escapes)
	}
	if stats.Addressable != groups {
		t.Fatalf("Stats().Addressable = %d, want %d", stats.Addressable, groups)
	}
}

// TestConcurrentCallsToOneGroupAreSerialized is the sharper version: every call
// hits a single group, so the mailbox is the only thing standing between them.
func TestConcurrentCallsToOneGroupAreSerialized(t *testing.T) {
	const calls = 2000

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MailboxSize = calls
		o.IdleTimeout = time.Hour
	})

	// Deliberately racy if turns overlap: read, yield, write.
	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	if err := rt.Register("racy", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		if n := concurrent.Add(1); n > maxConcurrent.Load() {
			maxConcurrent.Store(n)
		}
		var count uint64
		if cur, ok := s.Get("count"); ok {
			count = binary.BigEndian.Uint64(cur)
		}
		runtime.Gosched() // widen any window that exists
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, count+1)
		s.Set("count", buf)
		concurrent.Add(-1)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := rt.Call(context.Background(), "hot", "racy", nil); err != nil {
				t.Errorf("Call: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := maxConcurrent.Load(); got != 1 {
		t.Fatalf("observed %d turns running at once, want 1", got)
	}
	got, err := rt.Do(context.Background(), "hot", func(s core.GroupState) (any, error) {
		cur, _ := s.Get("count")
		return binary.BigEndian.Uint64(cur), nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if n := got.(uint64); n != calls {
		t.Fatalf("counted %d of %d calls", n, calls)
	}
}

// TestFullMailboxRejectsRatherThanBlocking covers the backpressure rule. An
// unbounded queue would convert one slow group into a cluster-wide stall, so a
// full mailbox is an immediate, retryable error.
func TestFullMailboxRejectsRatherThanBlocking(t *testing.T) {
	const mailbox = 4

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MailboxSize = mailbox
		o.IdleTimeout = time.Hour
	})

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	if err := rt.Register("block", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// One call occupies the turn goroutine.
	go func() {
		_, _ = rt.Call(context.Background(), "g", "block", nil)
	}()
	<-entered

	// Offer far more work than the mailbox can hold. Every one of these
	// either lands in the mailbox — and then blocks until release, which is
	// why they must be concurrent — or is refused outright.
	const offered = mailbox * 8
	var rejections atomic.Int64
	results := make(chan error, offered)
	for i := 0; i < offered; i++ {
		go func() {
			_, err := rt.Call(context.Background(), "g", "block", nil)
			if errors.Is(err, core.ErrGroupOverloaded) {
				rejections.Add(1)
			}
			results <- err
		}()
	}

	// Rejection must happen without anything being released: that is the
	// whole point. Bounded, so a regression fails with a message instead of
	// hanging for the test timeout.
	const yields = 5_000_000
	for i := 0; rejections.Load() == 0; i++ {
		if i > yields {
			t.Fatalf("offered %d calls to a mailbox of %d and none was refused after %d yields; "+
				"a full mailbox is queueing or blocking instead of rejecting", offered, mailbox, yields)
		}
		runtime.Gosched()
	}
	if got := rt.Stats().Rejections; got == 0 {
		t.Fatal("Stats().Rejections did not count the rejection")
	}

	close(release)
	for i := 0; i < offered; i++ {
		if err := <-results; err != nil && !errors.Is(err, core.ErrGroupOverloaded) {
			t.Fatalf("Call = %v, want nil or ErrGroupOverloaded", err)
		}
	}
}

// TestCallerDeadlineIsRespected covers the other half of the backpressure rule:
// a caller waiting on a slow turn returns when its context ends.
func TestCallerDeadlineIsRespected(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Hour })

	release := make(chan struct{})
	defer close(release)
	entered := make(chan struct{}, 1)
	if err := rt.Register("block", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		entered <- struct{}{}
		<-release
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	go func() { _, _ = rt.Call(context.Background(), "g", "block", nil) }()
	<-entered

	ctx, cancel := clock.WithTimeout(context.Background(), fc, time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := rt.Call(ctx, "g", "block", nil)
		done <- err
	}()

	// The idle timer plus the caller's deadline.
	fc.BlockUntil(2)
	fc.Advance(time.Second)

	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call = %v, want DeadlineExceeded", err)
	}
}

// TestPanickingGroupFuncDoesNotWedgeTheGroup is the containment rule. A panic
// would otherwise kill the group's only goroutine, taking that group offline
// until the process restarted — one bad input costing a key range.
func TestPanickingGroupFuncDoesNotWedgeTheGroup(t *testing.T) {
	rt, _ := newRuntime(t)
	if err := rt.Register("boom", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		panic("group function bug")
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "boom", nil); !errors.Is(err, core.ErrGroupFuncPanic) {
		t.Fatalf("Call = %v, want ErrGroupFuncPanic", err)
	}

	// The group must still work, and its state must still be there.
	for i := 1; i <= 2; i++ {
		got, err := rt.Call(context.Background(), "g", "increment", nil)
		if err != nil {
			t.Fatalf("Call after a panic: %v", err)
		}
		if n := binary.BigEndian.Uint64(got); n != uint64(i) {
			t.Fatalf("count = %d, want %d", n, i)
		}
	}
}

func TestClosedRuntimeRejectsCalls(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	rt, err := actor.New(actor.Options{Clock: fc})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := rt.Call(context.Background(), "g", "increment", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	if err := rt.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := rt.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if pending := fc.Pending(); pending != 0 {
		t.Fatalf("timer leak: %d timer(s) armed after Close", pending)
	}

	if _, err := rt.Call(context.Background(), "g", "increment", nil); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Call after Close = %v, want ErrClosed", err)
	}
	if got := rt.Stats().Active; got != 0 {
		t.Fatalf("Stats().Active = %d after Close, want 0", got)
	}
}

func TestNewRequiresAClock(t *testing.T) {
	if _, err := actor.New(actor.Options{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("New without a clock = %v, want ErrInvalidOption", err)
	}
}
