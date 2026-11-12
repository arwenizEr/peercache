package peercache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
)

// newCache builds a cache on a fake clock and asserts at teardown that it left
// no timer armed. Every test in this package uses it, so a leaked ticker in
// Close would fail immediately rather than in some unrelated later test.
func newCache(t *testing.T, opts ...peercache.Option) (peercache.Cache, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	base := []peercache.Option{peercache.WithNodeID("n0"), peercache.WithClock(fc)}
	c, err := peercache.New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			t.Errorf("Close: %v", err)
		}
		if pending := fc.Pending(); pending != 0 {
			t.Errorf("timer leak: %d timer(s) armed after Close", pending)
		}
	})
	return c, fc
}

func TestNewRequiresNodeID(t *testing.T) {
	if _, err := peercache.New(); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New without a node ID = %v, want ErrInvalidOption", err)
	}
}

func TestNewRejectsNilClockAndLogger(t *testing.T) {
	tests := map[string]peercache.Option{
		"clock":  peercache.WithClock(nil),
		"logger": peercache.WithLogger(nil),
	}
	for name, opt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := peercache.New(peercache.WithNodeID("n0"), opt)
			if !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("New with a nil %s = %v, want ErrInvalidOption", name, err)
			}
		})
	}
}

func TestNewIgnoresNilOptions(t *testing.T) {
	// A nil option in a slice built by a caller's helper should not be a
	// crash. There are no panics in library paths.
	c, err := peercache.New(nil, peercache.WithNodeID("n0"), nil)
	if err != nil {
		t.Fatalf("New with nil options = %v, want nil", err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestStatsReportsSelf(t *testing.T) {
	c, _ := newCache(t)
	stats := c.Stats()
	if stats.Self.ID != "n0" || stats.Self.Addr != "" {
		t.Fatalf("Stats().Self = %v, want n0 with no address: nothing here is dialled", stats.Self)
	}
	if stats.Members != 1 {
		t.Fatalf("Stats().Members = %d, want 1", stats.Members)
	}
}

func TestRegisterGroupFunc(t *testing.T) {
	c, _ := newCache(t)
	fn := func(context.Context, peercache.GroupState, []byte) ([]byte, error) { return nil, nil }

	if err := c.RegisterGroupFunc("increment", fn); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	if err := c.RegisterGroupFunc("increment", fn); !errors.Is(err, peercache.ErrDuplicateRegistration) {
		t.Fatalf("duplicate registration = %v, want ErrDuplicateRegistration", err)
	}
	if err := c.RegisterGroupFunc("", fn); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("empty name = %v, want ErrInvalidOption", err)
	}
	if err := c.RegisterGroupFunc("nilfn", nil); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("nil function = %v, want ErrInvalidOption", err)
	}
}

// TestCallOfAnUnregisteredFunction matters more than it looks: during a rolling
// upgrade a missing registration is a real, expected condition, and it must be
// distinguishable from every other failure.
func TestCallOfAnUnregisteredFunction(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("user:123")
	ctx := context.Background()

	if _, err := g.Call(ctx, "missing", nil); !errors.Is(err, peercache.ErrFuncNotRegistered) {
		t.Fatalf("Call of an unregistered function = %v, want ErrFuncNotRegistered", err)
	}

	fn := func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		return []byte("ran"), nil
	}
	if err := c.RegisterGroupFunc("known", fn); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	got, err := g.Call(ctx, "known", nil)
	if err != nil {
		t.Fatalf("Call of a registered function: %v", err)
	}
	if string(got) != "ran" {
		t.Fatalf("Call returned %q", got)
	}
}

func TestClosedCacheReportsErrClosed(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	c, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithClock(fc))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ns := c.Namespace("sessions")
	g := c.Group("user:123")
	ctx := context.Background()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent: a shutdown path that can be reached twice must
	// not fail the second time.
	if err := c.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if pending := fc.Pending(); pending != 0 {
		t.Fatalf("timer leak: %d timer(s) armed after Close", pending)
	}

	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace.Get after Close = %v, want ErrClosed", err)
	}
	if err := ns.Set(ctx, "k", nil, 0); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace.Set after Close = %v, want ErrClosed", err)
	}
	if _, err := ns.GetOrLoad(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace.GetOrLoad after Close = %v, want ErrClosed", err)
	}
	if _, err := g.Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Group.Get after Close = %v, want ErrClosed", err)
	}
	if _, err := g.Call(ctx, "anything", nil); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Group.Call after Close = %v, want ErrClosed", err)
	}
	fn := func(context.Context, peercache.GroupState, []byte) ([]byte, error) { return nil, nil }
	if err := c.RegisterGroupFunc("late", fn); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("RegisterGroupFunc after Close = %v, want ErrClosed", err)
	}

	// A namespace created after Close is usable only insofar as it reports
	// that the cache is gone.
	if _, err := c.Namespace("new").Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Namespace created after Close = %v, want ErrClosed", err)
	}
}

// TestCloseReportsAnExpiredDeadline guards against training callers to ignore
// the context they passed. There is nothing to drain on a single node, but
// succeeding silently on an expired deadline would be a lie that gets more
// expensive once Close has real work to do.
func TestCloseReportsAnExpiredDeadline(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	c, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithClock(fc))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close with a cancelled context = %v, want context.Canceled", err)
	}
	if pending := fc.Pending(); pending != 0 {
		t.Fatalf("timer leak: %d timer(s) armed after Close", pending)
	}
}

// TestCancelledContextIsReportedFirst pins a rule that has to hold for every
// operation: an expired deadline is the caller's answer, whatever the callee
// would otherwise have said.
func TestCancelledContextIsReportedFirst(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	g := c.Group("user:123")

	if err := ns.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// The key exists, so anything other than the context error would mean
	// the deadline check happened too late.
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Get = %v, want context.Canceled", err)
	}
	if err := ns.Set(ctx, "k", nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Set = %v, want context.Canceled", err)
	}
	if err := ns.Delete(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Delete = %v, want context.Canceled", err)
	}
	if _, err := ns.Exists(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.Exists = %v, want context.Canceled", err)
	}
	if _, err := ns.BatchGet(ctx, []string{"k"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.BatchGet = %v, want context.Canceled", err)
	}
	if _, err := ns.GetOrLoad(ctx, "k"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Namespace.GetOrLoad = %v, want context.Canceled", err)
	}
	if _, err := g.Call(ctx, "anything", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Group.Call = %v, want context.Canceled", err)
	}
}

func TestRouterSubmit(t *testing.T) {
	c, _ := newCache(t)
	router := peercache.NewRouter(c, func(s string) string { return s }, "apply")
	ctx := context.Background()

	// Submitting nothing succeeds: the caller's postcondition already holds.
	if err := router.Submit(ctx); err != nil {
		t.Fatalf("Submit of no messages = %v, want nil", err)
	}
	// An unregistered handler is reported per group, because the groups are
	// independent and a caller resubmitting needs to know which ones failed.
	err := router.Submit(ctx, "a", "b")
	if !errors.Is(err, peercache.ErrFuncNotRegistered) {
		t.Fatalf("Submit with no handler registered = %v, want ErrFuncNotRegistered", err)
	}

	// With the handler registered, the messages arrive — grouped, in
	// submission order, and as one batch per group rather than one call per
	// message. That last part is the whole point of the API.
	var mu sync.Mutex
	calls := 0
	seen := map[string][]string{}
	if err := c.RegisterGroupFunc("apply", func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
		msgs, err := peercache.Batch[string](arg)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		calls++
		seen[s.ID()] = append(seen[s.ID()], msgs...)
		mu.Unlock()
		return nil, nil
	}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	if err := router.Submit(ctx, "a", "b", "a", "a"); err != nil {
		t.Fatalf("Submit = %v, want nil", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 2 {
		t.Fatalf("the handler ran %d times for 4 messages across 2 groups, want 2; "+
			"a batch must be applied in one turn per group, not one per message", calls)
	}
	if got := seen["a"]; len(got) != 3 || got[0] != "a" || got[2] != "a" {
		t.Fatalf("group a saw %v, want its three messages in submission order", got)
	}
	if got := seen["b"]; len(got) != 1 {
		t.Fatalf("group b saw %v, want one message", got)
	}

	bad := peercache.NewRouter[string](c, nil, "apply")
	if err := bad.Submit(ctx, "a"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("Submit with no groupOf = %v, want ErrInvalidOption", err)
	}
}

func TestPartitioners(t *testing.T) {
	tests := []struct {
		name string
		p    peercache.Partitioner
		key  string
		want string
	}{
		{"by key", peercache.PartitionByKey(), "user:123/cart", "user:123/cart"},
		{"by prefix", peercache.PartitionByPrefix("/"), "user:123/cart", "user:123"},
		{"by prefix, first separator only", peercache.PartitionByPrefix("/"), "a/b/c", "a"},
		{"by prefix, separator absent", peercache.PartitionByPrefix("/"), "standalone", "standalone"},
		{"by prefix, leading separator", peercache.PartitionByPrefix("/"), "/leading", ""},
		{"by prefix, empty separator", peercache.PartitionByPrefix(""), "a/b", "a/b"},
		{"by func", peercache.PartitionByFunc(func(string) string { return "fixed" }), "anything", "fixed"},
		{"by func, nil", peercache.PartitionByFunc(nil), "anything", "anything"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.p.GroupKey(tc.key); got != tc.want {
				t.Fatalf("GroupKey(%q) = %q, want %q", tc.key, got, tc.want)
			}
		})
	}
}

// TestPartitionersAreStable is the property that actually matters: two pods
// must derive the same group from the same key, or the cluster has a split
// brain that no epoch check will catch.
func TestPartitionersAreStable(t *testing.T) {
	p := peercache.PartitionByPrefix("/")
	first := p.GroupKey("user:123/cart")
	for i := 0; i < 100; i++ {
		if got := p.GroupKey("user:123/cart"); got != first {
			t.Fatalf("GroupKey is not stable: %q then %q", first, got)
		}
	}
}

// TestSentinelErrorsAreDistinct guards against two sentinels being accidentally
// bound to the same object, which would make errors.Is silently over-match.
func TestSentinelErrorsAreDistinct(t *testing.T) {
	sentinels := map[string]error{
		"ErrNotImplemented":        peercache.ErrNotImplemented,
		"ErrNotFound":              peercache.ErrNotFound,
		"ErrClosed":                peercache.ErrClosed,
		"ErrGroupOverloaded":       peercache.ErrGroupOverloaded,
		"ErrStaleEpoch":            peercache.ErrStaleEpoch,
		"ErrNotOwner":              peercache.ErrNotOwner,
		"ErrNoLoader":              peercache.ErrNoLoader,
		"ErrLoaderPanic":           peercache.ErrLoaderPanic,
		"ErrLoaderFailed":          peercache.ErrLoaderFailed,
		"ErrFuncNotRegistered":     peercache.ErrFuncNotRegistered,
		"ErrDuplicateRegistration": peercache.ErrDuplicateRegistration,
		"ErrInvalidOption":         peercache.ErrInvalidOption,
		"ErrNodeUnreachable":       peercache.ErrNodeUnreachable,
	}
	for nameA, a := range sentinels {
		if a == nil {
			t.Fatalf("%s is nil", nameA)
		}
		for nameB, b := range sentinels {
			if nameA == nameB {
				continue
			}
			if errors.Is(a, b) {
				t.Fatalf("%s matches %s; sentinels must be distinct", nameA, nameB)
			}
		}
	}
}

// TestSweeperReclaimsExpiredEntries proves the cache's single background
// sweeper is wired to every namespace. Expiry is enforced on read regardless,
// so this is about memory, not visibility.
func TestSweeperReclaimsExpiredEntries(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("sessions", peercache.WithShards(4))
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		if err := ns.Set(ctx, key(i), []byte("v"), time.Minute); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if got := c.Stats().OwnedKeys; got != 100 {
		t.Fatalf("OwnedKeys = %d, want 100", got)
	}

	fc.Advance(2 * time.Minute)

	// The sweeper is parked on the ticker; drive it until it has reclaimed
	// everything. Yielding rather than sleeping, bounded by the go test
	// timeout if the wiring is broken.
	for c.Stats().OwnedKeys != 0 {
		gosched()
		fc.Advance(time.Second)
	}
}
