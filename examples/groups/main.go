// Command groups demonstrates the group actor runtime.
//
// A group is a virtual actor: addressable always, activated lazily, deactivated
// when idle, and while activated it owns exactly one goroutine. Every operation
// on it runs as one turn on that goroutine, which is why there is no lock in
// the API and why a group function needs none.
//
// Single-node. ./examples/ownership shows which node owns which group.
//
// Run it with:
//
//	go run ./examples/groups
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	var events sync.Map // core.EventKind -> *atomic.Int64

	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithGroupIdleTimeout(50*time.Millisecond),
		peercache.WithMaxActiveGroups(8),
		peercache.WithEventHandler(func(e peercache.Event) {
			counter, _ := events.LoadOrStore(e.Kind, &atomic.Int64{})
			counter.(*atomic.Int64).Add(1)
		}),
	)
	if err != nil {
		return fmt.Errorf("constructing the cache: %w", err)
	}
	defer func() { _ = cache.Close(context.Background()) }()
	ctx := context.Background()

	if err := serialization(ctx, cache); err != nil {
		return err
	}
	if err := stateAndTimers(ctx, cache); err != nil {
		return err
	}
	if err := backpressure(ctx, cache); err != nil {
		return err
	}
	if err := activationLifecycle(ctx, cache); err != nil {
		return err
	}

	fmt.Println("\nlifecycle events observed:")
	for _, kind := range []peercache.EventKind{
		peercache.EventActivate, peercache.EventDeactivate,
		peercache.EventCreate, peercache.EventUpdate, peercache.EventDelete,
	} {
		if counter, ok := events.Load(kind); ok {
			fmt.Printf("  %-11v %d\n", kind, counter.(*atomic.Int64).Load())
		}
	}
	return nil
}

func serialization(ctx context.Context, cache peercache.Cache) error {
	const callers = 1000

	// No lock anywhere in this function. It does not need one: only one turn
	// per group ever runs, so the read-modify-write below cannot interleave
	// with itself.
	if err := cache.RegisterGroupFunc("increment", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		var n uint64
		if cur, ok := s.Get("count"); ok {
			n = binary.BigEndian.Uint64(cur)
		}
		n++
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		s.Set("count", buf)
		return buf, nil
	}); err != nil {
		return fmt.Errorf("registering increment: %w", err)
	}

	group := cache.Group("tenant:acme")
	var retries atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A thousand callers against a mailbox of sixty-four will
			// certainly be refused sometimes. That is the design, not a
			// failure: ErrGroupOverloaded is retryable, and retrying is
			// what a caller is expected to do.
			for {
				_, err := group.Call(ctx, "increment", nil)
				if err == nil {
					return
				}
				if !errors.Is(err, peercache.ErrGroupOverloaded) {
					fmt.Fprintln(os.Stderr, "call failed:", err)
					return
				}
				retries.Add(1)
				runtime.Gosched()
			}
		}()
	}
	wg.Wait()

	total, err := group.Get(ctx, "count")
	if err != nil {
		return fmt.Errorf("reading the counter: %w", err)
	}
	fmt.Printf("%d concurrent callers, no locks, final count %d (%d calls were refused and retried)\n",
		callers, binary.BigEndian.Uint64(total), retries.Load())
	return nil
}

func stateAndTimers(ctx context.Context, cache peercache.Cache) error {
	fired := make(chan struct{}, 1)
	if err := cache.RegisterGroupFunc("sweep", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		s.Set("swept", []byte("yes"))
		select {
		case fired <- struct{}{}:
		default:
		}
		return nil, nil
	}); err != nil {
		return fmt.Errorf("registering sweep: %w", err)
	}
	if err := cache.RegisterGroupFunc("schedule", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		// A timer belongs to the group and survives deactivation: the
		// schedule is kept, the clock timer is re-armed on reactivation.
		s.ScheduleTimer("sweep", 20*time.Millisecond, "sweep", nil)
		return nil, nil
	}); err != nil {
		return fmt.Errorf("registering schedule: %w", err)
	}

	group := cache.Group("workflow:42")
	if err := group.Set(ctx, "stage", []byte("submitted")); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	if _, err := group.Call(ctx, "schedule", nil); err != nil {
		return fmt.Errorf("schedule: %w", err)
	}

	<-fired
	items, err := group.Items(ctx)
	if err != nil {
		return fmt.Errorf("items: %w", err)
	}
	fmt.Printf("\ntimer fired inside its own turn; group holds %d keys: stage=%s swept=%s\n",
		len(items), items["stage"], items["swept"])
	return nil
}

func backpressure(ctx context.Context, cache peercache.Cache) error {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	if err := cache.RegisterGroupFunc("slow", func(context.Context, peercache.GroupState, []byte) ([]byte, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		return nil, nil
	}); err != nil {
		return fmt.Errorf("registering slow: %w", err)
	}

	group := cache.Group("saturated")
	go func() { _, _ = group.Call(ctx, "slow", nil) }()
	<-entered

	// Offer far more than the mailbox holds. A full mailbox is refused
	// immediately rather than queued: an unbounded queue would turn one slow
	// group into unbounded memory, and waiting would only delay the moment
	// the caller learns it is saturated.
	var rejected atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 512; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := group.Call(ctx, "slow", nil); errors.Is(err, peercache.ErrGroupOverloaded) {
				rejected.Add(1)
			}
		}()
	}
	for rejected.Load() == 0 {
		runtime.Gosched()
	}
	fmt.Printf("\nmailbox saturated: %d of 512 calls refused with ErrGroupOverloaded (retryable)\n",
		rejected.Load())

	close(release)
	wg.Wait()
	return nil
}

func activationLifecycle(ctx context.Context, cache peercache.Cache) error {
	const groups = 20

	for i := 0; i < groups; i++ {
		g := cache.Group(fmt.Sprintf("session:%d", i))
		if err := g.Set(ctx, "seen", []byte("1")); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	stats := cache.Stats()
	// The active count may sit slightly above the cap for a moment. Eviction
	// is signalled after the call that tripped the cap is already enqueued,
	// and the victim exits when its current turn ends — making the caller
	// wait for someone else's turn would couple unrelated latencies. The cap
	// is a bound on steady state, not an instantaneous invariant.
	fmt.Printf("\n%d groups touched; %d addressable, %d active (cap 8, briefly exceeded while victims exit)\n",
		groups, stats.AddressableGroups, stats.ActiveGroups)

	// Evicted and deactivated groups keep their state. Only the goroutine
	// goes; the record stays.
	for i := 0; i < groups; i++ {
		id := fmt.Sprintf("session:%d", i)
		if _, err := cache.Group(id).Get(ctx, "seen"); err != nil {
			return fmt.Errorf("group %s lost its state: %w", id, err)
		}
	}
	fmt.Println("every evicted group still has its state; only the goroutine was released")
	return nil
}
