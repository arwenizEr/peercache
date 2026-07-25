package actor_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/actor"
	"github.com/peercache/peercache/internal/core"
)

// waitFor spins until cond holds, bounded so a regression fails with a message
// rather than hanging for the test timeout. No sleeping.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	const yields = 5_000_000
	for i := 0; !cond(); i++ {
		if i > yields {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func TestIdleDeactivationReleasesTheGoroutine(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Minute })
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "increment", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got := rt.Stats().Active; got != 1 {
		t.Fatalf("Stats().Active = %d after a call, want 1", got)
	}

	// The turn goroutine is parked on its idle timer.
	fc.BlockUntil(1)
	fc.Advance(time.Minute)

	waitFor(t, "the group to deactivate", func() bool { return rt.Stats().Active == 0 })

	// It remains addressable, which is the point of separating records from
	// activations.
	if got := rt.Stats().Addressable; got != 1 {
		t.Fatalf("Stats().Addressable = %d after deactivation, want 1", got)
	}
}

// TestDeactivationPreservesState is half of the M2 acceptance criterion: a
// group that deactivates and reactivates preserves state.
func TestDeactivationPreservesState(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Minute })
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := rt.Call(context.Background(), "g", "increment", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}

	fc.BlockUntil(1)
	fc.Advance(time.Minute)
	waitFor(t, "deactivation", func() bool { return rt.Stats().Active == 0 })

	// Reactivate: the counter must continue rather than restart.
	got, err := rt.Call(context.Background(), "g", "increment", nil)
	if err != nil {
		t.Fatalf("Call after reactivation: %v", err)
	}
	if n := binary.BigEndian.Uint64(got); n != 4 {
		t.Fatalf("count = %d after reactivation, want 4: state was lost", n)
	}
}

// TestDeactivationPreservesPendingTimers is the other half. A timer scheduled
// before deactivation must still fire afterwards, on its original deadline.
func TestDeactivationPreservesPendingTimers(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Minute })

	var fired atomic.Int64
	if err := rt.Register("onTimer", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		fired.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("schedule", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.ScheduleTimer("wake", 10*time.Minute, "onTimer", []byte("arg"))
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "schedule", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	// A group with a pending timer is exempt from idle deactivation:
	// deactivating would stop its clock timer and nothing would ever wake it.
	fc.Advance(time.Minute * 2)
	if got := rt.Stats().Active; got != 1 {
		t.Fatalf("Stats().Active = %d; a group with a pending timer was idle-deactivated "+
			"and its timer would never fire", got)
	}

	// Force it out anyway, as the LRU cap would.
	rt.Deactivate("g")
	if got := rt.Stats().Active; got != 0 {
		t.Fatalf("Stats().Active = %d after a forced deactivation, want 0", got)
	}
	if got := fired.Load(); got != 0 {
		t.Fatalf("the timer fired %d times while deactivated, want 0", got)
	}

	// Reactivating re-arms the surviving schedule for the time remaining.
	if _, err := rt.Call(context.Background(), "g", "schedule2", nil); err == nil {
		t.Fatal("expected an unregistered-function error")
	}
	if _, err := rt.Do(context.Background(), "g", func(core.GroupState) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Do: %v", err)
	}

	fc.Advance(10 * time.Minute)
	waitFor(t, "the preserved timer to fire", func() bool { return fired.Load() == 1 })

	// Exactly once: a re-armed schedule must not double-fire.
	fc.Advance(time.Hour)
	if got := fired.Load(); got != 1 {
		t.Fatalf("the timer fired %d times, want exactly 1", got)
	}
}

// TestTimerFiresWhileActive is the ordinary path.
func TestTimerFiresWhileActive(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Hour })

	var fired atomic.Int64
	var sawArg atomic.Value
	if err := rt.Register("onTimer", func(_ context.Context, _ core.GroupState, arg []byte) ([]byte, error) {
		sawArg.Store(string(arg))
		fired.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("schedule", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.ScheduleTimer("wake", time.Minute, "onTimer", []byte("payload"))
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "schedule", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	fc.Advance(30 * time.Second)
	if got := fired.Load(); got != 0 {
		t.Fatalf("the timer fired early (%d times)", got)
	}

	fc.Advance(30 * time.Second)
	waitFor(t, "the timer to fire", func() bool { return fired.Load() == 1 })
	if got := sawArg.Load(); got != "payload" {
		t.Fatalf("the timer delivered %v, want \"payload\"", got)
	}
}

func TestCancelTimer(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Hour })

	var fired atomic.Int64
	if err := rt.Register("onTimer", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		fired.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("schedule", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.ScheduleTimer("wake", time.Minute, "onTimer", nil)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("cancel", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.CancelTimer("wake")
		s.CancelTimer("never-existed") // not an error
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "schedule", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if _, err := rt.Call(context.Background(), "g", "cancel", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	fc.Advance(time.Hour)
	if got := fired.Load(); got != 0 {
		t.Fatalf("a cancelled timer fired %d times", got)
	}
}

// TestReschedulingATimerReplacesIt checks that re-arming a name does not
// accumulate duplicates, which would make a self-rescheduling timer double with
// every tick.
func TestReschedulingATimerReplacesIt(t *testing.T) {
	rt, fc := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Hour })

	var fired atomic.Int64
	if err := rt.Register("onTimer", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		fired.Add(1)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("schedule", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.ScheduleTimer("wake", time.Minute, "onTimer", nil)
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := rt.Call(context.Background(), "g", "schedule", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}

	fc.Advance(2 * time.Minute)
	waitFor(t, "the timer to fire", func() bool { return fired.Load() >= 1 })
	if got := fired.Load(); got != 1 {
		t.Fatalf("the timer fired %d times after five schedules of one name, want 1", got)
	}
}

// TestActiveCapEvictsLeastRecentlyActivated covers the global cap. Each
// activation costs a goroutine, so this is the knob that bounds memory.
func TestActiveCapEvictsLeastRecentlyActivated(t *testing.T) {
	const cap = 4
	const groups = 12

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MaxActive = cap
		o.IdleTimeout = time.Hour // isolate eviction from idle deactivation
	})
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for i := 0; i < groups; i++ {
		if _, err := rt.Call(context.Background(), fmt.Sprintf("g%d", i), "increment", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}

	waitFor(t, "the active count to settle within the cap", func() bool {
		return rt.Stats().Active <= cap
	})
	if got := rt.Stats().Evictions; got == 0 {
		t.Fatal("Stats().Evictions = 0; the cap was met without evicting anything")
	}
	if got := rt.Stats().Addressable; got != groups {
		t.Fatalf("Stats().Addressable = %d, want %d: eviction dropped records, not just goroutines",
			got, groups)
	}

	// Evicted groups keep their state.
	for i := 0; i < groups; i++ {
		id := fmt.Sprintf("g%d", i)
		got, err := rt.Do(context.Background(), id, func(s core.GroupState) (any, error) {
			cur, ok := s.Get("count")
			if !ok {
				return uint64(0), nil
			}
			return binary.BigEndian.Uint64(cur), nil
		})
		if err != nil {
			t.Fatalf("Do(%s): %v", id, err)
		}
		if n := got.(uint64); n != 1 {
			t.Fatalf("group %s lost its state across eviction: count = %d, want 1", id, n)
		}
	}
}

func TestUnlimitedActiveGroups(t *testing.T) {
	const groups = 50

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MaxActive = -1
		o.IdleTimeout = time.Hour
	})
	if err := rt.Register("increment", counterFunc); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for i := 0; i < groups; i++ {
		if _, err := rt.Call(context.Background(), fmt.Sprintf("g%d", i), "increment", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}
	if got := rt.Stats().Active; got != groups {
		t.Fatalf("Stats().Active = %d with an unlimited cap, want %d", got, groups)
	}
	if got := rt.Stats().Evictions; got != 0 {
		t.Fatalf("Stats().Evictions = %d with an unlimited cap, want 0", got)
	}
}

// TestEscapedStateIsInertAndCounted is the §3.3 rule. A group function that
// keeps its State and writes through it later must not corrupt a subsequent
// turn, must not panic, and must be visible in the counters.
func TestEscapedStateIsInertAndCounted(t *testing.T) {
	rt, _ := newRuntime(t, func(o *actor.Options) { o.IdleTimeout = time.Hour })

	escaped := make(chan core.GroupState, 1)
	if err := rt.Register("leak", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.Set("legitimate", []byte("written inside the turn"))
		escaped <- s
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := rt.Register("read", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		val, _ := s.Get("legitimate")
		return val, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "leak", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	leaked := <-escaped

	// Writing through the escaped State, concurrently with a legitimate
	// turn. Under -race this is where an unguarded map would be caught.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			leaked.Set("smuggled", []byte("should not appear"))
			leaked.Delete("legitimate")
			_ = leaked.Keys()
			_, _ = leaked.Get("legitimate")
		}
	}()
	for i := 0; i < 100; i++ {
		if _, err := rt.Call(context.Background(), "g", "read", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}
	wg.Wait()

	// The escapee changed nothing.
	got, err := rt.Call(context.Background(), "g", "read", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(got) != "written inside the turn" {
		t.Fatalf("the escaped State modified the group: %q", got)
	}
	present, err := rt.Do(context.Background(), "g", func(s core.GroupState) (any, error) {
		_, ok := s.Get("smuggled")
		return ok, nil
	})
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	if present.(bool) {
		t.Fatal("the escaped State wrote a key into the group")
	}

	if got := rt.Stats().Escapes; got == 0 {
		t.Fatal("Stats().Escapes = 0; the violation was silent")
	}
}

// TestLifecycleEventsAreDelivered covers the six event kinds M2 owns.
func TestLifecycleEventsAreDelivered(t *testing.T) {
	var mu sync.Mutex
	var seen []core.EventKind

	rt, fc := newRuntime(t, func(o *actor.Options) {
		o.IdleTimeout = time.Minute
		o.OnEvent = func(e core.Event) {
			mu.Lock()
			seen = append(seen, e.Kind)
			mu.Unlock()
		}
	})
	if err := rt.Register("write", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
		s.Set("k", []byte("v1")) // create
		s.Set("k", []byte("v2")) // update
		s.Delete("k")            // delete
		s.Emit(core.Event{Kind: core.EventCustom, Name: "custom"})
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := rt.Call(context.Background(), "g", "write", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	fc.BlockUntil(1)
	fc.Advance(time.Minute)
	waitFor(t, "deactivation", func() bool { return rt.Stats().Active == 0 })

	mu.Lock()
	defer mu.Unlock()
	counts := map[core.EventKind]int{}
	for _, k := range seen {
		counts[k]++
	}
	for _, want := range []core.EventKind{
		core.EventActivate, core.EventCreate, core.EventUpdate,
		core.EventDelete, core.EventCustom, core.EventDeactivate,
	} {
		if counts[want] == 0 {
			t.Errorf("no %v event was delivered; saw %v", want, seen)
		}
	}
}
