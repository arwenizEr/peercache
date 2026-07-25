package clock_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
)

// The tests in this file exist to make specific interleavings happen. The race
// detector only reports races on paths a test actually takes, so a lock-ordering
// or hand-over bug in a path nothing exercises is invisible. Each test here
// targets one hand-over that would otherwise never be attempted.

// TestCancelRacesWithDeadline drives the hand-over in timerCtx: the deadline
// firing and the cancel function running at the same instant. Whichever wins,
// Done must be closed exactly once, Err must be non-nil, and the timer must not
// survive.
func TestCancelRacesWithDeadline(t *testing.T) {
	for i := 0; i < 200; i++ {
		fc := clock.NewFake(clock.Epoch)
		fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

		ctx, cancel := clock.WithTimeout(context.Background(), fc, time.Second)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancel() }()
		go func() { defer wg.Done(); fc.Advance(time.Second) }()
		wg.Wait()

		<-ctx.Done()
		if ctx.Err() == nil {
			t.Fatalf("iteration %d: Done is closed but Err is nil", i)
		}
		if got := fc.Pending(); got != 0 {
			t.Fatalf("iteration %d: %d timer(s) survived the race", i, got)
		}
	}
}

// TestParentCancelRacesWithDeadline is the same hand-over from the other side:
// the parent ending while the child's own deadline fires.
func TestParentCancelRacesWithDeadline(t *testing.T) {
	for i := 0; i < 200; i++ {
		fc := clock.NewFake(clock.Epoch)
		fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

		parent, cancelParent := context.WithCancel(context.Background())
		ctx, cancel := clock.WithTimeout(parent, fc, time.Second)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cancelParent() }()
		go func() { defer wg.Done(); fc.Advance(time.Second) }()
		wg.Wait()

		<-ctx.Done()
		if ctx.Err() == nil {
			t.Fatalf("iteration %d: Err is nil", i)
		}
		cancel()
		if got := fc.Pending(); got != 0 {
			t.Fatalf("iteration %d: %d timer(s) survived", i, got)
		}
	}
}

// TestBlockUntilCancelRacesWithNotify drives an abandoned blocker being
// satisfied at the same moment it gives up. A blocker that is both closed and
// removed twice would panic; one that is neither would leak.
func TestBlockUntilCancelRacesWithNotify(t *testing.T) {
	for i := 0; i < 200; i++ {
		fc := clock.NewFake(clock.Epoch)
		fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = fc.BlockUntilContext(ctx, 1)
		}()
		go func() {
			defer wg.Done()
			cancel()
			fc.NewTimer(time.Second).Stop()
		}()
		wg.Wait()
		cancel()
	}
}

// TestStopRacesWithFiring drives Stop against a timer that is being fired. The
// fake pops a timer, releases the lock, and only then runs the callback; a Stop
// arriving inside that window must not corrupt the heap.
func TestStopRacesWithFiring(t *testing.T) {
	const timers = 64

	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	for round := 0; round < 50; round++ {
		armed := make([]clock.Timer, 0, timers)
		for i := 0; i < timers; i++ {
			armed = append(armed, fc.AfterFunc(time.Millisecond, func() {}))
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, timer := range armed {
				timer.Stop()
			}
		}()
		go func() {
			defer wg.Done()
			fc.Advance(time.Millisecond)
		}()
		wg.Wait()

		// Whatever the interleaving, nothing may remain armed.
		fc.Advance(time.Second)
		for _, timer := range armed {
			timer.Stop()
		}
		if got := fc.Pending(); got != 0 {
			t.Fatalf("round %d: %d timer(s) left armed", round, got)
		}
	}
}

// TestResetRacesWithFiring is the same window, with Reset instead of Stop.
// Reset removes an entry from the heap and pushes it back; doing that to a
// timer already popped for firing is the case most likely to corrupt the index
// bookkeeping.
func TestResetRacesWithFiring(t *testing.T) {
	const timers = 64

	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	for round := 0; round < 50; round++ {
		armed := make([]clock.Timer, 0, timers)
		for i := 0; i < timers; i++ {
			armed = append(armed, fc.AfterFunc(time.Millisecond, func() {}))
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, timer := range armed {
				timer.Reset(2 * time.Millisecond)
			}
		}()
		go func() {
			defer wg.Done()
			fc.Advance(time.Millisecond)
		}()
		wg.Wait()

		fc.Advance(time.Hour)
		for _, timer := range armed {
			timer.Stop()
		}
		if got := fc.Pending(); got != 0 {
			t.Fatalf("round %d: %d timer(s) left armed", round, got)
		}
	}
}

// TestConcurrentAdvancesAreSerialised checks that two goroutines advancing at
// once cannot interleave into a state where time has moved less than the sum of
// their advances.
func TestConcurrentAdvancesAreSerialised(t *testing.T) {
	const goroutines = 8
	const each = 100

	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				fc.Advance(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	want := clock.Epoch.Add(goroutines * each * time.Millisecond)
	if got := fc.Now(); !got.Equal(want) {
		t.Fatalf("Now() = %v, want %v: advances were lost or interleaved", got, want)
	}
}
