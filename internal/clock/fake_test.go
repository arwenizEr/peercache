package clock_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
)

// newFake returns a fake clock whose misuse reports fail the test, which is how
// every test in the repository should construct one.
func newFake(t *testing.T) *clock.Fake {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("unexpected clock misuse: %s", msg) })
	return fc
}

func TestFakeNowOnlyMovesWhenAdvanced(t *testing.T) {
	fc := newFake(t)
	if got := fc.Now(); !got.Equal(clock.Epoch) {
		t.Fatalf("Now() = %v, want %v", got, clock.Epoch)
	}
	fc.Advance(90 * time.Minute)
	if want := clock.Epoch.Add(90 * time.Minute); !fc.Now().Equal(want) {
		t.Fatalf("Now() = %v, want %v", fc.Now(), want)
	}
	if got := fc.Since(clock.Epoch); got != 90*time.Minute {
		t.Fatalf("Since(epoch) = %v, want 90m", got)
	}
}

func TestFakeTimersFireInDeadlineOrder(t *testing.T) {
	fc := newFake(t)
	var mu sync.Mutex
	var order []string

	record := func(name string) func() {
		return func() {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
		}
	}
	// Created out of order on purpose: deadline, not creation, decides.
	fc.AfterFunc(3*time.Second, record("c"))
	fc.AfterFunc(1*time.Second, record("a"))
	fc.AfterFunc(2*time.Second, record("b"))

	fc.Advance(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"a", "b", "c"}
	if len(order) != len(want) {
		t.Fatalf("fired %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("fired %v, want %v", order, want)
		}
	}
}

// TestFakeEqualDeadlinesFireInCreationOrder pins the tie-break rule. Without it
// the firing order of same-instant timers would depend on heap layout, and a
// test that passed today would fail after an unrelated timer was added.
func TestFakeEqualDeadlinesFireInCreationOrder(t *testing.T) {
	const n = 32
	fc := newFake(t)
	var mu sync.Mutex
	var order []int

	for i := 0; i < n; i++ {
		i := i
		fc.AfterFunc(time.Second, func() {
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		})
	}
	fc.Advance(time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != n {
		t.Fatalf("fired %d timers, want %d", len(order), n)
	}
	for i, got := range order {
		if got != i {
			t.Fatalf("timer %d fired at position %d; order was %v", got, i, order)
		}
	}
}

// TestFakeNowInsideCallbackIsTimerDeadline is the property that makes a
// multi-timer advance behave like real time: a callback must not be able to see
// the end of the advance window, only its own deadline.
func TestFakeNowInsideCallbackIsTimerDeadline(t *testing.T) {
	fc := newFake(t)
	seen := make(map[string]time.Time)
	var mu sync.Mutex

	for _, d := range []time.Duration{time.Second, 2 * time.Second} {
		d := d
		fc.AfterFunc(d, func() {
			mu.Lock()
			seen[d.String()] = fc.Now()
			mu.Unlock()
		})
	}
	fc.Advance(time.Hour)

	mu.Lock()
	defer mu.Unlock()
	for _, d := range []time.Duration{time.Second, 2 * time.Second} {
		want := clock.Epoch.Add(d)
		if got := seen[d.String()]; !got.Equal(want) {
			t.Fatalf("callback at %v saw Now()=%v, want %v", d, got, want)
		}
	}
	if want := clock.Epoch.Add(time.Hour); !fc.Now().Equal(want) {
		t.Fatalf("after advance Now()=%v, want %v", fc.Now(), want)
	}
}

// TestFakeCallbackCanScheduleInsideSameAdvance covers the case that makes the
// advance loop non-trivial: a timer armed by a callback, still inside the
// window, must fire in the same advance.
func TestFakeCallbackCanScheduleInsideSameAdvance(t *testing.T) {
	fc := newFake(t)
	var mu sync.Mutex
	var order []string

	fc.AfterFunc(time.Second, func() {
		mu.Lock()
		order = append(order, "first")
		mu.Unlock()
		fc.AfterFunc(time.Second, func() {
			mu.Lock()
			order = append(order, "second")
			mu.Unlock()
		})
	})

	fc.Advance(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "first" || order[1] != "second" {
		t.Fatalf("order = %v, want [first second]", order)
	}
}

func TestFakeStopPreventsFiring(t *testing.T) {
	fc := newFake(t)
	fired := make(chan struct{}, 1)
	timer := fc.AfterFunc(time.Second, func() { fired <- struct{}{} })

	if !timer.Stop() {
		t.Fatal("Stop() = false on an armed timer, want true")
	}
	if timer.Stop() {
		t.Fatal("second Stop() = true, want false")
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after Stop, want 0", got)
	}

	fc.Advance(time.Hour)
	select {
	case <-fired:
		t.Fatal("stopped timer fired")
	default:
	}
}

func TestFakeResetReschedules(t *testing.T) {
	fc := newFake(t)
	timer := fc.NewTimer(time.Second)

	if was := timer.Reset(10 * time.Second); !was {
		t.Fatal("Reset() = false on an armed timer, want true")
	}
	fc.Advance(5 * time.Second)
	select {
	case <-timer.C():
		t.Fatal("timer fired before its new deadline")
	default:
	}

	fc.Advance(5 * time.Second)
	select {
	case at := <-timer.C():
		if want := clock.Epoch.Add(10 * time.Second); !at.Equal(want) {
			t.Fatalf("timer delivered %v, want %v", at, want)
		}
	default:
		t.Fatal("timer did not fire at its new deadline")
	}
}

func TestFakeTickerRepeatsAndDrops(t *testing.T) {
	fc := newFake(t)
	tk := fc.NewTicker(time.Second)
	defer tk.Stop()

	// Ten ticks, one unread buffer slot: nine are dropped, exactly as a
	// real ticker drops.
	fc.Advance(10 * time.Second)
	select {
	case at := <-tk.C():
		if want := clock.Epoch.Add(time.Second); !at.Equal(want) {
			t.Fatalf("first tick %v, want %v", at, want)
		}
	default:
		t.Fatal("ticker did not tick")
	}
	select {
	case extra := <-tk.C():
		t.Fatalf("ticker queued a second tick %v; ticks must be dropped, not queued", extra)
	default:
	}

	fc.Advance(time.Second)
	select {
	case <-tk.C():
	default:
		t.Fatal("ticker stopped ticking after a dropped tick")
	}
}

func TestFakeTickerStopEndsIt(t *testing.T) {
	fc := newFake(t)
	tk := fc.NewTicker(time.Second)
	tk.Stop()
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after ticker Stop, want 0", got)
	}
	fc.Advance(time.Hour)
	select {
	case <-tk.C():
		t.Fatal("stopped ticker ticked")
	default:
	}
}

func TestFakeAfterDeliversOnce(t *testing.T) {
	fc := newFake(t)
	ch := fc.After(time.Second)
	fc.Advance(time.Second)
	if at := <-ch; !at.Equal(clock.Epoch.Add(time.Second)) {
		t.Fatalf("After delivered %v", at)
	}
	fc.Advance(time.Hour)
	select {
	case at := <-ch:
		t.Fatalf("After delivered twice: %v", at)
	default:
	}
}

// TestFakeBlockUntilSynchronisesWithWaiter is the sanctioned alternative to a
// sleep: park until the code under test has armed its timer, then fire it.
func TestFakeBlockUntilSynchronisesWithWaiter(t *testing.T) {
	fc := newFake(t)
	done := make(chan time.Time, 1)

	go func() { done <- <-fc.After(time.Minute) }()

	fc.BlockUntil(1)
	fc.Advance(time.Minute)

	if at := <-done; !at.Equal(clock.Epoch.Add(time.Minute)) {
		t.Fatalf("waiter woke at %v, want %v", at, clock.Epoch.Add(time.Minute))
	}
}

func TestFakeBlockUntilContextGivesUp(t *testing.T) {
	fc := newFake(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := fc.BlockUntilContext(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("BlockUntilContext = %v, want context.Canceled", err)
	}
	// The abandoned blocker must not keep a reference that a later notify
	// would close twice.
	fc.NewTimer(time.Second).Stop()
}

func TestFakeBlockUntilReturnsImmediatelyWhenSatisfied(t *testing.T) {
	fc := newFake(t)
	timer := fc.NewTimer(time.Second)
	defer timer.Stop()
	if err := fc.BlockUntilContext(context.Background(), 1); err != nil {
		t.Fatalf("BlockUntilContext = %v, want nil", err)
	}
}

func TestFakeSleepRespectsDeadlineAndContext(t *testing.T) {
	fc := newFake(t)

	slept := make(chan error, 1)
	go func() { slept <- fc.Sleep(context.Background(), time.Minute) }()
	fc.BlockUntil(1)
	fc.Advance(time.Minute)
	if err := <-slept; err != nil {
		t.Fatalf("Sleep = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() { cancelled <- fc.Sleep(ctx, time.Hour) }()
	fc.BlockUntil(1)
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep after cancel = %v, want context.Canceled", err)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after cancelled Sleep, want 0: Sleep leaked its timer", got)
	}
}

func TestFakeSleepZeroIsImmediate(t *testing.T) {
	fc := newFake(t)
	if err := fc.Sleep(context.Background(), 0); err != nil {
		t.Fatalf("Sleep(0) = %v, want nil", err)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Sleep(0) armed %d timer(s), want 0", got)
	}
}

func TestFakeFlushReleasesAlreadyDueTimer(t *testing.T) {
	fc := newFake(t)
	timer := fc.NewTimer(-time.Second)
	select {
	case <-timer.C():
		t.Fatal("timer fired without an advance")
	default:
	}
	fc.Flush()
	select {
	case <-timer.C():
	default:
		t.Fatal("Flush did not release an already-due timer")
	}
}

func TestFakeMisuseIsRecordedNotPanicked(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	var got []string
	fc.SetMisuseHandler(func(msg string) { got = append(got, msg) })

	fc.Advance(-time.Second)
	fc.NewTicker(0)
	fc.AdvanceTo(clock.Epoch.Add(-time.Hour))

	if len(got) != 3 {
		t.Fatalf("handler saw %d misuses (%v), want 3", len(got), got)
	}
	if len(fc.Misuses()) != 3 {
		t.Fatalf("Misuses() = %v, want 3 entries", fc.Misuses())
	}
	if !fc.Now().Equal(clock.Epoch) {
		t.Fatalf("a misused advance moved the clock to %v", fc.Now())
	}
}

func TestFakeAdvanceToMovesForward(t *testing.T) {
	fc := newFake(t)
	fired := make(chan struct{}, 1)
	fc.AfterFunc(time.Minute, func() { fired <- struct{}{} })
	fc.AdvanceTo(clock.Epoch.Add(2 * time.Minute))
	select {
	case <-fired:
	default:
		t.Fatal("AdvanceTo did not fire a timer inside the window")
	}
}

// TestFakeConcurrentUse is the -race exercise: many goroutines arming, resetting
// and stopping timers while another advances. It asserts nothing beyond "no
// race, no deadlock, no lost timer", which is exactly what it is for.
func TestFakeConcurrentUse(t *testing.T) {
	const goroutines = 16
	const rounds = 50

	fc := newFake(t)
	var wg sync.WaitGroup
	var fired sync.WaitGroup
	stop := make(chan struct{})

	fired.Add(goroutines * rounds)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				var once sync.Once
				timer := fc.AfterFunc(time.Millisecond, func() {
					once.Do(fired.Done)
				})
				timer.Reset(time.Millisecond)
			}
		}()
	}

	advancer := make(chan struct{})
	go func() {
		defer close(advancer)
		for {
			select {
			case <-stop:
				return
			default:
			}
			fc.Advance(time.Millisecond)
		}
	}()

	wg.Wait()
	// Drain anything still armed after the writers finished.
	fc.Advance(time.Second)
	close(stop)
	<-advancer

	fired.Wait()
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0", got)
	}
}
