package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// Ticker.Reset had no test at all until this file, which is worse than it
// sounds: it is where period and deadline have to change together, and it was
// hand-edited during a self-review to make them do so. A fix nobody tested
// is a fix nobody has any reason to believe.

// TestFakeTickerResetChangesThePeriod is the basic contract.
func TestFakeTickerResetChangesThePeriod(t *testing.T) {
	fc := newFake(t)
	tk := fc.NewTicker(time.Second)
	defer tk.Stop()

	fc.Advance(time.Second)
	drain(t, tk, "first tick at the original period")

	tk.Reset(10 * time.Second)

	// The old period must no longer fire.
	fc.Advance(time.Second)
	assertNoTick(t, tk, "the ticker fired at its old period after Reset")

	// The new one must.
	fc.Advance(9 * time.Second)
	drain(t, tk, "tick at the new period")

	// And it repeats at the new period, not the old.
	fc.Advance(9 * time.Second)
	assertNoTick(t, tk, "the ticker fired early on its second new-period tick")
	fc.Advance(time.Second)
	drain(t, tk, "second tick at the new period")
}

// TestFakeTickerResetRephasesFromNow pins that Reset restarts the interval from
// the moment of the call rather than from the last tick. A ticker reset half a
// period in must wait a whole new period.
func TestFakeTickerResetRephasesFromNow(t *testing.T) {
	fc := newFake(t)
	tk := fc.NewTicker(10 * time.Second)
	defer tk.Stop()

	fc.Advance(5 * time.Second) // halfway to the first tick
	assertNoTick(t, tk, "ticked early")

	tk.Reset(10 * time.Second)

	// Five more seconds would have reached the original deadline. It must
	// not fire: the clock was rephased.
	fc.Advance(5 * time.Second)
	assertNoTick(t, tk, "Reset kept the old deadline instead of rephasing")

	fc.Advance(5 * time.Second)
	drain(t, tk, "tick ten seconds after Reset")
}

// TestFakeTickerResetOnAStoppedTickerRevivesIt matches time.Ticker, where Reset
// after Stop starts it again.
func TestFakeTickerResetOnAStoppedTickerRevivesIt(t *testing.T) {
	fc := newFake(t)
	tk := fc.NewTicker(time.Second)
	defer tk.Stop()

	tk.Stop()
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after Stop, want 0", got)
	}

	tk.Reset(time.Second)
	if got := fc.Pending(); got != 1 {
		t.Fatalf("Pending() = %d after Reset, want 1: the ticker was not revived", got)
	}
	fc.Advance(time.Second)
	drain(t, tk, "tick after a stopped ticker was reset")
}

// TestFakeTickerResetToNonPositiveIsMisuse covers the branch that stops the
// ticker rather than panicking, since time.Ticker.Reset panics here and
// library paths never panic.
func TestFakeTickerResetToNonPositiveIsMisuse(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	var misuses []string
	fc.SetMisuseHandler(func(msg string) { misuses = append(misuses, msg) })

	tk := fc.NewTicker(time.Second)
	tk.Reset(0)

	if len(misuses) != 1 {
		t.Fatalf("misuses = %v, want exactly one report", misuses)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0: a ticker reset to a non-positive period must stop", got)
	}
	fc.Advance(time.Hour)
	assertNoTick(t, tk, "a ticker reset to a non-positive period still fired")
}

// TestFakeTickerResetIsAtomic is the reason the Reset body holds the lock across
// both fields. If period and deadline were updated separately, an advance
// landing between them would schedule the next tick from the new period against
// the old deadline, or the reverse.
//
// The interleaving cannot be forced deterministically, so this drives the two
// against each other and checks the ticker is still coherent afterwards: it
// still exists, fires on its current period, and never fires early.
func TestFakeTickerResetIsAtomic(t *testing.T) {
	const rounds = 500

	fc := newFake(t)
	tk := fc.NewTicker(time.Second)
	defer tk.Stop()

	for round := 0; round < rounds; round++ {
		done := make(chan struct{})
		go func() {
			defer close(done)
			tk.Reset(time.Second)
		}()
		fc.Advance(time.Second)
		<-done

		// Drain whatever this round produced; the point is coherence, not
		// an exact count, since the reset may land before or after the
		// advance.
		select {
		case <-tk.C():
		default:
		}

		if got := fc.Pending(); got != 1 {
			t.Fatalf("round %d: Pending() = %d, want exactly the one ticker", round, got)
		}
	}

	// Still on a one-second period and still coherent.
	fc.Advance(time.Second)
	drain(t, tk, "tick after concurrent resets")
}

func drain(t *testing.T, tk clock.Ticker, what string) {
	t.Helper()
	select {
	case <-tk.C():
	default:
		t.Fatalf("expected %s", what)
	}
}

func assertNoTick(t *testing.T, tk clock.Ticker, complaint string) {
	t.Helper()
	select {
	case at := <-tk.C():
		t.Fatalf("%s (delivered %v)", complaint, at)
	default:
	}
}

// The real clock's Reset, After and dead-ticker paths had no coverage either.
// They are thin, but "thin" is how a wrapper that returns the wrong channel
// goes unnoticed.

func TestRealTimerReset(t *testing.T) {
	timer := clock.Real().NewTimer(time.Hour)
	defer timer.Stop()

	if was := timer.Reset(time.Millisecond); !was {
		t.Fatal("Reset() = false on an armed timer, want true")
	}
	<-timer.C()

	if was := timer.Reset(time.Millisecond); was {
		t.Fatal("Reset() = true on an already-fired timer, want false")
	}
	<-timer.C()
}

func TestRealClockAfter(t *testing.T) {
	<-clock.Real().After(time.Millisecond)
}

func TestRealSleepCompletes(t *testing.T) {
	if err := clock.Real().Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatalf("Sleep = %v, want nil", err)
	}
}

// TestRealTickerResetToNonPositiveStops covers the divergence from
// time.Ticker.Reset, which panics on a non-positive period.
func TestRealTickerResetToNonPositiveStops(t *testing.T) {
	tk := clock.Real().NewTicker(time.Millisecond)
	defer tk.Stop()

	<-tk.C()
	tk.Reset(0) // must stop rather than panic

	// Draining a stopped ticker must not block forever; there is nothing to
	// receive, which is checked non-blockingly.
	select {
	case <-tk.C():
		// A tick delivered before the stop is acceptable.
	default:
	}
}

// TestDeadTickerIsInert covers the value returned when a non-positive period is
// requested. Every method must be safe to call on it.
func TestDeadTickerIsInert(t *testing.T) {
	tk := clock.Real().NewTicker(0)
	if tk.C() != nil {
		t.Fatal("a dead ticker exposed a live channel")
	}
	tk.Reset(time.Second)
	if tk.C() != nil {
		t.Fatal("a dead ticker came alive after Reset")
	}
	tk.Stop()
	tk.Stop()
}
