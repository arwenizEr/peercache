package clock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// The real clock is thin, and the tests for it are correspondingly thin. What
// matters is that it does not panic where the standard library would and that
// its timers actually stop, because those are the two places a wrapper usually
// gets it wrong.

func TestRealNowMovesForward(t *testing.T) {
	c := clock.Real()
	first := c.Now()
	second := c.Now()
	if second.Before(first) {
		t.Fatalf("Now() went backwards: %v then %v", first, second)
	}
	if c.Since(first) < 0 {
		t.Fatalf("Since() = %v, want a non-negative duration", c.Since(first))
	}
}

func TestRealTimerFires(t *testing.T) {
	timer := clock.Real().NewTimer(time.Millisecond)
	defer timer.Stop()
	<-timer.C()
}

func TestRealAfterFuncTimerHasNoChannel(t *testing.T) {
	done := make(chan struct{})
	timer := clock.Real().AfterFunc(time.Millisecond, func() { close(done) })
	defer timer.Stop()
	if timer.C() != nil {
		t.Fatal("AfterFunc timer exposed a channel; time.AfterFunc does not")
	}
	<-done
}

func TestRealStopPreventsFiring(t *testing.T) {
	timer := clock.Real().NewTimer(time.Hour)
	if !timer.Stop() {
		t.Fatal("Stop() = false on an armed timer")
	}
	select {
	case <-timer.C():
		t.Fatal("stopped timer fired")
	default:
	}
}

// TestRealNonPositiveTickerDoesNotPanic covers the divergence from the standard
// library. time.NewTicker panics on a non-positive period and library paths
// never panic, so a dead ticker is returned instead.
func TestRealNonPositiveTickerDoesNotPanic(t *testing.T) {
	tk := clock.Real().NewTicker(0)
	defer tk.Stop()
	if tk.C() != nil {
		t.Fatal("a dead ticker must not expose a live channel")
	}
	tk.Reset(-time.Second)
}

func TestRealTickerTicks(t *testing.T) {
	tk := clock.Real().NewTicker(time.Millisecond)
	defer tk.Stop()
	<-tk.C()
	tk.Reset(time.Millisecond)
	<-tk.C()
}

func TestRealSleepRespectsContext(t *testing.T) {
	c := clock.Real()
	if err := c.Sleep(context.Background(), 0); err != nil {
		t.Fatalf("Sleep(0) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Sleep(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep with a cancelled context = %v, want Canceled", err)
	}
}
