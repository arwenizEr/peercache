package clock

import (
	"context"
	"time"
)

// realClock is the production [Clock]. It is stateless, so a single value is
// shared by every caller.
type realClock struct{}

// Real returns the [Clock] backed by the operating system.
//
// This is the only place in the repository permitted to call into the time
// package for the current instant. Test code may use it too, but only to
// measure the test's own wall-clock cost — never to wait.
//
// realClock is zero-sized, so the returned interface value allocates nothing.
func Real() Clock { return realClock{} }

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Since(t time.Time) time.Duration { return time.Since(t) }

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (realClock) NewTimer(d time.Duration) Timer { return &realTimer{t: time.NewTimer(d)} }

func (realClock) AfterFunc(d time.Duration, f func()) Timer {
	return &realTimer{t: time.AfterFunc(d, f), fn: true}
}

func (realClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		// time.NewTicker panics on a non-positive duration and CLAUDE.md
		// forbids panics in library paths. A ticker that never fires is
		// the least destructive answer; [Fake] reports the same misuse
		// loudly so tests catch it.
		return deadTicker{}
	}
	return &realTicker{t: time.NewTicker(d)}
}

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// realTimer adapts [time.Timer] to [Timer].
type realTimer struct {
	t *time.Timer
	// fn records that this timer came from AfterFunc and therefore has no
	// usable channel.
	fn bool
}

func (r *realTimer) C() <-chan time.Time {
	if r.fn {
		return nil
	}
	return r.t.C
}

func (r *realTimer) Stop() bool { return r.t.Stop() }

func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

// realTicker adapts [time.Ticker] to [Ticker].
type realTicker struct{ t *time.Ticker }

func (r *realTicker) C() <-chan time.Time { return r.t.C }

func (r *realTicker) Reset(d time.Duration) {
	if d <= 0 {
		r.t.Stop()
		return
	}
	r.t.Reset(d)
}

func (r *realTicker) Stop() { r.t.Stop() }

// deadTicker is the result of asking for a ticker with a non-positive period.
// It never fires and Stop is a no-op.
type deadTicker struct{}

func (deadTicker) C() <-chan time.Time { return nil }

func (deadTicker) Reset(time.Duration) {}

func (deadTicker) Stop() {}
