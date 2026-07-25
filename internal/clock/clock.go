package clock

import (
	"context"
	"time"
)

// Clock abstracts every source of time.
//
// SPEC.md §3.6 specifies only Now, After and NewTimer. That is not enough to
// build the group actor runtime on: M2 needs one-shot callbacks that do not
// require a goroutine parked on a channel (AfterFunc), periodic work
// (NewTicker) and a wait that respects a caller's context (Sleep). Growing the
// interface in M2 would churn every implementation and every call site, so it
// is grown here instead. See docs/decisions/0002-clock-interface.md.
type Clock interface {
	// Now returns the current time. For [Fake] this is simulated time.
	Now() time.Time

	// Since returns the duration elapsed since t. It is exactly
	// Now().Sub(t) and exists so that callers never reach for time.Since.
	Since(t time.Time) time.Duration

	// After returns a channel that receives the current time once, after at
	// least d has elapsed. The channel is buffered, so a caller that never
	// receives leaks nothing beyond the timer itself.
	//
	// A [Fake] timer is only released by [Fake.Advance]. Code that calls
	// After with a non-positive d still requires an advance (of zero or
	// more) before the send happens.
	After(d time.Duration) <-chan time.Time

	// NewTimer returns a [Timer] that fires once after d.
	NewTimer(d time.Duration) Timer

	// AfterFunc returns a [Timer] that runs f in its own goroutine after d.
	// The returned timer's C channel is nil, matching [time.AfterFunc].
	//
	// For [Fake], f runs on the goroutine that called [Fake.Advance], not a
	// new one, so that a test's advance is complete only once every
	// callback it triggered has returned. f must therefore not block on
	// another advance.
	AfterFunc(d time.Duration, f func()) Timer

	// NewTicker returns a [Ticker] that fires every d. d must be positive;
	// a non-positive d yields a ticker that never fires and reports misuse.
	NewTicker(d time.Duration) Ticker

	// Sleep blocks for d or until ctx is done, whichever happens first. It
	// returns ctx.Err() if the context ended first and nil otherwise. There
	// is deliberately no context-free sleep on this interface: an
	// uninterruptible wait is how a shutdown path outlives its deadline.
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer is a one-shot alarm, mirroring the useful half of [time.Timer].
type Timer interface {
	// C is the channel on which the time is delivered. It is nil for
	// timers created by [Clock.AfterFunc].
	C() <-chan time.Time

	// Stop prevents the timer from firing. It reports whether the call
	// stopped a timer that had not yet fired. Stopping an already-fired or
	// already-stopped timer returns false and is not an error.
	Stop() bool

	// Reset reschedules the timer to fire after d measured from now. It
	// reports whether the timer was active at the time of the call.
	//
	// As with [time.Timer.Reset], resetting a timer whose value has already
	// been delivered but not yet received leaves that stale value in the
	// channel. Drain before resetting if that matters.
	Reset(d time.Duration) bool
}

// Ticker is a repeating alarm, mirroring [time.Ticker].
//
// As with the real ticker, a tick is dropped rather than queued if the receiver
// has not drained the previous one. Ticker ownership implies a Stop; an
// unstopped ticker on a [Fake] is a leak that [Fake.Pending] will report.
type Ticker interface {
	// C is the channel on which ticks are delivered.
	C() <-chan time.Time

	// Reset changes the tick period to d and schedules the next tick d from
	// now. d must be positive.
	Reset(d time.Duration)

	// Stop halts the ticker. It does not close the channel.
	Stop()
}

// Epoch is an arbitrary fixed instant used as the default starting time for
// [NewFake]. It is deliberately not the Unix epoch: a zero-ish timestamp makes
// off-by-one sign errors invisible, and a time.Time zero value makes IsZero
// checks pass by accident.
var Epoch = time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
