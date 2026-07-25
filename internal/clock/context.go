package clock

import (
	"context"
	"sync"
	"time"
)

// WithTimeout returns a copy of parent that is cancelled after d has elapsed on
// clk, together with a cancel function that must be called to release
// resources.
//
// This exists because [context.WithTimeout] reads the real wall clock. A test
// that injects a fake clock everywhere else and then relies on a real context
// deadline has quietly reintroduced the thing the fake clock was there to
// remove: it must now wait in real time for the deadline to fire, which is a
// sleep by another name. Deadline propagation is a correctness requirement in
// this project, so it has to be as controllable as every other timer.
//
// In production, pass [Real] and the behaviour is that of
// [context.WithTimeout].
func WithTimeout(parent context.Context, clk Clock, d time.Duration) (context.Context, context.CancelFunc) {
	return WithDeadline(parent, clk, clk.Now().Add(d))
}

// WithDeadline returns a copy of parent that is cancelled once clk reaches
// deadline.
//
// If parent already carries an earlier or equal deadline, parent's wins and
// this degrades to [context.WithCancel], matching [context.WithDeadline].
func WithDeadline(parent context.Context, clk Clock, deadline time.Time) (context.Context, context.CancelFunc) {
	if cur, ok := parent.Deadline(); ok && !cur.After(deadline) {
		return context.WithCancel(parent)
	}

	c := &timerCtx{
		Context:  parent,
		deadline: deadline,
		done:     make(chan struct{}),
	}

	// Propagate parent cancellation without a goroutine. context.AfterFunc
	// registers with the parent's own machinery and returns a stop
	// function, so a long-lived parent with many short-lived children costs
	// nothing once the children are cancelled.
	//
	// AfterFunc runs its callback immediately, on a new goroutine, if
	// parent is already done. The stop function therefore has to be handed
	// over under the lock: otherwise cancel can run before the field is
	// assigned and the registration is never released.
	c.adoptStop(context.AfterFunc(parent, func() { c.cancel(parent.Err()) }))

	remaining := deadline.Sub(clk.Now())
	if remaining <= 0 {
		c.cancel(context.DeadlineExceeded)
		return c, func() { c.cancel(context.Canceled) }
	}

	// Same hand-over hazard as above: with a real clock a very short
	// deadline can fire before AfterFunc has returned.
	c.adoptTimer(clk.AfterFunc(remaining, func() { c.cancel(context.DeadlineExceeded) }))

	return c, func() { c.cancel(context.Canceled) }
}

// timerCtx is a [context.Context] whose deadline is measured on an injected
// [Clock] rather than on the wall clock.
type timerCtx struct {
	context.Context

	deadline time.Time
	done     chan struct{}

	mu         sync.Mutex
	err        error
	timer      Timer
	stopParent func() bool
}

// Deadline implements [context.Context].
func (c *timerCtx) Deadline() (time.Time, bool) { return c.deadline, true }

// Done implements [context.Context].
func (c *timerCtx) Done() <-chan struct{} { return c.done }

// Err implements [context.Context].
func (c *timerCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// adoptStop takes ownership of the parent-cancellation registration. If the
// context was already cancelled while the registration was being created, the
// registration is released here rather than stored and forgotten.
func (c *timerCtx) adoptStop(stop func() bool) {
	c.mu.Lock()
	if c.err == nil {
		c.stopParent = stop
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	stop()
}

// adoptTimer takes ownership of the deadline timer, with the same
// already-cancelled hand-over rule as [timerCtx.adoptStop].
func (c *timerCtx) adoptTimer(t Timer) {
	c.mu.Lock()
	if c.err == nil {
		c.timer = t
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	t.Stop()
}

// cancel ends the context with err. The first caller wins; later calls, whether
// from the deadline firing, the parent ending, or the returned cancel function,
// are no-ops. That is what makes the cancel function safe to defer
// unconditionally.
func (c *timerCtx) cancel(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	if err == nil {
		// A parent that reports no error cannot be the reason we are
		// stopping; treat it as an ordinary cancellation rather than
		// leaving Err nil while Done is closed, which would violate the
		// context contract.
		err = context.Canceled
	}
	c.err = err
	timer, stopParent := c.timer, c.stopParent
	// Done is closed while the lock is held so that Err can never report a
	// non-nil error through a Done channel that is still open. The window
	// is small, but a caller that selects on Done and then reads Err is the
	// normal pattern, and half of it being true is worse than neither.
	close(c.done)
	c.mu.Unlock()

	if timer != nil {
		timer.Stop()
	}
	if stopParent != nil {
		stopParent()
	}
}

var _ context.Context = (*timerCtx)(nil)
