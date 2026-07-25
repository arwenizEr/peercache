package singleflight

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/core"
)

// Func is the work being collapsed. It returns a value, the time-to-live to
// store it under, and an error.
//
// The context it receives is detached from any individual caller's
// cancellation, so a load is not abandoned because the first caller gave up. It
// still carries a deadline if the [Group] was given one.
type Func func(ctx context.Context) ([]byte, time.Duration, error)

// Result is the outcome of one collapsed load.
type Result struct {
	// Value and TTL are what the loader returned.
	Value []byte
	TTL   time.Duration
	// Err is the loader's error, or the reason the loader could not
	// complete.
	Err error
	// Shared reports whether more than one caller received this result. It
	// is the observable evidence that collapsing happened, and it is what
	// makes "exactly one invocation under a thousand misses" testable from
	// the outside.
	Shared bool
}

// call is one in-flight load.
type call struct {
	done chan struct{}

	// Written by the leader before done is closed, read by everyone after.
	// The close is the happens-before edge, so no lock is needed on either
	// side of it.
	val []byte
	ttl time.Duration
	err error

	// waiters counts everyone attached to this call, leader included.
	waiters atomic.Int64
}

// Group collapses concurrent loads. The zero value is ready to use.
//
// A Group is safe for concurrent use.
type Group struct {
	mu    sync.Mutex
	calls map[string]*call

	inflight atomic.Int64
	loads    atomic.Int64
	collapse atomic.Int64
}

// Do runs fn for key unless a load for key is already running, in which case it
// waits for that one instead.
//
// It returns when the load completes or when ctx is done, whichever happens
// first. A caller whose context expires abandons the wait; the load continues
// for everybody else, because the alternative is that one impatient caller
// makes nine hundred patient ones fail.
//
// If fn panics or calls [runtime.Goexit], every waiter receives an error rather
// than inheriting the panic. A loader is application code, and a bug in it
// should not take down goroutines that merely asked for a value.
func (g *Group) Do(ctx context.Context, key string, fn Func) Result {
	if err := ctx.Err(); err != nil {
		return Result{Err: err}
	}

	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*call)
	}
	c, running := g.calls[key]
	if !running {
		c = &call{done: make(chan struct{})}
		g.calls[key] = c
	}
	c.waiters.Add(1)
	g.mu.Unlock()

	g.inflight.Add(1)
	defer g.inflight.Add(-1)

	if !running {
		g.loads.Add(1)
		// The loader runs on its own goroutine, not the leader's. If it
		// ran inline, the leader would be committed to waiting for the
		// whole load regardless of its own deadline — and "no path
		// blocks past its context deadline" has to hold for the leader
		// as much as for anyone else.
		go g.run(ctx, key, c, fn)
	} else {
		g.collapse.Add(1)
	}

	select {
	case <-c.done:
		return Result{
			Value:  c.val,
			TTL:    c.ttl,
			Err:    c.err,
			Shared: c.waiters.Load() > 1,
		}
	case <-ctx.Done():
		// The load keeps running for the remaining waiters. If this was
		// the leader, the detached context it was given is unaffected.
		return Result{Err: ctx.Err()}
	}
}

// run executes the loader and publishes the result to every waiter.
//
// The loader's context is derived from the leader's with cancellation stripped,
// so request-scoped values such as a trace identifier still propagate while the
// leader's deadline does not. Bounding the load is the caller's job: whatever
// wraps fn should impose its own timeout, because the right bound is a property
// of the namespace, not of whichever caller arrived first.
//
// If fn never returns, this goroutine never returns. That is the loader's bug,
// and it is why the namespace gives every load a deadline.
func (g *Group) run(ctx context.Context, key string, c *call, fn Func) {
	loadCtx := context.WithoutCancel(ctx)

	normalReturn := false
	recovered := false

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()

		if !normalReturn && !recovered {
			// runtime.Goexit: a loader that called t.Fatal, or
			// something equally exotic. There is no value and no
			// error, so invent one rather than closing done with a
			// nil error and a nil value, which would look like a
			// successful load of nothing.
			c.err = fmt.Errorf("%w: loader exited without returning", core.ErrLoaderFailed)
		}
		close(c.done)
	}()

	func() {
		defer func() {
			if r := recover(); r != nil {
				recovered = true
				c.err = fmt.Errorf("%w: %v", core.ErrLoaderPanic, r)
			}
		}()
		c.val, c.ttl, c.err = fn(loadCtx)
	}()
	normalReturn = true
}

// Inflight returns how many callers are currently waiting on a load, leader
// included.
//
// This is a metric, and it is also the only way to prove from outside that a
// thousand callers were genuinely concurrent rather than merely sequential and
// fast. M7 exports it.
func (g *Group) Inflight() int64 { return g.inflight.Load() }

// Loads returns the total number of loader invocations.
func (g *Group) Loads() int64 { return g.loads.Load() }

// Collapsed returns the number of calls that attached to a load already in
// flight instead of starting one. Loads plus Collapsed is the number of Do
// calls that got past the initial context check.
func (g *Group) Collapsed() int64 { return g.collapse.Load() }
