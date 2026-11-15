package peercache

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
)

// readyPollInterval is how often [Cache.WaitReady] rechecks.
//
// Readiness has no event to wait on: it is a conjunction of three conditions
// owned by three different components, and a channel per condition would be
// three more things to close correctly. Polling a cheap predicate is the
// simpler shape, and this runs only during startup.
const readyPollInterval = 25 * time.Millisecond

// Readiness is why a node can be a cluster member and still be the wrong place
// to send traffic.
//
// The two facts are independent. A node becomes an
// owner the instant it joins — every peer recomputes the ranking and starts
// routing to it — but it holds nothing until catch-up runs. In between it
// answers ErrNotFound, confidently, for data that exists elsewhere. That is
// worse than being unavailable, because a caller cannot tell the difference.
//
// Kubernetes already has the vocabulary for this: a pod that is running but not
// ready receives no traffic. This exposes the same idea so the readiness probe
// can use it.
type readiness struct {
	cache *cache

	// caughtUp is set once CatchUp has completed at least once, or
	// immediately when there is nothing to catch up from.
	caughtUp atomic.Bool
	// draining is set by Drain, and clears nothing: a draining node never
	// becomes ready again.
	draining atomic.Bool
	// currentAt is one more than the membership epoch under which the last
	// complete catch-up began, or zero if none has completed. One more, so
	// that zero can mean "never" without reserving an epoch.
	currentAt atomic.Uint64
}

// ReadyState explains a readiness decision.
//
// It is a struct rather than a bare bool because "not ready" has several causes
// with different remedies, and an operator staring at a probe failure needs to
// know which one. A node waiting for gossip needs patience; a node that has
// never caught up needs a call to CatchUp; a draining node needs to be left
// alone to finish.
type ReadyState struct {
	// Ready reports whether this node should receive traffic.
	Ready bool `json:"ready"`
	// Reason names the first unmet condition, or "" when ready. It is
	// intended for a probe's response body and for logs.
	Reason string `json:"reason"`

	// Joined reports whether this node has learned any membership.
	Joined bool `json:"joined"`
	// Settled reports whether the membership view has stopped moving for
	// longer than the activation delay.
	Settled bool `json:"settled"`
	// CaughtUp reports whether this node has pulled the replicas it is
	// responsible for at least once.
	CaughtUp bool `json:"caughtUp"`
	// ReplicasCurrent reports whether this node has pulled, from every peer
	// in its view and without a failure, the replicas it ranks for under the
	// membership view it holds now.
	//
	// It is not part of Ready, deliberately. A node that is ready can serve
	// every request correctly while ReplicasCurrent is false; what it cannot
	// yet do is guarantee that losing another node loses nothing. That is a
	// question about the next pod to be replaced, not about this one, so it
	// is reported beside readiness rather than gating it. See
	// docs/decisions/open.md question 40.
	//
	// It goes false on every membership change this node observes, and true
	// again only when a call to [Cache.CatchUp] that began under the new view
	// returns nil. The replication factor is restored cluster-wide once every
	// node reports it under the same membership; a single node reporting it
	// says only that this node has done its part.
	ReplicasCurrent bool `json:"replicasCurrent"`
	// Draining reports whether shutdown has begun.
	Draining bool `json:"draining"`
	// Members is the size of the current view.
	Members int `json:"members"`
}

// Ready reports whether this node should receive traffic.
//
// It is false until every one of these holds:
//
//   - The node has learned a membership view. Before that it owns nothing and
//     would refuse everything.
//   - The view has been stable for longer than the activation delay. A node
//     mid-change is a node whose ownership answers are about to change.
//   - Catch-up has completed at least once, so the node holds the keys the
//     ranking says it owns.
//
// And it becomes permanently false once [Cache.Drain] has been called, because
// a node on its way out should stop receiving work before it stops being able
// to do it.
//
// A single-node cache with no membership authority is ready immediately: it owns
// everything, there is nobody to catch up from, and nothing can change.
func (c *cache) Ready() ReadyState {
	st := ReadyState{
		CaughtUp:        c.readiness.caughtUp.Load(),
		Draining:        c.readiness.draining.Load(),
		ReplicasCurrent: c.replicasCurrent(),
	}

	switch {
	case c.isClosed():
		st.Reason = "closed"
		return st
	case st.Draining:
		st.Reason = "draining"
		return st
	}

	if c.guard == nil {
		// No membership authority: a cluster of one, ready as soon as it
		// is constructed. Nothing can join, nothing can move, and there
		// is nowhere to catch up from.
		st.Ready, st.Joined, st.Settled, st.CaughtUp = true, true, true, true
		st.Members = 1
		return st
	}

	stats := c.guard.Stats()
	st.Members = stats.Members
	st.Joined = stats.Members > 0
	if !st.Joined {
		st.Reason = "no membership view yet"
		return st
	}

	st.Settled = c.guard.Settled()
	if !st.Settled {
		st.Reason = "membership has not settled"
		return st
	}
	if !st.CaughtUp {
		st.Reason = "replicas not yet pulled; call CatchUp"
		return st
	}

	st.Ready = true
	return st
}

// markCaughtUp records that catch-up has completed.
func (r *readiness) markCaughtUp() { r.caughtUp.Store(true) }

// markCurrent records that a catch-up which began under epoch completed with no
// failure.
//
// A plain store, not a maximum. Two concurrent catch-ups finishing out of order
// can leave the older epoch recorded, which reports "not current" when it might
// be; that errs towards telling an operator to wait, which is the safe direction
// for a signal that gates replacing the next pod.
func (r *readiness) markCurrent(epoch core.Epoch) { r.currentAt.Store(uint64(epoch) + 1) }

// replicasCurrent reports [ReadyState.ReplicasCurrent].
func (c *cache) replicasCurrent() bool {
	if c.guard == nil {
		// A cluster of one holds every copy there is.
		return true
	}
	at := c.readiness.currentAt.Load()
	return at != 0 && at-1 == uint64(c.guard.Stats().Epoch)
}

// String implements [fmt.Stringer], so a probe handler can write the state
// directly into a response body.
func (s ReadyState) String() string {
	if s.Ready {
		return fmt.Sprintf("ready (members=%d replicasCurrent=%v)", s.Members, s.ReplicasCurrent)
	}
	return fmt.Sprintf("not ready: %s (members=%d joined=%v settled=%v caughtUp=%v replicasCurrent=%v draining=%v)",
		s.Reason, s.Members, s.Joined, s.Settled, s.CaughtUp, s.ReplicasCurrent, s.Draining)
}

// WaitReady blocks until this node is ready or ctx ends.
//
// It exists for the common startup shape — construct, join, catch up, wait,
// then serve — and it does not itself call [Cache.CatchUp]. That remains
// explicit, because a node deciding for itself when to pull an entire replica
// set is the behaviour catch-up was designed to avoid.
func (c *cache) WaitReady(ctx context.Context) error {
	ctx = ctxOrBackground(ctx)
	for {
		if st := c.Ready(); st.Ready {
			return nil
		} else if st.Draining || c.isClosed() {
			return fmt.Errorf("peercache: %s: %w", st.Reason, ErrClosed)
		}
		// Poll on the injected clock rather than a real timer, so a test
		// controls this wait like every other in the library. The
		// interval is short because readiness is checked during startup,
		// where a hundred milliseconds of extra delay is a hundred
		// milliseconds a pod spends out of service for nothing.
		timer := c.opts.Clock.NewTimer(readyPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("peercache: waiting for readiness: %w: %s",
				ctx.Err(), c.Ready())
		case <-timer.C():
		}
	}
}
