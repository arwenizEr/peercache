package peercache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/wire"
	"github.com/peercache/peercache/transport"
)

// invalidationQueue is how many pending broadcasts are held before they are
// dropped.
//
// Small on purpose. These are hints; a backlog of them is worth less than the
// memory it occupies, because by the time a stale hint is delivered the TTL it
// was racing has usually expired anyway.
const invalidationQueue = 256

// invalidationTimeout bounds one broadcast round.
//
// It is deliberately short and deliberately not the caller's deadline. A write
// must not wait on a hint sent for somebody else's benefit, and a peer that
// cannot answer within this has already cost more than the round trip it was
// meant to save.
const invalidationTimeout = 2 * time.Second

// invalidator delivers invalidation hints without letting them affect writes.
//
// # Why this is not a direct call
//
// The first version broadcast inline, on the writing caller's context. That was
// wrong in a way only a fault-injection test found. A dropped message on this
// fabric looks like silence rather than an error
// (docs/decisions/0004-dropped-messages-look-like-silence.md), so a peer that
// had gone away did not fail the send — it simply never replied. With a caller
// passing context.Background(), which is ordinary, Set blocked forever.
//
// So a best-effort optimisation had become the slowest possible dependency of
// the write path: exactly the inversion CLAUDE.md non-negotiable 6 exists to
// prevent, and a path that could block past any deadline because it had none.
//
// The queue makes the property structural rather than aspirational. Enqueueing
// never blocks and never fails; a full queue drops the hint and counts it. The
// writer's involvement ends at a channel send, and correctness cannot depend on
// something the writer never waits for.
type invalidator struct {
	cache *cache

	queue chan invalidation
	done  chan struct{}
	wg    sync.WaitGroup

	// base is cancelled by Close, and every outbound invalidation derives
	// from it.
	//
	// Signalling alone is not enough. A send already in flight would keep
	// running, and its own timeout is measured on the injected clock — which
	// under a fake clock does not advance unless a test advances it. Close
	// would then wait for a deadline that never arrives. Cancelling the
	// parent makes the in-flight call return at once, so Close is bounded by
	// the send returning rather than by the clock moving.
	base       context.Context
	cancelBase context.CancelFunc

	sent    atomic.Int64
	dropped atomic.Int64
	failed  atomic.Int64
}

// invalidation is one pending hint.
type invalidation struct {
	namespace string
	keys      []string
	stamp     core.Stamp
}

func newInvalidator(c *cache) *invalidator {
	base, cancel := context.WithCancel(context.Background())
	iv := &invalidator{
		cache:      c,
		queue:      make(chan invalidation, invalidationQueue),
		done:       make(chan struct{}),
		base:       base,
		cancelBase: cancel,
	}
	iv.wg.Add(1)
	go iv.run()
	return iv
}

// enqueue schedules a broadcast. It never blocks.
//
// A full queue drops the hint rather than waiting for room, because waiting
// here would reintroduce the exact coupling this type exists to remove: the
// writer would be paying, in its own latency, for the health of its peers.
func (iv *invalidator) enqueue(namespace string, keys []string, stamp core.Stamp) {
	if len(keys) == 0 {
		return
	}
	// Copy the keys: the caller may reuse its slice, and this outlives the
	// call.
	owned := make([]string, len(keys))
	copy(owned, keys)

	select {
	case iv.queue <- invalidation{namespace: namespace, keys: owned, stamp: stamp}:
	default:
		iv.dropped.Add(1)
	}
}

// run delivers queued hints until Close.
func (iv *invalidator) run() {
	defer iv.wg.Done()
	for {
		select {
		case <-iv.done:
			return
		case inv := <-iv.queue:
			iv.deliver(inv)
		}
	}
}

// deliver sends one hint to every peer, under its own bounded budget.
func (iv *invalidator) deliver(inv invalidation) {
	c := iv.cache
	if c.transport == nil || c.guard == nil {
		return
	}
	self := c.self().ID
	req := &wire.InvalidateRequest{Namespace: inv.namespace, Keys: inv.keys, Stamp: inv.stamp}

	for _, node := range c.guard.Table().Members() {
		if node.ID == self {
			continue
		}
		select {
		case <-iv.done:
			// Shutting down. Remaining peers lose this hint and fall
			// back to their TTL, which is what they would have done
			// had the message been dropped in flight.
			return
		default:
		}

		// A fresh budget per peer. One unreachable node must not consume
		// the whole round's time and starve the peers after it.
		ctx, cancel := clock.WithTimeout(iv.base, c.opts.Clock, invalidationTimeout)
		err := c.route.invoke(ctx, node, transport.MethodInvalidate, req, nil)
		cancel()

		if err != nil {
			iv.failed.Add(1)
			c.opts.Logger.Debug("invalidation not delivered",
				"node", node.ID, "namespace", inv.namespace, "error", err)
			continue
		}
		iv.sent.Add(1)
	}
}

// Close stops the delivery goroutine and waits for it.
//
// It waits rather than only signalling, so that when Close returns there is no
// goroutine left holding a reference to the cache. M3 lost a milestone to a
// Close that only signalled.
func (iv *invalidator) Close() {
	close(iv.done)
	// Cancel before waiting, so a send already on the wire returns now
	// rather than when its deadline expires.
	iv.cancelBase()
	iv.wg.Wait()
}

// counters reports delivery activity for [Stats].
func (iv *invalidator) counters() (sent, dropped, failed int64) {
	return iv.sent.Load(), iv.dropped.Load(), iv.failed.Load()
}
