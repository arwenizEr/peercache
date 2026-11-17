package peercache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
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
// the write path: exactly the inversion a best-effort invalidation must never
// cause, and a path that could block past any deadline because it had none.
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

	// peers holds a sender per peer, touched only by run. See deliver.
	peers map[string]*hintSender
}

// hintSender delivers hints to one peer, on its own goroutine, from its own
// bounded queue: a peer that answers slowly or not at all backs up its own
// queue, and the others' hints go on arriving.
type hintSender struct {
	rounds chan hintRound
}

// hintRound is what one pass of run took from the queue, for one peer at the
// address the pass saw it at. The batch is shared by every peer's round and
// read-only.
type hintRound struct {
	node  core.Node
	batch []invalidation
}

// hintPeerQueue is how many rounds wait for one peer before its rounds are
// dropped. A sender takes every round waiting when it sends, so the queue
// fills only behind a peer that does not answer — one whose copies will
// expire on their TTL before a backlog reaches them anyway.
const hintPeerQueue = 1024

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

// invalidationBatch is the most queued hints run takes in one pass and hands
// to the peers' senders together.
const invalidationBatch = 256

// hintMessageMax is about the most hints one message to a peer carries.
//
// A peer's sender takes every round queued for it while it was sending and
// sends them as one message per namespace, so under write pressure hints share
// messages instead of each paying a round trip to every peer. Nothing waits
// for a batch to fill: at a low write rate a message holds one hint and leaves
// at once, as before. The bound is what keeps up: a sender that can carry
// this many per round trip drains a burst of writes over loopback gRPC that
// one at a time dropped nearly every hint of (docs/benchmarks.md).
const hintMessageMax = 4096

// hint tells the cluster that key changed to stamp, unless no peer can hold a
// copy of it.
//
// A namespace opened with a negative L1 TTL has no non-owned tier, and every
// node opens a namespace with the same options, so its peers hold no copy for
// a hint to drop: it would be a message to every peer per write for nothing.
func (n *namespace) hint(key string, stamp core.Stamp) {
	if n.opts.L1TTL < 0 {
		return
	}
	n.cache.invalidator.enqueue(n.name, []string{key}, stamp)
}

// run hands queued hints to the peers' senders until Close.
func (iv *invalidator) run() {
	defer iv.wg.Done()
	for {
		var batch []invalidation
		select {
		case <-iv.done:
			return
		case inv := <-iv.queue:
			batch = append(batch, inv)
		}
	more:
		for len(batch) < invalidationBatch {
			select {
			case inv := <-iv.queue:
				batch = append(batch, inv)
			default:
				break more
			}
		}
		iv.deliver(batch)
	}
}

// deliver hands a batch of hints to every peer's sender, never waiting.
//
// Each peer has its own. Sent one peer after another, as hints first were, a
// peer that did not answer held every peer after it for its timeout, and every
// hint after that. Now a slow peer backs up its own queue and, past
// hintPeerQueue rounds, loses hints of its own, counted as dropped like a full
// queue's here.
func (iv *invalidator) deliver(batch []invalidation) {
	c := iv.cache
	if c.transport == nil || c.guard == nil {
		return
	}
	self := c.selfID()
	members := c.guard.Table().Members()
	present := make(map[string]struct{}, len(members))
	for _, node := range members {
		if node.ID == self {
			continue
		}
		present[node.ID] = struct{}{}
		s := iv.peers[node.ID]
		if s == nil {
			if iv.peers == nil {
				iv.peers = map[string]*hintSender{}
			}
			s = &hintSender{rounds: make(chan hintRound, hintPeerQueue)}
			iv.peers[node.ID] = s
			iv.wg.Add(1)
			go iv.send(s)
		}
		select {
		case s.rounds <- hintRound{node: node, batch: batch}:
		default:
			iv.dropped.Add(int64(hintCount(batch)))
		}
	}
	// A departed peer's sender finishes what it holds and stops.
	for id, s := range iv.peers {
		if _, ok := present[id]; !ok {
			close(s.rounds)
			delete(iv.peers, id)
		}
	}
}

func hintCount(batch []invalidation) int {
	n := 0
	for _, inv := range batch {
		n += len(inv.keys)
	}
	return n
}

// send delivers one peer's hints until its queue is closed or Close: every
// round waiting at once, up to about hintMessageMax hints, as one message per
// namespace.
func (iv *invalidator) send(s *hintSender) {
	defer iv.wg.Done()
	c := iv.cache
	var merged []invalidation
	for {
		var node core.Node
		select {
		case <-iv.done:
			// Shutting down. This peer loses what is queued for it and
			// falls back to its TTL, which is what it would have done
			// had the messages been dropped in flight.
			return
		case r, ok := <-s.rounds:
			if !ok {
				return
			}
			node = r.node
			merged = append(merged[:0], r.batch...)
		}
	more:
		for len(merged) < hintMessageMax {
			select {
			case r, ok := <-s.rounds:
				if !ok {
					break more // closed: send what is held, then stop above
				}
				node = r.node // the latest address
				merged = append(merged, r.batch...)
			default:
				break more
			}
		}
		for _, req := range requests(merged) {
			ctx, cancel := clock.WithTimeout(iv.base, c.opts.Clock, invalidationTimeout)
			err := c.route.invoke(ctx, node, transport.MethodInvalidate, req, nil)
			cancel()
			if err != nil {
				iv.failed.Add(1)
				c.opts.Logger.Debug("invalidation not delivered",
					"node", node.ID, "namespace", req.Namespace, "error", err)
				continue
			}
			iv.sent.Add(1)
		}
		clear(merged) // drop the keys the slice still points at
	}
}

// requests groups hints into one request per namespace, in the order the
// namespaces first appear. Each request's first hint goes in Keys and Stamp,
// which every release reads, and the rest in Entries: a peer on the release
// before Entries applies the first hint of each and lets its TTL bound the
// others, as it would a dropped hint.
func requests(batch []invalidation) []*wire.InvalidateRequest {
	var out []*wire.InvalidateRequest
	byNS := map[string]*wire.InvalidateRequest{}
	for _, inv := range batch {
		req := byNS[inv.namespace]
		if req == nil {
			req = &wire.InvalidateRequest{Namespace: inv.namespace, Keys: inv.keys, Stamp: inv.stamp}
			byNS[inv.namespace] = req
			out = append(out, req)
			continue
		}
		for _, key := range inv.keys {
			req.Entries = append(req.Entries, wire.InvalidateEntry{Key: key, Stamp: inv.stamp})
		}
	}
	return out
}

// Close stops the delivery goroutine and waits for it.
//
// It waits rather than only signalling, so that when Close returns there is no
// goroutine left holding a reference to the cache.
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
