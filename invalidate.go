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
// buffer: a peer that answers slowly or not at all backs up its own buffer,
// and the others' hints go on arriving.
type hintSender struct {
	mu sync.Mutex
	// node is the peer at the address the latest round saw it at.
	node core.Node
	// pending is what waits for the peer, hints counts the keys in it, and
	// closed is set when the peer leaves the view.
	pending []invalidation
	hints   int
	closed  bool
	// wake is signalled when pending grows or closed is set.
	wake chan struct{}
}

// hintPeerMax bounds the hints waiting for one peer. Past it a round's hints
// for that peer are dropped and counted. Bounded in hints rather than rounds,
// as Redis bounds a client's output buffer in bytes rather than replies: a
// round is anything from one hint to a few hundred, so a bound in rounds was
// no bound on memory. A sender takes everything waiting each time it sends,
// so the buffer fills only behind a peer that does not answer — one whose
// copies expire on their TTL before a backlog this long would reach it.
const hintPeerMax = 16384

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
// hint after that. Now a slow peer backs up its own buffer and, past
// hintPeerMax hints, loses hints of its own, counted as dropped like a full
// queue's here.
func (iv *invalidator) deliver(batch []invalidation) {
	c := iv.cache
	if c.transport == nil || c.guard == nil {
		return
	}
	self := c.selfID()
	n := hintCount(batch)
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
			s = &hintSender{wake: make(chan struct{}, 1)}
			iv.peers[node.ID] = s
			iv.wg.Add(1)
			go iv.send(s)
		}
		s.mu.Lock()
		s.node = node
		if s.hints+n > hintPeerMax {
			s.mu.Unlock()
			iv.dropped.Add(int64(n))
			continue
		}
		s.pending = append(s.pending, batch...)
		s.hints += n
		s.mu.Unlock()
		s.signal()
	}
	// A departed peer's sender finishes what it holds and stops.
	for id, s := range iv.peers {
		if _, ok := present[id]; !ok {
			s.mu.Lock()
			s.closed = true
			s.mu.Unlock()
			s.signal()
			delete(iv.peers, id)
		}
	}
}

func (s *hintSender) signal() {
	select {
	case s.wake <- struct{}{}:
	default: // already signalled; the sender will see everything pending
	}
}

// take removes up to about hintMessageMax hints from the front of what is
// pending, reporting the peer's address and whether the sender should stop.
func (s *hintSender) take() (batch []invalidation, node core.Node, stop bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pending) == 0 {
		return nil, s.node, s.closed
	}
	k, hints := 0, 0
	for k < len(s.pending) && hints < hintMessageMax {
		hints += len(s.pending[k].keys)
		k++
	}
	batch = append([]invalidation(nil), s.pending[:k]...)
	if k == len(s.pending) {
		s.pending = nil
	} else {
		// Copied down rather than resliced, so the front's keys are
		// not held by the array behind the rest.
		rest := copy(s.pending, s.pending[k:])
		clear(s.pending[rest:])
		s.pending = s.pending[:rest]
	}
	s.hints -= hints
	return batch, s.node, false
}

func hintCount(batch []invalidation) int {
	n := 0
	for _, inv := range batch {
		n += len(inv.keys)
	}
	return n
}

// send delivers one peer's hints until it leaves the view or Close: everything
// waiting, up to about hintMessageMax hints a time, as one message per
// namespace.
func (iv *invalidator) send(s *hintSender) {
	defer iv.wg.Done()
	c := iv.cache
	for {
		select {
		case <-iv.done:
			// Shutting down. This peer loses what is waiting for it and
			// falls back to its TTL, which is what it would have done
			// had the messages been dropped in flight.
			return
		case <-s.wake:
		}
		for {
			batch, node, stop := s.take()
			if stop {
				return
			}
			if batch == nil {
				break
			}
			for _, req := range requests(batch) {
				select {
				case <-iv.done:
					return
				default:
				}
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
		}
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
