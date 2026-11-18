package peercache

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/replication"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// handoffQueue bounds pending replica hand-offs.
//
// Sized like the replication queue and for the same reason: these carry data,
// so a dropped one is a real loss of redundancy rather than a missed hint. It is
// still finite, because an unbounded queue in front of a slow peer is a memory
// leak with a scheduling delay — and unlike replication, a full queue here does
// not drop anything. The copy goes back into the replica store instead.
const handoffQueue = 4096

// handoffTimeout bounds one attempt to place a moved replica.
//
// Reconciliation runs on a membership change, which is exactly when peers are
// least likely to answer promptly, so this is generous. A hand-off that times
// out keeps the copy rather than losing it, so the cost of being wrong here is
// memory, not data.
const handoffTimeout = 10 * time.Second

// handoffWorkers is how many hand-offs are in flight at once.
//
// It was one. A hand-off is a round trip to each node that now ranks for the
// key, so one sender moved a key per two round trips: on kind, about 400 a
// second from a pod left with 117,000 out-of-place copies by a rolling
// restart, which took five minutes to drain, every one of them with the
// pod's copies of those keys held only where they no longer belong. Eight, as
// for asynchronous replication (replicaWorkers). Hand-offs of different keys
// are independent. Two of one key can overlap — a newer copy arriving while
// one is in flight is drained by the next pass — and every receiver keeps the
// newer of the two by order, as it does for any copy; a put-back of the older
// is refused the same way.
const handoffWorkers = 8

// handoff moves replicas this node has stopped ranking for to the nodes that now
// rank for them, and drops them only once they have landed.
//
// # The defect this exists to fix
//
// Reconciliation used to drop a replica the moment this node stopped ranking for
// its key, decided from the local ranking alone, with nothing asking whether a
// node that now ranks for it holds a copy. That was identified as a hazard
// (docs/decisions/open.md question 38), tested for, and not reproduced — the kill-and-rejoin
// churn it tried does not take ranking away from the holder of a last copy.
//
// A partition does. Writes accepted while one node is cut off place their copies
// among the nodes the writer can see, at ranking positions computed *without*
// the absent node. When it returns, the ranking changes underneath every one of
// those keys: the rejoining node ranks for many of them and holds none, and the
// nodes that do hold them stop ranking and drop them. The copies do not move —
// they evaporate. `TestMajoritySideWritesSurviveAHealAndTwoDestructions`
// reproduces it, and the counters make the mechanism plain: twenty-one replicas
// evicted cluster-wide, twenty keys lost.
//
// Catch-up cannot repair this. It pulls from whoever has the key, and after the
// eviction nobody does.
//
// # Why a hand-off rather than a smarter eviction test
//
// The condition that would make dropping safe — "a node that ranks for this key
// holds it" — is not answerable locally. Asking costs a round trip per key on a
// membership change, which is the same round trip a hand-off costs, except that
// the hand-off also fixes the problem instead of merely detecting it.
//
// It is the same move `drain.go` makes for a departing node, applied to keys
// that departed rather than nodes. The receiving side needs no new RPC: the
// replicate handler already files an incoming copy in the owned store when the
// receiver owns the key and in the replica store when it does not.
//
// # Why it never drops on failure
//
// Every failure path keeps the copy. A full queue, an unreachable peer, a
// namespace with no resolvable targets — all of them put the entry back. The
// cost of keeping a replica too long is memory that the next reconciliation
// reclaims; the cost of dropping one too early is the defect above.
type handoff struct {
	cache *cache

	queue chan handoffWork
	done  chan struct{}
	wg    sync.WaitGroup

	// base is cancelled by Close so an in-flight send returns at once rather
	// than waiting out a deadline measured on the injected clock.
	base       context.Context
	cancelBase context.CancelFunc

	rehomed  atomic.Int64
	returned atomic.Int64
}

// handoffWork is one moved replica.
type handoffWork struct {
	namespace string
	key       string
	entry     replication.Entry
}

func newHandoff(c *cache) *handoff {
	base, cancel := context.WithCancel(context.Background())
	h := &handoff{
		cache:      c,
		queue:      make(chan handoffWork, handoffQueue),
		done:       make(chan struct{}),
		base:       base,
		cancelBase: cancel,
	}
	for i := 0; i < handoffWorkers; i++ {
		h.wg.Add(1)
		go h.run()
	}
	return h
}

// room reports how many more hand-offs the queue can take now. Reconciliation
// drains no more orphans than this, so that the queue's bound paces the work
// rather than turning it into a drain-and-return loop.
func (h *handoff) room() int {
	return cap(h.queue) - len(h.queue)
}

// enqueue schedules a hand-off, restoring the copy if it cannot be scheduled.
func (h *handoff) enqueue(w handoffWork) {
	select {
	case h.queue <- w:
	default:
		h.restore(w)
	}
}

// restore puts a copy back in the replica store.
//
// Through the ordinary Put, so the stamp check applies: if a newer copy of this
// key arrived while the hand-off was in flight, the restore is correctly
// refused. A returning copy is not privileged for having been in transit.
func (h *handoff) restore(w handoffWork) {
	h.cache.replicas.Put(w.namespace, w.key, w.entry)
	h.returned.Add(1)
	// Still out of place, so a later tick tries it again.
	h.cache.promoter.markDirty()
}

func (h *handoff) run() {
	defer h.wg.Done()
	for {
		select {
		case <-h.done:
			return
		case w := <-h.queue:
			ctx, cancel := clock.WithTimeout(h.base, h.cache.opts.Clock, handoffTimeout)
			h.place(ctx, w)
			cancel()
		}
	}
}

// place sends one moved replica to the nodes that now rank for it.
//
// A single acceptance is enough to drop the local copy: the key is then held by
// a node the current ranking names, which is the condition eviction always
// needed and never checked. Sending to every target rather than stopping at the
// first restores the replication factor in the same pass, which is what makes a
// membership change self-healing instead of merely non-destructive.
//
// # Two of the restore paths here have no test, and this says which
//
// `TestAHandOffThatCannotBePlacedKeepsTheCopy` covers the one that matters — a
// peer that cannot be reached — and the two below are not covered:
//
//   - the namespace having disappeared. Namespaces are never removed, so this
//     cannot happen today.
//   - a full queue, in `enqueue`. It takes 4096 orphans outstanding at once.
//
// Both are guards on conditions the public API cannot currently produce,
// and both do the same thing as the covered path — put the copy back —
// which is why they are recorded here rather than reached through a test that
// would have to contort the package to arrange them. Every root-package test is
// an external one by convention, and breaking that for two defensive branches
// is a worse trade than this paragraph.
//
// If a namespace ever becomes removable, the first of these stops being
// theoretical and needs a test rather than a note.
func (h *handoff) place(ctx context.Context, w handoffWork) {
	c := h.cache
	ns, ok := c.namespaceIfExists(w.namespace)
	if !ok {
		h.restore(w)
		return
	}

	targets, _, err := c.replicaTargets(ns.groupKeyFor(w.key), ns.opts.ReplicationFactor)
	if err != nil || len(targets) == 0 {
		// No resolvable destination. Keeping the copy is the only safe
		// direction: this node no longer ranks for the key, so if it drops
		// it now and nobody else has it, it is gone.
		h.restore(w)
		return
	}

	// From carries the node that *wrote* this value, not the node forwarding
	// it, and that distinction is load-bearing.
	//
	// A replica records From so that split brain is detectable: two different
	// writers for one key under one membership epoch means two nodes believed
	// they owned it, and [replication.Store.Put] reports exactly that
	// comparison. A hand-off is a relocation, not a write. Stamping it with
	// this node's ID would make the receiver see the original owner's
	// replication and this forward as two writers, count a split brain that
	// did not happen, and poison the one counter the README says deserves
	// investigation whenever it is non-zero.
	//
	// An empty From is passed on empty. It means the writer is unknown: a
	// copy demoted from this node's owned store, which does not record who
	// wrote a key it adopted or promoted (see promoter.demote). Naming this
	// node instead was the very claim this paragraph warns against, made
	// for the copies that had lost their writer.
	from := w.entry.From
	left, live := w.entry.Remaining(c.opts.Clock.Now().UnixNano())
	if !live {
		// Expired here, so expired on every other holder too: there is
		// nothing to hand on, and handing it on would revive it.
		return
	}
	req := &wire.ReplicateRequest{
		Namespace: w.namespace,
		Key:       w.key,
		Value:     w.entry.Value,
		TTLNanos:  left,
		Stamp:     w.entry.Stamp,
		From:      from,
		Order:     w.entry.Order,
		Prev:      w.entry.Prev,
	}

	placed := false
	for _, node := range targets {
		select {
		case <-h.done:
			if !placed {
				h.restore(w)
			}
			return
		default:
		}
		var resp wire.ReplicateResponse
		if err := c.route.invoke(ctx, node, transport.MethodReplicate, req, &resp); err != nil {
			continue
		}
		placed = true
	}

	if !placed {
		h.restore(w)
		return
	}
	if c.traced(w.namespace, w.key) {
		c.record(w.namespace, w.key, keytraceHandOff, w.entry.Order, w.entry.From, w.entry.Stamp.Version, w.entry.Value, "")
	}
	h.rehomed.Add(1)
}

// Close stops the worker and waits for it.
func (h *handoff) Close() {
	close(h.done)
	h.cancelBase()
	h.wg.Wait()
}

// counters reports hand-off activity.
func (h *handoff) counters() (rehomed, returned int64) {
	return h.rehomed.Load(), h.returned.Load()
}
