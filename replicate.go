package peercache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// replicaQueue bounds pending asynchronous fan-out.
//
// Larger than the invalidation queue, because these carry data rather than
// hints: dropping one loses a replica's copy until the next write to that key,
// which widens the data-loss window replication exists to bound. It is still
// finite, because an unbounded queue in front of a slow peer is a memory leak
// with a scheduling delay.
//
// It is the total across the fan-out workers: each holds an equal share.
const replicaQueue = 4096

// replicaWorkers is how many keys' asynchronous fan-out run at once.
//
// It was one: a node sent one replica write at a time, so its asynchronous
// replication rate was one over the round trip, whatever its load. On kind at
// about 140,000 orders a minute, the queue in front of that one sender
// overflowed and dropped some 250,000 replica writes a minute — writes that had
// reported success and were never copied. A key always goes to the same
// worker, so one key's copies still travel one at a time and in order.
const replicaWorkers = 8

// restoreBacklogMax and restoreWorkers size the fan-out of restored keys,
// which has a backlog and senders of its own.
//
// It shared the live queue first. A restart restores a node's whole share at
// once — on kind, 249,000 keys in a minute on one pod — and the burst filled
// the queue live writes use and dropped live copies along with restored ones
// (224,622 drops, docs/m16-results.md). So restored keys were given a queue of
// their own, of 1,024, and its drops were counted apart, on the reasoning that
// a restored key's replicas usually still hold it from before the restart.
//
// Under a rolling restart they do not: the nodes holding those replicas were
// restarted too, and hold nothing. On kind at 3.7 million keys, 98% of every
// pod's restored keys had their copies dropped — 1.37 million keys, about 2.7
// million missing copies, a third of the cluster's redundancy gone with no
// write lost and nothing to show for it but this counter.
//
// So a restored key is never dropped for want of room. It is noted — its
// namespace, key and order, not its value — in a backlog the senders work
// through at the network's pace, and its value is read from the owned store
// when its turn comes. The backlog is bounded only as a guard, far above any
// share a node restores; past it a key is dropped and counted in
// RestoreReplicationsDropped, which otherwise stays zero.
//
// A key's restored copy and a later write's copy may arrive in either order; a
// replica keeps the newer, as it does for any copy. A key written since it was
// restored is skipped: the write replicated it.
const (
	restoreBacklogMax = 4 << 20
	restoreWorkers    = 8
)

// replicaTimeout bounds one asynchronous fan-out attempt.
//
// Synchronous replication uses the caller's deadline instead — a caller that
// asked to wait for durability has said what its budget is, and substituting
// this one would either cut it short or extend it.
const replicaTimeout = 5 * time.Second

// replicator fans owned writes out to the nodes that rank as their replicas.
//
// # Async and sync are the same path with a different wait
//
// Both modes send the same request to the same nodes. The only difference is
// whether the owner waits for the answer, and that is deliberate: a system
// where the two modes take different code paths is one where the rarely-used
// mode is the broken one.
//
// Asynchronous is the default, configurable per namespace, and its cost is
// a measured data-loss window rather than an unknown: between a write returning
// and the replica holding it, the value exists on exactly one node.
type replicator struct {
	cache *cache

	// queues holds one channel per worker; a key's work always goes to
	// queues[workerFor(namespace, key)].
	queues []chan replicaWork
	// restored is the backlog of restored keys; see restoreBacklogMax.
	restored restoreBacklog
	done     chan struct{}
	wg       sync.WaitGroup

	// base is cancelled by Close so an in-flight send returns at once
	// rather than waiting out a deadline measured on the injected clock.
	// The invalidator learned this the hard way; see invalidate.go.
	base       context.Context
	cancelBase context.CancelFunc

	sent      atomic.Int64
	dropped   atomic.Int64
	failed    atomic.Int64
	conflicts atomic.Int64
	// restoreDropped counts restored keys' copies dropped because their
	// backlog was at restoreBacklogMax.
	restoreDropped atomic.Int64
}

// restoredKey is one restored key waiting to go to its replicas.
type restoredKey struct {
	ns    *namespace
	key   string
	order uint64
	// tombstone is a restored deletion, sent as one; anything else is sent
	// as the owned store holds it when its turn comes.
	tombstone bool
}

// restoreBacklog is a first-in first-out list of restored keys.
type restoreBacklog struct {
	mu    sync.Mutex
	items []restoredKey
	head  int
	// wake has room for one signal: a sender asleep on it is woken by the
	// next push, and pushes while nobody sleeps leave one signal behind.
	wake chan struct{}
	// bytes is what the queued keys are charged; the memory budget counts
	// it. On kind a failover's restores queued enough keys to hold 80 MB.
	bytes atomic.Int64
}

// restoredKeySlot is the size of a restoredKey on 64-bit: a pointer, a
// string header, an order and a padded bool.
const restoredKeySlot = 40

// restoredKeyCharge is what one queued key is charged: its slot, twice over
// for the slice's growth by doubling, and its key.
func restoredKeyCharge(k restoredKey) int64 {
	return 2*restoredKeySlot + int64(len(k.key))
}

// push appends k unless the backlog is at max, and reports whether it did.
func (b *restoreBacklog) push(k restoredKey, max int) bool {
	b.mu.Lock()
	if len(b.items)-b.head >= max {
		b.mu.Unlock()
		return false
	}
	b.items = append(b.items, k)
	b.bytes.Add(restoredKeyCharge(k))
	b.mu.Unlock()
	select {
	case b.wake <- struct{}{}:
	default:
	}
	return true
}

// pop takes the oldest key, if any. The slice is released once drained, so a
// burst's backing array does not outlive it.
func (b *restoreBacklog) pop() (restoredKey, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.head == len(b.items) {
		return restoredKey{}, false
	}
	k := b.items[b.head]
	b.bytes.Add(-restoredKeyCharge(k))
	b.items[b.head] = restoredKey{}
	b.head++
	if b.head == len(b.items) {
		b.items, b.head = nil, 0
	} else {
		// Another sender may be asleep with work left.
		select {
		case b.wake <- struct{}{}:
		default:
		}
	}
	return k, true
}

// replicaWork is one key's fan-out.
type replicaWork struct {
	namespace string
	key       string
	value     []byte
	ttl       time.Duration
	stamp     core.Stamp
	deleted   bool
	// order is the write's order; see wire.ReplicateRequest.
	order uint64
	// prev is the order of the value the write replaced.
	prev    uint64
	targets []core.Node
	// persister is set for this node's own writes to a persistent
	// namespace: the copy carries its durableThrough, read as it is sent.
	persister *persister
}

func newReplicator(c *cache) *replicator {
	base, cancel := context.WithCancel(context.Background())
	r := &replicator{
		cache:      c,
		queues:     make([]chan replicaWork, replicaWorkers),
		restored:   restoreBacklog{wake: make(chan struct{}, 1)},
		done:       make(chan struct{}),
		base:       base,
		cancelBase: cancel,
	}
	for i := range r.queues {
		r.queues[i] = make(chan replicaWork, replicaQueue/replicaWorkers)
		r.wg.Add(1)
		go r.run(r.queues[i])
	}
	for i := 0; i < restoreWorkers; i++ {
		r.wg.Add(1)
		go r.runRestored()
	}
	return r
}

// enqueueRestored notes a restored key for its replicas. It never blocks: a
// restore has callers waiting for it.
func (r *replicator) enqueueRestored(k restoredKey) {
	if !r.restored.push(k, restoreBacklogMax) {
		r.restoreDropped.Add(1)
	}
}

// runRestored sends restored keys to their replicas until Close.
func (r *replicator) runRestored() {
	defer r.wg.Done()
	for {
		k, ok := r.restored.pop()
		if !ok {
			select {
			case <-r.done:
				return
			case <-r.restored.wake:
			}
			continue
		}
		select {
		case <-r.done:
			return
		default:
		}
		w, send := r.restoredWork(k)
		if !send {
			continue
		}
		ctx, cancel := clock.WithTimeout(r.base, r.cache.opts.Clock, replicaTimeout)
		r.fanOut(ctx, w)
		cancel()
	}
}

// restoredWork builds the fan-out of a restored key as things stand now,
// and reports false when there is nothing to send: the key is no longer this
// node's, has been written since it was restored, has expired, or has no
// replicas to go to.
func (r *replicator) restoredWork(k restoredKey) (replicaWork, bool) {
	ns := k.ns
	if g := r.cache.guard; g != nil {
		owner, _, err := g.Table().Owner(ns.groupKeyFor(k.key))
		if err != nil || owner.ID != r.cache.selfID() {
			return replicaWork{}, false
		}
	}
	w := replicaWork{namespace: ns.name, key: k.key, order: k.order}
	if k.tombstone {
		if order, live := ns.store.Order(k.key); live && order > k.order {
			return replicaWork{}, false
		}
		w.stamp = ns.stamp(math.MaxUint64)
		w.deleted = true
	} else {
		val, version, expiresAt, order, live := ns.store.Peek(k.key)
		if !live || order != k.order {
			return replicaWork{}, false
		}
		w.value = val
		w.stamp = ns.stamp(version)
		if expiresAt != 0 {
			w.ttl = time.Duration(expiresAt - r.cache.opts.Clock.Now().UnixNano())
			if w.ttl <= 0 {
				return replicaWork{}, false
			}
		}
	}
	targets, _, err := r.cache.replicaTargets(ns.groupKeyFor(k.key), ns.opts.ReplicationFactor)
	if err != nil || len(targets) == 0 {
		return replicaWork{}, false
	}
	w.targets = targets
	return w, true
}

// targets returns the nodes that should hold a copy of this group key, not
// counting this one.
//
// Replicas are the runners-up in the rendezvous ranking, which is what makes
// promotion free: when the owner dies, the first replica becomes the owner on
// every node at once, without an election, because they all compute the same
// ranking from the same membership.
// The rf < 2 test is a fast path, not a correctness guard, and saying so
// matters: at RF=1 the ranking is just the owner, and the self-filter below
// removes it, so replication factor 1 sends nothing whether or not this branch
// exists. Breaking the branch on purpose proved that — removing it changed no
// behaviour and no test. It stays because computing a ranking to discard it is
// wasted work on every write in the common case, which is what RF=1 is.
func (c *cache) replicaTargets(groupKey string, rf int) ([]core.Node, core.Epoch, error) {
	if c.guard == nil || rf < 2 {
		return nil, 0, nil
	}
	// Holders, not Replicas: this runs on every replicated write, and
	// Replicas copies, sorts and clones the whole membership to return at
	// most three nodes (open question 46). The ranking is the same, which
	// internal/ownership tests. Only the targets kept are cloned, because
	// they outlive this call on the fan-out queue.
	ranked, count, epoch := c.guard.Table().Holders(groupKey, rf)
	if count == 0 {
		return nil, epoch, fmt.Errorf("peercache: no members to replicate %q to: %w", groupKey, core.ErrNotFound)
	}
	self := c.selfID()
	out := make([]core.Node, 0, count)
	for _, n := range ranked[:count] {
		if n.ID == self {
			continue
		}
		out = append(out, n.Clone())
	}
	return out, epoch, nil
}

// enqueue schedules asynchronous fan-out. It never blocks.
//
// A full queue drops the work and counts it, for the same reason the
// invalidation queue does: making a write wait on the health of its peers is
// the coupling asynchronous replication exists to avoid. The difference is that
// a dropped replication is a real loss of redundancy, so it is counted
// separately and reported in Stats rather than logged and forgotten.
func (r *replicator) enqueue(w replicaWork) {
	if len(w.targets) == 0 {
		return
	}
	select {
	case r.queues[workerFor(w.namespace, w.key, len(r.queues))] <- w:
	default:
		r.dropped.Add(1)
	}
}

// workerFor picks the fan-out worker for a key: FNV-1a over the namespace and
// the key. The same key always maps to the same worker, which is what keeps
// one key's copies in order while different keys' travel at once.
func workerFor(namespace, key string, n int) int {
	const offset, prime = 14695981039346656037, 1099511628211
	h := uint64(offset)
	for i := 0; i < len(namespace); i++ {
		h ^= uint64(namespace[i])
		h *= prime
	}
	h ^= 0xff // a separator, so ("ab", "c") and ("a", "bc") differ
	h *= prime
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= prime
	}
	return int(h % uint64(n))
}

func (r *replicator) run(queue chan replicaWork) {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			return
		case w := <-queue:
			ctx, cancel := clock.WithTimeout(r.base, r.cache.opts.Clock, replicaTimeout)
			r.fanOut(ctx, w)
			cancel()
		}
	}
}

// fanOut sends one key to every target at once, reporting every failure.
//
// Every target is attempted even after one fails. A replica set exists so that
// losing one node is survivable; abandoning the rest because the first was
// unreachable would turn one dead peer into zero replicas.
func (r *replicator) fanOut(ctx context.Context, w replicaWork) error {
	req := &wire.ReplicateRequest{
		Namespace: w.namespace,
		Key:       w.key,
		Value:     w.value,
		TTLNanos:  int64(w.ttl),
		Stamp:     w.stamp,
		From:      r.cache.selfID(),
		Deleted:   w.deleted,
		Order:     w.order,
		Prev:      w.prev,
	}
	if w.persister != nil {
		req.Durable = w.persister.durableThrough()
	}

	// Every target at once. One after another, a synchronous write at
	// replication factor 3 waited two round trips rather than one, and its
	// second replica waited out the first's timeout when the first did not
	// answer. Each key's copies still leave in order: a worker, or a
	// synchronous caller, sends a key's next write only after this returns.
	if len(w.targets) == 1 {
		return r.sendTo(ctx, req, w.targets[0])
	}
	errs := make([]error, len(w.targets))
	var wg sync.WaitGroup
	for i, node := range w.targets[1:] {
		wg.Add(1)
		go func(i int, node core.Node) {
			defer wg.Done()
			errs[i] = r.sendTo(ctx, req, node)
		}(i+1, node)
	}
	errs[0] = r.sendTo(ctx, req, w.targets[0])
	wg.Wait()
	return errors.Join(errs...)
}

// sendTo sends one copy to one target, counting the outcome.
func (r *replicator) sendTo(ctx context.Context, req *wire.ReplicateRequest, node core.Node) error {
	select {
	case <-r.done:
		return nil
	default:
	}
	var resp wire.ReplicateResponse
	if err := r.cache.route.invoke(ctx, node, transport.MethodReplicate, req, &resp); err != nil {
		r.failed.Add(1)
		return fmt.Errorf("peercache: replicating %q to %s: %w", req.Key, node.ID, err)
	}
	if resp.Conflict {
		// Two nodes wrote this key in one epoch. Counted, never
		// merged: the design rules it out, and picking a winner
		// here would be a resolution policy invented on the spot.
		r.conflicts.Add(1)
		r.cache.opts.Logger.Warn("conflicting write detected by replica",
			"namespace", req.Namespace, "key", req.Key, "replica", node.ID)
	}
	r.sent.Add(1)
	return nil
}

// replicate fans a write out, waiting only if the namespace asked for it.
//
// Synchronous mode runs on the caller's context and returns its errors, so a
// caller that asked for durability learns whether it got it. Asynchronous mode
// returns immediately and the write is durable on one node until the queue
// drains — the window measured in docs/benchmarks.md.
func (r *replicator) replicate(ctx context.Context, sync bool, w replicaWork) error {
	if len(w.targets) == 0 {
		return nil
	}
	if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)
}

// Close stops the fan-out goroutine and waits for it.
func (r *replicator) Close() {
	close(r.done)
	r.cancelBase()
	r.wg.Wait()
}

// counters reports fan-out activity for [Stats].
func (r *replicator) counters() (sent, dropped, failed, conflicts int64) {
	return r.sent.Load(), r.dropped.Load(), r.failed.Load(), r.conflicts.Load()
}

// restoreDrops reports restored keys' copies dropped; see enqueueRestored.
func (r *replicator) restoreDrops() int64 { return r.restoreDropped.Load() }
