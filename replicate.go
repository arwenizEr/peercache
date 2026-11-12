package peercache

import (
	"context"
	"errors"
	"fmt"
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
const replicaQueue = 4096

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

	queue chan replicaWork
	done  chan struct{}
	wg    sync.WaitGroup

	// base is cancelled by Close so an in-flight send returns at once
	// rather than waiting out a deadline measured on the injected clock.
	// The invalidator learned this the hard way; see invalidate.go.
	base       context.Context
	cancelBase context.CancelFunc

	sent      atomic.Int64
	dropped   atomic.Int64
	failed    atomic.Int64
	conflicts atomic.Int64
}

// replicaWork is one key's fan-out.
type replicaWork struct {
	namespace string
	key       string
	value     []byte
	ttl       time.Duration
	stamp     core.Stamp
	deleted   bool
	targets   []core.Node
}

func newReplicator(c *cache) *replicator {
	base, cancel := context.WithCancel(context.Background())
	r := &replicator{
		cache:      c,
		queue:      make(chan replicaWork, replicaQueue),
		done:       make(chan struct{}),
		base:       base,
		cancelBase: cancel,
	}
	r.wg.Add(1)
	go r.run()
	return r
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
	ranked, epoch, err := c.guard.Table().Replicas(groupKey, rf)
	if err != nil {
		return nil, epoch, err
	}
	self := c.self().ID
	out := make([]core.Node, 0, len(ranked))
	for _, n := range ranked {
		if n.ID == self {
			continue
		}
		out = append(out, n)
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
	case r.queue <- w:
	default:
		r.dropped.Add(1)
	}
}

func (r *replicator) run() {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			return
		case w := <-r.queue:
			ctx, cancel := clock.WithTimeout(r.base, r.cache.opts.Clock, replicaTimeout)
			r.fanOut(ctx, w)
			cancel()
		}
	}
}

// fanOut sends one key to every target, reporting the first failure.
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
		From:      r.cache.self().ID,
		Deleted:   w.deleted,
	}

	var failures error
	for _, node := range w.targets {
		select {
		case <-r.done:
			return failures
		default:
		}

		var resp wire.ReplicateResponse
		if err := r.cache.route.invoke(ctx, node, transport.MethodReplicate, req, &resp); err != nil {
			r.failed.Add(1)
			failures = errors.Join(failures,
				fmt.Errorf("peercache: replicating %q to %s: %w", w.key, node.ID, err))
			continue
		}
		if resp.Conflict {
			// Two nodes wrote this key in one epoch. Counted, never
			// merged: the design rules it out, and picking a winner
			// here would be a resolution policy invented on the spot.
			r.conflicts.Add(1)
			r.cache.opts.Logger.Warn("conflicting write detected by replica",
				"namespace", w.namespace, "key", w.key, "replica", node.ID)
		}
		r.sent.Add(1)
	}
	return failures
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
