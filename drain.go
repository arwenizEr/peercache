package peercache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Drain hands this node's work away and stops accepting more, without closing
// it.
//
// # The order is the whole design
//
// A PreStop hook runs while the pod is still routable, so the sequence has to
// remove this node from consideration *before* it stops being able to answer,
// not after. Backwards, every step is a window in which requests arrive at a
// node that has already given away the ability to serve them.
//
//  1. Mark unready. Kubernetes removes the pod from the service endpoints, so
//     new client traffic stops arriving. Peers still route to it, because
//     ownership is not readiness.
//  2. Announce departure through gossip. Peers recompute the ranking without
//     this node and stop routing to it. This is the step that actually moves
//     ownership, and it is deliberately before the transfer: the new owners
//     need to be the new owners before they are handed anything, or a transfer
//     would arrive at a node that considers itself a replica and files it as
//     one.
//  3. Push owned keys to their new owners. Only now is it clear where they go.
//  4. Deactivate groups, so their state is flushed and their goroutines exit.
//
// # What Drain is not
//
// It is not Close. A drained cache still answers reads for whatever it holds
// and its transport still serves, because a peer that has not yet noticed the
// departure will still ask, and answering is better than refusing. Call Close
// afterwards.
//
// It is also not atomic and cannot be. Between steps 2 and 3 the keys have new
// owners that do not hold them yet; a read in that window falls back to a
// replica if one exists and misses if not. Synchronous replication closes that
// window, which is one more reason it exists.
func (c *cache) Drain(ctx context.Context) error {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return ErrClosed
	}
	if !c.readiness.draining.CompareAndSwap(false, true) {
		// Already draining. Idempotent rather than an error: a PreStop
		// hook that fires twice is a retry, not a mistake.
		return nil
	}

	c.drainRounds.Add(1)

	if c.guard == nil || c.transport == nil {
		// A single node has nowhere to hand anything to. Deactivating
		// groups is still worth doing, so their state is not left
		// behind an idle timer.
		c.groups.DeactivateAll()
		return nil
	}

	// Step 2: leave the cluster, so peers stop ranking this node.
	//
	// Errors are reported but not fatal. A node that cannot announce its
	// departure will be detected as failed instead, which is slower and
	// noisier but not wrong — and giving up here would skip the transfer,
	// which is the part that actually saves data.
	var problems error
	if err := c.opts.Membership.Leave(ctx); err != nil {
		problems = errors.Join(problems, fmt.Errorf("peercache: announcing departure: %w", err))
		c.opts.Logger.Warn("could not announce departure; peers will detect the failure instead",
			"error", err)
	}

	// Step 3: push what this node owns to whoever owns it now.
	if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}

	// Step 4: stop the group goroutines.
	c.groups.DeactivateAll()
	return problems
}

// handOff pushes every owned key to its new owner.
//
// It reuses the replication path rather than inventing a transfer protocol.
// That is not a shortcut: a handed-off key and a replicated key are the same
// thing — a value plus the stamp that orders it — and the receiving side
// already knows how to refuse one that is older than what it holds. A separate
// protocol would be a second implementation of the same decision.
//
// The receiver files it as a replica and its own reconciliation promotes it,
// because by now the ranking says it is the owner. That indirection is what
// makes the handover safe against the ranking changing again mid-transfer: the
// stamp decides, not the sender's opinion of who should have it.
func (c *cache) handOff(ctx context.Context) error {
	c.mu.RLock()
	names := make([]string, len(c.order))
	copy(names, c.order)
	c.mu.RUnlock()

	var problems error
	for _, name := range names {
		ns, ok := c.namespaceIfExists(name)
		if !ok || ns.err != nil || ns.store == nil {
			continue
		}

		after := ""
		for {
			if err := ctx.Err(); err != nil {
				return errors.Join(problems, err)
			}

			entries, more := ns.store.SnapshotFrom(c.pages, "\x00drain\x00"+ns.name, after, handOffPageSize, nil)
			if len(entries) == 0 {
				break
			}

			for _, e := range entries {
				after = e.Key
				groupKey := ns.groupKeyFor(e.Key)

				// Where does it belong now? The Leave above has
				// already removed this node from the ranking, so
				// this is the answer the rest of the cluster is
				// using.
				targets, _, err := c.replicaTargets(groupKey, maxInt(ns.opts.ReplicationFactor, 2))
				if err != nil || len(targets) == 0 {
					continue
				}

				value, err := ns.decode(e.Value)
				if err != nil {
					problems = errors.Join(problems, err)
					continue
				}
				var ttl int64
				if e.ExpiresAt != 0 {
					ttl = e.ExpiresAt - c.opts.Clock.Now().UnixNano()
					if ttl <= 0 {
						continue
					}
				}

				// Synchronous: a drain that returned before the
				// keys landed would be a drain that did nothing,
				// and the pod is about to stop.
				err = c.replicator.replicate(ctx, true, replicaWork{
					namespace: name,
					key:       e.Key,
					value:     ns.encode(value),
					ttl:       durationFromNanos(ttl),
					stamp:     ns.stamp(e.Version),
					order:     e.Order,
					targets:   targets,
				})
				if err != nil {
					problems = errors.Join(problems,
						fmt.Errorf("peercache: handing off %q: %w", e.Key, err))
					continue
				}
				c.keysHandedOff.Add(1)
			}

			if !more {
				break
			}
		}
	}
	return problems
}

// handOffPageSize bounds one page of a hand-off, for the same reason catch-up
// pages: a message proportional to the keyspace is the shape of this project's
// worst defect.
const handOffPageSize = 512

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// durationFromNanos converts a nanosecond count to a Duration.
func durationFromNanos(n int64) time.Duration { return time.Duration(n) }
