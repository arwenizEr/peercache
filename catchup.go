package peercache

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/replication"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// catchUpPageSize bounds one catch-up response.
//
// There is deliberately no per-page timeout constant. Catch-up runs on the
// caller's context: it is an explicit operation the application asked for, so
// the application's deadline is the right one, and substituting a library
// constant would either cut short a deliberate bulk transfer or extend a
// caller who wanted to give up.
//
// A page is a message, and a message proportional to the keyspace is the shape
// of every out-of-memory this project has already fixed once. 512 keys of a few
// hundred bytes is a message in the hundreds of kilobytes: large enough that a
// full namespace transfers in a sane number of round trips, small enough that
// one of them cannot be the reason a node dies.
const catchUpPageSize = 512

// catchUp pulls the replicas this node should be holding from their owners.
//
// # Why a returning node cannot just wait
//
// Replication is a stream of writes, not a state transfer. A node that restarts
// comes back with empty memory, and the only keys it will ever receive are the
// ones written *after* it returned — every key that was quiet at the moment it
// died stays missing until somebody happens to write it. Since a replica exists
// precisely to survive the owner's death, a replica set silently missing its
// cold keys is a replica set that will fail when it is finally needed.
//
// So a returning node asks. It names itself, each owner computes which keys
// that node ranks for, and sends them. The asker does not enumerate what it
// wants — it cannot, having forgotten — and both sides derive the answer from
// the same rendezvous ranking.
//
// # Why this is not the same as promotion
//
// A killed process and a partitioned one look identical from outside and are
// entirely different problems. This handles the first: empty memory, nothing to
// disagree about, just missing data. A node returning from a *partition* still
// holds its old state and its old belief about ownership, and its stale copies
// are refused on arrival by the stamp check rather than by anything here.
type catchUp struct {
	cache *cache

	pulled   atomic.Int64
	runs     atomic.Int64
	failures atomic.Int64
}

func newCatchUp(c *cache) *catchUp {
	return &catchUp{cache: c}
}

// Run pulls every namespace this node should be replicating, from every peer.
//
// It is deliberately explicit rather than automatic on join. A node that has
// just started is usually also serving traffic, and pulling the entire replica
// set at that moment competes with the requests it exists to answer. The
// application decides when it can afford it — after readiness, during a quiet
// period, or not at all if the namespace is reconstructible from a loader.
//
// Errors from individual peers are counted and not returned: catching up from
// three of four owners is strictly better than catching up from none, and a
// caller cannot do anything useful with one peer's failure except try again.
func (c *cache) CatchUp(ctx context.Context) error {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return ErrClosed
	}
	if c.guard == nil || c.transport == nil {
		// Nothing to catch up from. A single node holds everything it
		// owns and replicates to nobody.
		c.readiness.markCaughtUp()
		if c.guard != nil {
			c.readiness.markCurrent(c.guard.Stats().Epoch)
		}
		return nil
	}
	// The epoch is read before the run, not after. A membership change while
	// pages are in flight means some of them were chosen under a ranking that
	// no longer holds, and reading it afterwards would report that run as
	// current for a view it never saw.
	epoch := c.guard.Stats().Epoch
	err := c.catchup.run(ctx)
	if err == nil {
		c.readiness.markCurrent(epoch)
	}
	// Marked even when individual peers failed. Catching up from three of
	// four owners leaves this node far better placed than never trying, and
	// a readiness gate that never opens because one peer was briefly
	// unreachable is a gate that gets disabled.
	c.readiness.markCaughtUp()
	return err
}

func (cu *catchUp) run(ctx context.Context) error {
	c := cu.cache
	cu.runs.Add(1)

	self := c.selfID()
	members := c.guard.Table().Members()

	// Namespaces are per-node configuration, so this node can only ask for
	// the ones it has been told about. A namespace it has never opened is
	// one it has no replication factor for, and guessing would be worse
	// than waiting for the application to open it.
	c.mu.RLock()
	names := make([]string, len(c.order))
	copy(names, c.order)
	c.mu.RUnlock()

	// Everything lands in the replica store first, whether this node owns it
	// or merely replicates it, and reconciliation sorts it out afterwards.
	//
	// That is not laziness: the promoter already knows how to move a
	// replica into the owned store while preserving the previous owner's
	// version sequence, and having catch-up decide for itself would be a
	// second implementation of promotion that has to agree with the first
	// forever.
	defer c.promoter.reconcile()

	var failures error
	for _, name := range names {
		ns, ok := c.namespaceIfExists(name)
		if !ok || ns.err != nil || ns.opts.ReplicationFactor < 2 {
			continue
		}
		for _, peer := range members {
			if peer.ID == self {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := cu.fromPeer(ctx, ns, peer); err != nil {
				cu.failures.Add(1)
				c.opts.Logger.Warn("catch-up failed",
					"namespace", name, "peer", peer.ID, "error", err)
				// Collected rather than returned: one unreachable peer
				// is no reason to skip the others. Without this line
				// the variable is declared and never assigned, and
				// CatchUp returns nil after reaching nobody.
				failures = errors.Join(failures, fmt.Errorf(
					"peercache: catch-up of %q from %s: %w", name, peer.ID, err))
			}
		}
	}
	return failures
}

// fromPeer pages one namespace's replicas out of one owner.
func (cu *catchUp) fromPeer(ctx context.Context, ns *namespace, peer core.Node) error {
	if err := cu.pages(ctx, ns, peer, false); err != nil {
		return err
	}
	// And the copies the peer holds, as a replica, of what this node owns.
	// After a restart faster than failure detection nobody was promoted in
	// this node's place, so those copies are the newest there are; without
	// this pass a restarted owner recovered every key it replicates and none
	// of the keys it owns. Found by M16's kill -9 on kind.
	return cu.pages(ctx, ns, peer, true)
}

// pages pulls one kind of catch-up page from one peer until it is exhausted.
func (cu *catchUp) pages(ctx context.Context, ns *namespace, peer core.Node, replicas bool) error {
	c := cu.cache
	after := ""

	for {
		// Checked before every page, not once. A node over budget would
		// only shed what it pulled, so it stops and reports the catch-up
		// as incomplete rather than pulling and shedding in a loop.
		if c.pressure.over() {
			// run names the namespace and peer when it collects this.
			return fmt.Errorf("stopped after key %q: %w", after, ErrOverMemoryBudget)
		}
		var resp wire.CatchUpResponse
		err := c.route.invoke(ctx, peer, transport.MethodCatchUp, &wire.CatchUpRequest{
			Namespace:         ns.name,
			Requester:         c.selfID(),
			ReplicationFactor: ns.opts.ReplicationFactor,
			After:             after,
			Limit:             catchUpPageSize,
			Replicas:          replicas,
		}, &resp)
		if err != nil {
			return err
		}

		for _, e := range resp.Entries {
			// The writer, when the peer relayed a copy it holds; the
			// peer itself, when it sent what it owns.
			from := e.From
			if from == "" {
				from = peer.ID
			}
			// Through the replica store's ordinary Put, so the stamp
			// check applies: a page that arrives after a live write
			// must not undo it. Catch-up is not privileged just
			// because it is bulk.
			now := c.opts.Clock.Now().UnixNano()
			c.observeOrder(e.Order)
			accepted, conflict := c.replicas.Put(ns.name, e.Key, replication.Entry{
				Value: e.Value,
				Stamp: e.Stamp,
				// The page carries the time left; held as a deadline.
				ExpiresAt:  replication.DeadlineFrom(now, e.TTLNanos),
				ReceivedAt: now,
				Order:      e.Order,
				From:       from,
				Prev:       e.Prev,
			})
			if accepted {
				cu.pulled.Add(1)
			}
			if conflict {
				c.splitBrain.Add(1)
			}
			if c.traced(ns.name, e.Key) {
				c.recordCopy(ns.name, e.Key, keytraceCatchUp, accepted, conflict, e.Order, from, e.Stamp.Version, e.Value, "from "+peer.ID)
			}
			after = e.Key
		}

		// Paced after the page, by what it carried, so the rate holds over
		// the whole catch-up whatever the page size and however the keys
		// fall between peers. See WithCatchUpRate.
		if rate := c.opts.CatchUpRate; rate > 0 && len(resp.Entries) > 0 {
			wait := time.Duration(len(resp.Entries)) * time.Second / time.Duration(rate)
			if err := c.opts.Clock.Sleep(ctx, wait); err != nil {
				return err
			}
		}

		if !resp.More || len(resp.Entries) == 0 {
			return nil
		}
	}
}

// counters reports catch-up activity.
func (cu *catchUp) counters() (pulled, runs, failures int64) {
	return cu.pulled.Load(), cu.runs.Load(), cu.failures.Load()
}
