package peercache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/replication"
)

// promoter reacts to membership changes by reconciling what this node holds
// against what it should hold.
//
// # Promotion is not an election
//
// Nothing here decides who owns what. Rendezvous hashing already did: when a
// node disappears, the node that ranked second for each of its keys ranks first
// on every surviving node at the same instant, computed from the same
// membership with no messages exchanged. That is the whole promotion protocol
// and ownership already implements it.
//
// What was missing is that the new owner has to *hold the data*. This is that:
// on a membership change, keys this node held as a replica and now owns move
// into its owned store, and keys it no longer ranks for are dropped. A promoted
// key keeps the previous owner's version so that cached copies elsewhere in the
// cluster are correctly judged stale rather than fresh.
//
// # Why this is not on the read path
//
// The alternative is to check the replica store on every miss in the owned
// store, which is tempting because it needs no membership watcher at all. It
// was rejected: it puts a second map lookup on every miss for the benefit of
// the rare moments after a failure, and it leaves the node permanently unable
// to answer "how many keys do I own", because the answer would depend on which
// store a reader happened to consult.
type promoter struct {
	cache *cache

	stop    chan struct{}
	stopped chan struct{}
	once    sync.Once

	// promoted and evicted count keys moved into the owned store and
	// dropped from the replica store. Both spike on a membership change and
	// are flat otherwise; a promoter that is busy under stable membership is
	// a promoter reacting to something that is not settling.
	promoted atomic.Int64
	evicted  atomic.Int64
	// rounds counts reconciliations, so that the counters above can be read
	// as a rate rather than a total.
	rounds atomic.Int64
}

func newPromoter(c *cache) *promoter {
	return &promoter{
		cache:   c,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

// start hooks reconciliation onto the guard's view updates.
//
// Deliberately *not* a second watcher on the membership authority. Two
// independent watchers have no ordering between them, so a promoter that
// subscribed separately could reconcile before the ownership table held the new
// view — it would see the dead node still owning its keys, promote nothing, and
// then wait forever for an event already delivered. That is not a theoretical
// race: it is what the promotion acceptance test caught.
//
// Hanging off the guard means reconciliation runs after the table is updated,
// by construction.
func (p *promoter) start(guard *ownership.Guard) {
	close(p.stopped) // no goroutine of its own to wait for
	if guard == nil {
		return
	}
	guard.OnUpdate(func() {
		select {
		case <-p.stop:
			return
		default:
		}
		p.cache.restoreGen.Add(1)
		p.reconcile()
	})
	// Reconcile once now, for a node that starts holding replicas — which
	// happens when catch-up runs before any membership change.
	p.reconcile()
}

// reconcile moves keys between the replica store and the owned store to match
// the current membership.
func (p *promoter) reconcile() {
	c := p.cache
	if c.guard == nil || c.isClosed() {
		return
	}
	p.rounds.Add(1)

	self := c.self().ID
	for _, name := range c.replicas.Namespaces() {
		ns, ok := c.namespaceIfExists(name)
		if !ok {
			// A namespace this node holds replicas for but has never
			// been asked about locally. Skip it — do *not* create it.
			//
			// Creating it here was the first version and it was a
			// trap. Namespace options are fixed at creation and
			// silently ignored afterwards, so a
			// namespace conjured with defaults gets replication
			// factor 1 — and the application's later
			// Namespace(name, WithReplicationFactor(3)) returns that
			// same crippled handle. The node then evicts replicas it
			// should hold and never promotes the ones handed to it.
			//
			// In examples/rolling-upgrade that lost 53 of 300 keys in a *graceful*
			// drain, and it took the example to find it: every test
			// created its namespaces before anything could race them.
			//
			// Skipping costs nothing. The replicas stay in the
			// replica store, and the next reconciliation after the
			// application opens the namespace picks them up with the
			// options it actually asked for.
			continue
		}

		if ns.err != nil {
			// Misconfigured here: it has no store to promote into. Its
			// replicas stay where they are, and the application's handle
			// keeps reporting the configuration error.
			continue
		}

		rf := ns.opts.ReplicationFactor

		// Promote first, then evict. The other order would drop a key
		// this node is about to own, in the window between the two
		// passes, and a reader arriving in that window would be told
		// the key does not exist.
		taken := c.replicas.Drain(name, func(key string) bool {
			owner, _, err := c.guard.Table().Owner(ns.groupKeyFor(key))
			return err == nil && owner.ID == self
		})
		p.restore(ns, taken)

		// Hand off rather than evict.
		//
		// Dropping a replica because *this* node stopped ranking for it says
		// nothing about whether a node that now ranks for it holds a copy,
		// and after a partition heals it usually does not: the copies were
		// placed at ranking positions computed without the absent node, so
		// its return moves every one of those keys to a node holding
		// nothing. See handoff.go.
		//
		// Draining with the inverted predicate removes the orphans under the
		// same lock that Evict used, so a concurrent Put cannot strand a copy
		// behind the pass. Every failure path in the hand-off puts the entry
		// back, so the local copy survives anything except a successful
		// placement elsewhere.
		if rf < 2 {
			// A namespace that replicates nothing has no replica set for a
			// copy to belong to, so there is nowhere to hand one. Anything
			// here arrived under a configuration that no longer applies and
			// is dropped.
			//
			// Handing off instead would not merely be pointless, it would
			// not terminate: replicaTargets returns nothing below RF 2, so
			// every hand-off would fail, restore the copy, and orphan it
			// again on the next reconciliation.
			//
			// Except a copy whose sender this node still ranks as the key's
			// owner. That is a hand-off in flight: a draining node announces
			// its departure and then pushes its keys, and a receiver whose
			// view has not yet caught up files them here rather than in its
			// owned store. Dropping them at that moment loses the only copy
			// of an RF=1 key in a graceful shutdown. Kept, they are promoted
			// by the reconciliation that follows the receiver's view
			// catching up, or dropped by it if another node now owns them.
			// TestDrainSavesDataWithoutReplication lost one key in about one
			// run in a hundred to this before it was kept.
			//
			// And never a copy of a key this node owns. Promotion above has
			// already run, so such a copy arrived after it — between the two
			// passes of this reconciliation — and is promoted by the
			// replicate handler's own check or by the next reconciliation.
			// Logging every eviction found exactly that: two keys in 1,500
			// runs, each owned by the node that dropped it.
			table := c.guard.Table()
			p.evicted.Add(int64(c.replicas.EvictWhere(name, func(key string, e replication.Entry) bool {
				owner, _, err := table.Owner(ns.groupKeyFor(key))
				if err != nil {
					return true
				}
				return owner.ID == self || (e.From != "" && owner.ID == e.From)
			})))
			continue
		}

		orphans := c.replicas.Drain(name, func(key string) bool {
			return !c.stillReplicaFor(ns.groupKeyFor(key), rf, self)
		})
		for key, e := range orphans {
			p.evicted.Add(1)
			c.handoff.enqueue(handoffWork{namespace: name, key: key, entry: e})
		}
	}
}

// restore moves promoted replicas into the namespace's owned store, continuing
// the previous owner's version sequence.
func (p *promoter) restore(ns *namespace, taken map[string]replication.Entry) {
	now := p.cache.opts.Clock.Now().UnixNano()
	for key, e := range taken {
		left, live := e.Remaining(now)
		if !live {
			// Expired on the owner's schedule while held here. Restoring it
			// would bring back a key every other holder has already let go of.
			continue
		}
		if ns.store.Restore(key, e.Value, time.Duration(left), e.Stamp.Version, e.Order) {
			p.promoted.Add(1)
			// The old owner may have died before flushing this write. The
			// record is idempotent under the merge, so writing it again
			// costs a duplicate at worst and closes that gap at best.
			if ns.persister != nil {
				ns.persister.record(key, e.Value, e.ExpiresAt, e.Order, false)
			}
		}
		// A refused Restore means a newer value was written here while the
		// drain ran. The replica copy is correctly discarded; it is older by
		// definition.
	}
}

// promoteKey promotes one replica this node has just found it owns, without
// waiting for the next reconciliation. See the replicate handler.
func (p *promoter) promoteKey(ns *namespace, key string) {
	if ns.store == nil {
		return
	}
	p.restore(ns, p.cache.replicas.Drain(ns.name, func(k string) bool { return k == key }))
}

// stillReplicaFor reports whether this node ranks as a replica for a group key
// under the current membership.
//
// It runs for every replica this node holds, on every reconciliation — once a
// second — so it ranks with Holders, which does not allocate, rather than
// Replicas, which copies, sorts and clones the membership per call. On M16's
// kind cluster, with 160,000 replicas a node, the allocating version was half
// of an idle node's CPU and most of its garbage collection.
func (c *cache) stillReplicaFor(groupKey string, rf int, self string) bool {
	if rf < 2 {
		return false
	}
	ranked, n, _ := c.guard.Table().Holders(groupKey, rf)
	if n == 0 {
		// Membership is unknown. Keeping the copy is the safe direction:
		// dropping data because the view is momentarily empty would turn
		// a gossip hiccup into a loss of redundancy.
		return true
	}
	for _, node := range ranked[:n] {
		if node.ID == self {
			return true
		}
	}
	return false
}

// namespaceIfExists returns a namespace without creating one.
func (c *cache) namespaceIfExists(name string) (*namespace, bool) {
	c.mu.RLock()
	ns, ok := c.namespaces[name]
	c.mu.RUnlock()
	return ns, ok
}

// Close stops the watcher and waits for it.
func (p *promoter) Close() {
	p.once.Do(func() { close(p.stop) })
	<-p.stopped
}

// counters reports promotion activity.
func (p *promoter) counters() (promoted, evicted, rounds int64) {
	return p.promoted.Load(), p.evicted.Load(), p.rounds.Load()
}
