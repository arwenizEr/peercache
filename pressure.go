package peercache

import (
	"context"
	"math"
	"sync/atomic"
)

// PressureCheckInterval is how many writes pass between two inline checks of
// the memory budget.
//
// Checking on every write would mean summing every tier's counters on the
// write path, which is several hundred atomic loads at the default shard count.
// Checking every 64th bounds the overshoot between two checks to 64 entries,
// plus one write of any size, because a write of [pressureLargeWrite] bytes or
// more checks immediately. The periodic sweep also checks, once a second, which
// covers growth that is not a write at all — the L1 tier filling on reads.
const PressureCheckInterval = 64

// pressureLargeWrite is the value size at which a write checks the budget at
// once rather than waiting for its turn.
const pressureLargeWrite = 64 << 10

// pressure enforces the memory budget. See [WithMemoryBudget].
//
// # The order of the tiers
//
// Over budget, it sheds down to nine tenths of the budget, taking from the
// cheapest tier first:
//
//  1. L1, whole. Copies of keys owned elsewhere, bounded by a TTL of seconds;
//     losing them costs round trips and never data.
//  2. Replicas. Losing one lowers that key's replication factor until the owner
//     next writes it or this node next catches up; the owner still holds it.
//  3. Owned data. This loses the key, which state that is lossy by design
//     is allowed to do, and which a loader rebuilds.
//
// The low-water mark is what keeps it from shedding on every write once it has
// shed once: a cache at its budget would otherwise shed a little, grow a
// little, and shed again for as long as it was written to.
//
// # Why owned data is shed with a message
//
// A shed owned key in a replicated namespace sends its replicas a removal, the
// same tombstone a Delete sends. It frees their memory too, which matters in
// exactly the situation that causes pressure: a failover has just handed the
// survivors a dead node's share. It is not what keeps a shed key from being
// served at an older value later — the store's version floor does that, without
// depending on the message arriving. See store.Shed.
type pressure struct {
	c        *cache
	budget   int64
	lowWater int64

	writes    atomic.Uint64
	relieving atomic.Bool

	shedL1       atomic.Int64
	shedReplicas atomic.Int64
	shedOwned    atomic.Int64
	rounds       atomic.Int64
}

func newPressure(c *cache, budget int64) *pressure {
	return &pressure{c: c, budget: budget, lowWater: budget / 10 * 9}
}

// enabled reports whether a budget is configured.
func (p *pressure) enabled() bool { return p.budget > 0 }

// used returns the accounted size of every tier on this node.
func (p *pressure) used() int64 {
	c := p.c
	c.mu.RLock()
	namespaces := make([]*namespace, 0, len(c.order))
	for _, name := range c.order {
		namespaces = append(namespaces, c.namespaces[name])
	}
	c.mu.RUnlock()

	total := c.replicas.Bytes()
	for _, ns := range namespaces {
		if ns.store != nil {
			total += ns.store.Bytes()
		}
		if ns.l1 != nil {
			total += ns.l1.Bytes()
		}
	}
	return total
}

// over reports whether this node is above its budget. Catch-up asks before
// every page.
func (p *pressure) over() bool { return p.enabled() && p.used() > p.budget }

// noteWrite is called after a write of size value bytes lands in a tier.
func (p *pressure) noteWrite(size int) {
	if !p.enabled() {
		return
	}
	if size < pressureLargeWrite && p.writes.Add(1)%PressureCheckInterval != 0 {
		return
	}
	p.check()
}

// check sheds if this node is over its budget. Only one caller sheds at a time;
// the others carry on, because the one already shedding is doing what they
// would.
func (p *pressure) check() {
	if !p.enabled() || p.used() <= p.budget {
		return
	}
	if !p.relieving.CompareAndSwap(false, true) {
		return
	}
	defer p.relieving.Store(false)
	p.relieve()
}

// relieve sheds tiers in order until usage is at the low-water mark.
func (p *pressure) relieve() {
	c := p.c
	p.rounds.Add(1)
	used := p.used()
	excess := used - p.lowWater
	if excess <= 0 {
		return
	}

	c.mu.RLock()
	namespaces := make([]*namespace, 0, len(c.order))
	for _, name := range c.order {
		namespaces = append(namespaces, c.namespaces[name])
	}
	c.mu.RUnlock()

	// 1. L1.
	for _, ns := range namespaces {
		if excess <= 0 {
			break
		}
		if ns.l1 == nil {
			continue
		}
		entries := ns.l1.Len()
		excess -= ns.l1.Shed()
		p.shedL1.Add(int64(entries))
	}

	// 2. Replicas.
	if excess > 0 {
		before := c.replicas.ShedCount()
		excess -= c.replicas.Shed(excess)
		p.shedReplicas.Add(c.replicas.ShedCount() - before)
	}

	// 3. Owned data, a share from each namespace in turn so that one
	// namespace is not emptied while another is untouched.
	for excess > 0 {
		progress := false
		for _, ns := range namespaces {
			if excess <= 0 {
				break
			}
			if ns.store == nil {
				continue
			}
			share := excess / int64(len(namespaces))
			if share < 1 {
				share = excess
			}
			freed := ns.store.Shed(share, func(key string, _ uint64) {
				p.shedOwned.Add(1)
				ns.replicateShed(key)
			})
			if freed > 0 {
				progress = true
				excess -= freed
			}
		}
		if !progress {
			break
		}
	}

	if after := p.used(); after > p.budget {
		// Everything that could be shed was, and it is still over: the
		// remainder is in tiers this does not account for, or in writes
		// that landed during the round. Said once per round rather than
		// silently, because it means the budget is smaller than the
		// working set can ever fit in.
		c.opts.Logger.Warn("memory budget still exceeded after shedding",
			"budget", p.budget, "used", after)
	}
}

// replicateShed tells a shed key's replicas to drop their copies.
//
// Asynchronous whatever the namespace's replication mode, because the caller
// is relieving memory pressure and must not wait on the network to do it. A
// dropped message costs the replica's memory, not correctness: the version
// floor in the owned store already guarantees a rewrite outranks the copy.
func (n *namespace) replicateShed(key string) {
	if n.opts.ReplicationFactor < 2 {
		return
	}
	targets, _, err := n.cache.replicaTargets(n.groupKeyFor(key), n.opts.ReplicationFactor)
	if err != nil || len(targets) == 0 {
		return
	}
	_ = n.cache.replicator.replicate(context.Background(), false, replicaWork{
		namespace: n.name,
		key:       key,
		stamp:     n.stamp(math.MaxUint64),
		deleted:   true,
		targets:   targets,
	})
}

// counters reports the budget's activity for Stats.
func (p *pressure) counters() (used, budget, l1, replicas, owned int64) {
	return p.used(), p.budget, p.shedL1.Load(), p.shedReplicas.Load(), p.shedOwned.Load()
}
