package ownership

import (
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/arwenizEr/peercache/internal/core"
)

// scoredNode pairs a member with the precomputed hash of its identity, so that
// a lookup hashes the key once and mixes rather than hashing per node.
type scoredNode struct {
	node core.Node
	seed uint64
}

// snapshot is one immutable view of the membership.
//
// Snapshots are never mutated after publication. A new membership produces a
// new snapshot and an atomic pointer swap, which is what lets [Table.Owner] run
// without a lock: it is on the path of every activation and of every remote
// call, and a read lock there would put an atomic read-modify-write on
// one shared cache line in front of the entire cluster's traffic.
type snapshot struct {
	nodes []scoredNode
	epoch core.Epoch
}

// Table answers ownership questions for the current membership.
//
// It is safe for concurrent use. The zero value is not usable; call [NewTable].
type Table struct {
	current atomic.Pointer[snapshot]
}

// NewTable returns an empty table. Until [Table.Update] is called, every lookup
// reports [core.ErrNotFound]: a node that has not yet learned the membership
// owns nothing, which is the safe answer rather than "everything".
func NewTable() *Table {
	t := &Table{}
	t.current.Store(&snapshot{})
	return t
}

// Update replaces the membership view.
//
// nodes is copied and sorted by ID. The sort is *not* what makes two nodes
// agree — taking the maximum score is order-independent, so [Table.Owner]
// returns the same answer whatever order the members arrive in, sorted or not.
// What the sort actually buys is a deterministic tie-break when two members
// score identically on a key, and a stable ranking for [Table.Replicas].
//
// Recorded because deliberately breaking the code caught this comment claiming
// more than the code does: removing the sort left every ownership test passing,
// as it should have.
func (t *Table) Update(nodes []core.Node, epoch core.Epoch) {
	scored := make([]scoredNode, 0, len(nodes))
	for _, n := range nodes {
		if n.IsZero() {
			continue
		}
		scored = append(scored, scoredNode{node: n.Clone(), seed: xxhash.Sum64String(n.ID)})
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].node.ID < scored[j].node.ID })

	t.current.Store(&snapshot{nodes: scored, epoch: epoch})
}

// Epoch returns the epoch of the current view.
func (t *Table) Epoch() core.Epoch { return t.current.Load().epoch }

// Members returns the current membership, sorted by ID.
func (t *Table) Members() []core.Node {
	snap := t.current.Load()
	out := make([]core.Node, 0, len(snap.nodes))
	for _, sn := range snap.nodes {
		out = append(out, sn.node.Clone())
	}
	return out
}

// Owner returns the node that owns groupKey and the epoch the answer was
// computed under.
//
// An empty membership returns [core.ErrNotFound]. Callers must treat the epoch
// as part of the answer: acting on an owner without recording the epoch it came
// from makes staleness undetectable.
func (t *Table) Owner(groupKey string) (core.Node, core.Epoch, error) {
	snap := t.current.Load()
	node, ok := owner(snap, groupKey)
	if !ok {
		return core.Node{}, snap.epoch, fmt.Errorf("ownership: no members: %w", core.ErrNotFound)
	}
	return node, snap.epoch, nil
}

// Replicas returns the rf highest-scoring nodes for groupKey, owner first.
//
// Rendezvous hashing gives this for free: the ordering by score is a total
// order over the members, so the replica set is just its prefix. Ownership
// only needs the head, but implementing it here keeps the scoring in one place
// rather than having replication invent a second answer.
func (t *Table) Replicas(groupKey string, rf int) ([]core.Node, core.Epoch, error) {
	if rf < 1 {
		return nil, 0, fmt.Errorf("ownership: replication factor %d must be positive: %w",
			rf, core.ErrInvalidOption)
	}
	snap := t.current.Load()
	if len(snap.nodes) == 0 {
		return nil, snap.epoch, fmt.Errorf("ownership: no members: %w", core.ErrNotFound)
	}

	keyHash := xxhash.Sum64String(groupKey)
	ranked := make([]scoredNode, len(snap.nodes))
	copy(ranked, snap.nodes)
	sort.Slice(ranked, func(i, j int) bool {
		si, sj := mix(keyHash, ranked[i].seed), mix(keyHash, ranked[j].seed)
		if si != sj {
			return si > sj
		}
		// Deterministic tie-break, so two nodes never disagree because
		// their sorts were unstable in different ways.
		return ranked[i].node.ID < ranked[j].node.ID
	})

	if rf > len(ranked) {
		rf = len(ranked)
	}
	out := make([]core.Node, 0, rf)
	for _, sn := range ranked[:rf] {
		out = append(out, sn.node.Clone())
	}
	return out, snap.epoch, nil
}

// owner returns the highest-scoring node in snap for groupKey.
//
// This is the hot path: one hash of the key, then a multiply-and-mix per
// member. It allocates nothing.
func owner(snap *snapshot, groupKey string) (core.Node, bool) {
	if len(snap.nodes) == 0 {
		return core.Node{}, false
	}

	keyHash := xxhash.Sum64String(groupKey)
	best := 0
	bestScore := mix(keyHash, snap.nodes[0].seed)
	for i := 1; i < len(snap.nodes); i++ {
		score := mix(keyHash, snap.nodes[i].seed)
		// Ties break towards the lower ID. Members are sorted by ID, so
		// the first one to reach a given score wins and later equals are
		// rejected — every node computes the same winner.
		if score > bestScore {
			best, bestScore = i, score
		}
	}
	return snap.nodes[best].node, true
}

// mix combines a key hash and a node hash into a score.
//
// The multiplication decorrelates the two inputs before the avalanche; a bare
// XOR would leave the score's high bits dominated by whichever input had them
// set, so a node whose identity hashed to a large value would win far more keys
// than its share. The finalizer is the standard 64-bit murmur3 mixer.
func mix(keyHash, nodeSeed uint64) uint64 {
	x := keyHash ^ (nodeSeed * 0x9E3779B97F4A7C15)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
