package rebalance

import (
	"fmt"
	"sort"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
)

// Move is one key's change of home.
type Move struct {
	// Key is the group key whose placement changed.
	Key string
	// From is where it lived under the old view; empty if it did not exist
	// there, which happens when the replication factor grows or a node
	// joins and takes a slot nobody previously filled.
	From string
	// To is where it lives under the new view.
	To string
	// Role says whether this key is moving as an owner or as a replica.
	Role Role
}

// Role distinguishes the two kinds of placement.
type Role int

const (
	// Owner means the node serves reads and writes for this key.
	RoleOwner Role = iota
	// Replica means the node holds a copy against the owner's failure.
	RoleReplica
)

// String implements [fmt.Stringer].
func (r Role) String() string {
	if r == RoleOwner {
		return "owner"
	}
	return "replica"
}

// Plan is the set of moves one membership change requires.
type Plan struct {
	// Moves is what has to happen, sorted by key then role for
	// determinism. Two nodes computing the same plan produce identical
	// slices, which is what makes a plan comparable in a test and
	// reproducible in an incident.
	Moves []Move
	// Unchanged counts keys whose placement did not change at all. It is
	// reported rather than inferred because it is the number that matters:
	// a rebalance is judged by how much it *leaves alone*.
	Unchanged int
}

// Total returns the number of moves.
func (p Plan) Total() int { return len(p.Moves) }

// ForNode returns the moves that involve a particular node, in either
// direction.
func (p Plan) ForNode(id string) []Move {
	var out []Move
	for _, m := range p.Moves {
		if m.From == id || m.To == id {
			out = append(out, m)
		}
	}
	return out
}

// Compute returns the moves needed to go from one membership view to another,
// for a given set of keys.
//
// # Why this takes keys rather than deriving them
//
// Rendezvous hashing has no notion of a partition or a shard: placement is a
// function of the key, so there is nothing enumerable between "one key" and
// "the whole keyspace". A planner that wanted to work without a key list would
// have to invent virtual buckets, which would mean a second placement scheme
// that has to agree with the first forever.
//
// So the caller supplies the keys it actually holds. That is exactly the set
// that can move, and it is already in hand — a node planning a rebalance is
// reading its own store.
//
// # What "only the affected partitions" means here
//
// SPEC.md §4 requires that planning "compute only the affected partitions, never
// a full reshuffle". With rendezvous hashing that is a property of the hash
// rather than of this function: adding or removing one node of N changes the
// placement of roughly 1/N of keys and leaves the rest exactly where they are.
// What this function does is *report* that, so a test can assert the bound
// instead of trusting it.
func Compute(before, after *ownership.Table, keys []string, rf int) (Plan, error) {
	if rf < 1 {
		return Plan{}, fmt.Errorf("rebalance: replication factor %d must be positive: %w",
			rf, core.ErrInvalidOption)
	}
	if before == nil || after == nil {
		return Plan{}, fmt.Errorf("rebalance: both views are required: %w", core.ErrInvalidOption)
	}

	var plan Plan
	for _, key := range keys {
		oldSet, _, oldErr := before.Replicas(key, rf)
		newSet, _, newErr := after.Replicas(key, rf)

		// An empty view is not a plan to move everything. A node that has
		// lost membership entirely should wait for it to come back, not
		// evacuate itself to nowhere.
		if newErr != nil {
			continue
		}
		if oldErr != nil {
			// Nothing was placed before — a cluster that has just
			// formed. Everything is new rather than moved.
			for i, n := range newSet {
				plan.Moves = append(plan.Moves, Move{
					Key: key, To: n.ID, Role: roleAt(i),
				})
			}
			continue
		}

		// Compare *sets*, not slots.
		//
		// This is the correction that matters. Comparing slot by slot
		// counts a cascade: remove the owner of a key and every
		// remaining node shifts up one position, so all rf slots look
		// different and the plan reports rf moves. The first version did
		// exactly that and produced twice the expected total.
		//
		// But a node that was replica #1 and is now the owner already
		// holds the data. It is promoted, not moved, and promotion in
		// this system costs a map drain rather than a network transfer.
		// The only genuine move is a node that is in the new replica set
		// and was not in the old one, because that is the only node that
		// has to be sent anything.
		was := make(map[string]int, len(oldSet))
		for i, n := range oldSet {
			was[n.ID] = i
		}
		moved := false
		for i, n := range newSet {
			if _, held := was[n.ID]; held {
				// Already had it. A role change here is a
				// promotion or a demotion, neither of which
				// transfers bytes.
				continue
			}
			moved = true
			plan.Moves = append(plan.Moves, Move{
				Key: key, From: departedFrom(oldSet, was, newSet), To: n.ID, Role: roleAt(i),
			})
		}
		if !moved {
			plan.Unchanged++
		}
	}

	sort.Slice(plan.Moves, func(i, j int) bool {
		if plan.Moves[i].Key != plan.Moves[j].Key {
			return plan.Moves[i].Key < plan.Moves[j].Key
		}
		return plan.Moves[i].Role < plan.Moves[j].Role
	})
	return plan, nil
}

// departedFrom names a node that held the key before and does not now, for the
// Move's From field.
//
// It is a diagnostic rather than an instruction: the transfer is a push from
// whoever still has the data, and after a failure the departed node is by
// definition unable to send anything. Naming it makes a plan readable — "this
// key moved because n4 left" — without implying n4 does the work.
func departedFrom(oldSet []core.Node, was map[string]int, newSet []core.Node) string {
	still := make(map[string]bool, len(newSet))
	for _, n := range newSet {
		still[n.ID] = true
	}
	for _, n := range oldSet {
		if !still[n.ID] {
			return n.ID
		}
	}
	return ""
}

// roleAt maps a position in the replica ranking to a role. The head owns; the
// rest replicate.
func roleAt(i int) Role {
	if i == 0 {
		return RoleOwner
	}
	return RoleReplica
}

// ExpectedMoveFraction returns the fraction of keys that should move when a
// cluster goes from oldSize to newSize members, at a given replication factor.
//
// It exists so a test can assert against a derived number rather than a guessed
// one. "Within 10%" of an unstated figure means nothing; this is the figure.
//
// For a single node joining or leaving, the keys that need a transfer are those
// where the changed node was — or becomes — a member of the replica set: rf of
// max(oldSize, newSize) nodes, so rf/larger of the keyspace.
//
// Note that this counts keys needing a *transfer*, not slots that changed. A
// node promoted from replica to owner already holds the data; only a node newly
// entering a replica set has to be sent anything. Conflating the two doubles
// the figure, which is a mistake worth naming because the first version of the
// planner made it.
func ExpectedMoveFraction(oldSize, newSize, rf int) float64 {
	if oldSize <= 0 || newSize <= 0 || rf < 1 {
		return 0
	}
	larger := oldSize
	if newSize > larger {
		larger = newSize
	}
	if larger == 0 {
		return 0
	}
	// Each of the rf slots has a 1/larger chance of being the node that
	// appeared or disappeared. Capped at 1: with rf >= cluster size every
	// node holds every key and a membership change touches all of them.
	fraction := float64(rf) / float64(larger)
	if fraction > 1 {
		return 1
	}
	return fraction
}
