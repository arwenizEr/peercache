package rebalance_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/rebalance"
)

// tableOf builds an ownership view over nodes named n0..n(size-1).
func tableOf(size int) *ownership.Table {
	t := ownership.NewTable()
	nodes := make([]core.Node, 0, size)
	for i := 0; i < size; i++ {
		nodes = append(nodes, core.Node{ID: fmt.Sprintf("n%d", i)})
	}
	t.Update(nodes, core.Epoch(size))
	return t
}

// tableWithout builds a view over the same names with one removed.
func tableWithout(size int, drop string) *ownership.Table {
	t := ownership.NewTable()
	var nodes []core.Node
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("n%d", i)
		if id == drop {
			continue
		}
		nodes = append(nodes, core.Node{ID: id})
	}
	t.Update(nodes, core.Epoch(size+1))
	return t
}

func keysN(n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, fmt.Sprintf("key-%d", i))
	}
	return out
}

// TestRemovingANodeMovesOnlyItsShare is the docs/design.md §4 requirement that
// planning "compute only the affected partitions, never a full reshuffle".
//
// The bound is derived rather than guessed. Removing one node of nine should
// move roughly rf/9 of the placements — the slots that node occupied — and
// leave every other placement exactly where it was.
func TestRemovingANodeMovesOnlyItsShare(t *testing.T) {
	const size, rf, keys = 9, 3, 20_000

	before := tableOf(size)
	after := tableWithout(size, "n4")

	plan, err := rebalance.Compute(before, after, keysN(keys), rf)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}

	expected := rebalance.ExpectedMoveFraction(size, size-1, rf) * float64(keys)
	got := float64(plan.Total())
	deviation := math.Abs(got-expected) / expected

	t.Logf("removing 1 of %d at rf=%d over %d keys: %d moves (expected ~%.0f, %.1f%% off), "+
		"%d keys untouched", size, rf, keys, plan.Total(), expected, deviation*100, plan.Unchanged)

	if deviation > 0.10 {
		t.Fatalf("moved %d placements, expected ~%.0f (%.1f%% off, budget 10%%)",
			plan.Total(), expected, deviation*100)
	}

	// The far more important assertion: everything else stayed put. A
	// planner that reshuffled could still hit the move count above by
	// moving different keys, so the count alone is not evidence.
	//
	// The threshold is derived, not guessed. A key is untouched when the
	// departed node was not in its replica set, which is 1 - rf/size of
	// them. An earlier version of this test asserted a round 70% and failed
	// against correct behaviour — a reminder that a made-up bound catches
	// made-up bugs.
	wantUntouched := float64(keys) * (1 - float64(rf)/float64(size))
	untouchedDeviation := math.Abs(float64(plan.Unchanged)-wantUntouched) / wantUntouched
	if untouchedDeviation > 0.05 {
		t.Fatalf("%d of %d keys were untouched, expected ~%.0f (%.1f%% off); removing one "+
			"node of %d must leave everything it did not hold exactly where it is",
			plan.Unchanged, keys, wantUntouched, untouchedDeviation*100, size)
	}
}

// TestAddingANodeMovesOnlyOntoIt is the same bound in the other direction, and
// it also pins the property that makes rendezvous hashing worth using: a new
// node takes keys *from* others and never causes two existing nodes to swap.
func TestAddingANodeMovesOnlyOntoIt(t *testing.T) {
	const size, rf, keys = 8, 3, 20_000

	before := tableOf(size)
	after := tableOf(size + 1) // n8 joins

	plan, err := rebalance.Compute(before, after, keysN(keys), rf)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}

	expected := rebalance.ExpectedMoveFraction(size, size+1, rf) * float64(keys)
	got := float64(plan.Total())
	deviation := math.Abs(got-expected) / expected

	t.Logf("adding 1 to %d at rf=%d over %d keys: %d moves (expected ~%.0f, %.1f%% off)",
		size, rf, keys, plan.Total(), expected, deviation*100)

	if deviation > 0.10 {
		t.Fatalf("moved %d placements, expected ~%.0f (%.1f%% off, budget 10%%)",
			plan.Total(), expected, deviation*100)
	}

	// Every move must land on the new node. If two existing nodes traded a
	// key between them, the placement function is not stable and a rolling
	// upgrade would move far more than it needs to.
	for _, m := range plan.Moves {
		if m.To != "n8" {
			t.Fatalf("key %q moved from %q to %q as %s; adding a node must only move keys "+
				"*onto* it, never between existing nodes", m.Key, m.From, m.To, m.Role)
		}
	}
}

// TestNoChangeMeansNoMoves is the degenerate case, and it is worth a test
// because a planner that always found work would make every gossip flap a
// cluster-wide transfer.
func TestNoChangeMeansNoMoves(t *testing.T) {
	const keys = 5_000
	view := tableOf(6)

	plan, err := rebalance.Compute(view, tableOf(6), keysN(keys), 3)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if plan.Total() != 0 {
		t.Fatalf("an unchanged membership produced %d moves; a gossip flap that changes "+
			"nothing would transfer the keyspace", plan.Total())
	}
	if plan.Unchanged != keys {
		t.Fatalf("Unchanged = %d, want %d", plan.Unchanged, keys)
	}
	_ = view
}

// TestAnEmptyNewViewPlansNothing covers the failure mode that would be worst:
// a node that has lost membership entirely must wait, not evacuate itself to
// nowhere.
func TestAnEmptyNewViewPlansNothing(t *testing.T) {
	plan, err := rebalance.Compute(tableOf(5), ownership.NewTable(), keysN(100), 3)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	if plan.Total() != 0 {
		t.Fatalf("planning against an empty view produced %d moves; a node that has lost "+
			"its view would evacuate to nowhere", plan.Total())
	}
}

// TestPlansAreDeterministic is what lets two nodes reason about the same
// rebalance, and what lets a test compare one.
func TestPlansAreDeterministic(t *testing.T) {
	before, after := tableOf(7), tableWithout(7, "n3")
	keys := keysN(2_000)

	first, err := rebalance.Compute(before, after, keys, 3)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}
	for i := 0; i < 5; i++ {
		again, err := rebalance.Compute(before, after, keys, 3)
		if err != nil {
			t.Fatalf("Compute: %v", err)
		}
		if len(again.Moves) != len(first.Moves) {
			t.Fatalf("run %d produced %d moves, first produced %d",
				i, len(again.Moves), len(first.Moves))
		}
		for j := range first.Moves {
			if again.Moves[j] != first.Moves[j] {
				t.Fatalf("run %d move %d = %+v, first = %+v",
					i, j, again.Moves[j], first.Moves[j])
			}
		}
	}
}

// TestForNodeSelectsBothDirections checks the accessor a draining node uses to
// find its own work.
func TestForNodeSelectsBothDirections(t *testing.T) {
	plan, err := rebalance.Compute(tableOf(5), tableWithout(5, "n2"), keysN(2_000), 2)
	if err != nil {
		t.Fatalf("Compute: %v", err)
	}

	leaving := plan.ForNode("n2")
	if len(leaving) == 0 {
		t.Fatal("the departing node has no moves; it held keys and they must go somewhere")
	}
	for _, m := range leaving {
		if m.From != "n2" && m.To != "n2" {
			t.Fatalf("ForNode(n2) returned %+v, which does not involve n2", m)
		}
	}
	// It is leaving, so nothing should be moving *onto* it.
	for _, m := range leaving {
		if m.To == "n2" {
			t.Fatalf("a departing node is being sent %q; it is not in the new view", m.Key)
		}
	}
}

func TestComputeValidates(t *testing.T) {
	if _, err := rebalance.Compute(tableOf(3), tableOf(3), nil, 0); err == nil {
		t.Fatal("Compute with rf=0 succeeded")
	}
	if _, err := rebalance.Compute(nil, tableOf(3), nil, 1); err == nil {
		t.Fatal("Compute with a nil view succeeded")
	}
}

// TestExpectedMoveFractionIsSane guards the derived bound the tests above lean
// on. A wrong expectation would make those tests assert something plausible and
// false.
func TestExpectedMoveFractionIsSane(t *testing.T) {
	cases := []struct {
		old, new, rf int
		want         float64
	}{
		{9, 8, 1, 1.0 / 9},
		{9, 8, 3, 3.0 / 9},
		{8, 9, 3, 3.0 / 9},
		{3, 2, 3, 1}, // rf >= cluster size: every node holds everything
		{0, 5, 3, 0},
	}
	for _, c := range cases {
		got := rebalance.ExpectedMoveFraction(c.old, c.new, c.rf)
		if math.Abs(got-c.want) > 1e-9 {
			t.Fatalf("ExpectedMoveFraction(%d, %d, %d) = %v, want %v",
				c.old, c.new, c.rf, got, c.want)
		}
	}
}
