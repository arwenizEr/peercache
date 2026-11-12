package ownership_test

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
)

// Ownership acceptance parameters, from SPEC.md section 4.
const (
	acceptanceNodes = 7
	acceptanceKeys  = 100_000
	// uniformityTolerance is the "within 5% of uniform" bound.
	uniformityTolerance = 0.05
)

func nodes(n int) []core.Node {
	out := make([]core.Node, n)
	for i := range out {
		out[i] = core.Node{ID: fmt.Sprintf("n%d", i), Addr: fmt.Sprintf("10.0.0.%d:7946", i+1)}
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tenant:%d/user:%d", i%997, i)
	}
	return out
}

func newTable(t *testing.T, members []core.Node, epoch core.Epoch) *ownership.Table {
	t.Helper()
	tbl := ownership.NewTable()
	tbl.Update(members, epoch)
	return tbl
}

// TestOwnershipIsUniform is half of the ownership acceptance criterion: in a seven-node
// cluster, ownership of 100k group keys is within 5% of uniform.
//
// This is a property of the hash alone, so it needs no cluster and no clock —
// which is the point: a distribution bug should fail in milliseconds, not be
// inferred from a load graph.
func TestOwnershipIsUniform(t *testing.T) {
	tbl := newTable(t, nodes(acceptanceNodes), 1)
	ks := keys(acceptanceKeys)

	counts := map[string]int{}
	for _, k := range ks {
		node, epoch, err := tbl.Owner(k)
		if err != nil {
			t.Fatalf("Owner(%q): %v", k, err)
		}
		if epoch != 1 {
			t.Fatalf("Owner returned epoch %d, want 1", epoch)
		}
		counts[node.ID]++
	}

	if len(counts) != acceptanceNodes {
		t.Fatalf("only %d of %d nodes own anything", len(counts), acceptanceNodes)
	}

	ideal := float64(acceptanceKeys) / acceptanceNodes
	worst := 0.0
	for _, n := range nodes(acceptanceNodes) {
		got := float64(counts[n.ID])
		deviation := math.Abs(got-ideal) / ideal
		if deviation > worst {
			worst = deviation
		}
		t.Logf("%s owns %6d keys (%+.2f%% from uniform)", n.ID, counts[n.ID], (got/ideal-1)*100)
	}
	if worst > uniformityTolerance {
		t.Fatalf("worst deviation %.2f%% exceeds the %.0f%% tolerance", worst*100, uniformityTolerance*100)
	}
}

// TestRemovingANodeMovesOnlyItsKeys is the other half, and the more important
// one: removing one node of seven moves approximately 1/7 of the keys **and no
// more**.
//
// Uniformity is easy; stability is what a hashing scheme is actually for. A
// function that redistributed unrelated keys would still look uniform and would
// turn one pod restart into a cluster-wide cache miss.
func TestRemovingANodeMovesOnlyItsKeys(t *testing.T) {
	all := nodes(acceptanceNodes)
	ks := keys(acceptanceKeys)

	before := newTable(t, all, 1)
	ownedBefore := make(map[string]string, len(ks))
	for _, k := range ks {
		node, _, err := before.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		ownedBefore[k] = node.ID
	}

	// Remove the last node.
	removed := all[len(all)-1].ID
	after := newTable(t, all[:len(all)-1], 2)

	moved, movedFromRemoved, movedFromOthers := 0, 0, 0
	for _, k := range ks {
		node, _, err := after.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if node.ID == ownedBefore[k] {
			continue
		}
		moved++
		if ownedBefore[k] == removed {
			movedFromRemoved++
		} else {
			movedFromOthers++
		}
	}

	// Not one key that belonged to a surviving node may move. This is the
	// assertion that a ring with badly chosen virtual nodes, or a modulo
	// hash, would fail outright.
	if movedFromOthers != 0 {
		t.Fatalf("%d keys moved between surviving nodes; only the departed node's keys may move",
			movedFromOthers)
	}

	expected := float64(acceptanceKeys) / acceptanceNodes
	deviation := math.Abs(float64(moved)-expected) / expected
	t.Logf("removing %s moved %d of %d keys (%.2f%% of the keyspace, ideal %.2f%%)",
		removed, moved, acceptanceKeys, float64(moved)/acceptanceKeys*100, 100.0/acceptanceNodes)
	if deviation > uniformityTolerance {
		t.Fatalf("moved %d keys, %.2f%% away from the expected %.0f", moved, deviation*100, expected)
	}
	if movedFromRemoved != moved {
		t.Fatalf("accounting mismatch: %d moved, %d from the removed node", moved, movedFromRemoved)
	}
}

// TestAddingANodeOnlyTakesKeys is the mirror image: a new node acquires its
// share and existing nodes never trade keys with each other.
func TestAddingANodeOnlyTakesKeys(t *testing.T) {
	all := nodes(acceptanceNodes + 1)
	ks := keys(acceptanceKeys)

	before := newTable(t, all[:acceptanceNodes], 1)
	after := newTable(t, all, 2)

	added := all[acceptanceNodes].ID
	movedToAdded, movedElsewhere := 0, 0
	for _, k := range ks {
		wasNode, _, err := before.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		isNode, _, err := after.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if wasNode.ID == isNode.ID {
			continue
		}
		if isNode.ID == added {
			movedToAdded++
		} else {
			movedElsewhere++
		}
	}

	if movedElsewhere != 0 {
		t.Fatalf("%d keys moved between existing nodes when one was added", movedElsewhere)
	}
	expected := float64(acceptanceKeys) / float64(acceptanceNodes+1)
	if deviation := math.Abs(float64(movedToAdded)-expected) / expected; deviation > uniformityTolerance {
		t.Fatalf("the new node took %d keys, %.2f%% away from the expected %.0f",
			movedToAdded, deviation*100, expected)
	}
	t.Logf("adding %s moved %d keys, all of them to the new node", added, movedToAdded)
}

// TestOwnerIsDeterministicAcrossMemberOrder is the property that makes the
// cluster agree at all. Two nodes handed the same members in different orders
// must resolve every key identically; if iteration order leaked into the
// answer, two pods would disagree and nothing above would notice.
func TestOwnerIsDeterministicAcrossMemberOrder(t *testing.T) {
	forward := nodes(acceptanceNodes)
	reversed := make([]core.Node, len(forward))
	for i, n := range forward {
		reversed[len(forward)-1-i] = n
	}

	a := newTable(t, forward, 1)
	b := newTable(t, reversed, 1)

	for _, k := range keys(10_000) {
		ownerA, _, err := a.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		ownerB, _, err := b.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if ownerA.ID != ownerB.ID {
			t.Fatalf("key %q resolves to %s in one order and %s in another", k, ownerA.ID, ownerB.ID)
		}
	}
}

func TestOwnerIsStableAcrossRepeatedCalls(t *testing.T) {
	tbl := newTable(t, nodes(acceptanceNodes), 1)
	first, _, err := tbl.Owner("tenant:acme/cart")
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	for i := 0; i < 1000; i++ {
		got, _, err := tbl.Owner("tenant:acme/cart")
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if got.ID != first.ID {
			t.Fatalf("Owner is not stable: %s then %s", first.ID, got.ID)
		}
	}
}

// TestEmptyMembershipOwnsNothing is the safe answer. A node that has not yet
// learned the membership must not conclude that it owns everything.
func TestEmptyMembershipOwnsNothing(t *testing.T) {
	tbl := ownership.NewTable()
	if _, _, err := tbl.Owner("k"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("Owner on an empty table = %v, want ErrNotFound", err)
	}
	if _, _, err := tbl.Replicas("k", 1); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("Replicas on an empty table = %v, want ErrNotFound", err)
	}
	if got := tbl.Members(); len(got) != 0 {
		t.Fatalf("Members = %v, want empty", got)
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	tbl := newTable(t, nodes(1), 1)
	for _, k := range keys(1000) {
		node, _, err := tbl.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if node.ID != "n0" {
			t.Fatalf("Owner = %s, want n0", node.ID)
		}
	}
}

// TestReplicasAreARankedPrefix checks the property replication depends on: the
// replica set is the top of a total order, so the owner is always first and
// growing rf only appends.
func TestReplicasAreARankedPrefix(t *testing.T) {
	tbl := newTable(t, nodes(acceptanceNodes), 1)

	for _, k := range keys(1000) {
		primary, _, err := tbl.Owner(k)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}

		var previous []core.Node
		for rf := 1; rf <= acceptanceNodes; rf++ {
			set, _, err := tbl.Replicas(k, rf)
			if err != nil {
				t.Fatalf("Replicas: %v", err)
			}
			if len(set) != rf {
				t.Fatalf("Replicas(%d) returned %d nodes", rf, len(set))
			}
			if set[0].ID != primary.ID {
				t.Fatalf("Replicas[0] = %s, want the owner %s", set[0].ID, primary.ID)
			}
			for i, n := range previous {
				if set[i].ID != n.ID {
					t.Fatalf("growing rf reordered the set at %d: %s then %s", i, n.ID, set[i].ID)
				}
			}
			seen := map[string]bool{}
			for _, n := range set {
				if seen[n.ID] {
					t.Fatalf("Replicas(%d) repeated %s", rf, n.ID)
				}
				seen[n.ID] = true
			}
			previous = set
		}
	}
}

func TestReplicasClampsAndValidates(t *testing.T) {
	tbl := newTable(t, nodes(3), 1)

	set, _, err := tbl.Replicas("k", 10)
	if err != nil {
		t.Fatalf("Replicas: %v", err)
	}
	if len(set) != 3 {
		t.Fatalf("Replicas(10) with 3 members returned %d nodes, want 3", len(set))
	}
	if _, _, err := tbl.Replicas("k", 0); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Replicas(0) = %v, want ErrInvalidOption", err)
	}
}

func TestUpdateAdvancesTheEpoch(t *testing.T) {
	tbl := ownership.NewTable()
	if got := tbl.Epoch(); got != 0 {
		t.Fatalf("Epoch = %d on a fresh table, want 0", got)
	}
	tbl.Update(nodes(3), 7)
	if got := tbl.Epoch(); got != 7 {
		t.Fatalf("Epoch = %d, want 7", got)
	}
	_, epoch, err := tbl.Owner("k")
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	if epoch != 7 {
		t.Fatalf("Owner reported epoch %d, want 7", epoch)
	}
}

func TestUpdateIgnoresZeroNodes(t *testing.T) {
	tbl := ownership.NewTable()
	tbl.Update([]core.Node{{ID: "n0"}, {}, {ID: "n1"}}, 1)
	if got := tbl.Members(); len(got) != 2 {
		t.Fatalf("Members = %v, want the two named nodes", got)
	}
}
