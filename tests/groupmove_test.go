package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestAnActiveGroupStopsServingWhenOwnershipMoves is M16's group finding.
//
// Ownership was checked only when a group activated. A group already active on
// a node that then lost it — a peer joined and outranks it, the routine case of
// a rolling restart — went on answering that node's own Keys, Items, Get, Set
// and Delete, and any Call a peer with a lagging view forwarded to it, while
// its new owner activated a second copy. docs/api.md promises ErrNotOwner off
// the owner, and decision 2 promises one activation.
func TestAnActiveGroupStopsServingWhenOwnershipMoves(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("ranking", peercache.WithReplicationFactor(3))
	}
	// A group the third node owns, with the first ranked second: while the
	// third is away the first owns it, and when it returns it moves back.
	first, third := f.ids[0], f.ids[2]
	var id string
	for i := 0; i < 10000 && id == ""; i++ {
		k := fmt.Sprintf("group-%d", i)
		p, err := f.caches[first].DescribeKey("ranking", k)
		if err != nil {
			t.Fatal(err)
		}
		if len(p.Holders) == 3 && p.Holders[0] == third && p.Holders[1] == first {
			id = k
		}
	}
	if id == "" {
		t.Fatal("no group ranked third, first")
	}

	f.hardKill(t, third)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	g := f.caches[first].Group(id)
	if err := g.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set on the interim owner: %v", err)
	}

	f.revive(t, third)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)

	if _, err := g.Keys(ctx); !errors.Is(err, peercache.ErrNotOwner) {
		t.Fatalf("Keys on the node that lost %q returned %v; want ErrNotOwner — it is still serving "+
			"an activation its new owner has a second copy of", id, err)
	}
	if err := g.Set(ctx, "k", []byte("stale")); !errors.Is(err, peercache.ErrNotOwner) {
		t.Fatalf("Set on the node that lost %q returned %v; want ErrNotOwner", id, err)
	}
}
