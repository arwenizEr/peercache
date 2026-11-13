package tests

import (
	"context"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestAWriteByAnOwnerWithALowerEpochSurvivesFailover is M16's rolling restart.
//
// Epochs are per view: each node counts the membership changes it has seen,
// so a node that restarted recently holds a lower number than peers that
// watched it leave and come back. Over a rolling restart a key's owner changes
// between nodes whose epochs are unrelated — and core.Stamp lets the epoch
// dominate. A returning owner's writes, stamped with its small epoch, are then
// refused by a replica holding the copy an interim owner stamped with a larger
// one; once the owner and the interim owner are gone, that replica is all that
// is left, and it holds the older value.
//
// On kind after one rolling restart the pods held epochs 5, 7, 9, 11 and 13,
// and version conflicts grew with them — 1.1 million on the highest.
func TestAWriteByAnOwnerWithALowerEpochSurvivesFailover(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("lineage",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	const key = "lineage/k"
	owner := f.ownerOf(t, key)
	if err := open(owner).Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("Set v1: %v", err)
	}

	// The owner leaves; an interim owner writes under its own, larger epoch.
	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	live := f.live()
	placed, err := f.caches[live[0]].DescribeKey("lineage", key)
	if err != nil {
		t.Fatal(err)
	}
	interim, replica := placed.Owner, ""
	for _, id := range live {
		if id != interim {
			replica = id
		}
	}
	if interim == "" || replica == "" {
		t.Fatalf("no interim owner (%q) or replica (%q) while %s is away", interim, replica, owner)
	}
	if err := open(interim).Set(ctx, key, []byte("v2"), 0); err != nil {
		t.Fatalf("Set v2 during the absence: %v", err)
	}

	// It returns with a fresh, smaller epoch, catches up, and writes as owner.
	f.revive(t, owner)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(owner)
	if err := f.caches[owner].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if mine, theirs := f.caches[owner].Describe().Epoch, f.caches[interim].Describe().Epoch; mine >= theirs {
		t.Fatalf("the returning owner is at epoch %d and the interim owner at %d; this test needs the owner lower",
			mine, theirs)
	}
	if got := f.ownerOf(t, key); got != owner {
		t.Fatalf("%s owns %q after the return, want %s", got, key, owner)
	}
	if err := open(owner).Set(ctx, key, []byte("v3"), 0); err != nil {
		t.Fatalf("Set v3 by the returned owner: %v", err)
	}

	// RF 3 survives two losses: the owner and the interim owner both fail,
	// and the replica must hold the acknowledged v3.
	f.hardKill(t, owner)
	f.hardKill(t, interim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 1)
	got, err := open(replica).Get(ctx, key)
	if err != nil || string(got) != "v3" {
		t.Fatalf("after two of three holders failed, %q = %q, %v on %s; want the acknowledged v3. "+
			"It refused v3 as older: its owner's epoch was lower than the interim owner's (%d conflicts)",
			key, got, err, replica, f.caches[replica].Stats().VersionConflicts)
	}
}
