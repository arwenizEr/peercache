package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestADemotedOwnerDoesNotResurrectAnOverwrittenValue: a node that loses a
// key's ownership — first to replica, then out of the replica set as more
// nodes join — must not keep the value in its owned store and, when ownership
// comes back to it after the newer holders die, serve that old value as the
// truth. The key may be lost; it must not go back in time.
//
// Found on kind through DescribeCluster's replica check: after a rolling
// restart the owned stores held 2.4M entries for about 1.2M keys.
func TestADemotedOwnerDoesNotResurrectAnOverwrittenValue(t *testing.T) {
	f := newFabric(t, 4)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		f.caches[id].Namespace("probe", peercache.WithReplicationFactor(3))
		return f.caches[id].Namespace("orders", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)

	// The key's full ranking decides the roles: N and M outrank X, which
	// outranks Y.
	const key = "order-1"
	p, err := f.caches[f.ids[0]].DescribeKey("probe", key)
	if err != nil {
		t.Fatalf("DescribeKey: %v", err)
	}
	n, m, x := p.Holders[0], p.Holders[1], p.Holders[2]

	// Only X and Y alive: X owns the key and writes v1.
	f.hardKill(t, n)
	f.hardKill(t, m)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)
	if err := open(x).Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("Set v1: %v", err)
	}

	// N returns and owns the key; X is demoted to replica. M returns and
	// outranks X too, which leaves the replica set.
	for _, id := range []string{n, m} {
		f.revive(t, id)
		open(id)
		f.clk.Advance(2 * testkit.DefaultJoinDelay)
		f.convergeTo(t, len(f.live()))
		for _, live := range f.live() {
			_ = f.caches[live].CatchUp(ctx)
		}
		f.settle(t)
	}
	if err := open(n).Set(ctx, key, []byte("v2"), 0); err != nil {
		t.Fatalf("Set v2: %v", err)
	}

	// The newer holders die. Ownership returns to X.
	f.hardKill(t, n)
	f.hardKill(t, m)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	got, err := open(x).Get(ctx, key)
	if err == nil && string(got) == "v1" {
		t.Fatalf("%s served v1, a value overwritten by v2 on the owners that died: a demoted owner kept it and brought it back", x)
	}
	if err != nil && !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get: %v", err)
	}
}
