package peercachetest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/peercachetest"
)

func openCarts(c *peercachetest.Cluster) {
	for i := 0; i < c.Size(); i++ {
		c.Cache(i).Namespace("carts", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
}

// TestAKilledOwnersKeyIsServedByItsReplica is what an application would write
// to check that a cart survives its owner's pod crashing.
func TestAKilledOwnersKeyIsServedByItsReplica(t *testing.T) {
	c := peercachetest.New(t, 3)
	openCarts(c)
	ctx := context.Background()
	if err := c.Cache(0).Namespace("carts").Set(ctx, "cart:7", []byte("2 lamps"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	owner := c.Owner("carts", "cart:7")
	c.Kill(owner)

	for i := 0; i < c.Size(); i++ {
		if i == owner {
			continue
		}
		got, err := c.Cache(i).Namespace("carts").Get(ctx, "cart:7")
		if err != nil || string(got) != "2 lamps" {
			t.Fatalf("node %d after the owner died: %q, %v", i, got, err)
		}
	}
	if now := c.Owner("carts", "cart:7"); now == owner {
		t.Fatalf("the dead node %d still owns the key", owner)
	}
}

// TestARestartedNodeCatchesUp: a restarted node comes back empty; after
// CatchUp it holds its share again.
func TestARestartedNodeCatchesUp(t *testing.T) {
	c := peercachetest.New(t, 3)
	openCarts(c)
	ctx := context.Background()
	if err := c.Cache(1).Namespace("carts").Set(ctx, "cart:9", []byte("1 desk"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	owner := c.Owner("carts", "cart:9")
	c.Restart(owner)
	ns := c.Cache(owner).Namespace("carts", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	if err := c.Cache(owner).CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	c.Settle()
	if got, err := ns.Get(ctx, "cart:9"); err != nil || string(got) != "1 desk" {
		t.Fatalf("the restarted owner after CatchUp: %q, %v", got, err)
	}
}

// TestAPartitionSplitsTheMembershipAndHealsBack: each side sees only itself,
// and after Heal every node sees every other.
func TestAPartitionSplitsTheMembershipAndHealsBack(t *testing.T) {
	c := peercachetest.New(t, 3)
	c.Partition([]int{0, 1}, []int{2})
	for i, want := range []int{2, 2, 1} {
		if got := c.Cache(i).Stats().Members; got != want {
			t.Fatalf("node %d sees %d members during the partition, want %d", i, got, want)
		}
	}
	c.Heal()
	for i := 0; i < c.Size(); i++ {
		if got := c.Cache(i).Stats().Members; got != 3 {
			t.Fatalf("node %d sees %d members after Heal, want 3", i, got)
		}
	}
}

// TestNothingMovesUntilTheTestMovesTheClock: a TTL expires when, and only
// when, the test advances past it.
func TestNothingMovesUntilTheTestMovesTheClock(t *testing.T) {
	c := peercachetest.New(t, 2)
	c.Cache(0).Namespace("sessions")
	c.Cache(1).Namespace("sessions")
	// Read on the owner: a non-owner's L1 copy is bounded by the L1 TTL,
	// a separate clock from the key's own.
	ns := c.Cache(c.Owner("sessions", "s")).Namespace("sessions")
	ctx := context.Background()
	start := c.Clock().Now()
	if !start.After(peercachetest.Epoch()) {
		t.Fatalf("New left the clock at %v; it advances it to form the cluster", start)
	}
	if err := ns.Set(ctx, "s", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	c.Advance(time.Minute - time.Nanosecond)
	if _, err := ns.Get(ctx, "s"); err != nil {
		t.Fatalf("Get before the TTL: %v", err)
	}
	c.Advance(time.Nanosecond)
	if _, err := ns.Get(ctx, "s"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get at the TTL = %v, want ErrNotFound", err)
	}
	if got := c.Clock().Now().Sub(start); got != time.Minute {
		t.Fatalf("the clock moved %v, want exactly the minute the test advanced", got)
	}
}

// TestNodeOptionsReachOneNode: per-node options apply to that node, restarts
// included.
func TestNodeOptionsReachOneNode(t *testing.T) {
	c := peercachetest.New(t, 2, peercachetest.WithNodeOptions(1, peercache.WithMemoryBudget(1<<20)))
	if c.Cache(0).Stats().MemoryBudget != 0 || c.Cache(1).Stats().MemoryBudget != 1<<20 {
		t.Fatalf("budgets %d, %d; want 0 and 1 MiB", c.Cache(0).Stats().MemoryBudget, c.Cache(1).Stats().MemoryBudget)
	}
	c.Restart(1)
	if c.Cache(1).Stats().MemoryBudget != 1<<20 {
		t.Fatal("a restarted node lost its options")
	}
}

// TestADrainedNodeRestartsIntoTheCluster is one step of a rolling upgrade:
// drain, stop, start. A node that left gracefully is remembered as departed,
// and must be admitted again when it returns.
func TestADrainedNodeRestartsIntoTheCluster(t *testing.T) {
	c := peercachetest.New(t, 3)
	openCarts(c)
	if err := c.Cache(2).Drain(context.Background()); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	c.Kill(2)
	c.Restart(2)
	for i := 0; i < c.Size(); i++ {
		if got := c.Cache(i).Stats().Members; got != 3 {
			t.Fatalf("node %d sees %d members after the drained node returned, want 3", i, got)
		}
	}
}
