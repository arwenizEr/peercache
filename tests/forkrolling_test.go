package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// TestAGracefulRollingUpgradeCountsNoFork replaces every node in turn — drain,
// stop, start, catch up — across the shop's mix of namespaces, and rewrites
// every key while each node is gone and again once it is back. Each rewrite
// moves a key to another writer: the node that took it over writes over the
// value the drained node handed it, and the returning node writes over that
// one after catching up, while replicas still hold the previous writer's copy.
// Every write has one owner and replaces the last, so nothing forks; every
// split counted is the detector inventing one. On kind the redeploy of the
// shop counted 290,000.
func TestAGracefulRollingUpgradeCountsNoFork(t *testing.T) {
	f := newFabric(t, 4)
	ctx := context.Background()
	store := persist.Memory()
	open := func(id string) {
		c := f.caches[id]
		c.Namespace("sync3", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
		c.Namespace("async2", peercache.WithReplicationFactor(2))
		c.Namespace("durable", peercache.WithReplicationFactor(3), peercache.WithSyncReplication(),
			peercache.WithPersistence(store))
	}
	for _, id := range f.ids {
		open(id)
	}
	writeAll := func(round int, down string) {
		t.Helper()
		var live []string
		for _, id := range f.ids {
			if id != down {
				live = append(live, id)
			}
		}
		for _, name := range []string{"sync3", "async2", "durable"} {
			for i := 0; i < 500; i++ {
				v := []byte(fmt.Sprintf("v%d", round))
				if err := f.caches[live[(i+round)%len(live)]].Namespace(name).Set(ctx, fmt.Sprintf("%s/%d", name, i), v, 0); err != nil {
					t.Fatalf("Set: %v", err)
				}
			}
		}
		f.settle(t)
	}
	writeAll(0, "")

	for round, victim := range append([]string{}, f.ids...) {
		if err := f.caches[victim].Drain(ctx); err != nil {
			t.Fatalf("Drain(%s): %v", victim, err)
		}
		f.hardKill(t, victim)
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		f.convergeTo(t, len(f.ids)-1)
		f.settle(t)
		writeAll(2*round+1, victim)
		f.revive(t, victim)
		open(victim)
		f.clk.Advance(2 * testkit.DefaultJoinDelay)
		f.converge(t)
		for _, id := range f.ids {
			if err := f.caches[id].CatchUp(ctx); err != nil {
				t.Fatalf("CatchUp(%s): %v", id, err)
			}
		}
		f.settle(t)
		writeAll(2*round+2, "")
	}

	var total int64
	for _, id := range f.ids {
		n := f.caches[id].Stats().SplitBrainDetections
		t.Logf("%s: %d forks", id, n)
		total += n
	}
	if total != 0 {
		t.Fatalf("a rolling upgrade counted %d forks; every write had one owner", total)
	}
}
