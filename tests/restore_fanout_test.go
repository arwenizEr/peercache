package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// TestARestoreDoesNotCrowdOutLiveReplication: a node restarted with nothing in
// memory restores its whole share from storage, and sends every restored key
// to its replicas. Those copies used to go through the queue live writes use,
// and on kind a restart's burst filled it and dropped live asynchronous copies
// for a minute (docs/m16-results.md, v1.1.0). Restored keys now queue apart:
// with the restore's burst overflowing its own queue, a live asynchronous
// write's copy is still queued, not dropped.
//
// The restarted node's peers are made silent, so no copy completes and every
// queue fills as fast as work arrives — the overflow is certain rather than a
// race with the network.
func TestARestoreDoesNotCrowdOutLiveReplication(t *testing.T) {
	f := newFabric(t, 3)
	store := persist.Memory()
	ctx := context.Background()
	open := func(id string) {
		f.caches[id].Namespace("durable", peercache.WithPersistence(store), peercache.WithReplicationFactor(2))
		f.caches[id].Namespace("live", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		open(id)
	}
	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }

	// More of the victim's keys than both the restore queue and the whole
	// live queue hold, so that sharing a queue would overflow it.
	const keys = 15_000
	victim := f.ids[0]
	var mine []string
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := durable(f.ids[1]).Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
		if f.ownerOf(t, k) == victim {
			mine = append(mine, k)
		}
	}
	if len(mine) < 4500 {
		t.Fatalf("%s owns %d keys; the test needs more than the live queue's 4,096", victim, len(mine))
	}
	flushAll(t, f)

	// Killed and back before its peers notice, as fastrestart_test.go does:
	// it owns the same share, and holds none of it.
	f.hardKill(t, victim)
	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(victim)

	for _, id := range f.ids {
		if id != victim {
			f.net.SetDropRate(victim, id, 1.0)
		}
	}
	before := f.caches[victim].Stats()

	// Every read restores its partition; every restored key goes out.
	for _, k := range mine {
		if got, err := durable(victim).Get(ctx, k); err != nil || string(got) != "v" {
			t.Fatalf("Get(%s) after the restart = %q, %v", k, got, err)
		}
	}
	// Then live asynchronous writes, owned here.
	live := f.caches[victim].Namespace("live")
	written := 0
	for i := 0; written < 200 && i < 10_000; i++ {
		k := fmt.Sprintf("live%d", i)
		if f.ownerOf(t, k) != victim {
			continue
		}
		if err := live.Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("live Set(%s): %v", k, err)
		}
		written++
	}

	after := f.caches[victim].Stats()
	if after.RestoreReplicationsDropped == before.RestoreReplicationsDropped {
		t.Fatalf("restoring %d keys to silent peers dropped no restored copy; the burst did not "+
			"overflow anything and the test would pass vacuously", len(mine))
	}
	if d := after.ReplicationsDropped - before.ReplicationsDropped; d != 0 {
		t.Fatalf("%d live replica copies dropped while a restore's copies filled their queue "+
			"(%d restored copies dropped): restored keys crowd out live writes",
			d, after.RestoreReplicationsDropped-before.RestoreReplicationsDropped)
	}
}
