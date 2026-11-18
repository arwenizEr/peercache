package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// TestARestoreDoesNotCrowdOutLiveReplication: a node restarted with nothing in
// memory restores its whole share from storage, and sends every restored key
// to its replicas. Those copies used to go through the queue live writes use,
// and on kind a restart's burst filled it and dropped live asynchronous copies
// for a minute (docs/m16-results.md, v1.1.0). Restored keys now wait apart, in
// a backlog that drops nothing: with the restore's burst far beyond anything a
// queue holds, a live asynchronous write's copy is still queued, and no
// restored key's copy is dropped either.
//
// The restarted node's peers are made silent, so no copy completes and every
// queue fills as fast as work arrives — the overflow of any bounded queue is
// certain rather than a race with the network.
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
	if n := after.PersistRestored - before.PersistRestored; n < int64(len(mine)) {
		t.Fatalf("restored %d of %d keys; the burst is smaller than this test needs", n, len(mine))
	}
	if d := after.ReplicationsDropped - before.ReplicationsDropped; d != 0 {
		t.Fatalf("%d live replica copies dropped while %d restored keys waited for silent peers: "+
			"restored keys crowd out live writes", d, len(mine))
	}
	if d := after.RestoreReplicationsDropped - before.RestoreReplicationsDropped; d != 0 {
		t.Fatalf("%d of %d restored keys' copies dropped while their peers were silent; "+
			"a restored key waits for its replicas, it is not given up on", d, len(mine))
	}
}

// TestEveryRestoredKeyReachesItsReplicasAfterAColdStart: when every node
// restarts with nothing in memory and restores its share from storage, every
// restored key is sent to its replicas, however many there are.
//
// Restored keys' copies went through a queue of 1,024 and were dropped when it
// was full, on the reasoning that a restored key's replicas still hold it from
// before the restart. After a rolling restart they do not — they restarted
// too. On kind at 3.7 million keys, 98% of every pod's restored keys had their
// copies dropped, about 2.7 million copies, and the cluster ran with a third
// of its redundancy missing and nothing lost to show for it.
//
// The nodes' Replicate requests are held while the restore runs, so its copies
// pile up behind the senders exactly as a slow network piles them up — a queue
// of any bound overflows — and are released once every key is restored.
func TestEveryRestoredKeyReachesItsReplicasAfterAColdStart(t *testing.T) {
	var mu sync.Mutex
	held := map[string]*heldReplicas{}
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1, withWrappers(func(id string, tr core.Transport) core.Transport {
		h := newHeldReplicas(tr)
		mu.Lock()
		held[id] = h
		mu.Unlock()
		return h
	}, nil))
	store := persist.Memory()
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("durable", peercache.WithPersistence(store), peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		open(id)
	}

	const keys = 15_000
	for i := 0; i < keys; i++ {
		if err := open(f.ids[i%3]).Set(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	flushAll(t, f)

	// Every node back with nothing in memory, before any notices.
	for _, id := range f.ids {
		f.hardKill(t, id)
	}
	for _, id := range f.ids {
		f.revive(t, id)
	}
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	for _, id := range f.ids {
		open(id)
	}
	if n := totalReplicas(f); n != 0 {
		t.Fatalf("%d replicas held after a cold start; the test needs none", n)
	}

	// Every read restores its partition on the key's owner, with every copy
	// it sends held.
	for _, id := range f.ids {
		held[id].hold()
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if got, err := open(f.ownerOf(t, k)).Get(ctx, k); err != nil || string(got) != "v" {
			t.Fatalf("Get(%s) after the cold start = %q, %v", k, got, err)
		}
	}

	for _, id := range f.ids {
		held[id].open()
	}
	waitForCache(t, "every restored key's copy to reach its replica", func() bool {
		return totalReplicas(f) >= keys
	})
	var dropped int64
	for _, id := range f.ids {
		dropped += f.caches[id].Stats().RestoreReplicationsDropped
	}
	if dropped != 0 {
		t.Fatalf("%d restored keys' copies dropped", dropped)
	}
}

// totalReplicas sums the replicas held across the fabric's live nodes.
func totalReplicas(f *fabric) (n int64) {
	for _, id := range f.live() {
		n += f.caches[id].Stats().ReplicaKeys
	}
	return n
}
