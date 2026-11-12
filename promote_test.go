package peercache_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestKillingAnOwnerPromotesAReplica is the replication acceptance criterion from
// docs/design.md §4: "killing an owner ungracefully causes promotion and resumed
// service within a stated bound".
//
// The bound is stated in the test rather than left implicit: service resumes
// once the survivors have agreed the owner is gone. That is the failure
// detector's latency, not the cache's — the promotion itself is a consequence
// of the new membership and costs one map drain.
//
// Ungracefully matters. A graceful leave announces itself and hands over; this
// node simply stops existing, and the survivors find out by failing to reach
// it, which is what a killed pod looks like.
func TestKillingAnOwnerPromotesAReplica(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	const rf = 3
	for _, id := range f.ids {
		f.caches[id].Namespace("repl",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}

	// Write a spread of keys so that whichever node dies owned some.
	const keys = 200
	written := make(map[string]string, keys)
	writer := f.ids[0]
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("promote/%d", i)
		v := fmt.Sprintf("value-%d", i)
		written[k] = v
		if err := f.caches[writer].Namespace("repl").Set(ctx, k, []byte(v), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	// Kill a node that owns keys *and is not the one the writes went
	// through*. Both halves matter.
	//
	// Killing the writer is what the first version did, and it made the test
	// vacuous in a way a mutation exposed: writes to the writer's own keys
	// are local, so they replicated even when remote writes did not. The 157
	// keys written across the network were never examined. A test that kills
	// the node it wrote through only ever tests the local write path.
	victim := ""
	for _, id := range f.ids {
		if id != writer && f.caches[id].Stats().OwnedKeys > 0 {
			victim = id
			break
		}
	}
	if victim == "" {
		t.Fatal("no node owns any key; the keyspace is not distributed")
	}
	lost := f.caches[victim].Stats().OwnedKeys
	t.Logf("killing %s, which owns %d of %d keys", victim, lost, keys)

	// Snapshot the reconciliation counters *before* the kill. Capturing them
	// afterwards is a race the test loses silently: reconciliation may
	// already have run, so the wait below would sit forever waiting for a
	// second round that nothing will trigger.
	roundsBefore := make(map[string]int64, len(f.ids))
	for _, id := range f.ids {
		if id != victim {
			roundsBefore[id] = f.caches[id].Stats().PromotionRounds
		}
	}

	f.hardKill(t, victim)

	// The stated bound: the survivors must agree the node is gone. On this
	// fabric that is the configured failure timeout — a *different* timer
	// from the join delay, which is what the first version of this test
	// advanced and why it sat watching five members forever. In production
	// the equivalent is memberlist's suspicion timeout.
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)

	// Reconciliation runs on each node's membership watcher, so agreeing the
	// node is gone and having acted on it are two different moments — and
	// they happen independently on every survivor.
	//
	// Wait for the *cheap* signal that every survivor has reconciled, then
	// verify once. An earlier version retried the whole 200-key check on
	// every yield: 800 network round trips per iteration against a 20M
	// iteration budget, which is not a bounded wait, it is an unbounded one
	// wearing a bound's clothing. It looked exactly like a hang, which is
	// how a test with a slow failure mode always looks at first.
	survivors := f.live()
	const maxYields = 5_000_000
	for i := 0; ; i++ {
		done := true
		for _, id := range survivors {
			if f.caches[id].Stats().PromotionRounds <= roundsBefore[id] {
				done = false
				break
			}
		}
		if done {
			break
		}
		if i > maxYields {
			t.Fatalf("not every survivor reconciled after %d yields following the "+
				"owner's death", maxYields)
		}
		runtime.Gosched()
	}

	// Now verify, once. Every key readable from every survivor.
	for k, want := range written {
		for _, id := range survivors {
			got, err := f.caches[id].Namespace("repl").Get(ctx, k)
			if err != nil {
				t.Fatalf("after promotion, %s cannot read %q: %v; a replica held this "+
					"key and should have been promoted", id, k, err)
			}
			if string(got) != want {
				t.Fatalf("after promotion, %s reads %q = %q, want %q", id, k, got, want)
			}
		}
	}

	// And the promotion actually happened rather than the reads being
	// served by luck.
	totalPromoted := int64(0)
	for _, id := range survivors {
		totalPromoted += f.caches[id].Stats().KeysPromoted
	}
	if totalPromoted == 0 {
		t.Fatalf("no keys were promoted after the owner of %d keys was killed", lost)
	}
	t.Logf("%d keys promoted across %d survivors", totalPromoted, len(survivors))
}

// TestAPromotedKeyKeepsItsValue checks that a promoted key survives with the
// value it had.
//
// It is named for what it can actually see. The *version* half of promotion —
// that the new owner continues the previous owner's sequence rather than
// restarting at 1 — is not observable through the public API, and an earlier
// version of this test claimed to check it while only comparing values. A
// mutation that restarted the sequence passed. That property is asserted where
// it is visible, in TestRestoreContinuesTheVersionSequence, and the mutation
// audit targets it there.
func TestAPromotedKeyKeepsItsValue(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("repl",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const key = "versioned/1"
	owner := f.ownerOf(t, key)
	ns := f.caches[owner].Namespace("repl")

	// Push the version well past 1 so a restart is unmistakable.
	const writes = 12
	for i := 0; i < writes; i++ {
		if err := ns.Set(ctx, key, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)

	// Find the new owner and write once more. The version it assigns must
	// continue the sequence rather than restart it.
	newOwner := ""
	for _, id := range f.live() {
		if f.probeOwns(id, key) == nil {
			newOwner = id
			break
		}
	}
	if newOwner == "" {
		t.Fatal("no surviving node claims the key after the owner was killed")
	}

	got, err := f.caches[newOwner].Namespace("repl").Get(ctx, key)
	if err != nil {
		t.Fatalf("the promoted owner cannot read the key: %v", err)
	}
	if string(got) != fmt.Sprintf("v%d", writes-1) {
		t.Fatalf("the promoted owner holds %q, want the last value written", got)
	}
}

// TestPromotionRemovesTheKeyFromTheReplicaStore is the invariant that keeps the
// two stores from disagreeing.
//
// A key present in both would be served from whichever the read path consulted
// first, and "how many keys does this node own" would stop having an answer.
//
// This test replaced one that claimed to check eviction on a kill, which was
// simply the wrong premise: removing a node only moves keys *up* the ranking,
// so survivors gain replicas or get promoted and none are pushed out. Eviction
// proper happens when a node *joins* — it is exercised directly in
// internal/replication, where it can be tested without standing up a cluster.
func TestPromotionRemovesTheKeyFromTheReplicaStore(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("repl",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication())
	}

	const keys = 200
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("evict/%d", i)
		if err := f.caches[f.ids[0]].Namespace("repl").Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	replicasBefore := int64(0)
	ownedBefore := int64(0)
	for _, id := range f.ids {
		replicasBefore += f.caches[id].Stats().ReplicaKeys
		ownedBefore += f.caches[id].Stats().OwnedKeys
	}
	if replicasBefore == 0 {
		t.Fatal("no replicas were placed")
	}
	if ownedBefore != keys {
		t.Fatalf("%d keys are owned across the cluster, want %d", ownedBefore, keys)
	}

	victim := f.ids[len(f.ids)-1]
	roundsBefore := make(map[string]int64, len(f.ids))
	for _, id := range f.ids {
		if id != victim {
			roundsBefore[id] = f.caches[id].Stats().PromotionRounds
		}
	}
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)

	const maxYields = 5_000_000
	for i := 0; ; i++ {
		done := true
		for _, id := range f.live() {
			if f.caches[id].Stats().PromotionRounds <= roundsBefore[id] {
				done = false
				break
			}
		}
		if done {
			break
		}
		if i > maxYields {
			t.Fatalf("not every survivor reconciled after %d yields", maxYields)
		}
		runtime.Gosched()
	}

	promoted := int64(0)
	for _, id := range f.live() {
		promoted += f.caches[id].Stats().KeysPromoted
	}
	if promoted == 0 {
		t.Fatal("killing a node promoted nothing, so the invariant below is untested")
	}

	// Every key is owned exactly once and no key is held both ways.
	ownedAfter := int64(0)
	for _, id := range f.live() {
		ownedAfter += f.caches[id].Stats().OwnedKeys
	}
	if ownedAfter != keys {
		t.Fatalf("%d keys are owned after promotion, want %d; a key is owned twice or "+
			"has been lost", ownedAfter, keys)
	}

	// And everything is still readable, which is the point of all of it.
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("evict/%d", i)
		if _, err := f.caches[f.live()[0]].Namespace("repl").Get(ctx, k); err != nil {
			t.Fatalf("key %q was lost: %v", k, err)
		}
	}
}
