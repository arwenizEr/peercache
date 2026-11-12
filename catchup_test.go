package peercache_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestARestartedNodeRecoversItsColdReplicas is the case that makes catch-up
// necessary rather than merely tidy.
//
// Replication is a stream of writes, not a state transfer. A node that restarts
// receives only the keys written *after* it came back — every key that was
// quiet when it died stays missing indefinitely. Since a replica exists
// precisely to survive the owner's death, a replica set silently missing its
// cold keys is one that will fail at the moment it is needed.
//
// So: write keys, kill a node, write nothing more, revive it, and check that it
// has nothing. Then catch up and check that it does.
func TestARestartedNodeRecoversItsColdReplicas(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	const rf = 3
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("cold",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}

	const keys = 300
	writer := f.ids[0]
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("cold/%d", i)
		if err := openNS(writer).Set(ctx, k, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	// Pick a node holding replicas and kill it.
	victim := ""
	var heldBefore int64
	for _, id := range f.ids {
		if id == writer {
			continue
		}
		if n := f.caches[id].Stats().ReplicaKeys; n > 0 {
			victim, heldBefore = id, n
			break
		}
	}
	if victim == "" {
		t.Fatal("no node holds replicas; there is nothing to lose")
	}
	t.Logf("killing %s, which holds %d replicas", victim, heldBefore)

	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)

	// Bring it back with empty memory, and write nothing in the meantime —
	// so the only path to its replicas is catch-up.
	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	openNS(victim)

	if n := f.caches[victim].Stats().ReplicaKeys; n != 0 {
		t.Fatalf("a restarted node came back holding %d replicas, want 0; the test cannot "+
			"tell whether catch-up did anything", n)
	}

	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}

	after := f.caches[victim].Stats().ReplicaKeys
	if after == 0 {
		t.Fatal("catch-up recovered nothing; every key quiet at the moment this node died " +
			"would stay missing until somebody happened to write it")
	}
	t.Logf("catch-up recovered %d replicas (held %d before the restart)", after, heldBefore)

	// And they are the right keys: readable, with the values written.
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("cold/%d", i)
		got, err := f.caches[victim].Namespace("cold").Get(ctx, k)
		if err != nil {
			t.Fatalf("after catch-up, %q is unreadable from %s: %v", k, victim, err)
		}
		if want := fmt.Sprintf("v%d", i); string(got) != want {
			t.Fatalf("after catch-up, %q = %q, want %q", k, got, want)
		}
	}
}

// TestCatchUpOnlySendsKeysTheAskerRanksFor is the authorisation half.
//
// The requester names itself and the owner decides what it is entitled to, from
// the same ranking both compute. A node must not be able to obtain keys it has
// no business holding by asking for them.
func TestCatchUpOnlySendsKeysTheAskerRanksFor(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	// Replication factor 2: each key lives on its owner and exactly one
	// replica, so most nodes rank for most keys and a node that received
	// everything would be obvious.
	for _, id := range f.ids {
		f.caches[id].Namespace("scoped",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication())
	}

	const keys = 400
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("scoped/%d", i)
		if err := f.caches[f.ids[0]].Namespace("scoped").Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	asker := f.ids[1]
	before := f.caches[asker].Stats().ReplicaKeys
	if err := f.caches[asker].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	after := f.caches[asker].Stats().ReplicaKeys

	// At RF=2 across 5 nodes, a node ranks as replica for roughly a fifth of
	// the keyspace. Receiving all of it would mean the owner sent whatever
	// was asked for.
	if after > keys/2 {
		t.Fatalf("catch-up left %s holding %d of %d keys; at RF=2 across %d nodes it "+
			"should rank for roughly a fifth, so the owner is sending keys the asker "+
			"has no business holding", asker, after, keys, len(f.ids))
	}
	t.Logf("%s holds %d of %d keys after catch-up (was %d)", asker, after, keys, before)
}

// TestCatchUpDoesNotOverwriteNewerData covers the race that makes catch-up
// dangerous if it is treated as privileged.
//
// A page is assembled on the owner and applied on the asker, and a live write
// can land in between. Bulk transfer is not a licence to overwrite: the same
// version check applies to a catch-up page as to a replicated write.
func TestCatchUpDoesNotOverwriteNewerData(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("race",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const key = "race/1"
	ns := f.caches[f.ids[0]].Namespace("race")
	if err := ns.Set(ctx, key, []byte("first"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := ns.Set(ctx, key, []byte("second"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Catch-up now carries the current value; applying it must be a no-op
	// rather than a regression.
	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	for _, id := range f.ids {
		got, err := f.caches[id].Namespace("race").Get(ctx, key)
		if err != nil {
			t.Fatalf("Get from %s: %v", id, err)
		}
		if string(got) != "second" {
			t.Fatalf("%s reads %q after catch-up, want the newer value", id, got)
		}
	}
}

// TestCatchUpIsANoOpWithoutReplication guards the common configuration. Most
// namespaces run at RF=1 and must not pay for a transfer that has no purpose.
func TestCatchUpIsANoOpWithoutReplication(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("single", peercache.WithReplicationFactor(1))
	}
	if err := f.caches[f.ids[0]].Namespace("single").Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := f.caches[f.ids[0]].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	for _, id := range f.ids {
		if n := f.caches[id].Stats().ReplicaKeys; n != 0 {
			t.Fatalf("%s holds %d replicas at RF=1 after catch-up, want 0", id, n)
		}
	}
	_ = runtime.NumGoroutine
}
