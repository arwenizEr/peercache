package peercache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/peercache/peercache"
)

// partitionedKey finds, in a three-node fabric, a key whose group is owned by
// one node while the key's own string would hash to a different one, and a
// third node to call from. It is the arrangement in which checking ownership by
// the raw key instead of the group key gives the wrong answer.
func partitionedKey(t *testing.T, f *fabric) (key, groupOwner, caller string) {
	t.Helper()
	for i := 0; i < 500; i++ {
		group := fmt.Sprintf("cust%d", i)
		k := group + ":order1"
		g, raw := f.ownerOf(t, group), f.ownerOf(t, k)
		if g == raw {
			continue
		}
		for _, id := range f.ids {
			if id != g {
				return k, g, id
			}
		}
	}
	t.Fatal("no key among 500 whose group owner differs from its raw-key owner")
	return "", "", ""
}

// TestAPartitionedNamespaceServesRemoteCallers: a namespace with a partitioner
// routes by group key, so the owner it reaches is the group's owner — and that
// owner must accept the request. Found by code review: the peer handler checked
// ownership of the raw key, which the partitioner had deliberately not used, so
// the group's owner refused every remote request whose key hashed elsewhere.
func TestAPartitionedNamespaceServesRemoteCallers(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("partitioned",
			peercache.WithPartitioner(peercache.PartitionByPrefix(":")),
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	key, owner, caller := partitionedKey(t, f)
	t.Logf("%s: group owned by %s, calling from %s", key, owner, caller)

	if err := openNS(caller).Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("remote Set of %s from %s: %v", key, caller, err)
	}
	if got, err := openNS(caller).Get(ctx, key); err != nil || string(got) != "v" {
		t.Fatalf("remote Get of %s from %s = %q, %v", key, caller, got, err)
	}
	if ok, err := openNS(caller).Exists(ctx, key); err != nil || !ok {
		t.Fatalf("remote Exists of %s from %s = %v, %v", key, caller, ok, err)
	}
	batch, err := openNS(caller).BatchGet(ctx, []string{key})
	if err != nil || string(batch[key]) != "v" {
		t.Fatalf("remote BatchGet of %s from %s = %v, %v", key, caller, batch, err)
	}
	if err := openNS(caller).Delete(ctx, key); err != nil {
		t.Fatalf("remote Delete of %s from %s: %v", key, caller, err)
	}
	if ok, _ := openNS(owner).Exists(ctx, key); ok {
		t.Fatalf("the remote Delete of %s did not reach the group's owner %s", key, owner)
	}
}

// TestBatchSetRoutesEachKeyToItsOwner: found by code review. BatchSet wrote
// every pair into the calling node's own store, with no routing, replication
// or invalidation, so on a cluster a key owned elsewhere landed where no read
// would ever look for it.
func TestBatchSetRoutesEachKeyToItsOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("batchset",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication(),
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	kv := map[string][]byte{}
	for i := 0; i < 30; i++ {
		kv[fmt.Sprintf("batchset/%d", i)] = []byte(fmt.Sprintf("v%d", i))
	}
	if err := openNS("n0").BatchSet(ctx, kv, 0); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	for _, via := range f.ids {
		for key, want := range kv {
			got, err := openNS(via).Get(ctx, key)
			if err != nil || string(got) != string(want) {
				t.Fatalf("Get %s via %s after a BatchSet through n0 = %q, %v", key, via, got, err)
			}
		}
	}
	// And they were replicated: no node holds them all as its own.
	if st := f.caches["n0"].Stats(); st.OwnedKeys == int64(len(kv)) {
		t.Fatalf("n0 owns all %d keys of its BatchSet; they were not routed", st.OwnedKeys)
	}
	var replicas int64
	for _, id := range f.ids {
		replicas += f.caches[id].Stats().ReplicaKeys
	}
	if replicas < int64(len(kv)) {
		t.Fatalf("%d replica copies for %d keys at RF=2; BatchSet did not replicate", replicas, len(kv))
	}
}

// TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash: found by code
// review. A namespace whose options fail validation is still registered, with
// no store; a peer request for it reached the nil store and panicked on the
// handler's goroutine, which takes the whole process down.
func TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	f.caches["n1"].Namespace("misconfigured", peercache.WithCodec("no-such-codec"))
	good := f.caches["n0"].Namespace("misconfigured", peercache.WithL1TTL(-1))
	f.settle(t)

	key := ""
	for i := 0; i < 200 && key == ""; i++ {
		if k := fmt.Sprintf("m/%d", i); f.ownerOf(t, k) == "n1" {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no key owned by n1")
	}
	for name, op := range map[string]func() error{
		"Set": func() error { return good.Set(ctx, key, []byte("v"), 0) },
		"Get": func() error { _, err := good.Get(ctx, key); return err },
		"BatchGet": func() error {
			_, err := good.BatchGet(ctx, []string{key})
			return err
		},
		"Delete": func() error { return good.Delete(ctx, key) },
	} {
		if err := op(); err == nil {
			t.Fatalf("%s against a key whose owner has the namespace misconfigured returned nil", name)
		}
	}
}
