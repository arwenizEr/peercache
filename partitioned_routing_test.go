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
