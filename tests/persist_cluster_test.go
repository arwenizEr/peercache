package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// Cluster acceptance for persistent namespaces (decision 0029). Every node of a
// fake-network fabric opens one namespace over a shared memory blob store.
//
// The fabric's hardKill closes the cache, and Close flushes; these tests flush
// explicitly before killing anyway, so that what they assert does not depend on
// it. What an ungraceful crash loses — the last flush interval — is measured
// separately, not asserted here.

func openDurable(f *fabric, store persist.BlobStore, opts ...peercache.NamespaceOption) {
	for _, id := range f.live() {
		f.caches[id].Namespace("durable", append([]peercache.NamespaceOption{peercache.WithPersistence(store)}, opts...)...)
	}
}

// flushAll advances past a flush interval and waits until no live node has
// anything pending.
func flushAll(t *testing.T, f *fabric) {
	t.Helper()
	f.clk.Advance(flushInterval)
	waitFor(t, "every node to flush", func() bool {
		for _, id := range f.live() {
			if f.caches[id].Stats().PersistPending != 0 {
				return false
			}
		}
		return true
	})
}

// killAndSettle kills id and waits for the survivors to agree on the smaller
// view and finish reconciling it.
func killAndSettle(t *testing.T, f *fabric, id string) {
	t.Helper()
	f.hardKill(t, id)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.live()))
	f.settle(t)
}

// TestKillingEveryHolderRestoresFromStorage is decision 0029's first
// criterion: at replication factor 1 a key's owner is its only holder, and
// killing it used to lose the key by design. With persistence the new owner
// restores it at its last flushed value.
func TestKillingEveryHolderRestoresFromStorage(t *testing.T) {
	f := newFabric(t, 3)
	store := persist.Memory()
	openDurable(f, store)
	ctx := context.Background()

	const keys = 120
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := f.caches[f.ids[0]].Namespace("durable").Set(ctx, k, []byte("v-"+k), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	flushAll(t, f)

	victim := f.ids[1]
	var lost []string
	for i := 0; i < keys; i++ {
		if k := fmt.Sprintf("k%d", i); f.ownerOf(t, k) == victim {
			lost = append(lost, k)
		}
	}
	if len(lost) == 0 {
		t.Fatalf("%s owns none of %d keys; the test would prove nothing", victim, keys)
	}
	killAndSettle(t, f, victim)

	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }
	for _, id := range f.live() {
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("k%d", i)
			// Retry-aware: just after a failover "ask again" is an
			// answer, not a loss.
			v, err := readWithRetryFrom(t, f, durable, id, k)
			if err != nil || v != "v-"+k {
				t.Fatalf("Get(%s) from %s after its only holder died = %q, %v; it must be restored",
					k, id, v, err)
			}
		}
	}
	var restored, owned int64
	for _, id := range f.live() {
		restored += f.caches[id].Stats().PersistRestored
		owned += f.caches[id].Stats().OwnedKeys
	}
	// A restore keeps only keys this node owns. One that kept everything
	// in the partition would hold copies nobody routes to, outside every
	// guarantee the owned store makes.
	if owned != keys {
		t.Fatalf("survivors own %d keys between them, want exactly %d: a restore installed keys "+
			"its node does not own", owned, keys)
	}
	if restored < int64(len(lost)) {
		t.Fatalf("%d keys restored, but the victim alone held %d", restored, len(lost))
	}
}

// TestAWholeClusterRestartRestoresEverything: every node gone, a new cluster
// on the same store, and every flushed key readable from any node of it.
func TestAWholeClusterRestartRestoresEverything(t *testing.T) {
	store := persist.Memory()
	ctx := context.Background()
	const keys = 120

	first := newFabric(t, 3)
	openDurable(first, store, peercache.WithReplicationFactor(2))
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := first.caches[first.ids[i%3]].Namespace("durable").Set(ctx, k, []byte("v-"+k), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	_ = first.caches[first.ids[0]].Namespace("durable").Delete(ctx, "k7")
	flushAll(t, first)
	for _, id := range first.ids {
		first.hardKill(t, id)
	}

	second := newFabric(t, 3)
	openDurable(second, store, peercache.WithReplicationFactor(2))
	for _, id := range second.ids {
		ns := second.caches[id].Namespace("durable")
		for i := 0; i < keys; i++ {
			k := fmt.Sprintf("k%d", i)
			v, err := ns.Get(ctx, k)
			if k == "k7" {
				if err == nil {
					t.Fatalf("the deleted k7 came back on %s as %q", id, v)
				}
				continue
			}
			if err != nil || string(v) != "v-"+k {
				t.Fatalf("Get(%s) from %s after a full restart = %q, %v", k, id, v, err)
			}
		}
	}
}

// TestAPromotedReplicasWriteOutranksItsOldOwner: the replica that takes over a
// key continues its order, so its next write wins over everything the dead
// owner flushed.
func TestAPromotedReplicasWriteOutranksItsOldOwner(t *testing.T) {
	f := newFabric(t, 3)
	store := persist.Memory()
	openDurable(f, store, peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	ctx := context.Background()

	const key = "promoted"
	owner := f.ownerOf(t, key)
	if err := f.caches[owner].Namespace("durable").Set(ctx, key, []byte("old owner"), 0); err != nil {
		t.Fatal(err)
	}
	flushAll(t, f)
	before := stored(t, store)[key]

	killAndSettle(t, f, owner)
	writer := f.live()[0]
	if err := f.caches[writer].Namespace("durable").Set(ctx, key, []byte("new owner"), 0); err != nil {
		t.Fatalf("Set after the failover: %v", err)
	}
	flushAll(t, f)

	after := stored(t, store)[key]
	if string(after.Value) != "new owner" || !after.Newer(&before) {
		t.Fatalf("after the failover the stored winner is %q (order %d, writer %s) against the old "+
			"owner's order %d; the promoted replica's write must outrank it",
			after.Value, after.Order, after.Writer, before.Order)
	}
	if after.Writer == owner {
		t.Fatalf("the winning record names the dead owner %s as its writer", owner)
	}
}

// blackholeStore silently drops every Put once dropping is set, as a store
// would whose writes were lost after being acknowledged.
type blackholeStore struct {
	persist.BlobStore
	dropping atomic.Bool
}

func (s *blackholeStore) Put(ctx context.Context, name string, data []byte) error {
	if s.dropping.Load() {
		return nil
	}
	return s.BlobStore.Put(ctx, name, data)
}

// TestARestoreDoesNotUndoAPromotedWrite: a replica promoted with a value
// storage never received must keep it when the failover's restore reads
// storage's older record. That holds only because promotion installs the
// value at the order it was written under; installed at order zero, any
// stored record would outrank it.
func TestARestoreDoesNotUndoAPromotedWrite(t *testing.T) {
	f := newFabric(t, 3)
	store := &blackholeStore{BlobStore: persist.Memory()}
	openDurable(f, store, peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	ctx := context.Background()

	const key = "promoted-unflushed"
	owner := f.ownerOf(t, key)
	if err := f.caches[owner].Namespace("durable").Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	flushAll(t, f)

	// From here storage loses everything: v2 reaches the replica and no
	// store, which is the window a crash leaves.
	store.dropping.Store(true)
	if err := f.caches[owner].Namespace("durable").Set(ctx, key, []byte("v2"), 0); err != nil {
		t.Fatal(err)
	}
	killAndSettle(t, f, owner)

	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }
	for _, id := range f.live() {
		v, err := readWithRetryFrom(t, f, durable, id, key)
		if err != nil || v != "v2" {
			t.Fatalf("Get from %s after the failover = %q, %v; the restore replaced the promoted "+
				"v2 with storage's older v1", id, v, err)
		}
	}
}
