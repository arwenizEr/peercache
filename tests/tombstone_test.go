package tests

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// TestAStaleTombstoneDoesNotDeleteANewerReplica: found by code review. A
// replicated deletion removed the replica's copy unconditionally — the comment
// above it claimed a stamp check that did not exist — so a deletion from an
// owner on an older epoch, arriving after a partition heals, removed a newer
// write from the key's current owner. The tombstone is sent here by hand, as
// the stale owner would send it, so the test needs no partition to stage.
func TestAStaleTombstoneDoesNotDeleteANewerReplica(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("tomb",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	key := ""
	for i := 0; i < 200 && key == ""; i++ {
		if k := fmt.Sprintf("tomb/%d", i); f.ownerOf(t, k) == "n0" {
			key = k
		}
	}
	if err := openNS("n0").Set(ctx, key, []byte("current"), 0); err != nil {
		t.Fatal(err)
	}
	if f.caches["n1"].Stats().ReplicaKeys != 1 {
		t.Fatalf("n1 holds %d replicas, want 1", f.caches["n1"].Stats().ReplicaKeys)
	}
	epoch := uint64(f.caches["n0"].Stats().Epoch)
	if epoch == 0 {
		t.Fatal("the cluster is at epoch 0; the test needs an older one to send")
	}

	client, err := f.net.Transport(core.Node{ID: "n0", Addr: "n0:7946"}).Dial(core.Node{ID: "n1", Addr: "n1:7946"})
	if err != nil {
		t.Fatal(err)
	}
	send := func(stamp core.Stamp) {
		t.Helper()
		req, err := wire.Encode(&wire.ReplicateRequest{
			Namespace: "tomb", Key: key, Deleted: true, Stamp: stamp, From: "n0",
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.Invoke(ctx, transport.MethodReplicate, req); err != nil {
			t.Fatalf("replicate: %v", err)
		}
	}

	// From an epoch before the copy's: older, however high its version.
	send(core.Stamp{Epoch: epoch - 1, Version: math.MaxUint64})
	if n := f.caches["n1"].Stats().ReplicaKeys; n != 1 {
		t.Fatalf("a tombstone from epoch %d removed a replica written at epoch %d", epoch-1, epoch)
	}

	// From the copy's own epoch, as a real deletion is: newer, and it goes.
	send(core.Stamp{Epoch: epoch, Version: math.MaxUint64})
	if n := f.caches["n1"].Stats().ReplicaKeys; n != 0 {
		t.Fatalf("a current tombstone left %d replicas", n)
	}
}
