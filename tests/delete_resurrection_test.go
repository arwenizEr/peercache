package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestALostDeleteDoesNotResurrectAnOlderValue is open question 42.
//
// Under asynchronous replication a Delete's tombstone can fail to reach a
// replica — a full queue drops it, an unreachable peer fails it — and nothing
// retries. The replica keeps the old value at, say, version 5. A later Set of
// the same key on the owner started again at version 1; the
// replica refused it as older; and when the owner died the replica was promoted
// holding the value that had been deleted and then overwritten. The caller's
// last write was lost and an older one served in its place.
//
// The tombstone is made to fail by partitioning the owner from its replica for
// exactly the Delete, which is the same loss as a dropped message with a cause
// the test can control.
func TestALostDeleteDoesNotResurrectAnOlderValue(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("lostdelete",
			peercache.WithReplicationFactor(2), // asynchronous, the default
			peercache.WithPartitioner(peercache.PartitionByPrefix(":")),
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	prefix := ""
	for i := 0; i < 64 && prefix == ""; i++ {
		if p := fmt.Sprintf("g%d", i); f.ownerOf(t, p) == "n0" {
			prefix = p
		}
	}
	if prefix == "" {
		t.Fatal("no group among 64 is owned by n0")
	}
	key := prefix + ":k"

	sent := func() int64 { return f.caches["n0"].Stats().ReplicationsSent }
	failed := func() int64 { return f.caches["n0"].Stats().ReplicationsFailed }

	// Version 5 on the owner, and on the replica.
	for i := 1; i <= 5; i++ {
		before := sent()
		if err := openNS("n0").Set(ctx, key, []byte(fmt.Sprintf("old-%d", i)), 0); err != nil {
			t.Fatal(err)
		}
		waitForCache(t, "the write to replicate", func() bool { return sent() > before })
	}

	// The Delete, with its tombstone lost.
	f.net.Partition([]string{"n0"}, []string{"n1"})
	before := failed()
	if err := openNS("n0").Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	waitForCache(t, "the tombstone to fail", func() bool { return failed() > before })
	f.net.Heal()

	// The rewrite reaches the replica.
	beforeSent := sent()
	if err := openNS("n0").Set(ctx, key, []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	waitForCache(t, "the rewrite to replicate", func() bool { return sent() > beforeSent })

	f.hardKill(t, "n0")
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 1)
	f.settle(t)

	got, err := openNS("n1").Get(ctx, key)
	if err != nil {
		t.Fatalf("Get %s from the promoted replica: %v", key, err)
	}
	if string(got) != "new" {
		t.Fatalf("%s was written five times, deleted with the tombstone lost, and rewritten "+
			"as \"new\"; after the owner died the replica served %q. The rewrite restarted "+
			"the key's version below the copy the replica still held", key, got)
	}
}
