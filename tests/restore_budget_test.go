package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/replication"
	"github.com/arwenizEr/peercache/internal/store"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// heldListStore holds every listing of the logs until released: a restore's
// scan lists the logs after it has read the partition's segments, so while it
// is held the scan holds what it read and has applied none of it.
type heldListStore struct {
	persist.BlobStore
	held    atomic.Int64
	release chan struct{}
}

func (s *heldListStore) List(ctx context.Context, prefix string) ([]string, error) {
	if strings.Contains(prefix, "/log/") {
		s.held.Add(1)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.BlobStore.List(ctx, prefix)
}

// TestARestoreCountsWhatItReadsAgainstTheBudget: the records a restore has
// read and not yet applied are in MemoryUsed, and leave it once applied.
//
// They were not counted. On kind a failover's restores, eight partitions at
// once per persistent namespace, held 150 to 200 MB of decoded records, and
// pods whose budget read 440 MB of 512 were OOM-killed at a 1 GiB limit.
func TestARestoreCountsWhatItReadsAgainstTheBudget(t *testing.T) {
	const keys = 2000
	val := make([]byte, 100)
	key := func(i int) string { return fmt.Sprintf("order:%06d", i) }
	ctx := context.Background()

	// Seeded, flushed and split into partition segments, so the restore
	// reads segments and no log is left for it to keep.
	mem := persist.Memory()
	{
		c, fc := newCache(t)
		ns := c.Namespace("orders", peercache.WithPersistence(mem))
		for i := 0; i < keys; i++ {
			if err := ns.Set(ctx, key(i), val, 0); err != nil {
				t.Fatal(err)
			}
		}
		flushed(t, c, fc, 1)
		before := c.Stats().PersistSplits
		fc.Advance(splitInterval)
		waitFor(t, "a split", func() bool { return c.Stats().PersistSplits > before })
		if err := c.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}

	held := &heldListStore{BlobStore: mem, release: make(chan struct{})}
	c, _ := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(held))
	go func() { _, _ = ns.Get(ctx, key(0)) }()
	waitFor(t, "restores holding what they read", func() bool { return held.held.Load() >= 1 })

	st := c.Stats()
	if st.OwnedKeys != 0 {
		t.Fatalf("%d keys applied while every restore is held; the test cannot tell what is held", st.OwnedKeys)
	}
	if st.MemoryUsed == 0 {
		t.Fatal("restores hold the records they read and MemoryUsed is 0: what a restore reads " +
			"is outside the budget")
	}

	close(held.release)
	// A partition is restored when a key in it is first wanted.
	for i := 0; i < keys; i++ {
		if _, err := ns.Get(ctx, key(i)); err != nil {
			t.Fatalf("Get(%s) after the restore: %v", key(i), err)
		}
	}
	// Nothing held now: the owned store alone, which nothing has deleted
	// from, so one slot per key.
	want := int64(0)
	for i := 0; i < keys; i++ {
		want += int64(len(key(i))+len(val)) + store.EntryOverhead
	}
	waitFor(t, "the restores to give back what they held", func() bool {
		return c.Stats().MemoryUsed == want
	})
}

// TestRestoredKeysWaitingForTheirReplicasCountAgainstTheBudget: the backlog of
// restored keys waiting to be sent to their replicas is in MemoryUsed while
// they wait, and leaves it once sent.
//
// It was not counted: on kind a failover's restores queued enough keys to
// hold 80 MB on one pod.
func TestRestoredKeysWaitingForTheirReplicasCountAgainstTheBudget(t *testing.T) {
	var mu sync.Mutex
	held := map[string]*heldReplicas{}
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1, withWrappers(func(id string, tr core.Transport) core.Transport {
		h := newHeldReplicas(tr)
		mu.Lock()
		held[id] = h
		mu.Unlock()
		return h
	}, nil))
	mem := persist.Memory()
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("durable", peercache.WithPersistence(mem), peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		open(id)
	}
	const keys = 4000
	key := func(i int) string { return fmt.Sprintf("k%05d", i) }
	for i := 0; i < keys; i++ {
		if err := open(f.ids[i%2]).Set(ctx, key(i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	flushAll(t, f)
	// Split, so that no log is left for the restores to keep.
	splits := func() (n int64) {
		for _, id := range f.live() {
			n += f.caches[id].Stats().PersistSplits
		}
		return n
	}
	before := splits()
	f.clk.Advance(splitInterval)
	waitFor(t, "a split", func() bool { return splits() > before })

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
		held[id].hold()
	}
	for i := 0; i < keys; i++ {
		if _, err := open(f.ownerOf(t, key(i))).Get(ctx, key(i)); err != nil {
			t.Fatalf("Get(%s): %v", key(i), err)
		}
	}

	// What the stores alone are charged: one slot per key, none removed.
	stores := func(st peercache.Stats) int64 {
		return st.OwnedKeys*int64(len(key(0))+1+int(store.EntryOverhead)) +
			st.ReplicaKeys*int64(len(key(0))+1+len("n0")+int(replication.EntryOverhead))
	}
	for _, id := range f.ids {
		st := f.caches[id].Stats()
		// All but the copies the senders hold, one each, are queued.
		queued := st.OwnedKeys - 8
		if extra := st.MemoryUsed - stores(st); extra < queued*int64(len(key(0))) {
			t.Fatalf("%s: %d restored keys wait for their replicas and MemoryUsed is %d over the "+
				"stores; the backlog is outside the budget", id, st.OwnedKeys, extra)
		}
	}

	for _, id := range f.ids {
		held[id].open()
	}
	waitForCache(t, "the backlog to drain and leave the budget", func() bool {
		for _, id := range f.ids {
			if st := f.caches[id].Stats(); st.ReplicaKeys == 0 || st.MemoryUsed != stores(st) {
				return false
			}
		}
		return totalReplicas(f) >= keys
	})
}
