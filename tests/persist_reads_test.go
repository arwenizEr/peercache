package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// What a persistent namespace reads from its store, counted. An object store
// charges per request, so a read made for nothing is a cost, not only time.

// countingStore counts reads by object name and listings by prefix.
type countingStore struct {
	persist.BlobStore
	mu    sync.Mutex
	gets  map[string]int
	lists map[string]int
}

func newCountingStore() *countingStore {
	return &countingStore{BlobStore: persist.Memory(), gets: map[string]int{}, lists: map[string]int{}}
}

func (s *countingStore) Get(ctx context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	s.gets[name]++
	s.mu.Unlock()
	return s.BlobStore.Get(ctx, name)
}

func (s *countingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	s.lists[prefix]++
	s.mu.Unlock()
	return s.BlobStore.List(ctx, prefix)
}

func (s *countingStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets, s.lists = map[string]int{}, map[string]int{}
}

// counts returns copies of the counts so far.
func (s *countingStore) counts() (gets, lists map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gets, lists = map[string]int{}, map[string]int{}
	for k, v := range s.gets {
		gets[k] = v
	}
	for k, v := range s.lists {
		lists[k] = v
	}
	return gets, lists
}

// TestARestoreReadsEachLogOnce: after a membership change every partition a
// node owns restores, and each reads the namespace's unsplit logs, which hold
// records of every partition. Each log is read from the store once, not once
// per partition: on an object store a restore of 64 partitions with a few
// hundred logs waiting for the splitter was tens of thousands of requests for
// the same few hundred objects.
func TestARestoreReadsEachLogOnce(t *testing.T) {
	f := newFabric(t, 3)
	store := newCountingStore()
	openDurable(f, store)
	ctx := context.Background()
	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }

	const keys = 400
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := durable(f.ids[0]).Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	flushAll(t, f)
	owner := f.ids[0]
	mine := keysOwnedBy(t, f, owner, keys)
	killAndSettle(t, f, f.ids[2])

	logs, err := store.BlobStore.List(ctx, "durable/log/")
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) == 0 {
		t.Fatalf("no unsplit logs before the restore; the test would pass vacuously")
	}
	store.reset()

	// One read at a time: restores that do not overlap share the logs too.
	for _, k := range mine {
		if _, err := durable(owner).Get(ctx, k); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}

	gets, lists := store.counts()
	partitions := 0
	for prefix := range lists {
		if !strings.HasSuffix(prefix, "/log/") {
			partitions++
		}
	}
	if partitions < 2 {
		t.Fatalf("%d partitions restored; the test needs several to share the logs", partitions)
	}
	for _, name := range logs {
		if n := gets[name]; n > 1 {
			t.Fatalf("log %s was read %d times by the restores of %d partitions, want once",
				name, n, partitions)
		}
	}
	t.Logf("%d partitions restored, %d logs each read once", partitions, len(logs))
}

// TestACompactionUnderItsThresholdReadsNothing: once a minute a node looks at
// every partition it compacts, and most of them, most minutes, have fewer
// segments than the threshold. Learning that takes a listing, not a read of
// the partition's snapshots, segments and every unsplit log.
func TestACompactionUnderItsThresholdReadsNothing(t *testing.T) {
	store := newCountingStore()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		writeThenFlush(t, c, fc, func() { _ = ns.Set(ctx, fmt.Sprintf("o%d", i), []byte("x"), 0) })
	}
	split(t, c, fc)
	// A tick looks at the partitions in order, p000 to p063, one at a time,
	// so once it has listed p063 it is done with every partition before it.
	// Segments in p063 itself are left out: the tick may still be on them.
	const last = "orders/p063/"
	names, _ := store.BlobStore.List(ctx, "")
	segments := 0
	for _, n := range names {
		if strings.Contains(n, "/seg-") && !strings.Contains(n, "/log/") && !strings.HasPrefix(n, last) {
			segments++
		}
	}
	if segments == 0 {
		t.Fatalf("no partition segments before %s to compact; the test would pass vacuously", last)
	}
	store.reset()

	// To the tick exactly (see toCompactionTick): an advance of a whole
	// interval went past it and could expire the tick's own deadline while
	// the tick ran, stopping it after the first partition. One race-enabled
	// run in forty did.
	toCompactionTick(fc)
	waitFor(t, "a compaction tick to reach the last partition", func() bool {
		_, lists := store.counts()
		return lists[last] >= 1
	})

	gets, _ := store.counts()
	for name := range gets {
		if strings.HasPrefix(name, last) {
			delete(gets, name)
		}
	}
	if len(gets) != 0 {
		t.Fatalf("a compaction tick with %d segments, under the threshold of %d, read %d objects: %v",
			segments, compactAfter, len(gets), gets)
	}
	if s := c.Stats(); s.PersistCompactions != 0 {
		t.Fatalf("PersistCompactions = %d under the threshold", s.PersistCompactions)
	}
}
