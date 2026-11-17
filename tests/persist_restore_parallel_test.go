package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
)

// TestPartitionsRestoreInParallel: after a membership change every partition a
// node owns restores again on its first access, and the restores run several
// at once rather than in a queue.
//
// They used to share one mutex. A node restarted onto a large namespace then
// restored its partitions one after another, and a caller of a partition near
// the back of the queue waited for every restore in front of it — on kind, a
// slow minute after each restart. Here storage is held while the reads arrive,
// so the restores that have started are counted where they stand.
func TestPartitionsRestoreInParallel(t *testing.T) {
	f := newFabric(t, 3)
	store := newHeldStore()
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
	// Split, so each partition's records are in its own segments. Unsplit,
	// every partition reads the same few logs, and those are read once and
	// shared (TestARestoreReadsEachLogOnce): the restores would all wait on
	// one read, and how many run at once would not show at the store.
	splitAll(t, f)

	owner := f.ids[0]
	mine := keysOwnedBy(t, f, owner, keys)
	if len(mine) < 16 {
		t.Fatalf("%s owns %d keys; the test needs many partitions' worth", owner, len(mine))
	}
	// A membership change, after which every partition restores again.
	killAndSettle(t, f, f.ids[2])

	var readers sync.WaitGroup
	defer readers.Wait() // after open, which runs first
	store.hold()
	defer store.open()
	for _, k := range mine {
		readers.Add(1)
		go func(k string) {
			defer readers.Done()
			_, _ = durable(owner).Get(ctx, k)
		}(k)
	}
	waitFor(t, "two partitions' restores to be reading storage at once", func() bool {
		return store.waiting.Load() >= 2
	})
}

// splitAll advances past a split interval and waits for the namespace's
// splitter, whichever node it is, to finish a split.
func splitAll(t *testing.T, f *fabric) {
	t.Helper()
	splits := func() (n int64) {
		for _, id := range f.live() {
			n += f.caches[id].Stats().PersistSplits
		}
		return n
	}
	before := splits()
	f.clk.Advance(splitInterval)
	waitFor(t, "a split", func() bool { return splits() > before })
}
