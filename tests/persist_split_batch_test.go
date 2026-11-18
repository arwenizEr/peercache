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

// splitWatch is a BlobStore that records how many logs are read before any
// is deleted.
type splitWatch struct {
	persist.BlobStore

	mu sync.Mutex
	// read counts logs read since the last log was deleted, and most is
	// the largest that count has been.
	read, most int
}

func (s *splitWatch) Get(ctx context.Context, name string) ([]byte, error) {
	if strings.Contains(name, "/log/") {
		s.mu.Lock()
		s.read++
		s.most = max(s.most, s.read)
		s.mu.Unlock()
	}
	return s.BlobStore.Get(ctx, name)
}

func (s *splitWatch) Delete(ctx context.Context, name string) error {
	if strings.Contains(name, "/log/") {
		s.mu.Lock()
		s.read = 0
		s.mu.Unlock()
	}
	return s.BlobStore.Delete(ctx, name)
}

// TestABacklogOfLogsIsSplitABatchAtATime: a splitter facing more logs than a
// batch reads them a batch at a time, deleting each batch before reading the
// next, and clears the whole backlog in one round.
//
// A split read every log at once, under one timeout, and deleted none until
// all were split. Under load one interval's logs took longer than the timeout,
// none were deleted, and each attempt had more to read: on kind at 3.7 million
// keys, 419 logs, 825 MB, read whole by every attempt, and every pod assigned
// the split was OOM-killed in turn.
func TestABacklogOfLogsIsSplitABatchAtATime(t *testing.T) {
	store := &splitWatch{BlobStore: persist.Memory()}
	c, fc := newCache(t)
	ns := c.Namespace("backlog", peercache.WithPersistence(store))
	ctx := context.Background()

	// Seventy flushes, one log each, 7 s of the 10 s before the first split:
	// more than two batches, and no split has run yet.
	const logs = 70
	for i := 0; i < logs; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		flushed(t, c, fc, 1)
	}
	held, err := store.List(ctx, "backlog/log/")
	if err != nil || len(held) < logs {
		t.Fatalf("%d logs held, %v; the test needs %d", len(held), err, logs)
	}

	// Restores read logs too, as each Set's partition came back; only what
	// the split reads is the subject here.
	store.mu.Lock()
	store.read, store.most = 0, 0
	store.mu.Unlock()
	before := c.Stats().PersistSplits
	fc.Advance(splitInterval)
	waitFor(t, "a split round", func() bool { return c.Stats().PersistSplits > before })

	if left, _ := store.List(ctx, "backlog/log/"); len(left) != 0 {
		t.Fatalf("%d of %d logs left after one split round", len(left), len(held))
	}
	store.mu.Lock()
	most := store.most
	store.mu.Unlock()
	if most > 32 {
		t.Fatalf("a split read %d logs before deleting any; it should read a batch, 32, at a time", most)
	}
	for i := 0; i < logs; i++ {
		if got, err := ns.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || string(got) != "v" {
			t.Fatalf("Get(k%d) = %q, %v after the split", i, got, err)
		}
	}
}
