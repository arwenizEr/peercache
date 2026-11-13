package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/segment"
	"github.com/arwenizEr/peercache/persist"
)

// Compaction tests for persistent namespaces (decision 0029).

const (
	compactInterval = time.Minute
	compactAfter    = 8
	tombstoneGrace  = time.Hour
)

// partitionOf finds the object-name prefix of key's partition by writing it
// alone and seeing where its segment lands. The tests do not assume the hash.
func partitionOf(t *testing.T, key string) string {
	t.Helper()
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	_ = ns.Set(context.Background(), key, []byte("x"), 0)
	flushed(t, c, fc, 1)
	names, _ := store.List(context.Background(), "")
	if len(names) != 1 {
		t.Fatalf("one write produced %d objects", len(names))
	}
	return names[0][:strings.LastIndex(names[0], "/")+1]
}

// keysIn returns n distinct keys that share key's partition.
func keysIn(t *testing.T, prefix string, n int) []string {
	t.Helper()
	var out []string
	for i := 0; len(out) < n && i < 100_000; i++ {
		k := fmt.Sprintf("p%d", i)
		if partitionOf(t, k) == prefix {
			out = append(out, k)
		}
	}
	if len(out) < n {
		t.Fatalf("found %d keys in %s, want %d", len(out), prefix, n)
	}
	return out
}

func objectsUnder(t *testing.T, store persist.BlobStore, prefix string) (snaps, segs []string) {
	t.Helper()
	names, err := store.List(context.Background(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		switch {
		case strings.HasPrefix(n[len(prefix):], "snap-"):
			snaps = append(snaps, n)
		case strings.HasPrefix(n[len(prefix):], "seg-"):
			segs = append(segs, n)
		}
	}
	return snaps, segs
}

// writeFlushed writes key once per flush, so each write is its own segment.
func writeFlushed(t *testing.T, c peercache.Cache, fc interface{ Advance(time.Duration) }, ns peercache.Namespace, key string, times int) {
	t.Helper()
	for i := 0; i < times; i++ {
		if err := ns.Set(context.Background(), key, []byte(fmt.Sprintf("%s-%d", key, i)), 0); err != nil {
			t.Fatal(err)
		}
		flushed(t, c, fc, 1)
	}
}

func compacted(t *testing.T, c peercache.Cache, fc interface{ Advance(time.Duration) }) {
	t.Helper()
	before := c.Stats().PersistCompactions
	fc.Advance(compactInterval)
	waitFor(t, "a compaction", func() bool { return c.Stats().PersistCompactions > before })
}

func TestCompactionFoldsSegmentsIntoASnapshot(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	prefix := partitionOf(t, "k")

	writeFlushed(t, c, fc, ns, "k", compactAfter+1)
	if _, segs := objectsUnder(t, store, prefix); len(segs) != compactAfter+1 {
		t.Fatalf("%d segments before compaction, want %d", len(segs), compactAfter+1)
	}
	compacted(t, c, fc)

	snaps, segs := objectsUnder(t, store, prefix)
	if len(snaps) != 1 || len(segs) != 0 {
		t.Fatalf("after compaction: %d snapshots and %d segments, want 1 and 0", len(snaps), len(segs))
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	c2, _ := newCache(t)
	v, err := c2.Namespace("orders", peercache.WithPersistence(store)).Get(context.Background(), "k")
	if want := fmt.Sprintf("k-%d", compactAfter); err != nil || string(v) != want {
		t.Fatalf("after compaction a cold start reads %q, %v; want %q", v, err, want)
	}
}

// TestATombstoneIsKeptForItsGrace: a compaction inside the grace keeps a
// deletion in the snapshot, so a late segment holding an older write of the
// key cannot bring it back; one after the grace drops it.
//
// The second half plants a tombstone already older than the grace rather
// than advancing the fake clock an hour: that fires every intermediate flush
// tick, thirty-six thousand of them, and made the test take minutes under
// load.
func TestATombstoneIsKeptForItsGrace(t *testing.T) {
	prefix := partitionOf(t, "k")
	others := keysIn(t, prefix, 1)

	t.Run("inside the grace", func(t *testing.T) {
		store := persist.Memory()
		c, fc := newCache(t)
		ns := c.Namespace("orders", peercache.WithPersistence(store))
		writeFlushed(t, c, fc, ns, "k", compactAfter)
		_ = ns.Delete(context.Background(), "k")
		flushed(t, c, fc, 1)
		compacted(t, c, fc)
		if r, ok := snapshotRecord(t, store, prefix, "k"); !ok || !r.Tombstone {
			t.Fatalf("a compaction inside the grace dropped the deletion: %+v, %v", r, ok)
		}
	})

	t.Run("past the grace", func(t *testing.T) {
		store := persist.Memory()
		old := uint64(clock.Epoch.Add(-2*tombstoneGrace).UnixMilli()) << 16
		if err := store.Put(context.Background(), prefix+"snap-0000000000000001-6e30.bin",
			segment.Encode(&segment.File{Kind: segment.KindSnapshot, Records: []segment.Record{
				{Namespace: "orders", Key: "k", Writer: "n0", Order: old, Tombstone: true},
			}})); err != nil {
			t.Fatal(err)
		}
		c, fc := newCache(t)
		ns := c.Namespace("orders", peercache.WithPersistence(store))
		writeFlushed(t, c, fc, ns, others[0], compactAfter)
		compacted(t, c, fc)
		if r, ok := snapshotRecord(t, store, prefix, "k"); ok {
			t.Fatalf("a compaction past the grace kept the deletion: %+v", r)
		}
		if _, ok := snapshotRecord(t, store, prefix, others[0]); !ok {
			t.Fatal("the compaction past the grace lost a live key")
		}
	})
}

// snapshotRecord returns key's record from the newest snapshot under prefix.
func snapshotRecord(t *testing.T, store persist.BlobStore, prefix, key string) (segment.Record, bool) {
	t.Helper()
	snaps, _ := objectsUnder(t, store, prefix)
	if len(snaps) == 0 {
		t.Fatal("no snapshot")
	}
	data, err := store.Get(context.Background(), snaps[len(snaps)-1])
	if err != nil {
		t.Fatal(err)
	}
	f, err := segment.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range f.Records {
		if r.Key == key {
			return r, true
		}
	}
	return segment.Record{}, false
}

// TestARestoreMergesEverySnapshot: two compactors at once each write a
// snapshot and delete only what their own covers. A record that survives only
// in the older of the two must still be restored.
func TestARestoreMergesEverySnapshot(t *testing.T) {
	prefix := partitionOf(t, "k")
	others := keysIn(t, prefix, 1)
	store := persist.Memory()
	ctx := context.Background()
	put := func(name string, f *segment.File) {
		if err := store.Put(ctx, prefix+name, segment.Encode(f)); err != nil {
			t.Fatal(err)
		}
	}
	// The older snapshot holds k; the newer, from the other compactor, never
	// saw the segment k was in, and that segment is already gone.
	put("snap-0000000000001000-6130.bin", &segment.File{Kind: segment.KindSnapshot,
		Covered: []string{prefix + "seg-0000000000001000-6130.bin"},
		Records: []segment.Record{{Namespace: "orders", Key: "k", Writer: "a0", Order: 0x1000, Value: []byte("only here")}}})
	put("snap-0000000000002000-6231.bin", &segment.File{Kind: segment.KindSnapshot,
		Records: []segment.Record{{Namespace: "orders", Key: others[0], Writer: "b1", Order: 0x2000, Value: []byte("other")}}})

	c, _ := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	if v, err := ns.Get(ctx, "k"); err != nil || string(v) != "only here" {
		t.Fatalf("k = %q, %v; a record held only by the older snapshot was not restored", v, err)
	}
	if v, err := ns.Get(ctx, others[0]); err != nil || string(v) != "other" {
		t.Fatalf("%s = %q, %v", others[0], v, err)
	}
}

// snapshotFailingStore refuses to write snapshots.
type snapshotFailingStore struct{ persist.BlobStore }

func (s snapshotFailingStore) Put(ctx context.Context, name string, data []byte) error {
	if strings.Contains(name, "/snap-") {
		return errors.New("store full")
	}
	return s.BlobStore.Put(ctx, name, data)
}

// TestAFailedSnapshotDeletesNothing: segments are deleted only once the
// snapshot that covers them is durable.
func TestAFailedSnapshotDeletesNothing(t *testing.T) {
	inner := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(snapshotFailingStore{inner}))
	prefix := partitionOf(t, "k")

	writeFlushed(t, c, fc, ns, "k", compactAfter)
	before := c.Stats().PersistCompactFailures
	fc.Advance(compactInterval)
	waitFor(t, "the compaction to fail", func() bool { return c.Stats().PersistCompactFailures > before })

	if _, segs := objectsUnder(t, inner, prefix); len(segs) != compactAfter {
		t.Fatalf("%d segments left after a failed snapshot, want all %d", len(segs), compactAfter)
	}
}
