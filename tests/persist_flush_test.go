package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/segment"
	"github.com/arwenizEr/peercache/persist"
)

// Flush tests for persistent namespaces (decision 0029). One node, a fake
// clock and the memory blob store: what reaches the store, and when.

// flushInterval mirrors the library's flush interval. A test that advances by
// it and finds nothing flushed has found a regression, not a timing problem:
// the flusher runs on the fake clock.
const flushInterval = 100 * time.Millisecond

// stored decodes every segment in store and returns the records by key, keeping
// the newest of each — the merge a restore performs.
func stored(t *testing.T, store persist.BlobStore) map[string]segment.Record {
	t.Helper()
	ctx := context.Background()
	names, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	out := map[string]segment.Record{}
	for _, name := range names {
		data, err := store.Get(ctx, name)
		if err != nil {
			t.Fatalf("Get(%s): %v", name, err)
		}
		f, err := segment.Decode(data)
		if err != nil {
			t.Fatalf("segment %s does not decode: %v", name, err)
		}
		for _, r := range f.Records {
			if cur, ok := out[r.Key]; !ok || r.Newer(&cur) {
				out[r.Key] = r
			}
		}
	}
	return out
}

func flushed(t *testing.T, c peercache.Cache, fc interface{ Advance(time.Duration) }, atLeast int64) {
	t.Helper()
	before := c.Stats().PersistFlushes
	fc.Advance(flushInterval)
	waitFor(t, "a flush", func() bool { return c.Stats().PersistFlushes >= before+atLeast })
}

func TestAPersistentWriteIsFlushedAsASegment(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	ctx := context.Background()

	if err := ns.Set(ctx, "o:1", []byte("pending"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if n, _ := store.List(ctx, ""); len(n) != 0 {
		t.Fatalf("%d objects written before the flush interval; persistence is write-behind", len(n))
	}
	if got := c.Stats().PersistPending; got != 1 {
		t.Fatalf("PersistPending = %d, want 1", got)
	}
	flushed(t, c, fc, 1)

	recs := stored(t, store)
	r, ok := recs["o:1"]
	if !ok {
		t.Fatalf("no record for the key; store holds %v", recs)
	}
	if string(r.Value) != "pending" || r.Tombstone || r.Writer != "n0" || r.Order == 0 || r.Namespace != "orders" {
		t.Fatalf("record = %+v", r)
	}
	if want := fc.Now().Add(time.Hour - flushInterval).UnixNano(); r.ExpiresAt != want {
		t.Fatalf("record deadline %d, want the owner's %d", r.ExpiresAt, want)
	}
	if got := c.Stats().PersistPending; got != 0 {
		t.Fatalf("PersistPending = %d after the flush", got)
	}
}

func TestAHotKeyCoalescesToOneRecord(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("hot", peercache.WithPersistence(store))
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		if err := ns.Set(ctx, "k", []byte(fmt.Sprint(i)), 0); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	if got := c.Stats().PersistRecordsWritten; got != 1 {
		t.Fatalf("1000 writes of one key wrote %d records, want 1", got)
	}
	if r := stored(t, store)["k"]; string(r.Value) != "999" {
		t.Fatalf("the coalesced record holds %q, want the last write", r.Value)
	}
}

func TestADeleteIsATombstone(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("del", peercache.WithPersistence(store))
	ctx := context.Background()
	_ = ns.Set(ctx, "k", []byte("v"), 0)
	flushed(t, c, fc, 1)
	written := stored(t, store)["k"]

	if err := ns.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	flushed(t, c, fc, 1)
	r := stored(t, store)["k"]
	if !r.Tombstone || len(r.Value) != 0 {
		t.Fatalf("after Delete the newest record is %+v, want a tombstone", r)
	}
	if !r.Newer(&written) {
		t.Fatalf("the tombstone (order %d) does not outrank the write it deletes (%d)", r.Order, written.Order)
	}
}

func TestAnAtomicWriteIsPersisted(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("ctr", peercache.WithPersistence(store))
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := ns.Increment(ctx, "n", 1, 0); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	if r := stored(t, store)["n"]; string(r.Value) != "3" {
		t.Fatalf("persisted counter %q, want 3", r.Value)
	}
}

func TestCloseFlushesWhatIsPending(t *testing.T) {
	store := persist.Memory()
	c, _ := newCache(t)
	ns := c.Namespace("close", peercache.WithPersistence(store))
	if err := ns.Set(context.Background(), "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if r, ok := stored(t, store)["k"]; !ok || string(r.Value) != "v" {
		t.Fatalf("after Close the store holds %+v; a graceful close must flush", r)
	}
}

func TestANamespaceWithoutPersistenceWritesNothing(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("plain")
	_ = ns.Set(context.Background(), "k", []byte("v"), 0)
	fc.Advance(flushInterval)
	if s := c.Stats(); s.PersistFlushes != 0 || s.PersistPending != 0 {
		t.Fatalf("a namespace without persistence shows persistence activity: %+v", s)
	}
}

// failingStore refuses Puts while failing is set.
type failingStore struct {
	persist.BlobStore
	failing atomic.Bool
	mu      sync.Mutex
	refused int
}

func (s *failingStore) Put(ctx context.Context, name string, data []byte) error {
	if s.failing.Load() {
		s.mu.Lock()
		s.refused++
		s.mu.Unlock()
		return errors.New("store unavailable")
	}
	return s.BlobStore.Put(ctx, name, data)
}

// TestAnUnavailableStoreLosesNothing: writes keep succeeding while the store
// refuses them, the pending set holds one record per key, and once it is back
// every key reaches it at its newest value.
func TestAnUnavailableStoreLosesNothing(t *testing.T) {
	store := &failingStore{BlobStore: persist.Memory()}
	store.failing.Store(true)
	c, fc := newCache(t)
	ns := c.Namespace("flaky", peercache.WithPersistence(store))
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("k%d", i), []byte("v1"), 0); err != nil {
			t.Fatalf("Set while the store is down: %v", err)
		}
	}
	flushed(t, c, fc, 1)
	if s := c.Stats(); s.PersistFailures == 0 || s.PersistPending != 50 {
		t.Fatalf("after a failed flush: failures %d, pending %d; want failures and all 50 kept",
			s.PersistFailures, s.PersistPending)
	}

	// Newer writes while still down must win over the kept records.
	for i := 0; i < 50; i += 2 {
		_ = ns.Set(ctx, fmt.Sprintf("k%d", i), []byte("v2"), 0)
	}
	flushed(t, c, fc, 1)
	if got := c.Stats().PersistPending; got != 50 {
		t.Fatalf("pending %d after rewriting half the keys, want 50: one record per key", got)
	}

	store.failing.Store(false)
	flushed(t, c, fc, 1)
	recs := stored(t, store)
	for i := 0; i < 50; i++ {
		want := "v1"
		if i%2 == 0 {
			want = "v2"
		}
		if r := recs[fmt.Sprintf("k%d", i)]; string(r.Value) != want {
			t.Fatalf("k%d persisted as %q, want %q", i, r.Value, want)
		}
	}
	if got := c.Stats().PersistPending; got != 0 {
		t.Fatalf("pending %d after the store came back", got)
	}
}

// gatedStore blocks each Put until released, then fails or succeeds as told.
type gatedStore struct {
	persist.BlobStore
	entered chan struct{}
	release chan error
}

func (s *gatedStore) Put(ctx context.Context, name string, data []byte) error {
	s.entered <- struct{}{}
	if err := <-s.release; err != nil {
		return err
	}
	return s.BlobStore.Put(ctx, name, data)
}

// TestAFailedFlushDoesNotUndoAWriteMadeDuringIt: a write that lands while a
// flush is in flight is newer than what that flush carried. If the flush then
// fails, putting its records back must not replace the newer write.
func TestAFailedFlushDoesNotUndoAWriteMadeDuringIt(t *testing.T) {
	inner := persist.Memory()
	store := &gatedStore{BlobStore: inner, entered: make(chan struct{}), release: make(chan error)}
	c, fc := newCache(t)
	ns := c.Namespace("race", peercache.WithPersistence(store))
	ctx := context.Background()

	_ = ns.Set(ctx, "k", []byte("old"), 0)
	fc.Advance(flushInterval)
	<-store.entered // the flush holds "old" and is waiting on the store
	if err := ns.Set(ctx, "k", []byte("new"), 0); err != nil {
		t.Fatal(err)
	}
	store.release <- errors.New("store unavailable")
	waitFor(t, "the failed flush", func() bool { return c.Stats().PersistFailures == 1 })

	go func() {
		<-store.entered
		store.release <- nil
	}()
	flushed(t, c, fc, 1)
	if r := stored(t, inner)["k"]; string(r.Value) != "new" {
		t.Fatalf("persisted %q; the failed flush's older record replaced the write made during it", r.Value)
	}
	// Close flushes nothing more, but must not block on the gate.
	go func() {
		for range store.entered {
			store.release <- nil
		}
	}()
}

// TestThePendingRecordIsACopy: a caller may reuse its buffer once Set returns.
func TestThePendingRecordIsACopy(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("copy", peercache.WithPersistence(store))
	buf := []byte("original")
	if err := ns.Set(context.Background(), "k", buf, 0); err != nil {
		t.Fatal(err)
	}
	copy(buf, "MUTATED!")
	flushed(t, c, fc, 1)
	if r := stored(t, store)["k"]; string(r.Value) != "original" {
		t.Fatalf("persisted %q; the record shared the caller's buffer", r.Value)
	}
}
