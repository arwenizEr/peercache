package tests

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// Restore tests for persistent namespaces (decision 0029): one cache writes and
// is closed, a second opens the same store.

// seedAndClose writes through a cache that is then closed, as a pod that
// shut down would be. Close flushes.
func seedAndClose(t *testing.T, store persist.BlobStore, write func(ns peercache.Namespace)) {
	t.Helper()
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	write(ns)
	fc.Advance(flushInterval)
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAColdStartRestoresWhatWasFlushed(t *testing.T) {
	store := persist.Memory()
	ctx := context.Background()
	// Written in two flushes, so its records sit in two segments and the
	// restore has to pick the newer, not the first it reads.
	seedAndClose(t, store, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "rewritten", []byte("first"), 0)
	})
	seedAndClose(t, store, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "rewritten", []byte("second"), 0)
	})
	seedAndClose(t, store, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "kept", []byte("v"), 0)
		_ = ns.Set(ctx, "deleted", []byte("gone"), 0)
		_ = ns.Delete(ctx, "deleted")
		_ = ns.Set(ctx, "short", []byte("expires"), 50*time.Millisecond)
		_, _ = ns.Increment(ctx, "count", 7, 0)
	})

	c, fc := newCache(t)
	fc.Advance(time.Second) // past "short"'s deadline on any clock
	ns := c.Namespace("orders", peercache.WithPersistence(store))

	if v, err := ns.Get(ctx, "kept"); err != nil || string(v) != "v" {
		t.Fatalf("kept = %q, %v; a flushed key must come back", v, err)
	}
	if v, err := ns.Get(ctx, "rewritten"); err != nil || string(v) != "second" {
		t.Fatalf("rewritten = %q, %v; the newer of two segments must win", v, err)
	}
	if v, err := ns.Get(ctx, "count"); err != nil || string(v) != "7" {
		t.Fatalf("count = %q, %v", v, err)
	}
	if _, err := ns.Get(ctx, "deleted"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("deleted = %v; a deleted key must not come back", err)
	}
	if _, err := ns.Get(ctx, "short"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("short = %v; an expired key must not come back", err)
	}
	if got := c.Stats().PersistRestored; got < 2 {
		t.Fatalf("PersistRestored = %d, want at least the two live keys", got)
	}
}

// TestARestoredKeysNextWriteOutranksItsHistory: the restoring node's clock is
// behind the one that wrote the record, and its next write must still win.
func TestARestoredKeysNextWriteOutranksItsHistory(t *testing.T) {
	store := persist.Memory()
	ctx := context.Background()

	ahead, fa := newCache(t)
	fa.Advance(10 * time.Second)
	nsA := ahead.Namespace("orders", peercache.WithPersistence(store))
	_ = nsA.Set(ctx, "k", []byte("from the future"), 0)
	if err := ahead.Close(ctx); err != nil {
		t.Fatal(err)
	}
	old := stored(t, store)["k"]

	behind, fb := newCache(t) // its clock is ten seconds behind
	nsB := behind.Namespace("orders", peercache.WithPersistence(store))
	if v, _ := nsB.Get(ctx, "k"); string(v) != "from the future" {
		t.Fatalf("restored %q", v)
	}
	_ = nsB.Set(ctx, "k", []byte("newer"), 0)
	flushed(t, behind, fb, 1)

	r := stored(t, store)["k"]
	if string(r.Value) != "newer" || !r.Newer(&old) {
		t.Fatalf("after a rewrite the store's winner is %q at order %d against %d; "+
			"a write made after a restore must outrank what it restored", r.Value, r.Order, old.Order)
	}
}

// TestAWriteBeforeTheFirstReadStillRestoresFirst: the first access may be a
// write. It waits for the restore, so a delete is not undone by the older
// stored value arriving after it.
func TestAWriteBeforeTheFirstReadStillRestoresFirst(t *testing.T) {
	store := persist.Memory()
	ctx := context.Background()
	seedAndClose(t, store, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "k", []byte("old"), 0)
	})

	c, _ := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	if err := ns.Delete(ctx, "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("after Delete as the first access, Get = %v; the restore ran after the delete "+
			"and brought the old value back", err)
	}
}

// listFailingStore fails List while failing is set.
type listFailingStore struct {
	persist.BlobStore
	failing atomic.Bool
}

func (s *listFailingStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.failing.Load() {
		return nil, errors.New("store unavailable")
	}
	return s.BlobStore.List(ctx, prefix)
}

// TestAnUnavailableStoreDegradesToMemory: a restore that cannot read the store
// does not make the namespace unavailable. It is counted, the key is served
// from memory, and a later access retries.
func TestAnUnavailableStoreDegradesToMemory(t *testing.T) {
	inner := persist.Memory()
	ctx := context.Background()
	seedAndClose(t, inner, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "k", []byte("v"), 0)
	})

	store := &listFailingStore{BlobStore: inner}
	store.failing.Store(true)
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))

	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get with the store down = %v; want memory's answer, a miss", err)
	}
	if got := c.Stats().PersistRestoreFailures; got != 1 {
		t.Fatalf("PersistRestoreFailures = %d, want 1", got)
	}

	store.failing.Store(false)
	fc.Advance(2 * time.Second) // past the retry interval
	if v, err := ns.Get(ctx, "k"); err != nil || string(v) != "v" {
		t.Fatalf("Get after the store returned = %q, %v; the restore must be retried", v, err)
	}
}

// corruptingStore returns damaged bytes for segment objects.
type corruptingStore struct{ persist.BlobStore }

func (s corruptingStore) Get(ctx context.Context, name string) ([]byte, error) {
	data, err := s.BlobStore.Get(ctx, name)
	if err == nil && strings.Contains(name, "/seg-") && len(data) > 0 {
		data = append([]byte(nil), data...)
		data[len(data)/2] ^= 0xFF
	}
	return data, err
}

// TestACorruptSegmentFailsTheRestore: a damaged object is not skipped, because
// restoring around it could bring back a value it superseded.
func TestACorruptSegmentFailsTheRestore(t *testing.T) {
	inner := persist.Memory()
	ctx := context.Background()
	seedAndClose(t, inner, func(ns peercache.Namespace) {
		_ = ns.Set(ctx, "k", []byte("v"), 0)
	})

	c, _ := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(corruptingStore{inner}))
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get = %v", err)
	}
	if got := c.Stats().PersistRestoreFailures; got != 1 {
		t.Fatalf("PersistRestoreFailures = %d; a corrupt segment must fail the restore", got)
	}
}
