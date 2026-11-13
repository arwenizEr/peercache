package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
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

// TestAShedKeyIsRestoredNotLost: with persistence, the memory budget shedding
// an owned key is a cache miss, not a data loss. Each key is written twice —
// once flushed, once possibly still pending when it is shed — and every one
// reads back at its newest value.
func TestAShedKeyIsRestoredNotLost(t *testing.T) {
	const (
		budget = 192 << 10
		value  = 2 << 10
		keys   = 300 // 600 KiB of values against a 192 KiB budget
	)
	store := persist.Memory()
	c, fc := newCache(t, peercache.WithMemoryBudget(budget))
	ns := c.Namespace("shed", peercache.WithPersistence(store))
	ctx := context.Background()
	val := func(gen string, i int) []byte {
		b := make([]byte, value)
		copy(b, fmt.Sprintf("%s-%d", gen, i))
		return b
	}

	for i := 0; i < keys; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("k%d", i), val("v1", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	fc.Advance(flushInterval)
	waitFor(t, "the first flush", func() bool { return c.Stats().PersistRecordsWritten >= keys })
	for i := 0; i < keys; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("k%d", i), val("v2", i), 0); err != nil {
			t.Fatal(err)
		}
	}
	if c.Stats().ShedOwned == 0 {
		t.Fatalf("nothing owned was shed; the test would prove nothing (used %d of %d)",
			c.Stats().MemoryUsed, budget)
	}

	for i := 0; i < keys; i++ {
		got, err := ns.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("k%d after shedding: %v; a shed key in a persistent namespace must come back", i, err)
		}
		if want := val("v2", i); string(got) != string(want) {
			t.Fatalf("k%d came back as %q, want its newest write", i, got[:8])
		}
	}
}

// blockingListStore blocks every List until its context ends, then records
// that it returned.
type blockingListStore struct {
	persist.BlobStore
	entered  chan struct{}
	returned atomic.Bool
}

func (s *blockingListStore) List(ctx context.Context, prefix string) ([]string, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	s.returned.Store(true)
	return nil, ctx.Err()
}

// TestCloseEndsARestoreInFlight: a restore runs on its own goroutine, and one
// reading a store that does not answer must not outlive Close — nor hold up
// its final flush, since a restore holds the flush lock while it reads. The
// fake clock never advances here, so only Close can end it.
func TestCloseEndsARestoreInFlight(t *testing.T) {
	store := &blockingListStore{BlobStore: persist.Memory(), entered: make(chan struct{}, 1)}
	fc := clock.NewFake(clock.Epoch)
	c, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithClock(fc))
	if err != nil {
		t.Fatal(err)
	}
	ns := c.Namespace("orders", peercache.WithPersistence(store))

	got := make(chan error, 1)
	go func() {
		_, err := ns.Get(context.Background(), "k")
		got <- err
	}()
	<-store.entered // the restore is reading, and will until its context ends

	closed := make(chan error, 1)
	go func() { closed <- c.Close(context.Background()) }()
	waitFor(t, "Close to return", func() bool {
		select {
		case err := <-closed:
			closed <- err
			return true
		default:
			return false
		}
	})
	if !store.returned.Load() {
		t.Fatal("Close returned while a restore was still reading the store")
	}
	<-got
}
