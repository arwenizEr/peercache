package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// cutOffStore fails every operation while cut is set: the object store as seen
// from a node whose network is gone.
type cutOffStore struct {
	persist.BlobStore
	cut atomic.Bool
}

var errCutOff = errors.New("store unreachable")

func (s *cutOffStore) Put(ctx context.Context, name string, data []byte) error {
	if s.cut.Load() {
		return errCutOff
	}
	return s.BlobStore.Put(ctx, name, data)
}

func (s *cutOffStore) Get(ctx context.Context, name string) ([]byte, error) {
	if s.cut.Load() {
		return nil, errCutOff
	}
	return s.BlobStore.Get(ctx, name)
}

func (s *cutOffStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.cut.Load() {
		return nil, errCutOff
	}
	return s.BlobStore.List(ctx, prefix)
}

func (s *cutOffStore) Delete(ctx context.Context, name string) error {
	if s.cut.Load() {
		return errCutOff
	}
	return s.BlobStore.Delete(ctx, name)
}

// TestAShedCounterIsNotCountedFromZeroWhenStorageIsUnreachable is open
// question 57 on the memory budget's path rather than the restore's.
//
// A persistent namespace's owner sheds a key under memory pressure: the key
// leaves memory and stays in storage, and its next access reads it back. If
// that read fails, the key is neither in memory nor known — and answering from
// memory then reports it absent, so an Increment counts from zero and its
// write outranks the stored value forever. On kind on 2026-11-18 the pods cut
// off from the object store were over their budget, and four stock counters
// came out hundreds of decrements above the order ledger.
//
// The answer must be the one a failed partition restore gives: ask again.
func TestAShedCounterIsNotCountedFromZeroWhenStorageIsUnreachable(t *testing.T) {
	const (
		budget   = 256 << 10
		counters = 50
		before   = 10
	)
	store := &cutOffStore{BlobStore: persist.Memory()}
	c, fc := newCache(t, peercache.WithMemoryBudget(budget))
	stock := c.Namespace("stock", peercache.WithPersistence(store))
	ctx := context.Background()

	for i := 0; i < counters; i++ {
		for j := 0; j < before; j++ {
			if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
				t.Fatalf("Increment: %v", err)
			}
		}
	}
	flushed(t, c, fc, 1)

	// Pressure: owned data is shed from every namespace in turn.
	bulk := c.Namespace("bulk")
	for i := 0; i < 4000; i++ {
		if err := bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if c.Stats().ShedOwned == 0 {
		t.Fatal("4 MB written into a 256 KB budget and nothing was shed")
	}

	store.cut.Store(true)
	refused := 0
	for i := 0; i < counters; i++ {
		key := fmt.Sprintf("item-%d", i)
		got, err := stock.Increment(ctx, key, -1, 0)
		if err != nil {
			if !errors.Is(err, peercache.ErrActivationPending) {
				t.Fatalf("Increment(%s) with storage unreachable: %v; want ErrActivationPending", key, err)
			}
			refused++
			continue
		}
		if got != -(before + 1) {
			t.Fatalf("Increment(%s) = %d with storage unreachable, want %d: a counter the budget "+
				"shed was counted from zero", key, got, -(before + 1))
		}
	}
	if refused == 0 {
		t.Fatal("no stock counter was shed, so this test checked nothing")
	}

	// Storage back: every counter continues from its own value.
	store.cut.Store(false)
	for i := 0; i < counters; i++ {
		key := fmt.Sprintf("item-%d", i)
		v, err := stock.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s) after storage returned: %v", key, err)
		}
		var n int
		fmt.Sscan(string(v), &n)
		if n > -before {
			t.Fatalf("%s = %d after storage returned; it was %d before the outage", key, n, -before)
		}
	}
}
