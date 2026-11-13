package store_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache/internal/store"
)

// withCounter gives the store a strictly increasing order source, as the
// cache's hybrid logical clock is.
func withCounter() (func(*store.Options), *atomic.Uint64) {
	var n atomic.Uint64
	return func(o *store.Options) { o.Order = func() uint64 { return n.Add(1) } }, &n
}

func TestWithoutAnOrderSourceEveryOrderIsZero(t *testing.T) {
	s, _ := newStore(t)
	if w := s.Put("k", []byte("v"), 0); w.Order != 0 {
		t.Fatalf("Put without an order source gave order %d", w.Order)
	}
	if _, order := s.Remove("k"); order != 0 {
		t.Fatalf("Remove without an order source gave order %d", order)
	}
}

func TestEveryWritePathTakesAnOrder(t *testing.T) {
	opt, _ := withCounter()
	s, _ := newStore(t, opt)

	w := s.Put("a", []byte("1"), 0)
	if got, ok := s.Order("a"); !ok || got != w.Order || w.Order == 0 {
		t.Fatalf("Put: order %d, stored %d (%v)", w.Order, got, ok)
	}
	if _, stored := s.SetIfAbsent("b", []byte("2"), 0); !stored {
		t.Fatal("SetIfAbsent did not store")
	}
	if got, _ := s.Order("b"); got <= w.Order {
		t.Fatalf("SetIfAbsent stored order %d, not after %d", got, w.Order)
	}
	res, err := s.Update("a", func([]byte, bool) (store.Mutation, bool, error) {
		return store.Mutation{Value: []byte("3")}, true, nil
	})
	if err != nil || !res.Wrote {
		t.Fatalf("Update: %+v, %v", res, err)
	}
	if got, _ := s.Order("a"); got != res.Order || res.Order <= w.Order {
		t.Fatalf("Update: order %d, stored %d, previous %d", res.Order, got, w.Order)
	}
	_, del := s.Remove("a")
	if del <= res.Order {
		t.Fatalf("Remove's order %d is not after the write it removed, %d", del, res.Order)
	}
	// A deletion of an absent key still gets an order: its tombstone is
	// persisted either way.
	if _, order := s.Remove("never-written"); order <= del {
		t.Fatalf("Remove of an absent key gave order %d", order)
	}
}

// TestAKeysOrdersRiseWithItsVersions: the order is issued under the shard
// lock, so whichever of two racing writes is stored last also has the larger
// order. Issued outside it, the stored value and the persisted winner could be
// different writes.
func TestAKeysOrdersRiseWithItsVersions(t *testing.T) {
	opt, _ := withCounter()
	s, _ := newStore(t, opt)

	const writers, each = 8, 2000
	var wg sync.WaitGroup
	var mu sync.Mutex
	byVersion := map[uint64]uint64{}
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				got := s.Put("hot", []byte(fmt.Sprint(w, i)), 0)
				mu.Lock()
				byVersion[got.Version] = got.Order
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	var prevVersion, prevOrder uint64
	for v := uint64(1); v <= writers*each; v++ {
		order, ok := byVersion[v]
		if !ok {
			t.Fatalf("version %d missing", v)
		}
		if v > 1 && order <= prevOrder {
			t.Fatalf("version %d has order %d, not after version %d's %d", v, order, prevVersion, prevOrder)
		}
		prevVersion, prevOrder = v, order
	}
	if stored, _ := s.Order("hot"); stored != prevOrder {
		t.Fatalf("the stored entry has order %d, but the last version's is %d", stored, prevOrder)
	}
}

func TestRestoreInstallsTheGivenOrder(t *testing.T) {
	opt, n := withCounter()
	s, _ := newStore(t, opt)
	if !s.Restore("k", []byte("promoted"), 0, 9, 4242) {
		t.Fatal("Restore refused")
	}
	if got, _ := s.Order("k"); got != 4242 {
		t.Fatalf("Restore stored order %d, want the replica's 4242", got)
	}
	if n.Load() != 0 {
		t.Fatalf("Restore issued %d new orders; a promoted value keeps its own", n.Load())
	}
}

func TestSnapshotCarriesTheOrder(t *testing.T) {
	opt, _ := withCounter()
	s, _ := newStore(t, opt)
	w := s.Put("k", []byte("v"), 0)
	page, _ := s.Snapshot("", 10, nil)
	if len(page) != 1 || page[0].Order != w.Order {
		t.Fatalf("Snapshot = %+v, want the entry with order %d", page, w.Order)
	}
}

func TestAnExpiredEntryHasNoOrder(t *testing.T) {
	opt, _ := withCounter()
	s, fc := newStore(t, opt)
	s.Put("k", []byte("v"), 1)
	fc.Advance(2)
	if _, ok := s.Order("k"); ok {
		t.Fatal("an expired entry reported an order; a restore would compare against a dead value")
	}
}

func TestMergeIfNewerKeepsWhateverIsNewer(t *testing.T) {
	opt, n := withCounter()
	s, _ := newStore(t, opt)

	if _, ok := s.MergeIfNewer("absent", []byte("restored"), 0, 10, false); !ok {
		t.Fatal("a record for an absent key was not installed")
	}
	if v, _, _ := s.Get("absent"); string(v) != "restored" {
		t.Fatalf("Get = %q", v)
	}
	if got, _ := s.Order("absent"); got != 10 {
		t.Fatalf("installed with order %d, want the record's 10", got)
	}

	n.Store(100)
	w := s.Put("live", []byte("memory"), 0) // order 101
	if _, ok := s.MergeIfNewer("live", []byte("older"), 0, w.Order-1, false); ok {
		t.Fatal("an older record replaced a newer in-memory value")
	}
	if _, ok := s.MergeIfNewer("live", []byte("same"), 0, w.Order, false); ok {
		t.Fatal("a record at the in-memory order replaced it")
	}
	if _, ok := s.MergeIfNewer("live", []byte("newer"), 0, w.Order+5, false); !ok {
		t.Fatal("a newer record was refused")
	}
	if v, _, _ := s.Get("live"); string(v) != "newer" {
		t.Fatalf("Get = %q, want the newer record", v)
	}
}

func TestMergeIfNewerAppliesTombstonesOnlyOverOlderValues(t *testing.T) {
	opt, n := withCounter()
	s, _ := newStore(t, opt)
	n.Store(50)
	w := s.Put("k", []byte("v"), 0)

	if _, ok := s.MergeIfNewer("k", nil, 0, w.Order-1, true); ok {
		t.Fatal("an older tombstone deleted a newer value")
	}
	if _, ok := s.MergeIfNewer("k", nil, 0, w.Order+1, true); !ok {
		t.Fatal("a newer tombstone did not delete")
	}
	if _, _, ok := s.Get("k"); ok {
		t.Fatal("the key survived a newer tombstone")
	}
	if _, ok := s.MergeIfNewer("never", nil, 0, 1, true); ok {
		t.Fatal("a tombstone for an absent key reported a change")
	}
	// A rewrite after the restored deletion starts above the removed
	// version, as after any deletion (open question 42).
	if v := s.Put("k", []byte("again"), 0).Version; v <= w.Version {
		t.Fatalf("rewrite got version %d, not above the deleted %d", v, w.Version)
	}
}

func TestMergeIfNewerReplacesAnExpiredEntry(t *testing.T) {
	opt, n := withCounter()
	s, fc := newStore(t, opt)
	n.Store(500)
	s.Put("k", []byte("dead"), 1)
	fc.Advance(2)
	if _, ok := s.MergeIfNewer("k", []byte("restored"), 0, 1, false); !ok {
		t.Fatal("an expired entry blocked a restore, though no reader can see it")
	}
}
