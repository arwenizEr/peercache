//go:build !race

// Excluded under the race detector, which pads allocations and so measures a
// heap this accounting is not for.

package store_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/store"
)

// liveHeap returns the heap in use after two collections.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestAccountedBytesCoverTheLiveHeap: what the store charges for its entries
// is at least the heap they take, including at the worst point, a map that has
// just doubled.
//
// The memory budget is enforced against this figure. It was a third below the
// heap, from an overhead estimated rather than measured, and on kind a pod
// budgeted at half its container limit, as docs/api.md advised, was
// OOM-killed once its replicas were complete.
func TestAccountedBytesCoverTheLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million entries")
	}
	for _, n := range []int{300_000, 1_000_000} {
		base := liveHeap()
		s, _ := newStore(t)
		for i := 0; i < n; i++ {
			s.Set(fmt.Sprintf("order:%016d", i), make([]byte, 48), 0)
		}
		used := int64(liveHeap() - base)
		if got := s.Bytes(); got < used {
			t.Errorf("%d entries: accounted %d bytes (%d each) for %d in live heap (%d each)",
				n, got, got/int64(n), used, used/int64(n))
		}
		runtime.KeepAlive(s)
	}
}

// TestAccountedBytesCoverTheLiveHeapAfterRemovals: the same, after most of the
// entries are gone, by every way they leave — shed, taken by a demotion,
// deleted, expired — and the heap falls with them. A Go map keeps the slots
// of what is deleted from it until it is rebuilt.
func TestAccountedBytesCoverTheLiveHeapAfterRemovals(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million entries")
	}
	const n = 1_000_000
	key := func(i int) string { return fmt.Sprintf("order:%016d", i) }
	gone := func(k string) bool { return k < key(n*3/5) }
	removals := map[string]func(s *store.Store, fc *clock.Fake){
		"shed":  func(s *store.Store, _ *clock.Fake) { s.Shed(s.Bytes()*3/5, nil) },
		"taken": func(s *store.Store, _ *clock.Fake) { s.TakeWhere(gone) },
		"deleted": func(s *store.Store, _ *clock.Fake) {
			for i := 0; i < n*3/5; i++ {
				s.Delete(key(i))
			}
		},
		"expired": func(s *store.Store, fc *clock.Fake) {
			fc.Advance(2 * time.Second)
			s.Sweep(s.Shards())
		},
	}
	for name, remove := range removals {
		t.Run(name, func(t *testing.T) {
			base := liveHeap()
			s, fc := newStore(t)
			for i := 0; i < n; i++ {
				ttl := time.Duration(0)
				if gone(key(i)) {
					ttl = time.Second
				}
				s.Set(key(i), make([]byte, 48), ttl)
			}
			peak := int64(liveHeap() - base)
			remove(s, fc)
			left := s.Len()
			if left > n/2 {
				t.Fatalf("%d of %d entries left; the removal under test took too few", left, n)
			}
			used := int64(liveHeap() - base)
			if got := s.Bytes(); got < used {
				t.Errorf("%d entries left: accounted %d bytes for %d in live heap", left, got, used)
			}
			if used*10 > peak*6 {
				t.Errorf("%d of %d entries left hold %d bytes of heap, %d%% of the %d at the peak; "+
					"the maps were not rebuilt", left, n, used, used*100/peak, peak)
			}
			runtime.KeepAlive(s)
		})
	}
}
