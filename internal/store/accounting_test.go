//go:build !race

// Excluded under the race detector, which pads allocations and so measures a
// heap this accounting is not for.

package store_test

import (
	"fmt"
	"runtime"
	"testing"
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
