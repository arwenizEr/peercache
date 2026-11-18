//go:build !race

// Excluded under the race detector, which pads allocations and so measures a
// heap this accounting is not for.

package replication_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/replication"
)

// liveHeap returns the heap in use after two collections.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestAccountedBytesCoverTheLiveHeap: what the replica store charges is at
// least the heap its replicas take, including at the worst point, a map that
// has just doubled. See the owned store's test of the same name.
func TestAccountedBytesCoverTheLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million entries")
	}
	for _, n := range []int{300_000, 1_000_000} {
		base := liveHeap()
		s := replication.NewStore()
		for i := 0; i < n; i++ {
			s.Put("orders", fmt.Sprintf("order:%016d", i), replication.Entry{
				Value: make([]byte, 48),
				Stamp: core.Stamp{Epoch: 1, Version: 1},
				From:  "shop-1",
			})
		}
		used := int64(liveHeap() - base)
		if got := s.Bytes(); got < used {
			t.Errorf("%d replicas: accounted %d bytes (%d each) for %d in live heap (%d each)",
				n, got, got/int64(n), used, used/int64(n))
		}
		runtime.KeepAlive(s)
	}
}
