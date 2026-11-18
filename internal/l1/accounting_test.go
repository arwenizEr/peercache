//go:build !race

// Excluded under the race detector, which pads allocations and so measures a
// heap this accounting is not for.

package l1_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/l1"
)

// liveHeap returns the heap in use after two collections.
func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestAccountedBytesCoverTheLiveHeap: what the tier charges for its entries is
// at least the heap they take, the library's own structures included.
//
// The memory budget is enforced against this figure. The owned and replica
// stores' overheads were estimates a third below the heap until kind
// OOM-killed pods over it; the tier's was the one estimate left unmeasured.
func TestAccountedBytesCoverTheLiveHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates hundreds of thousands of entries")
	}
	for _, n := range []int{10_000, 100_000, 300_000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			base := liveHeap()
			c, _ := newCache(t, func(o *l1.Options) { o.MaxBytes = 1 << 30 })
			for i := 0; i < n; i++ {
				c.Put(fmt.Sprintf("order:%016d", i), make([]byte, 48), core.Stamp{Version: 1}, time.Minute)
			}
			for i := 0; i < 1_000_000 && c.Len() < n; i++ {
				runtime.Gosched()
			}
			if c.Len() != n {
				t.Fatalf("%d of %d entries resident; the bound evicted some", c.Len(), n)
			}
			used := int64(liveHeap() - base)
			got := c.Bytes()
			t.Logf("%d entries: accounted %d each, live heap %d each", n, got/int64(n), used/int64(n))
			if got < used {
				t.Errorf("%d entries: accounted %d bytes (%d each) for %d in live heap (%d each)",
					n, got, got/int64(n), used, used/int64(n))
			}
			runtime.KeepAlive(c)
		})
	}
}
