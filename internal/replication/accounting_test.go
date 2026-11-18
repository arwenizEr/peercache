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

// TestAccountedBytesCoverTheLiveHeapAfterRemovals: the same, after most of
// the replicas are gone, by every way they leave — shed, drained for
// promotion or hand-off, deleted, expired. A Go map keeps the slots of what is
// deleted from it, so a store that charged only its live entries undercounted
// the heap by whatever it had held at its peak.
func TestAccountedBytesCoverTheLiveHeapAfterRemovals(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a million entries")
	}
	const n = 1_000_000
	key := func(i int) string { return fmt.Sprintf("order:%016d", i) }
	removals := map[string]func(s *replication.Store){
		"shed": func(s *replication.Store) { s.Shed(s.Bytes()*3/5, nil) },
		"drained": func(s *replication.Store) {
			s.DrainUpTo("orders", n*3/5, func(string) bool { return true })
		},
		"deleted": func(s *replication.Store) {
			for i := 0; i < n*3/5; i++ {
				s.Delete("orders", key(i))
			}
		},
		"expired": func(s *replication.Store) {
			for {
				if removed, _ := s.SweepExpired(2); removed == 0 {
					return
				}
			}
		},
	}
	for name, remove := range removals {
		t.Run(name, func(t *testing.T) {
			base := liveHeap()
			s := replication.NewStore()
			for i := 0; i < n; i++ {
				e := replication.Entry{
					Value: make([]byte, 48),
					Stamp: core.Stamp{Epoch: 1, Version: 1},
					From:  "shop-1",
				}
				if i < n*3/5 {
					e.ExpiresAt = 1 // expired at any now past it
				}
				s.Put("orders", key(i), e)
			}
			peak := int64(liveHeap() - base)
			remove(s)
			if left := s.Len(); left > n/2 {
				t.Fatalf("%d of %d replicas left; the removal under test took too few", left, n)
			}
			used := int64(liveHeap() - base)
			// Two fifths left, in a map rebuilt to hold them: well under
			// three fifths of the peak, which a map that kept its slots
			// stays near.
			if used*10 > peak*6 {
				t.Errorf("%d of %d replicas left hold %d bytes of heap, %d%% of the %d at the peak; "+
					"the map was not rebuilt", s.Len(), n, used, used*100/peak, peak)
			}
			if got := s.Bytes(); got < used {
				t.Errorf("%d replicas left: accounted %d bytes for %d in live heap (%d each); "+
					"the map kept the slots of what was removed", s.Len(), got, used, used/int64(s.Len()))
			}
			runtime.KeepAlive(s)
		})
	}
}
