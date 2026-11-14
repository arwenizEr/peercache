package store_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/pager"
	"github.com/arwenizEr/peercache/internal/store"
)

// BenchmarkSnapshotFullPass pages through a whole store as catch-up and
// hand-off do, 512 entries a page, through SnapshotFrom. Its cost per pass is
// what a restarted replica asks of each peer. Snapshot, which scanned and
// sorted everything above the cursor for every page, took 22 ms at 10,000
// keys, 2.3 s at 100,000 and 57 s (13.7 GB allocated) at 400,000.
func BenchmarkSnapshotFullPass(b *testing.B) {
	for _, n := range []int{10_000, 100_000, 400_000, 1_000_000} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			s := benchStore(b, store.DefaultShards)
			value := make([]byte, 32)
			for i := 0; i < n; i++ {
				s.Set(fmt.Sprintf("user:%08d", i), value, 0)
			}
			pages := pager.New(clock.Real(), 0, 0)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				after, total := "", 0
				for {
					page, more := s.SnapshotFrom(pages, "bench", after, 512, nil)
					total += len(page)
					if len(page) > 0 {
						after = page[len(page)-1].Key
					}
					if !more || len(page) == 0 {
						break
					}
				}
				if total != n {
					b.Fatalf("a full pass returned %d of %d entries", total, n)
				}
			}
		})
	}
}
