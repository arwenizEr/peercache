//go:build !race

// Excluded under the race detector, which pads allocations and so measures a
// heap this accounting is not for.

package segment_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache/internal/segment"
)

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// TestDecodedSizeCoversTheLiveHeap: what a decoded partition is charged is at
// least the heap its records take, with the newest-per-key map every reader
// builds over them. The memory budget counts restores by this figure; on kind
// they held 150 to 200 MB it did not count.
func TestDecodedSizeCoversTheLiveHeap(t *testing.T) {
	for _, n := range []int{10_000, 100_000} {
		records := make([]segment.Record, n)
		for i := range records {
			records[i] = segment.Record{
				Namespace: "orders",
				Key:       fmt.Sprintf("order:%016d", i),
				Writer:    "shop-1",
				Order:     uint64(i + 1),
				Value:     make([]byte, 48),
			}
		}
		data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})
		records = nil

		base := liveHeap()
		f, err := segment.Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		newest := make(map[string]*segment.Record)
		for i := range f.Records {
			newest[f.Records[i].Key] = &f.Records[i]
		}
		used := int64(liveHeap() - base)
		if got := segment.DecodedSize(f.Records); got < used {
			t.Errorf("%d records: charged %d (%d each) for %d of live heap (%d each)",
				n, got, got/int64(n), used, used/int64(n))
		} else {
			t.Logf("%d records: charged %d each, live heap %d each", n, got/int64(n), used/int64(n))
		}
		runtime.KeepAlive(newest)
		runtime.KeepAlive(data)
	}
}
