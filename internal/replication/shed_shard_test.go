package replication_test

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache/internal/replication"
)

// TestAShedRebuildsOnlyWhatItShedFrom: shedding a tenth of a namespace copies
// what is left of the shards it took from, not the whole namespace.
//
// A shed rebuilds the map it removed from, which is what frees the slots. With
// one map a namespace that copied the nine tenths left, under the namespace's
// lock, on every relief round: about 160 ms for a million replicas, with every
// copy arriving for the namespace waiting on it. Measured by what the shed
// allocates, which is what it copies.
func TestAShedRebuildsOnlyWhatItShedFrom(t *testing.T) {
	const n = 100_000
	s := replication.NewStore()
	for i := 0; i < n; i++ {
		e := entry("n1", 1, 1, "0123456789012345678901234567890123456789")
		e.Order = uint64(i + 1)
		s.Put("durable", fmt.Sprintf("key-%06d", i), e)
	}
	held := s.Bytes()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	freed := s.Shed(held/10, nil)
	runtime.ReadMemStats(&after)

	if freed < held/10 {
		t.Fatalf("shed freed %d bytes of the %d asked", freed, held/10)
	}
	// A whole rebuild copies nine tenths of the namespace, about half the
	// bytes it is charged; one shard's is a 64th of that.
	if copied := int64(after.TotalAlloc - before.TotalAlloc); copied > held/32 {
		t.Fatalf("a shed of a tenth of %d replicas allocated %d bytes, %d%% of the %d they are charged: "+
			"it rebuilt more than the shards it shed from", n, copied, copied*100/held, held)
	}
	if got := s.Len(); got > n-n/20 || got < n/2 {
		t.Fatalf("%d of %d replicas left after shedding a tenth of their bytes", got, n)
	}
}
