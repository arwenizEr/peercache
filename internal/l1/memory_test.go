package l1_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/l1"
)

func l1Charge(key string, val []byte) int64 {
	return int64(len(key)+len(val)) + l1.EntryOverhead
}

// settleBytes yields until Bytes stops moving towards want. Removals are
// reported by the underlying library's maintenance goroutine, so the figure
// lags a removal by however long that goroutine takes to be scheduled.
func settleBytes(c *l1.Cache, want int64) int64 {
	for i := 0; i < 1_000_000 && c.Bytes() != want; i++ {
		runtime.Gosched()
	}
	return c.Bytes()
}

// TestL1BytesTracksPutsAndRemovals is the L1 tier's share of M9's accounting.
func TestL1BytesTracksPutsAndRemovals(t *testing.T) {
	c, _ := newCache(t)
	if c.Bytes() != 0 {
		t.Fatalf("empty: Bytes = %d", c.Bytes())
	}

	c.Put("a", make([]byte, 100), core.Stamp{Version: 1}, time.Minute)
	c.Put("b", make([]byte, 50), core.Stamp{Version: 1}, time.Minute)
	want := l1Charge("a", make([]byte, 100)) + l1Charge("b", make([]byte, 50))
	if got := c.Bytes(); got != want {
		t.Fatalf("after two Puts: Bytes = %d, want %d", got, want)
	}

	// A replacement swaps the charge.
	c.Put("a", make([]byte, 10), core.Stamp{Version: 2}, time.Minute)
	want = l1Charge("a", make([]byte, 10)) + l1Charge("b", make([]byte, 50))
	if got := settleBytes(c, want); got != want {
		t.Fatalf("after a replacing Put: Bytes = %d, want %d", got, want)
	}

	// A rejected Put adds nothing.
	c.Put("a", make([]byte, 1000), core.Stamp{Version: 1}, time.Minute)
	if got := settleBytes(c, want); got != want {
		t.Fatalf("after a rejected Put: Bytes = %d, want %d", got, want)
	}

	c.Invalidate("b")
	want = l1Charge("a", make([]byte, 10))
	if got := settleBytes(c, want); got != want {
		t.Fatalf("after Invalidate: Bytes = %d, want %d", got, want)
	}
}

// TestL1BytesFollowsCapacityEviction checks the path nobody calls: the library
// evicting on its own because the entry count reached capacity.
func TestL1BytesFollowsCapacityEviction(t *testing.T) {
	const capacity = 200
	c, _ := newCache(t, func(o *l1.Options) { o.Capacity = capacity })
	for i := 0; i < capacity*5; i++ {
		c.Put(fmt.Sprintf("k:%04d", i), make([]byte, 20), core.Stamp{Version: 1}, time.Minute)
	}
	for i := 0; i < 1_000_000 && c.Len() > capacity; i++ {
		runtime.Gosched()
	}
	per := l1Charge("k:0000", make([]byte, 20))
	want := int64(c.Len()) * per
	if got := settleBytes(c, want); got != want {
		t.Fatalf("after capacity eviction: Bytes = %d, %d resident entries account for %d",
			got, c.Len(), want)
	}
}

// TestL1ShedEmptiesTheTier checks Shed: L1 is the first tier shed under memory
// pressure and it is shed whole. Everything in it is a copy of a key owned
// elsewhere and bounded by a TTL of seconds, so dropping it costs round trips,
// never data — and a whole-tier clear is exact, where a partial one would be
// accounted only once the library's maintenance goroutine caught up.
func TestL1ShedEmptiesTheTier(t *testing.T) {
	c, _ := newCache(t)
	for i := 0; i < 50; i++ {
		c.Put(fmt.Sprintf("k:%d", i), make([]byte, 30), core.Stamp{Version: 1}, time.Minute)
	}
	before := c.Bytes()
	if freed := c.Shed(); freed != before {
		t.Fatalf("Shed freed %d, the tier held %d", freed, before)
	}
	if c.Len() != 0 || c.Bytes() != 0 {
		t.Fatalf("after Shed: Len %d, Bytes %d", c.Len(), c.Bytes())
	}
	if got := c.Stats().Shed; got != 50 {
		t.Fatalf("Stats().Shed = %d, want 50", got)
	}
}
