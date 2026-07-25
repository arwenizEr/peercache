package l1_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/l1"
)

func benchCache(b *testing.B, capacity int) *l1.Cache {
	b.Helper()
	c, err := l1.New(l1.Options{Clock: clock.Real(), Capacity: capacity, MaxTTL: time.Hour})
	if err != nil {
		b.Fatalf("l1.New: %v", err)
	}
	b.Cleanup(func() {
		if err := c.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return c
}

func benchKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "user:" + strconv.Itoa(i) + "/session"
	}
	return out
}

// BenchmarkGetHit is the L1 tier's dominant operation. It includes the TTL
// check, which this package owns rather than delegating to the cache library.
func BenchmarkGetHit(b *testing.B) {
	const keys = 100_000

	c := benchCache(b, keys)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for i, k := range ks {
		c.Put(k, value, uint64(i+1), time.Hour)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := c.Get(ks[i%keys]); !ok {
			b.Fatal("miss")
		}
	}
}

func BenchmarkGetHitParallel(b *testing.B) {
	const keys = 100_000

	c := benchCache(b, keys)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for i, k := range ks {
		c.Put(k, value, uint64(i+1), time.Hour)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(ks[i%keys])
			i++
		}
	})
}

func BenchmarkGetMiss(b *testing.B) {
	c := benchCache(b, 1024)
	ks := benchKeys(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := c.Get(ks[i%len(ks)]); ok {
			b.Fatal("unexpected hit")
		}
	}
}

// BenchmarkPut includes the version check, which reads the resident entry
// before writing. That read is what stops an out-of-order update from
// resurrecting stale data, and this is what it costs.
func BenchmarkPut(b *testing.B) {
	const keys = 100_000

	c := benchCache(b, keys)
	ks := benchKeys(keys)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(ks[i%keys], value, uint64(i+1), time.Hour)
	}
}

func BenchmarkInvalidateOlderThan(b *testing.B) {
	const keys = 100_000

	c := benchCache(b, keys)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for i, k := range ks {
		c.Put(k, value, uint64(i+1), time.Hour)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.InvalidateOlderThan(ks[i%keys], 1)
	}
}
