package metrics_test

import (
	"testing"

	"github.com/arwenizEr/peercache/internal/metrics"
)

// The cost of an observation decides whether instrumentation can be on by
// default. The local read path measures a p99 of 515 ns; anything that
// adds a meaningful fraction of that has to be opt-in with the number stated,
// rather than quietly making every read slower.

func BenchmarkObserve(b *testing.B) {
	var h metrics.Histogram
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(uint64(i%1000) * 100)
	}
}

// BenchmarkObserveParallel is the number that matters: the read path is
// concurrent, and three atomic adds on shared cache lines is exactly the shape
// that degrades under contention.
func BenchmarkObserveParallel(b *testing.B) {
	var h metrics.Histogram
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			h.Observe(uint64(i%1000) * 100)
			i++
		}
	})
}

// BenchmarkObserveFastPath is the common case: a sub-microsecond local read,
// landing in one of the first buckets so the scan exits almost immediately.
func BenchmarkObserveFastPath(b *testing.B) {
	var h metrics.Histogram
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(300)
	}
}

// BenchmarkObserveSlowPath is a sample past every bound, which walks the whole
// scan. It bounds the worst case.
func BenchmarkObserveSlowPath(b *testing.B) {
	var h metrics.Histogram
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.Observe(20_000_000_000)
	}
}

func BenchmarkQuantile(b *testing.B) {
	var h metrics.Histogram
	for i := 0; i < 100_000; i++ {
		h.Observe(uint64(i%5000) * 200)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if h.Quantile(0.99) == 0 {
			b.Fatal("empty")
		}
	}
}
