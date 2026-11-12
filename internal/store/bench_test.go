package store_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/store"
)

// benchStore builds a store on the real clock with the sweeper disabled, so a
// benchmark measures the data path and not the sweeper's rotation.
func benchStore(b *testing.B, shards int) *store.Store {
	b.Helper()
	s, err := store.New(store.Options{Clock: clock.Real(), Shards: shards})
	if err != nil {
		b.Fatalf("store.New: %v", err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return s
}

func benchKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "user:" + strconv.Itoa(i) + "/session"
	}
	return out
}

// shuffled returns 0..n-1 in a fixed pseudo-random order, so that a benchmark
// does not walk a freshly built map in insertion order.
func shuffled(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	state := uint64(0x9E3779B97F4A7C15)
	for i := n - 1; i > 0; i-- {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		j := int(state % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}
	return out
}

// BenchmarkGetHot measures the read path with the whole working set resident in
// processor cache. What is left is the code: hash, lock, lookup, counter.
func BenchmarkGetHot(b *testing.B) {
	const keys = 1024

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, 0)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := s.Get(ks[i%keys]); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetCold measures the same path at a million keys in random order,
// where the memory hierarchy dominates and the code does not.
func BenchmarkGetCold(b *testing.B) {
	const keys = 1_000_000

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, 0)
	}
	order := shuffled(keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := s.Get(ks[order[i%keys]]); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetWithTTL measures the cost of the expiry check, which reads the
// clock. Keys without a TTL skip it entirely; this is what that saves.
func BenchmarkGetWithTTL(b *testing.B) {
	const keys = 1024

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, time.Hour)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := s.Get(ks[i%keys]); !ok {
			b.Fatal("miss")
		}
	}
}

// BenchmarkGetHotCopied measures what copy-on-read would cost, by doing to the
// result exactly what a copying Get would do internally.
//
// This is the evidence behind docs/decisions/0007: the aliasing contract is
// sharp, and choosing it has to be justified by a number rather than by an
// assertion that copying is "obviously" too slow.
func BenchmarkGetHotCopied(b *testing.B) {
	const keys = 1024

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, 0)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, _, ok := s.Get(ks[i%keys])
		if !ok {
			b.Fatal("miss")
		}
		clone := make([]byte, len(got))
		copy(clone, got)
		if len(clone) != 64 {
			b.Fatal("short copy")
		}
	}
}

func BenchmarkGetMiss(b *testing.B) {
	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := s.Get(ks[i%len(ks)]); ok {
			b.Fatal("unexpected hit")
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	const keys = 1_000_000

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, 0)
	}
	order := shuffled(keys)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(ks[order[i%keys]])
			i++
		}
	})
}

func BenchmarkSet(b *testing.B) {
	const keys = 1 << 16

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(ks[i%keys], value, time.Hour)
	}
}

func BenchmarkSetParallel(b *testing.B) {
	const keys = 1 << 16

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Set(ks[i%keys], value, time.Hour)
			i++
		}
	})
}

// BenchmarkSetParallelByShardCount is the measurement that justifies 256
// shards. Contention under parallel writes is the only reason the number is
// not 1.
func BenchmarkSetParallelByShardCount(b *testing.B) {
	const keys = 1 << 16
	ks := benchKeys(keys)
	value := make([]byte, 64)

	for _, shards := range []int{1, 8, 64, 256, 1024} {
		b.Run(strconv.Itoa(shards), func(b *testing.B) {
			s := benchStore(b, shards)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.Set(ks[i%keys], value, time.Hour)
					i++
				}
			})
		})
	}
}

// BenchmarkSweepClean measures a sweep over shards with nothing to reclaim,
// which is the steady state and therefore the cost the sweeper imposes on a
// healthy system.
func BenchmarkSweepClean(b *testing.B) {
	const keys = 100_000

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, time.Hour)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Sweep(8)
	}
}

func BenchmarkExists(b *testing.B) {
	const keys = 1024

	s := benchStore(b, store.DefaultShards)
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		s.Set(k, value, 0)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !s.Exists(ks[i%keys]) {
			b.Fatal("miss")
		}
	}
}
