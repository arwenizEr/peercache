package peercache_test

import (
	"context"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/persist"
)

// benchCache builds a cache on the real clock, since a benchmark measures real
// work. The sweeper ticks once a second and does nothing, which is the
// production configuration.
func benchCache(b *testing.B) peercache.Cache {
	b.Helper()
	c, err := peercache.New(peercache.WithNodeID("bench"), peercache.WithClock(clock.Real()))
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() {
		if err := c.Close(context.Background()); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return c
}

// benchKeys returns n precomputed keys, so that key construction does not end
// up dominating a measurement of the cache.
func benchKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = key(i)
	}
	return out
}

// BenchmarkNamespaceGet measures the acceptance criterion's operation at the
// size it is stated for.
func BenchmarkNamespaceGet(b *testing.B) {
	const keys = 1_000_000

	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	// A shuffled order defeats the sequential locality of a freshly built
	// map, which no real workload has.
	order := shuffledIndices(keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Get(ctx, ks[order[i%len(order)]]); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

// BenchmarkNamespaceGetSmall isolates the per-operation overhead from the
// memory-hierarchy cost: at this size the whole working set is in cache, so
// what remains is the code path itself.
func BenchmarkNamespaceGetSmall(b *testing.B) {
	const keys = 1024

	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Get(ctx, ks[i%len(ks)]); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

func BenchmarkNamespaceGetParallel(b *testing.B) {
	const keys = 1_000_000

	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(keys)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	order := shuffledIndices(keys)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := ns.Get(ctx, ks[order[i%len(order)]]); err != nil {
				b.Fatalf("Get: %v", err)
			}
			i++
		}
	})
}

func BenchmarkNamespaceMiss(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Get(ctx, ks[i%len(ks)]); err == nil {
			b.Fatal("expected a miss")
		}
	}
}

func BenchmarkNamespaceSet(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.Set(ctx, ks[i%len(ks)], value, time.Minute); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

func BenchmarkNamespaceSetParallel(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := ns.Set(ctx, ks[i%len(ks)], value, time.Minute); err != nil {
				b.Fatalf("Set: %v", err)
			}
			i++
		}
	})
}

// BenchmarkPersistentSet is BenchmarkNamespaceSet in a persistent namespace:
// the store write plus a hybrid-logical-clock order and a record in the dirty
// map. The flush runs on its own goroutine and is not on this path.
func BenchmarkPersistentSet(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithPersistence(persist.Memory()))
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	value := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.Set(ctx, ks[i%len(ks)], value, time.Minute); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}

// BenchmarkNamespaceIncrement is decision 0027's cost criterion: a local
// Increment within twice a local Set. It does a parse and a format on top of a
// Set's work, inside the store lock rather than outside it.
func BenchmarkNamespaceIncrement(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(1 << 16)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Increment(ctx, ks[i%len(ks)], 1, time.Minute); err != nil {
			b.Fatalf("Increment: %v", err)
		}
	}
}

// BenchmarkNamespaceCompareAndSwap measures a successful swap: the comparison
// and the write, both under the lock.
func BenchmarkNamespaceCompareAndSwap(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	a, z := make([]byte, 64), make([]byte, 64)
	z[0] = 1
	if err := ns.Set(ctx, "k", a, 0); err != nil {
		b.Fatal(err)
	}
	vals := [2][]byte{a, z}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if ok, err := ns.CompareAndSwap(ctx, "k", vals[i%2], vals[(i+1)%2], 0); err != nil || !ok {
			b.Fatalf("CompareAndSwap = %v, %v", ok, err)
		}
	}
}

// BenchmarkGetOrLoadHit measures the resident path through singleflight, which
// is the common case: the loader is only reached on a miss.
func BenchmarkGetOrLoadHit(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			b.Fatal("the loader ran on a hit")
			return nil, 0, nil
		}))
	ctx := context.Background()
	ks := benchKeys(1024)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.GetOrLoad(ctx, ks[i%len(ks)]); err != nil {
			b.Fatalf("GetOrLoad: %v", err)
		}
	}
}

func BenchmarkBatchGet(b *testing.B) {
	const batch = 100

	c := benchCache(b)
	ns := c.Namespace("bench")
	ctx := context.Background()
	ks := benchKeys(1 << 16)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := (i * batch) % (len(ks) - batch)
		if _, err := ns.BatchGet(ctx, ks[start:start+batch]); err != nil {
			b.Fatalf("BatchGet: %v", err)
		}
	}
}

// BenchmarkNamespaceGetCompressed measures what compression costs on the read
// path, which is why it is off by default.
func BenchmarkNamespaceGetCompressed(b *testing.B) {
	const keys = 1024

	c := benchCache(b)
	ctx := context.Background()
	ks := benchKeys(keys)
	// Large and compressible, so the codec actually engages.
	value := make([]byte, 0, 4096)
	for len(value) < 4096 {
		value = append(value, "peercache-"...)
	}

	for _, tc := range []struct {
		name string
		opts []peercache.NamespaceOption
	}{
		{"plain", nil},
		{"lz4", []peercache.NamespaceOption{peercache.WithCompression("lz4", 128)}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ns := c.Namespace("bench-"+tc.name, tc.opts...)
			for _, k := range ks {
				if err := ns.Set(ctx, k, value, 0); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ns.Get(ctx, ks[i%len(ks)]); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
		})
	}
}
