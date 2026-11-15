package peercache_test

import (
	"context"
	"runtime"
	"sort"
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

// BenchmarkNamespaceGetHotKeys is a cached read in a namespace with
// WithHotKeyTracking(20), over 1,024 keys: the cost of counting accesses,
// which one read in 16 pays with a lock and the rest with an atomic add.
// BenchmarkNamespaceGetSmallPlain is the same without tracking.
func BenchmarkNamespaceGetHotKeys(b *testing.B) {
	benchGetTracked(b, peercache.WithHotKeyTracking(20))
}

// BenchmarkNamespaceGetSmallPlain is BenchmarkNamespaceGetHotKeys without the
// option, for comparison in one sitting.
func BenchmarkNamespaceGetSmallPlain(b *testing.B) {
	benchGetTracked(b)
}

func benchGetTracked(b *testing.B, opts ...peercache.NamespaceOption) {
	c := benchCache(b)
	ns := c.Namespace("bench", opts...)
	ctx := context.Background()
	ks := benchKeys(1024)
	for _, k := range ks {
		_ = ns.Set(ctx, k, make([]byte, 64), 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.Get(ctx, ks[i%len(ks)]); err != nil {
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

// BenchmarkPersistLossWindow measures decision 0029's loss window on a real
// disk: the time from a Set returning to its record being durable in a segment
// file — fsynced, renamed into place, directory synced. A crash inside that
// window loses the write. Run it with -benchtime Nx; each iteration is one
// write and its wait, so ns/op is the mean window, and p50_ms, p99_ms and
// max_ms are its distribution.
func BenchmarkPersistLossWindow(b *testing.B) {
	store, err := persist.Dir(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithPersistence(store))
	ctx := context.Background()
	clk := clock.Real()
	value := make([]byte, 64)
	windows := make([]time.Duration, 0, b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		before := c.Stats().PersistFlushes
		if err := ns.Set(ctx, key(i), value, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
		acked := clk.Now()
		// The flush counter moves only once every segment of the flush is
		// durable, so this is acknowledged-to-durable, not to-enqueued.
		for c.Stats().PersistFlushes == before {
			runtime.Gosched()
		}
		windows = append(windows, clk.Since(acked))
	}
	b.StopTimer()

	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	b.ReportMetric(ms(windows[len(windows)/2]), "p50_ms")
	b.ReportMetric(ms(windows[len(windows)*99/100]), "p99_ms")
	b.ReportMetric(ms(windows[len(windows)-1]), "max_ms")
}

// BenchmarkPersistFlushSpread is one flush of writes spread over the whole
// keyspace, so that it touches every partition and writes one segment per
// partition: the flush a loaded node actually does. ns/op is that flush,
// write acknowledged to every segment durable.
func BenchmarkPersistFlushSpread(b *testing.B) {
	store, err := persist.Dir(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithPersistence(store))
	ctx := context.Background()
	value := make([]byte, 64)
	const keys = 4096 // about 16 per partition

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// The previous iteration ended with nothing pending and its flush
		// counted, so the next flush holds exactly this batch.
		before := c.Stats().PersistFlushes
		for k := 0; k < keys; k++ {
			if err := ns.Set(ctx, key(i*keys+k), value, 0); err != nil {
				b.Fatalf("Set: %v", err)
			}
		}
		for c.Stats().PersistFlushes == before || c.Stats().PersistPending != 0 {
			runtime.Gosched()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(c.Stats().PersistRecordsWritten)/float64(b.N), "records/flush")
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

// BenchmarkGetOrLoadHitWithRefreshAhead is BenchmarkGetOrLoadHit in a
// namespace with WithRefreshAhead and keys an hour from expiry: the cost of
// checking the window on every hit, which is all a hit outside it pays.
func BenchmarkGetOrLoadHitWithRefreshAhead(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithRefreshAhead(time.Second), peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			b.Fatal("the loader ran on a hit outside the window")
			return nil, 0, nil
		}))
	ctx := context.Background()
	ks := benchKeys(1024)
	value := make([]byte, 64)
	for _, k := range ks {
		if err := ns.Set(ctx, k, value, time.Hour); err != nil {
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

// BenchmarkGetOrLoadNegativeHit is a probe for a key that does not exist,
// answered from the remembered absence (decision 0034). Compare
// BenchmarkGetOrLoadNotFound: the same probe with nothing remembered, against
// a loader that answers instantly — a real one is a database round trip.
func BenchmarkGetOrLoadNegativeHit(b *testing.B) {
	benchNotFound(b, peercache.WithNegativeCaching(time.Hour))
}

// BenchmarkGetOrLoadNotFound is the probe without negative caching: every call
// runs the loader, here one that does no work at all.
func BenchmarkGetOrLoadNotFound(b *testing.B) {
	benchNotFound(b)
}

func benchNotFound(b *testing.B, opts ...peercache.NamespaceOption) {
	c := benchCache(b)
	opts = append(opts, peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			return nil, 0, peercache.ErrNotFound
		}))
	ns := c.Namespace("bench", opts...)
	ctx := context.Background()
	ks := benchKeys(1024)
	for _, k := range ks {
		_, _ = ns.GetOrLoad(ctx, k)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ns.GetOrLoad(ctx, ks[i%len(ks)]); err == nil {
			b.Fatal("GetOrLoad found a key that does not exist")
		}
	}
}

// BenchmarkBatchGetOrLoadCold reads 50 keys none of which is resident, through
// a batch loader (one call) and through the per-key loader (fifty), both of
// which do no work: what is measured is the cache's own cost per miss. A real
// loader adds one database query to the first and fifty to the second.
func BenchmarkBatchGetOrLoadCold(b *testing.B) {
	value := make([]byte, 64)
	for _, tc := range []struct {
		name string
		opt  peercache.NamespaceOption
	}{
		{"batch", peercache.WithBatchLoader(func(_ context.Context, keys []string) (map[string][]byte, time.Duration, error) {
			out := make(map[string][]byte, len(keys))
			for _, k := range keys {
				out[k] = value
			}
			return out, 0, nil
		})},
		{"perkey", peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) {
			return value, 0, nil
		})},
	} {
		b.Run(tc.name, func(b *testing.B) {
			c := benchCache(b)
			ns := c.Namespace("bench", tc.opt)
			ctx := context.Background()
			ks := benchKeys(50)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				for _, k := range ks {
					_ = ns.Delete(ctx, k)
				}
				b.StartTimer()
				if got, err := ns.BatchGetOrLoad(ctx, ks); err != nil || len(got) != len(ks) {
					b.Fatalf("BatchGetOrLoad = %d, %v", len(got), err)
				}
			}
		})
	}
}

// BenchmarkBatchGetOrLoadWarm is BatchGetOrLoad of 50 resident keys: its cost
// over BatchGet when nothing needs loading.
func BenchmarkBatchGetOrLoadWarm(b *testing.B) {
	c := benchCache(b)
	ns := c.Namespace("bench", peercache.WithBatchLoader(func(context.Context, []string) (map[string][]byte, time.Duration, error) {
		b.Fatal("the loader ran on resident keys")
		return nil, 0, nil
	}))
	ctx := context.Background()
	ks := benchKeys(50)
	for _, k := range ks {
		_ = ns.Set(ctx, k, make([]byte, 64), 0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if got, err := ns.BatchGetOrLoad(ctx, ks); err != nil || len(got) != len(ks) {
			b.Fatalf("BatchGetOrLoad = %d, %v", len(got), err)
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
