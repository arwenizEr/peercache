package singleflight_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/singleflight"
)

// BenchmarkUncontended measures the cost of collapsing when there is nothing to
// collapse. Every GetOrLoad on a cold key pays this, so it should be small.
func BenchmarkUncontended(b *testing.B) {
	var g singleflight.Group
	ctx := context.Background()
	load := func(context.Context) ([]byte, time.Duration, error) { return nil, 0, nil }

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if r := g.Do(ctx, "k", load); r.Err != nil {
			b.Fatalf("Do: %v", r.Err)
		}
	}
}

// BenchmarkDistinctKeys measures the map and lock behaviour when callers are
// spread across keys, which is the normal case.
func BenchmarkDistinctKeys(b *testing.B) {
	const keyspace = 4096

	var g singleflight.Group
	ctx := context.Background()
	load := func(context.Context) ([]byte, time.Duration, error) { return nil, 0, nil }
	keys := make([]string, keyspace)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if r := g.Do(ctx, keys[i%keyspace], load); r.Err != nil {
				b.Fatalf("Do: %v", r.Err)
			}
			i++
		}
	})
}

// BenchmarkOneHotKey is the stampede case: every goroutine wants the same cold
// key. The measurement is of the collapsing machinery, since the loader is
// trivial.
func BenchmarkOneHotKey(b *testing.B) {
	var g singleflight.Group
	ctx := context.Background()
	load := func(context.Context) ([]byte, time.Duration, error) { return []byte("v"), time.Minute, nil }

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if r := g.Do(ctx, "hot", load); r.Err != nil {
				b.Fatalf("Do: %v", r.Err)
			}
		}
	})
}
