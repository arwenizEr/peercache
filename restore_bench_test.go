package peercache_test

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/persist"
)

// slowStore is a blob store whose every call takes at least delay once
// enabled: the round trip to an object store that persist.Memory does not
// have, and that a restore pays once per object it lists or reads.
type slowStore struct {
	persist.BlobStore
	delay atomic.Int64
}

func (s *slowStore) wait(ctx context.Context) {
	if d := time.Duration(s.delay.Load()); d > 0 {
		_ = clock.Real().Sleep(ctx, d)
	}
}

func (s *slowStore) Get(ctx context.Context, name string) ([]byte, error) {
	s.wait(ctx)
	return s.BlobStore.Get(ctx, name)
}

func (s *slowStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.wait(ctx)
	return s.BlobStore.List(ctx, prefix)
}

// BenchmarkRestoreAfterRestart is the minute after a restart, reduced: a node
// comes back to a namespace whose data is in storage, and every partition is
// restored on its first access while callers wait for it.
//
// A namespace is filled and closed once. Each iteration opens a fresh cache on
// the same store, with a fixed latency on every store call, and 64 readers
// each read keys spread over the whole keyspace at once, the way traffic
// arrives after a restart. ns/op is the time until every reader has its
// answers; p50_ms and max_ms are the wait of a reader's first read, which is
// what a caller of a node that has just restarted sees.
func BenchmarkRestoreAfterRestart(b *testing.B) {
	for _, tc := range []struct {
		name  string
		keys  int
		delay time.Duration
	}{
		{"keys=50000/latency=2ms", 50_000, 2 * time.Millisecond},
		{"keys=200000/latency=2ms", 200_000, 2 * time.Millisecond},
	} {
		b.Run(tc.name, func(b *testing.B) {
			store := &slowStore{BlobStore: persist.Memory()}
			fill, err := peercache.New(peercache.WithNodeID("bench"), peercache.WithClock(clock.Real()))
			if err != nil {
				b.Fatal(err)
			}
			ctx := context.Background()
			ns := fill.Namespace("restore", peercache.WithPersistence(store))
			value := make([]byte, 64)
			for i := 0; i < tc.keys; i++ {
				if err := ns.Set(ctx, key(i), value, 0); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			if err := fill.Close(ctx); err != nil {
				b.Fatalf("Close: %v", err)
			}
			store.delay.Store(int64(tc.delay))

			const readers = 64
			var firsts []time.Duration
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c, err := peercache.New(peercache.WithNodeID("bench"), peercache.WithClock(clock.Real()))
				if err != nil {
					b.Fatal(err)
				}
				rns := c.Namespace("restore", peercache.WithPersistence(store))
				clk := clock.Real()
				start := clk.Now()
				var wg sync.WaitGroup
				var mu sync.Mutex
				for r := 0; r < readers; r++ {
					wg.Add(1)
					go func(r int) {
						defer wg.Done()
						// Each reader reads its own stride of the keyspace,
						// so together they touch every partition.
						for k, n := r, 0; n < 32; k, n = k+readers*7, n+1 {
							if _, err := rns.Get(ctx, key(k%tc.keys)); err != nil {
								b.Errorf("Get: %v", err)
								return
							}
							if n == 0 {
								mu.Lock()
								firsts = append(firsts, clk.Since(start))
								mu.Unlock()
							}
						}
					}(r)
				}
				wg.Wait()
				b.StopTimer()
				if err := c.Close(ctx); err != nil {
					b.Fatalf("Close: %v", err)
				}
				b.StartTimer()
			}
			b.StopTimer()
			sort.Slice(firsts, func(i, j int) bool { return firsts[i] < firsts[j] })
			ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
			if len(firsts) > 0 {
				b.ReportMetric(ms(firsts[len(firsts)/2]), "p50_ms")
				b.ReportMetric(ms(firsts[len(firsts)-1]), "max_ms")
			}
		})
	}
}
