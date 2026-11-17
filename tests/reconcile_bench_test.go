package tests

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/peercachetest"
)

// BenchmarkIdleTick is one tick of a settled cluster that has nothing to do:
// five nodes, an RF=3 namespace holding keys, membership unchanged for longer
// than the settling window, no traffic. ns/op is advancing the clock one tick
// and waiting for the cluster to go quiet; reconciles/op is how many full
// reconciliations that tick started across the five nodes.
//
// A tick with nothing to do should cost next to nothing whatever the cluster
// holds. Every tick used to reconcile in full on every node, in time
// proportional to what the node held; on kind, at about 400,000 keys a pod, the
// shop ran at a fifteenth of its throughput on an empty store.
func BenchmarkIdleTick(b *testing.B) {
	for _, keys := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			c := peercachetest.New(b, 5)
			ctx := context.Background()
			for i := 0; i < c.Size(); i++ {
				c.Cache(i).Namespace("held",
					peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
			}
			ns := c.Cache(0).Namespace("held")
			val := make([]byte, 32)
			for i := 0; i < keys; i++ {
				if err := ns.Set(ctx, fmt.Sprintf("k%d", i), val, 0); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			// Past the settling window after the last membership change.
			for i := 0; i < 15; i++ {
				c.Advance(time.Second)
				c.Settle()
			}
			rounds := func() (n int64) {
				for i := 0; i < c.Size(); i++ {
					n += c.Cache(i).Stats().PromotionRounds
				}
				return n
			}
			before := rounds()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				c.Advance(time.Second)
				c.Settle()
			}
			b.StopTimer()
			b.ReportMetric(float64(rounds()-before)/float64(b.N), "reconciles/op")
		})
	}
}

// BenchmarkStrayTick is one tick of a settled three-node cluster with one
// out-of-place copy to hand on: the tick a node runs, every second, while it
// works through a backlog of strays left by a rolling restart. ns/op is filing
// the stray, advancing the clock one tick and waiting for the cluster to go
// quiet; scans/op is how many promotion scans that tick started.
//
// A tick like this should cost the hand-off, whatever the node holds. It used
// to rank every replica the node held for ownership as well, under the
// namespace's replica lock; on kind, at 1.3 million replicas a pod, that was a
// third of a core for as long as the backlog lasted.
func BenchmarkStrayTick(b *testing.B) {
	for _, keys := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			f := quietFabric(b)
			ctx := context.Background()
			val := make([]byte, 32)
			for i := 0; i < keys; i++ {
				if err := f.caches[f.ids[i%3]].Namespace("quiet").Set(ctx, fmt.Sprintf("held/%d", i), val, 0); err != nil {
					b.Fatalf("Set: %v", err)
				}
			}
			for i := 0; i < 12; i++ {
				f.clk.Advance(time.Second)
				f.settle(b)
			}
			stray := f.ids[2]
			scans := func() (n int64) {
				for _, id := range f.ids {
					n += f.caches[id].Stats().PromotionScans
				}
				return n
			}
			before := scans()
			b.ResetTimer()
			sent := 0
			for i := 0; sent < b.N; i++ {
				key := fmt.Sprintf("stray/%d", i)
				p, err := f.caches[stray].DescribeKey("quiet", key)
				if err != nil {
					b.Fatal(err)
				}
				if slices.Contains(p.Holders, stray) {
					continue
				}
				sendStray(b, f, p.Holders[0], stray, key, "v")
				f.clk.Advance(time.Second)
				f.settle(b)
				sent++
			}
			b.StopTimer()
			b.ReportMetric(float64(scans()-before)/float64(b.N), "scans/op")
		})
	}
}
