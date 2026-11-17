package tests

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// BenchmarkHintsOverGRPC measures invalidation hints under a burst of writes,
// over real gRPC on loopback: three nodes, one writing keys it owns as fast
// as it can, and every hint delivered or dropped before the clock stops. ns/op
// is a write and its hints to two peers; msgs/hint is how many messages each
// hint cost per peer, and dropped/hint how many hints a full queue discarded.
//
// The end of delivery is found with a sentinel: a key both peers hold a copy
// of, written last. Hints reach a peer in order, so once both read the
// sentinel's new value every hint before it has arrived or been dropped.
func BenchmarkHintsOverGRPC(b *testing.B) {
	if testing.Short() {
		b.Skip("binds loopback sockets")
	}
	nodes := newGRPCCluster(b, 3)
	ctx := context.Background()
	for _, n := range nodes {
		n.cache.Namespace("hints")
	}
	writer := nodes[0]
	ns := writer.cache.Namespace("hints")
	owned := func(n int, prefix string) []string {
		keys := make([]string, 0, n)
		for i := 0; len(keys) < n; i++ {
			k := fmt.Sprintf("%s/%d", prefix, i)
			if p, err := writer.cache.DescribeKey("hints", k); err == nil && p.Owner == writer.id {
				keys = append(keys, k)
			}
		}
		return keys
	}

	sentinel := owned(1, fmt.Sprintf("sentinel%d", b.N))[0]
	if err := ns.Set(ctx, sentinel, []byte("before"), 0); err != nil {
		b.Fatalf("Set: %v", err)
	}
	for _, n := range nodes[1:] {
		if got, err := n.cache.Namespace("hints").Get(ctx, sentinel); err != nil || string(got) != "before" {
			b.Fatalf("priming %s: %q, %v", n.id, got, err)
		}
	}
	keys := owned(b.N, fmt.Sprintf("bench%d", b.N))
	val := make([]byte, 64)
	before := writer.cache.Stats()

	b.ResetTimer()
	for _, k := range keys {
		if err := ns.Set(ctx, k, val, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
	if err := ns.Set(ctx, sentinel, []byte("after"), 0); err != nil {
		b.Fatalf("Set: %v", err)
	}
	for _, n := range nodes[1:] {
		for {
			got, err := n.cache.Namespace("hints").Get(ctx, sentinel)
			if err == nil && string(got) == "after" {
				break
			}
			runtime.Gosched()
		}
	}
	b.StopTimer()

	after := writer.cache.Stats()
	hints := float64(b.N+1) * float64(len(nodes)-1)
	b.ReportMetric(float64(after.InvalidationsSent+after.InvalidationsFailed-
		before.InvalidationsSent-before.InvalidationsFailed)/hints, "msgs/hint")
	b.ReportMetric(float64(after.InvalidationsDropped-before.InvalidationsDropped)/float64(b.N+1), "dropped/hint")
}
