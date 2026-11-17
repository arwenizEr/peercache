package tests

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache"
)

// BenchmarkAsyncFanOutOverGRPC measures how fast a node's asynchronous
// replication drains, over real gRPC on loopback: it writes keys its node owns
// into an RF=3 namespace in chunks small enough for the queue, and waits for
// each chunk's copies to land before the next. ns/op is a write plus its two
// copies; dropped/op, writes whose fan-out the queue refused anyway, should
// stay at zero.
//
// It exists for replicaWorkers: with one sender per node the drain ran at one
// round trip per copy, and on kind the queue in front of it overflowed.
func BenchmarkAsyncFanOutOverGRPC(b *testing.B) {
	if testing.Short() {
		b.Skip("binds loopback sockets")
	}
	nodes := newGRPCCluster(b, 3)
	ctx := context.Background()
	for _, n := range nodes {
		n.cache.Namespace("fanout", peercache.WithReplicationFactor(3))
	}
	writer := nodes[0]
	ns := writer.cache.Namespace("fanout")

	owned := func(n int, prefix string) []string {
		keys := make([]string, 0, n)
		for i := 0; len(keys) < n; i++ {
			k := fmt.Sprintf("%s/%d", prefix, i)
			if p, err := writer.cache.DescribeKey("fanout", k); err == nil && p.Owner == writer.id {
				keys = append(keys, k)
			}
		}
		return keys
	}
	// Warm the connections, as TestAsyncLossWindowOverGRPC does.
	for _, k := range owned(200, "warm") {
		if err := ns.Set(ctx, k, []byte("v"), 0); err != nil {
			b.Fatalf("warm-up: %v", err)
		}
	}
	waitForReplication(b, nodes)

	keys := owned(b.N, fmt.Sprintf("bench%d", b.N))
	val := make([]byte, 64)
	dropped := writer.cache.Stats().ReplicationsDropped
	const chunk = 1024 // a quarter of the queue, so hashing never overfills a worker
	b.ResetTimer()
	for start := 0; start < len(keys); start += chunk {
		end := min(start+chunk, len(keys))
		want := totalReplicaKeys(nodes) + int64(end-start)*2
		for _, k := range keys[start:end] {
			if err := ns.Set(ctx, k, val, 0); err != nil {
				b.Fatalf("Set: %v", err)
			}
		}
		for totalReplicaKeys(nodes) < want {
			if writer.cache.Stats().ReplicationsDropped != dropped {
				b.Fatal("the queue dropped a chunk sized to fit it")
			}
			runtime.Gosched()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(writer.cache.Stats().ReplicationsDropped-dropped)/float64(b.N), "dropped/op")
}

// BenchmarkSyncSetOverGRPC is a synchronous write at replication factor 3 over
// real gRPC on loopback: the store write and both copies, acknowledged. ns/op
// is one write; the copies leave at once, so it is about one round trip rather
// than two.
func BenchmarkSyncSetOverGRPC(b *testing.B) {
	if testing.Short() {
		b.Skip("binds loopback sockets")
	}
	nodes := newGRPCCluster(b, 3)
	ctx := context.Background()
	for _, n := range nodes {
		n.cache.Namespace("sync", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	writer := nodes[0]
	ns := writer.cache.Namespace("sync")
	keys := make([]string, 0, 4096)
	for i := 0; len(keys) < cap(keys); i++ {
		k := fmt.Sprintf("sync/%d", i)
		if p, err := writer.cache.DescribeKey("sync", k); err == nil && p.Owner == writer.id && len(p.Holders) == 3 {
			keys = append(keys, k)
		}
	}
	val := make([]byte, 64)
	for _, k := range keys[:200] { // warm the connections
		if err := ns.Set(ctx, k, val, 0); err != nil {
			b.Fatalf("warm-up: %v", err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.Set(ctx, keys[i%len(keys)], val, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}
