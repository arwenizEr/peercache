package tests

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// benchFabric is the smallest fabric a benchmark can use: n caches on one fake
// network, every transport counted. The fabric the tests use takes a
// *testing.T throughout, and a benchmark has a *testing.B.
type benchFabric struct {
	ids      []string
	caches   map[string]peercache.Cache
	counters map[string]*countingTransport
}

func newBenchFabric(b *testing.B, n int) *benchFabric {
	b.Helper()
	clk := clock.NewFake(clock.Epoch)
	net := testkit.NewNetwork(clk, 1)
	reg := testkit.NewRegistry()
	mcfg := testkit.MembershipConfig{FailureTimeout: testkit.DefaultFailureTimeout, JoinDelay: testkit.DefaultJoinDelay}

	f := &benchFabric{caches: map[string]peercache.Cache{}, counters: map[string]*countingTransport{}}
	nodes := map[string]core.Node{}
	members := map[string]*testkit.Membership{}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		nodes[id] = core.Node{ID: id, Addr: id + ":7946"}
		f.ids = append(f.ids, id)
		members[id] = testkit.NewMembership(nodes[id], net, clk, reg, mcfg)
	}
	for _, id := range f.ids {
		ct := &countingTransport{Transport: net.Transport(nodes[id]), counts: map[string]int{}}
		f.counters[id] = ct
		c, err := peercache.New(
			peercache.WithNodeID(id),
			peercache.WithClock(clk),
			peercache.WithMembership(members[id]),
			peercache.WithTransport(ct),
			peercache.WithActivationDelay(-1),
		)
		if err != nil {
			b.Fatalf("New(%s): %v", id, err)
		}
		f.caches[id] = c
	}
	clk.Advance(2 * testkit.DefaultJoinDelay)
	for i := 0; ; i++ {
		settled := true
		for _, id := range f.ids {
			if f.caches[id].Stats().Members != n {
				settled = false
			}
		}
		if settled {
			break
		}
		if i > 5_000_000 {
			b.Fatal("membership did not converge")
		}
		runtime.Gosched()
	}
	b.Cleanup(func() {
		for _, id := range f.ids {
			_ = f.caches[id].Close(context.Background())
		}
		net.Close()
	})
	return f
}

// owner finds a key's owner by asking each node whether it may activate the
// key's group, as fabric.ownerOf does.
func (f *benchFabric) owner(key string) string {
	for _, id := range f.ids {
		if _, err := f.caches[id].Group(key).Keys(context.Background()); err == nil {
			return id
		}
	}
	return ""
}

// BenchmarkRemoteGetOneHotKey is every goroutine on one node reading one key
// owned elsewhere, with the L1 tier disabled so that every read is a miss. It
// is the worst case decision 0028 exists for: rpcs/op is how many requests
// the owner receives per read. Before coalescing it was 1.
func BenchmarkRemoteGetOneHotKey(b *testing.B) {
	f := newBenchFabric(b, 3)
	const key = "hot"
	owner := f.owner(key)
	reader := f.ids[0]
	if reader == owner {
		reader = f.ids[1]
	}
	ctx := context.Background()
	if err := f.caches[owner].Namespace("bench", peercache.WithL1TTL(-1)).Set(ctx, key, make([]byte, 64), 0); err != nil {
		b.Fatalf("Set: %v", err)
	}
	ns := f.caches[reader].Namespace("bench", peercache.WithL1TTL(-1))

	before := f.counters[reader].get(transport.MethodGet)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := ns.Get(ctx, key); err != nil {
				b.Errorf("Get: %v", err)
				return
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(f.counters[reader].get(transport.MethodGet)-before)/float64(b.N), "rpcs/op")
}

// BenchmarkColdRemoteGet reads distinct keys owned elsewhere once each, so
// every read is an L1 miss that costs one round trip. With replica reads that
// trip goes to whichever holder the reader is assigned; owner_rpcs/op is the
// share that still reaches an owner.
func BenchmarkColdRemoteGet(b *testing.B) {
	for _, mode := range []struct {
		name string
		opts []peercache.NamespaceOption
	}{
		{"owner", []peercache.NamespaceOption{peercache.WithReplicationFactor(3), peercache.WithSyncReplication()}},
		{"replica-reads", []peercache.NamespaceOption{peercache.WithReplicationFactor(3), peercache.WithSyncReplication(),
			peercache.WithReplicaReads()}},
	} {
		b.Run(mode.name, func(b *testing.B) {
			f := newBenchFabric(b, 5)
			ctx := context.Background()
			for _, id := range f.ids {
				f.caches[id].Namespace("cold", mode.opts...)
			}
			reader := f.ids[0]
			keys := make([]string, 0, b.N)
			for i := 0; len(keys) < b.N; i++ {
				key := fmt.Sprintf("cold/%d", i)
				owner := f.owner(key)
				if owner == reader {
					continue
				}
				if err := f.caches[owner].Namespace("cold").Set(ctx, key, make([]byte, 64), 0); err != nil {
					b.Fatalf("Set: %v", err)
				}
				keys = append(keys, key)
			}
			// Every setup write queued an invalidation to every peer.
			// Left in flight they are delivered during the timed loop
			// and, measured, were a tenth of its CPU; the heap the setup
			// built is the rest of what does not belong to a read.
			want := int64(len(keys) * (len(f.ids) - 1))
			for i := 0; ; i++ {
				var done int64
				for _, id := range f.ids {
					s := f.caches[id].Stats()
					done += s.InvalidationsSent + s.InvalidationsFailed + s.InvalidationsDropped
				}
				if done >= want {
					break
				}
				if i > 50_000_000 {
					b.Fatalf("invalidations did not drain: %d of %d", done, want)
				}
				runtime.Gosched()
			}
			runtime.GC()

			ns := f.caches[reader].Namespace("cold")
			before := f.counters[reader].get(transport.MethodGet)
			rbefore := f.counters[reader].get(transport.MethodReplicaGet)
			var referred, served int64
			for _, id := range f.ids {
				referred -= f.caches[id].Stats().ReplicaReadsReferred
				served -= f.caches[id].Stats().ReplicaReadsServed
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := ns.Get(ctx, keys[i]); err != nil {
					b.Fatalf("Get: %v", err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(f.counters[reader].get(transport.MethodGet)-before)/float64(b.N), "owner_rpcs/op")
			b.ReportMetric(float64(f.counters[reader].get(transport.MethodReplicaGet)-rbefore)/float64(b.N), "replica_rpcs/op")
			for _, id := range f.ids {
				referred += f.caches[id].Stats().ReplicaReadsReferred
				served += f.caches[id].Stats().ReplicaReadsServed
			}
			b.ReportMetric(float64(served)/float64(b.N), "served/op")
			b.ReportMetric(float64(referred)/float64(b.N), "declined/op")
		})
	}
}
