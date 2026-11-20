package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// BenchmarkPersistentReplicatedSet is a synchronous write on its owner in a
// persistent namespace at replication factor 3: store, record, and two copies
// placed, each carrying the owner's durable bound (open question 68). The
// owner has caught up, so the bound is read on every copy sent.
func BenchmarkPersistentReplicatedSet(b *testing.B) {
	f := newBenchFabric(b, 5)
	ctx := context.Background()
	store := persist.Memory()
	for _, id := range f.ids {
		f.caches[id].Namespace("durable", peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(), peercache.WithPersistence(store))
	}
	owner := f.ids[0]
	_ = f.caches[owner].CatchUp(ctx)
	keys := make([]string, 0, 1024)
	for i := 0; len(keys) < cap(keys); i++ {
		if k := fmt.Sprintf("w/%d", i); f.owner(k) == owner {
			keys = append(keys, k)
		}
	}
	ns := f.caches[owner].Namespace("durable")
	val := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ns.Set(ctx, keys[i%len(keys)], val, 0); err != nil {
			b.Fatalf("Set: %v", err)
		}
	}
}
