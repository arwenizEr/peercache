package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/transport"
)

// TestAFailedSyncWriteStillInvalidates is open question 45: a synchronous write
// that cannot reach a replica reports the failure with the value already
// stored on its owner, and must still tell the other pods their copies are
// stale. It used to return before enqueuing the invalidation, so a degraded
// write was staler than an asynchronous one until every pod's L1 TTL passed.
//
// The TTL here is long and the clock never moves, so the only thing that can
// show the reader the new value is the invalidation.
func TestAFailedSyncWriteStillInvalidates(t *testing.T) {
	ops := []struct {
		name  string
		prime string
		write func(ns peercache.Namespace, key string) error
		check func(t *testing.T, val []byte, err error)
	}{
		{"Set", "v1",
			func(ns peercache.Namespace, key string) error {
				return ns.Set(context.Background(), key, []byte("v2"), 0)
			},
			func(t *testing.T, val []byte, err error) {
				if err != nil || string(val) != "v2" {
					t.Fatalf("reader after a degraded Set = %q, %v; want v2", val, err)
				}
			}},
		{"Delete", "v1",
			func(ns peercache.Namespace, key string) error {
				return ns.Delete(context.Background(), key)
			},
			func(t *testing.T, val []byte, err error) {
				if !errors.Is(err, peercache.ErrNotFound) {
					t.Fatalf("reader after a degraded Delete = %q, %v; want ErrNotFound", val, err)
				}
			}},
		{"Increment", "1",
			func(ns peercache.Namespace, key string) error {
				_, err := ns.Increment(context.Background(), key, 1, 0)
				return err
			},
			func(t *testing.T, val []byte, err error) {
				if err != nil || string(val) != "2" {
					t.Fatalf("reader after a degraded Increment = %q, %v; want 2", val, err)
				}
			}},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			r := newReplicaFabric(t, replicaFabricConfig{nodes: 3, rf: 2, sync: true})
			for _, id := range r.ids {
				r.caches[id].Namespace("plain", peercache.WithReplicationFactor(2),
					peercache.WithSyncReplication(), peercache.WithL1TTL(30*time.Second))
			}
			key := "q45/" + op.name
			holders := r.holders(t, key)
			owner, replica := holders[0], holders[1]
			var reader string
			for _, id := range r.ids {
				if id != owner && id != replica {
					reader = id
				}
			}

			primed := r.caches[owner].Stats().InvalidationsSent
			if err := r.caches[owner].Namespace("plain").Set(context.Background(), key, []byte(op.prime), 0); err != nil {
				t.Fatalf("priming Set: %v", err)
			}
			// The priming write's own invalidations must be out of the way,
			// or they could satisfy the wait for the degraded write's.
			waitFor(t, "the priming invalidations", func() bool {
				return r.caches[owner].Stats().InvalidationsSent >= primed+2
			})
			if v, err := r.caches[reader].Namespace("plain").Get(context.Background(), key); err != nil || string(v) != op.prime {
				t.Fatalf("priming read = %q, %v", v, err)
			}
			if n := r.caches[reader].Stats().L1Entries; n != 1 {
				t.Fatalf("reader holds %d L1 entries, want its copy of the key", n)
			}

			r.filters[owner].set(func(to, method string) error {
				if to == replica && method == transport.MethodReplicate {
					return fmt.Errorf("test refuses replication: %w", core.ErrNodeUnreachable)
				}
				return nil
			})
			sent := r.caches[owner].Stats().InvalidationsSent
			if err := op.write(r.caches[owner].Namespace("plain"), key); !errors.Is(err, core.ErrNodeUnreachable) {
				t.Fatalf("degraded %s = %v; want the replication failure reported", op.name, err)
			}
			waitFor(t, "the invalidation to be delivered", func() bool {
				return r.caches[owner].Stats().InvalidationsSent >= sent+2
			})

			val, err := r.caches[reader].Namespace("plain").Get(context.Background(), key)
			op.check(t, val, err)
		})
	}
}
