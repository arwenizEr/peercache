package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/transport"
)

// TestAFailedSyncReplicationReportsTheWriteAsHeld is open question 37's option
// 2, found by M16's real-world run: a synchronous write applied on its owner
// whose replication then fails is reported as ErrUnderReplicated — "written,
// held by fewer nodes than its factor" — not as an opaque failure. An
// application deciding whether to retry a non-idempotent operation needs to
// know the write happened, and so the sentinel must survive the hop a write
// from a non-owner takes to its owner and back.
func TestAFailedSyncReplicationReportsTheWriteAsHeld(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 3, rf: 2, sync: true})
	ctx := context.Background()

	for i, via := range []string{"owner", "forwarded"} {
		t.Run(via, func(t *testing.T) {
			key := fmt.Sprintf("held-%d", i)
			holders := r.holders(t, key)
			owner, replica := holders[0], holders[1]
			writer := owner
			if via == "forwarded" {
				for _, id := range r.ids {
					if id != owner && id != replica {
						writer = id
					}
				}
			}
			r.filters[owner].set(func(to, method string) error {
				if to == replica && method == transport.MethodReplicate {
					return fmt.Errorf("test refuses replication: %w", core.ErrNodeUnreachable)
				}
				return nil
			})
			t.Cleanup(func() { r.filters[owner].set(nil) })
			degraded := r.caches[owner].Stats().DegradedWrites

			err := r.caches[writer].Namespace("hot").Set(ctx, key, []byte("v"), 0)
			if !errors.Is(err, peercache.ErrUnderReplicated) {
				t.Fatalf("a sync write whose replica refused returned %v via %s; want ErrUnderReplicated, "+
					"which says the write is held", err, via)
			}
			if v, gerr := r.caches[owner].Namespace("hot").Get(ctx, key); gerr != nil || string(v) != "v" {
				t.Fatalf("the owner holds %q, %v; ErrUnderReplicated must mean the write happened", v, gerr)
			}
			if got := r.caches[owner].Stats().DegradedWrites; got != degraded+1 {
				t.Fatalf("DegradedWrites %d -> %d; a write held below its factor is a degraded write", degraded, got)
			}
		})
	}
}
