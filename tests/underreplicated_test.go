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

// TestAForwardedHeldWriteKeepsItsResult is the result half of the same report.
// A write forwarded to its owner whose replication failed has still happened,
// and its result is the only way the caller can act on it: an Increment's new
// value decides "out of stock", a SetIfAbsent's true decides "this request
// created the order". Found by M16: across the hop both came back as zero
// values beside ErrUnderReplicated, because the owner answered with an error
// and no body.
func TestAForwardedHeldWriteKeepsItsResult(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 3, rf: 2, sync: true})
	ctx := context.Background()

	// writerFor arranges key's owner to refuse replication to its replica,
	// and returns the third node, which forwards.
	writerFor := func(t *testing.T, key string) peercache.Namespace {
		t.Helper()
		holders := r.holders(t, key)
		owner, replica := holders[0], holders[1]
		r.filters[owner].set(func(to, method string) error {
			if to == replica && method == transport.MethodReplicate {
				return fmt.Errorf("test refuses replication: %w", core.ErrNodeUnreachable)
			}
			return nil
		})
		t.Cleanup(func() { r.filters[owner].set(nil) })
		for _, id := range r.ids {
			if id != owner && id != replica {
				return r.caches[id].Namespace("hot")
			}
		}
		t.Fatal("no third node")
		return nil
	}

	t.Run("Increment", func(t *testing.T) {
		ns := writerFor(t, "count")
		v, err := ns.Increment(ctx, "count", 7, 0)
		if !errors.Is(err, peercache.ErrUnderReplicated) || v != 7 {
			t.Fatalf("forwarded held Increment returned %d, %v; want 7 and ErrUnderReplicated", v, err)
		}
	})
	t.Run("SetIfAbsent", func(t *testing.T) {
		ns := writerFor(t, "claim")
		stored, err := ns.SetIfAbsent(ctx, "claim", []byte("x"), 0)
		if !errors.Is(err, peercache.ErrUnderReplicated) || !stored {
			t.Fatalf("forwarded held SetIfAbsent returned %v, %v; want true and ErrUnderReplicated", stored, err)
		}
	})
	t.Run("CompareAndSwap", func(t *testing.T) {
		if err := r.caches[r.ids[0]].Namespace("hot").Set(ctx, "swap", []byte("a"), 0); err != nil {
			t.Fatal(err)
		}
		ns := writerFor(t, "swap")
		swapped, err := ns.CompareAndSwap(ctx, "swap", []byte("a"), []byte("b"), 0)
		if !errors.Is(err, peercache.ErrUnderReplicated) || !swapped {
			t.Fatalf("forwarded held CompareAndSwap returned %v, %v; want true and ErrUnderReplicated", swapped, err)
		}
	})
	t.Run("Touch", func(t *testing.T) {
		if err := r.caches[r.ids[0]].Namespace("hot").Set(ctx, "touch", []byte("a"), 0); err != nil {
			t.Fatal(err)
		}
		ns := writerFor(t, "touch")
		existed, err := ns.Touch(ctx, "touch", time.Hour)
		if !errors.Is(err, peercache.ErrUnderReplicated) || !existed {
			t.Fatalf("forwarded held Touch returned %v, %v; want true and ErrUnderReplicated", existed, err)
		}
	})
}
