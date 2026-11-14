package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// remoteKey returns a key node via does not own.
func remoteKey(t *testing.T, f *fabric, via string) string {
	t.Helper()
	for _, k := range []string{"s:a", "s:b", "s:c", "s:d", "s:e", "s:f", "s:g", "s:h"} {
		if f.ownerOf(t, k) != via {
			return k
		}
	}
	t.Fatal("no key owned elsewhere")
	return ""
}

// TestANonOwnedCopyDoesNotOutliveItsKey: a session with one second left is
// read through a node that does not own it, which caches it in L1. When the
// session expires on its owner, the copy must go with it — not linger for the
// rest of the namespace's five-second L1 TTL. Found writing peercachetest.
func TestANonOwnedCopyDoesNotOutliveItsKey(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("sessions")
	}
	via := f.ids[0]
	key := remoteKey(t, f, via)
	if err := f.caches[f.ownerOf(t, key)].Namespace("sessions").Set(ctx, key, []byte("v"), time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns := f.caches[via].Namespace("sessions")
	if _, err := ns.Get(ctx, key); err != nil {
		t.Fatalf("Get through %s: %v", via, err)
	}
	if got := f.caches[via].Stats().L1Entries; got != 1 {
		t.Fatalf("the non-owner holds %d L1 copies, want 1", got)
	}
	f.clk.Advance(time.Second)
	if got, err := ns.Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get through %s after the key expired = %q, %v; its L1 copy outlived it", via, got, err)
	}
	got, err := ns.BatchGet(ctx, []string{key})
	if err != nil || len(got) != 0 {
		t.Fatalf("BatchGet after the key expired = %q, %v", got, err)
	}
}

// TestABatchReadCopyDoesNotOutliveItsKey is the same through BatchGet, whose
// entries carry each key's remaining lifetime.
func TestABatchReadCopyDoesNotOutliveItsKey(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("sessions")
	}
	via := f.ids[0]
	key := remoteKey(t, f, via)
	if err := f.caches[f.ownerOf(t, key)].Namespace("sessions").Set(ctx, key, []byte("v"), time.Second); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns := f.caches[via].Namespace("sessions")
	if got, err := ns.BatchGet(ctx, []string{key}); err != nil || len(got) != 1 {
		t.Fatalf("BatchGet = %q, %v", got, err)
	}
	f.clk.Advance(time.Second)
	if got, err := ns.Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get after the key expired = %q, %v; the batch's L1 copy outlived it", got, err)
	}
}

// TestALoadedCopyDoesNotOutliveItsKey: a value loaded on its owner with a
// one-second TTL, fetched through a non-owner's GetOrLoad, is loaded again
// once it expires — not served from the non-owner's L1 for five seconds.
func TestALoadedCopyDoesNotOutliveItsKey(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	var loads atomic.Int64
	for _, id := range f.ids {
		f.caches[id].Namespace("profiles", peercache.WithLoader(
			func(context.Context, string) ([]byte, time.Duration, error) {
				loads.Add(1)
				return []byte("p"), time.Second, nil
			}))
	}
	via := f.ids[0]
	key := remoteKey(t, f, via)
	ns := f.caches[via].Namespace("profiles")
	if _, err := ns.GetOrLoad(ctx, key); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	f.clk.Advance(time.Second)
	if _, err := ns.GetOrLoad(ctx, key); err != nil {
		t.Fatalf("GetOrLoad after expiry: %v", err)
	}
	if got := loads.Load(); got != 2 {
		t.Fatalf("the loader ran %d times; the expired value was served from L1 instead of reloaded", got)
	}
}
