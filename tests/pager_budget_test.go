package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestACatchUpPassServedHereIsChargedToTheBudget: the sorted key list a node
// keeps for a catch-up pass it serves is memory the budget counts. On kind a
// node serving a partition heal's catch-ups held 33 MB of such lists while its
// budget read 461 MB against a live heap of 567, and it was OOM-killed.
func TestACatchUpPassServedHereIsChargedToTheBudget(t *testing.T) {
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n0", peercache.WithMemoryBudget(1<<30)))
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("paged", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)
	ctx := context.Background()
	const n = 20_000
	written := 0
	for i := 0; written < n; i++ {
		k := fmt.Sprintf("k%06d", i)
		if f.ownerOf(t, k) != "n0" {
			continue
		}
		if err := open("n0").Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		written++
	}
	f.settle(t)

	before := f.caches["n0"].Stats().MemoryUsed
	if page := catchUpPage(t, f, "n1", "n0", "paged", 2, false); len(page) == 0 {
		t.Fatal("the first page of the pass was empty")
	}
	if grew, want := f.caches["n0"].Stats().MemoryUsed-before, int64(n*16); grew < want {
		t.Fatalf("serving a %d-key catch-up pass charged %d bytes, want at least %d for its key list",
			n, grew, want)
	}
}
