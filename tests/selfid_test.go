package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// selfCounting counts calls to Self.
type selfCounting struct {
	core.MembershipAuthority
	calls *atomic.Int64
}

func (m selfCounting) Self() core.Node {
	m.calls.Add(1)
	return m.MembershipAuthority.Self()
}

// TestAWriteDoesNotAskMembershipWhoItIs: a node's ID is read once, when its
// cache is made. Self clones the whole node, metadata map and all, and a
// replicated write used to ask for it a dozen times on the writer alone: over
// gRPC those clones were 7% of a synchronous write's allocations.
func TestAWriteDoesNotAskMembershipWhoItIs(t *testing.T) {
	var calls atomic.Int64
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, ownership.DefaultActivationDelay,
		withWrappers(nil, func(id string, m core.MembershipAuthority) core.MembershipAuthority {
			return selfCounting{MembershipAuthority: m, calls: &calls}
		}))
	ns := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("selfid",
			peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		ns(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	ctx := context.Background()
	const writes = 50
	before := calls.Load()
	for i := 0; i < writes; i++ {
		// From every node, so owned and forwarded writes both count.
		id := f.ids[i%len(f.ids)]
		if err := writeWithRetry(t, f, ns, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatalf("write from %s: %v", id, err)
		}
		if _, err := ns(id).Get(ctx, fmt.Sprintf("k%d", i)); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if n := calls.Load() - before; n != 0 {
		t.Fatalf("%d writes and reads asked membership for this node %d times; its ID is known", writes, n)
	}
}
