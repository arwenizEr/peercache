package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// newForkFabric is a three-node fabric whose transports can refuse calls, with
// "fork" open at synchronous RF 3 everywhere.
func newForkFabric(t *testing.T) (*fabric, map[string]*filteringTransport, func(string) peercache.Namespace) {
	t.Helper()
	filters := map[string]*filteringTransport{}
	var mu sync.Mutex
	f := newFabricFull(t, 3, true, testkit.MembershipConfig{}, -1,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			ft := &filteringTransport{Transport: tr}
			mu.Lock()
			filters[id] = ft
			mu.Unlock()
			return ft
		}, nil))
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("fork", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	return f, filters, open
}

// TestAWriteOverAValueItNeverReceivedIsAFork is split-brain detection's
// positive case, made deterministic. An owner's write reaches one replica and
// not the other; the owner dies; the replica that missed it is promoted and
// writes over the older value it does hold. The replica that has the missed
// write now receives a copy that did not replace it: the key's history forked,
// an acknowledged write was overwritten unseen, and that replica is the only
// node that can tell.
func TestAWriteOverAValueItNeverReceivedIsAFork(t *testing.T) {
	f, filters, open := newForkFabric(t)
	ctx := context.Background()
	const key = "fork/k"
	p, err := f.caches[f.ids[0]].DescribeKey("fork", key)
	if err != nil || len(p.Holders) != 3 {
		t.Fatalf("DescribeKey: %v, %v", p.Holders, err)
	}
	owner, next, last := p.Holders[0], p.Holders[1], p.Holders[2]

	if err := open(owner).Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("Set v1: %v", err)
	}
	filters[owner].set(func(to, method string) error {
		if to == next && method == transport.MethodReplicate {
			return fmt.Errorf("test drops it: %w", core.ErrNodeUnreachable)
		}
		return nil
	})
	if err := open(owner).Set(ctx, key, []byte("v2"), 0); !errors.Is(err, peercache.ErrUnderReplicated) {
		t.Fatalf("Set v2 with one replica refusing: %v, want ErrUnderReplicated", err)
	}
	filters[owner].set(nil)

	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	before := f.caches[last].Stats().SplitBrainDetections
	if err := open(next).Set(ctx, key, []byte("v3"), 0); err != nil {
		t.Fatalf("Set v3 by the promoted replica: %v", err)
	}
	if got := f.caches[last].Stats().SplitBrainDetections - before; got != 1 {
		t.Fatalf("%s holds v2 and received v3, which replaced v1 instead; it counted %d forks, want 1", last, got)
	}
	// The writer is told too: the replica answers its replication with the
	// conflict, which is how an owner learns that its write forked a copy.
	if got := f.caches[next].Stats().SplitBrainDetections; got != 1 {
		t.Fatalf("the promoted owner counted %d forks; the replica's answer should have told it of 1", got)
	}
}

// TestARollingRestartCountsNoFork is the false positive the epoch rule
// produced by the tens of thousands on kind. An owner leaves, an interim owner
// writes, the owner returns with a smaller epoch, catches up and writes again:
// every write replaced the one before it, and no node may call that a split.
func TestARollingRestartCountsNoFork(t *testing.T) {
	f, _, open := newForkFabric(t)
	ctx := context.Background()
	const key = "fork/r"
	owner := f.ownerOf(t, key)
	for i := 0; i < 3; i++ {
		if err := open(owner).Set(ctx, key, []byte(fmt.Sprintf("a%d", i)), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	interim := f.live()[0]
	for i := 0; i < 3; i++ {
		if err := open(interim).Set(ctx, key, []byte(fmt.Sprintf("b%d", i)), 0); err != nil {
			t.Fatalf("Set during the absence: %v", err)
		}
	}
	f.revive(t, owner)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(owner)
	if err := f.caches[owner].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := open(owner).Set(ctx, key, []byte(fmt.Sprintf("c%d", i)), 0); err != nil {
			t.Fatalf("Set after the return: %v", err)
		}
	}
	for _, id := range f.ids {
		if n := f.caches[id].Stats().SplitBrainDetections; n != 0 {
			t.Fatalf("%s counted %d forks in a history where every write replaced the one before", id, n)
		}
	}
}
