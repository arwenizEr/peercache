package tests

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// quietFabric is a settled three-node fabric with an RF=2 namespace "quiet"
// holding a few hundred keys, its clock moved well past the last membership
// change.
func quietFabric(t testing.TB, opts ...fabricOption) *fabric {
	t.Helper()
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1, opts...)
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("quiet", peercache.WithReplicationFactor(2))
	}
	for i := 0; i < 300; i++ {
		if err := f.caches[f.ids[i%3]].Namespace("quiet").Set(ctx, fmt.Sprintf("q%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	// Past the ten-second settling window after the last view.
	for i := 0; i < 12; i++ {
		f.clk.Advance(time.Second)
		f.settle(t)
	}
	return f
}

// TestAnIdleTickDoesNotReconcile: on a cluster whose membership has not
// changed for a while and where nothing has arrived out of place, a tick does
// no reconciliation.
//
// Every tick used to run a full reconciliation, which computes a ranking for
// every replica the node holds, twice. It found nothing to do, and cost time
// in proportion to what the node held: about 130 ns an entry, 8 ms a tick at
// 100,000 keys in five nodes, and on kind, at about 400,000 keys a pod, a
// fifteenth of the throughput of the same cluster empty.
func TestAnIdleTickDoesNotReconcile(t *testing.T) {
	f := quietFabric(t)
	before := map[string]int64{}
	for _, id := range f.ids {
		before[id] = f.caches[id].Stats().PromotionRounds
	}
	for i := 0; i < 5; i++ {
		f.clk.Advance(time.Second)
		f.settle(t)
	}
	for _, id := range f.ids {
		if n := f.caches[id].Stats().PromotionRounds - before[id]; n != 0 {
			t.Errorf("%s ran %d reconciliations in five idle ticks", id, n)
		}
	}
}

// TestAStrayReplicaIsHandedOnOnAQuietCluster: a replica that arrives at a node
// that does not rank for it, on a cluster that is otherwise quiet, still moves
// to the nodes that do on the next tick.
//
// Skipping idle ticks must not skip this. The periodic pass exists for copies
// that arrive where the receiver's ranking does not put them — a hand-off sent
// under a sender's older view is the usual one — and no membership change
// follows to reconcile them.
func TestAStrayReplicaIsHandedOnOnAQuietCluster(t *testing.T) {
	f := quietFabric(t)
	ctx := context.Background()

	const key = "stray"
	p, err := f.caches[f.ids[0]].DescribeKey("quiet", key)
	if err != nil || len(p.Holders) != 2 {
		t.Fatalf("DescribeKey: %v, %v", p.Holders, err)
	}
	owner := p.Holders[0]
	var stray string
	for _, id := range f.ids {
		if !slices.Contains(p.Holders, id) {
			stray = id
		}
	}

	sendStray(t, f, owner, stray, key, "only here")
	held := f.caches[stray].Stats().ReplicaKeys

	f.clk.Advance(time.Second)
	f.settle(t)
	waitForCache(t, "the stray copy to leave "+stray, func() bool {
		return f.caches[stray].Stats().ReplicaKeys < held
	})
	got, err := f.caches[owner].Namespace("quiet").Get(ctx, key)
	if err != nil || string(got) != "only here" {
		t.Fatalf("the owner %s reads %q, %v; the stray copy was dropped rather than handed on", owner, got, err)
	}
}

// sendStray files a copy of key at stray as if owner had replicated it there.
func sendStray(t testing.TB, f *fabric, owner, stray, key, value string) {
	t.Helper()
	client, err := f.net.Transport(f.nodes[owner]).Dial(f.nodes[stray])
	if err != nil {
		t.Fatal(err)
	}
	req, err := wire.Encode(&wire.ReplicateRequest{
		Namespace: "quiet",
		Key:       key,
		Value:     []byte(value),
		Stamp:     core.Stamp{Epoch: uint64(f.caches[owner].Stats().Epoch), Version: 1},
		From:      owner,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Invoke(context.Background(), transport.MethodReplicate, req); err != nil {
		t.Fatalf("Replicate to %s: %v", stray, err)
	}
}

// TestAStrayBacklogLargerThanTheHandOffQueueIsHandedOn: more out-of-place
// copies than the hand-off queue holds arrive at a node on a quiet cluster.
// A tick drains only what the queue has room for, so the rest must bring the
// next tick back rather than wait for a membership change.
func TestAStrayBacklogLargerThanTheHandOffQueueIsHandedOn(t *testing.T) {
	f := quietFabric(t)
	stray := f.ids[2]
	baseline := f.caches[stray].Stats().ReplicaKeys

	const strays = 4096 + 300 // the hand-off queue holds 4,096
	sent := 0
	for i := 0; sent < strays; i++ {
		key := fmt.Sprintf("backlog/%d", i)
		p, err := f.caches[stray].DescribeKey("quiet", key)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(p.Holders, stray) {
			continue
		}
		sendStray(t, f, p.Holders[0], stray, key, "v")
		sent++
	}

	for i := 0; i < 6 && f.caches[stray].Stats().ReplicaKeys > baseline; i++ {
		f.clk.Advance(time.Second)
		f.settle(t)
		waitForCache(t, "the hand-off queue to drain", func() bool {
			s := f.caches[stray].Stats()
			return s.ReplicaKeys == baseline || s.ReplicasRehomed >= int64(min(strays, 4096*(i+1)))
		})
	}
	if n := f.caches[stray].Stats().ReplicaKeys - baseline; n != 0 {
		t.Fatalf("%d of %d stray copies are still on %s after six ticks", n, strays, stray)
	}
}

// TestACopyAHandOffPutsBackIsTriedAgainOnAQuietCluster: a hand-off that cannot
// place a copy puts it back, and on a quiet cluster the next tick must try it
// again rather than leave it until a membership change.
func TestACopyAHandOffPutsBackIsTriedAgainOnAQuietCluster(t *testing.T) {
	held := map[string]*heldReplicas{}
	f := quietFabric(t, withWrappers(func(id string, tr core.Transport) core.Transport {
		held[id] = newHeldReplicas(tr)
		return held[id]
	}, nil))
	ctx := context.Background()

	const key = "put-back"
	p, err := f.caches[f.ids[0]].DescribeKey("quiet", key)
	if err != nil {
		t.Fatal(err)
	}
	owner := p.Holders[0]
	var stray string
	for _, id := range f.ids {
		if !slices.Contains(p.Holders, id) {
			stray = id
		}
	}
	sendStray(t, f, owner, stray, key, "kept")
	baseline := f.caches[stray].Stats().ReplicaKeys

	held[stray].refuse.Store(true)
	f.clk.Advance(time.Second)
	f.settle(t)
	waitForCache(t, "the hand-off to fail and put the copy back", func() bool {
		return f.caches[stray].Stats().ReplicasRetained > 0
	})
	held[stray].refuse.Store(false)

	f.clk.Advance(time.Second)
	f.settle(t)
	waitForCache(t, "the copy to be handed on at the next tick", func() bool {
		return f.caches[stray].Stats().ReplicaKeys < baseline
	})
	got, err := f.caches[owner].Namespace("quiet").Get(ctx, key)
	if err != nil || string(got) != "kept" {
		t.Fatalf("the owner %s reads %q, %v", owner, got, err)
	}
}

// TestHandOffsRunInParallel: a node with many out-of-place copies hands
// several of them on at once rather than one after another.
//
// One sender moved one key per round trip to each of its new holders: on kind,
// about 400 a second from a pod a rolling restart had left with 117,000 strays,
// five minutes to drain. Here the stray node's Replicate requests are held
// where they stand, so the hand-offs in flight are counted.
func TestHandOffsRunInParallel(t *testing.T) {
	held := map[string]*heldReplicas{}
	f := quietFabric(t, withWrappers(func(id string, tr core.Transport) core.Transport {
		held[id] = newHeldReplicas(tr)
		return held[id]
	}, nil))
	stray := f.ids[2]

	sent := 0
	for i := 0; sent < 64; i++ {
		key := fmt.Sprintf("parallel/%d", i)
		p, err := f.caches[stray].DescribeKey("quiet", key)
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(p.Holders, stray) {
			continue
		}
		sendStray(t, f, p.Holders[0], stray, key, "v")
		sent++
	}

	held[stray].hold()
	defer held[stray].open()
	f.clk.Advance(time.Second)
	waitForCache(t, "two hand-offs to be in flight at once", func() bool {
		return held[stray].inFlight.Load() >= 2
	})
}
