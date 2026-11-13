package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// TestAHandOffBacklogLargerThanItsQueueIsNotChurned is M16's rolling restart.
//
// A node joining displaces existing holders from the ranking of part of the
// keyspace, and every displaced copy is an orphan to hand on. The hand-off
// queue is bounded, and reconciliation used to drain every orphan at once:
// whatever did not fit was put straight back, counted as retained, and drained
// again on the next round, a second later. On kind one pod put 10.5 million
// copies back to rehome 97,000. An orphan that does not fit belongs where it
// is until there is room, not in a loop around the queue.
//
// The worker is held on its first hand-off so that the queue fills, the way a
// slow network and a busy receiver fill it on a real cluster.
func TestAHandOffBacklogLargerThanItsQueueIsNotChurned(t *testing.T) {
	filters := map[string]*filteringTransport{}
	var mu sync.Mutex
	f := newFabricFull(t, 4, true, testkit.MembershipConfig{}, -1,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			ft := &filteringTransport{Transport: tr}
			mu.Lock()
			filters[id] = ft
			mu.Unlock()
			return ft
		}, nil))
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("backlog",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	joiner := f.ids[3]
	f.hardKill(t, joiner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	f.settle(t)

	const keys = 60000 // about 10,000 orphans a sender, over its 4,096-slot queue
	writer := f.live()[0]
	for i := 0; i < keys; i++ {
		if err := open(writer).Set(ctx, fmt.Sprintf("backlog/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Hold every hand-off to the joiner until released.
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	for id, ft := range filters {
		if id == joiner {
			continue
		}
		ft.set(func(to, method string) error {
			if to == joiner && method == transport.MethodReplicate {
				<-release
			}
			return nil
		})
	}

	f.revive(t, joiner)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(joiner)
	for i := 0; i < 5; i++ {
		f.clk.Advance(time.Second)
		f.settle(t)
	}

	retained := func() (evicted, rehomed, kept int64) {
		for _, id := range f.live() {
			s := f.caches[id].Stats()
			evicted, rehomed, kept = evicted+s.ReplicasEvicted, rehomed+s.ReplicasRehomed, kept+s.ReplicasRetained
		}
		return
	}
	evicted, _, kept := retained()
	t.Logf("while held: evicted %d, retained %d", evicted, kept)
	if evicted == 0 {
		t.Fatal("the join displaced nothing; the test has no backlog")
	}
	if kept != 0 {
		t.Fatalf("while the worker was held, %d copies were drained and put back (%d evicted); a backlog "+
			"larger than the queue must wait in the replica store, not cycle through it every round", kept, evicted)
	}

	once.Do(func() { close(release) })
	// Rounds until eviction stops: the queue's pace takes several of them,
	// and a backlog that waits must still all be rehomed in the end.
	for last, still := int64(-1), 0; still < 3; {
		f.clk.Advance(time.Second)
		f.settle(t)
		waitForHandoffsToDrain(t, f)
		if e, _, _ := retained(); e == last {
			still++
		} else {
			last, still = e, 0
		}
	}
	var replicas int64
	for _, id := range f.live() {
		replicas += f.caches[id].Stats().ReplicaKeys
	}
	if replicas != keys {
		t.Fatalf("the cluster holds %d replica copies of %d keys at RF 2, want one each; orphans left "+
			"waiting were never handed on", replicas, keys)
	}
	evicted, rehomed, kept := retained()
	t.Logf("after: evicted %d, rehomed %d, retained %d", evicted, rehomed, kept)
	if kept != 0 || rehomed != evicted {
		t.Fatalf("after release: evicted %d, rehomed %d, retained %d; every orphan must be rehomed once",
			evicted, rehomed, kept)
	}
}
