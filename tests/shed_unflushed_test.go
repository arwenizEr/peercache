package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// unflushedFor mirrors persister.unflushedFor at the default flush interval:
// one interval and the flush timeout. The budget keeps replicas of persistent
// writes younger than this.
const unflushedFor = flushInterval + 10*time.Second

// shedFixture is two nodes, n0 owning the keys under test and n1 holding
// their replicas under a memory budget, over a store that can be made to lose
// every flush.
type shedFixture struct {
	f     *fabric
	store *blackholeStore
	keys  []string
}

func newShedFixture(t *testing.T) *shedFixture {
	t.Helper()
	const budget = 1 << 20
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n1", peercache.WithMemoryBudget(budget)))
	store := &blackholeStore{BlobStore: persist.Memory()}
	openDurable(f, store, peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	for _, id := range f.ids {
		f.caches[id].Namespace("filler")
	}
	f.settle(t)

	fx := &shedFixture{f: f, store: store}
	for i := 0; len(fx.keys) < 50; i++ {
		if k := fmt.Sprintf("order-%d", i); f.ownerOf(t, k) == "n0" {
			fx.keys = append(fx.keys, k)
		}
	}
	return fx
}

// write sets every key under test through its owner, n0. The writes are
// synchronous, so each has a replica on n1 when it returns.
func (fx *shedFixture) write(t *testing.T) {
	t.Helper()
	ns := fx.f.caches["n0"].Namespace("durable")
	for _, k := range fx.keys {
		if err := ns.Set(context.Background(), k, make([]byte, 1<<10), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
}

// overfill writes n1 twice past its budget with data it owns, in values large
// enough that every write checks the budget. Relieving that takes every
// replica n1 may shed and then some of the filler.
func (fx *shedFixture) overfill(t *testing.T) peercache.Stats {
	t.Helper()
	ns := fx.f.caches["n1"].Namespace("filler")
	written := 0
	for i := 0; written < 32; i++ {
		k := fmt.Sprintf("filler-%d", i)
		if fx.f.ownerOf(t, k) != "n1" {
			continue
		}
		if err := ns.Set(context.Background(), k, make([]byte, 64<<10), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
		written++
	}
	st := fx.f.caches["n1"].Stats()
	if st.ShedOwned == 0 {
		t.Fatalf("2 MB written into a 1 MB budget and no owned data was shed (%+v); "+
			"the pressure this test is about was not constructed", st)
	}
	return st
}

// TestAnUnflushedWriteSurvivesItsReplicaNodeShedding: a persistent write's
// replica is not shed while its owner may not yet have flushed it, so the owner
// dying in that window does not take an acknowledged write with it.
//
// Shed, it was the write's only other holder. On kind at 3.9 million keys, pods
// over their budget shed the replicas of orders written a second earlier, and a
// kill -9 of their owner lost 20 acknowledged orders.
func TestAnUnflushedWriteSurvivesItsReplicaNodeShedding(t *testing.T) {
	fx := newShedFixture(t)
	ctx := context.Background()

	// Every flush from here is lost, as an owner killed before its flush
	// would lose it.
	fx.store.dropping.Store(true)
	fx.write(t)
	// A second on, past the flush interval and inside its timeout: the age
	// of the writes kind lost, whose flush was slow, not over.
	fx.f.clk.Advance(time.Second)
	fx.overfill(t)

	killAndSettle(t, fx.f, "n0")
	ns := fx.f.caches["n1"].Namespace("durable")
	lost := 0
	for _, k := range fx.keys {
		if _, err := ns.Get(ctx, k); err != nil {
			lost++
		}
	}
	if lost > 0 {
		t.Fatalf("%d of %d acknowledged writes lost: their replicas were shed before their "+
			"owner flushed them, and then the owner died", lost, len(fx.keys))
	}
}

// TestAFlushedWritesReplicaIsShedAsBefore: once a persistent write is past the
// window its owner flushes in, its replica is shed like any other. Keeping the
// young ones must not keep all of them.
func TestAFlushedWritesReplicaIsShedAsBefore(t *testing.T) {
	fx := newShedFixture(t)
	fx.write(t)
	flushAll(t, fx.f)
	fx.f.clk.Advance(unflushedFor)

	before := fx.f.caches["n1"].Stats().ShedReplicas
	st := fx.overfill(t)
	if shed := st.ShedReplicas - before; shed < int64(len(fx.keys)) {
		t.Fatalf("%d replicas shed of %d flushed writes past the window; a flushed write's "+
			"replica must be shed before owned data is", shed, len(fx.keys))
	}
}
