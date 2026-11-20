package tests

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
	"github.com/arwenizEr/peercache/transport"
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

func newShedFixture(t *testing.T, opts ...fabricOption) *shedFixture {
	t.Helper()
	const budget = 1 << 20
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		append([]fabricOption{withNodeOptions("n1", peercache.WithMemoryBudget(budget))}, opts...)...)
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

	// Every flush from here is lost, as an owner killed before its flush
	// would lose it.
	fx.store.dropping.Store(true)
	fx.write(t)
	// A second on, past the flush interval and inside its timeout: the age
	// of the writes kind lost, whose flush was slow, not over.
	fx.f.clk.Advance(time.Second)
	fx.overfill(t)

	killAndSettle(t, fx.f, "n0")
	durable := func(id string) peercache.Namespace { return fx.f.caches[id].Namespace("durable") }
	lost := 0
	for _, k := range fx.keys {
		if _, err := readWithRetryFrom(t, fx.f, durable, "n1", k); err != nil {
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

// TestAnOwnersShedKeepsTheReplicasOfItsUnflushedWrites is the owner's side of
// open question 60, found by TestDurableSimulation. A persistent namespace's
// owner shed a key it had written during a store outage, and the shed told
// the key's replicas to drop their copies: for a namespace without
// persistence a shed is a loss and the replicas follow it. Here the write was
// still only in the owner's pending records and in those copies. The owner
// then died with its flush failing, and the next owner, its copy gone,
// restored the older stored value; the replica that still had the write was
// not the owner and never promoted it.
//
// A persistent key is not lost when it is shed — it stays in storage and comes
// back — so its replicas have nothing to follow and are left alone.
func TestAnOwnersShedKeepsTheReplicasOfItsUnflushedWrites(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n0", peercache.WithMemoryBudget(256<<10)))
	stores := map[string]*cutOffStore{}
	backing := persist.Memory()
	for _, id := range f.ids {
		stores[id] = &cutOffStore{BlobStore: backing}
	}
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("stock", peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(), peercache.WithPersistence(stores[id]))
	}
	for _, id := range f.ids {
		open(id)
		f.caches[id].Namespace("filler")
	}
	f.settle(t)
	ctx := context.Background()

	var keys []string
	for i := 0; len(keys) < 40; i++ {
		if k := fmt.Sprintf("item-%d", i); f.ownerOf(t, k) == "n0" {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if _, err := open("n0").Increment(ctx, k, -1, 0); err != nil {
			t.Fatal(err)
		}
	}
	flushAll(t, f)

	// The owner's store goes; the second decrements stay pending on it, and
	// in the replicas the synchronous writes placed.
	stores["n0"].cut.Store(true)
	for _, k := range keys {
		if _, err := open("n0").Increment(ctx, k, -1, 0); err != nil {
			t.Fatal(err)
		}
	}
	filler := f.caches["n0"].Namespace("filler")
	for i := 0; i < 2000; i++ {
		_ = filler.Set(ctx, fmt.Sprintf("b%d", i), make([]byte, 1<<10), 0)
	}
	if f.caches["n0"].Stats().ShedOwned == 0 {
		t.Fatal("the owner shed nothing")
	}

	// The owner dies before its store comes back.
	f.dead["n0"] = true
	_ = f.caches["n0"].Close(ctx)
	f.memberships["n0"].Close()
	f.net.Unregister("n0")
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	var lost []string
	for _, k := range keys {
		v, err := readWithRetryFrom(t, f, open, "n1", k)
		if err != nil || v != "-2" {
			lost = append(lost, fmt.Sprintf("%s=%q (%v)", k, v, err))
		}
	}
	if len(lost) > 0 {
		t.Fatalf("%d of %d acknowledged decrements were lost after their owner shed them and died "+
			"unflushed, though synchronous replication had placed them: %v", len(lost), len(keys), lost)
	}
}

// restartOwner kills the fixture's owner, n0, with its writes unflushed and
// brings it back under the same name before its peer's failure detector
// notices: it still owns every key under test and holds none of them, and it
// has not caught up.
func (fx *shedFixture) restartOwner(t *testing.T) {
	t.Helper()
	fx.store.dropping.Store(true)
	// hardKill, but its final flush failing is the point, not an error.
	fx.f.dead["n0"] = true
	_ = fx.f.caches["n0"].Close(context.Background())
	fx.f.memberships["n0"].Close()
	fx.f.net.Unregister("n0")
	fx.store.dropping.Store(false)
	fx.f.revive(t, "n0")
	fx.f.caches["n0"].Namespace("durable", peercache.WithPersistence(fx.store),
		peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	fx.f.caches["n0"].Namespace("filler")
	// In rounds: the rejoin's timer may be armed after a single advance has
	// passed, and then waits for the next one (1 run in about 100 did).
	for round := 0; ; round++ {
		if round == 10 {
			t.Fatalf("n0 sees %d members after %d join delays, want 2", fx.f.caches["n0"].Stats().Members, round)
		}
		fx.f.clk.Advance(testkit.DefaultJoinDelay)
		joined := false
		for i := 0; i < 100_000 && !joined; i++ {
			joined = fx.f.caches["n0"].Stats().Members == 2 && fx.f.caches["n1"].Stats().Members == 2
			runtime.Gosched()
		}
		if joined {
			break
		}
	}
	fx.f.convergeTo(t, 2)
}

// catchUp runs CatchUp on id, as the shop's catchUpLoop does before it serves.
func (fx *shedFixture) catchUp(t *testing.T, id string) {
	t.Helper()
	if err := fx.f.caches[id].CatchUp(context.Background()); err != nil {
		t.Fatalf("CatchUp(%s): %v", id, err)
	}
}

// lostFrom counts the keys under test id cannot read.
func (fx *shedFixture) lostFrom(t *testing.T, id string) int {
	t.Helper()
	durable := func(id string) peercache.Namespace { return fx.f.caches[id].Namespace("durable") }
	lost := 0
	for _, k := range fx.keys {
		if _, err := readWithRetryFrom(t, fx.f, durable, id, k); err != nil {
			lost++
		}
	}
	return lost
}

// TestAnUnflushedWriteSurvivesItsOwnersRestartAndItsReplicaShedding is the
// kill -9 loss of kind run v115: 15 acknowledged orders, all owned by the
// killed pod and written in the second before its SIGKILL, gone from every
// pod. The owner died before flushing them, came back within a second under
// the same name, and could not finish its catch-up for minutes because it was
// over its own budget. Its replicas held the writes, but only for the window
// keepFrom allowed, which assumed the owner was alive to flush; past it they
// shed the copies, and the catch-up that finally ran found nothing.
//
// The owner had caught up before it was killed, so its replicas have its
// word on what it had stored; the writes after it are kept for as long as it
// takes the owner to come back for them.
func TestAnUnflushedWriteSurvivesItsOwnersRestartAndItsReplicaShedding(t *testing.T) {
	fx := newShedFixture(t)
	fx.catchUp(t, "n0")
	fx.catchUp(t, "n1")
	fx.write(t)
	fx.restartOwner(t)

	// The owner's catch-up is held off past the window; its replica comes
	// under pressure meanwhile.
	fx.f.clk.Advance(unflushedFor + time.Second)
	if st := fx.overfill(t); st.ShedReplicas != 0 {
		t.Errorf("%d replicas shed; the copies of a restarted owner's unflushed writes must be kept", st.ShedReplicas)
	}

	fx.catchUp(t, "n0")
	if lost := fx.lostFrom(t, "n0"); lost > 0 {
		t.Fatalf("%d of %d acknowledged writes lost: their owner died before flushing them, "+
			"and its replica shed them once they were older than the flush window", lost, len(fx.keys))
	}
}

// TestARestartedOwnersCaughtUpWritesAreShedOnceStored: what the replica keeps
// for a restarted owner it lets go once the owner has caught up and stored
// them. Keeping a dead writer's copies must not keep them forever once it is
// back.
func TestARestartedOwnersCaughtUpWritesAreShedOnceStored(t *testing.T) {
	fx := newShedFixture(t)
	fx.catchUp(t, "n0")
	fx.catchUp(t, "n1")
	fx.write(t)
	fx.restartOwner(t)
	fx.catchUp(t, "n0")
	flushAll(t, fx.f)
	fx.f.clk.Advance(unflushedFor + time.Second)
	// One write after the flush carries the owner's new bound.
	if err := fx.f.caches["n0"].Namespace("durable").Set(context.Background(), fx.keys[0], []byte("again"), 0); err != nil {
		t.Fatal(err)
	}

	before := fx.f.caches["n1"].Stats().ShedReplicas
	st := fx.overfill(t)
	if shed := st.ShedReplicas - before; shed < int64(len(fx.keys)-1) {
		t.Fatalf("%d replicas shed of %d stored writes; a restarted owner that has caught up and "+
			"flushed must release them", shed, len(fx.keys)-1)
	}
}

// catchUpFailingTransport fails this node's catch-up requests while failing
// is set, as the restarted pod's were on kind: refused by its own budget, page
// after page, for minutes.
type catchUpFailingTransport struct {
	core.Transport
	failing *atomic.Bool
}

func (t *catchUpFailingTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &catchUpFailingClient{PeerClient: inner, failing: t.failing}, nil
}

type catchUpFailingClient struct {
	core.PeerClient
	failing *atomic.Bool
}

func (c *catchUpFailingClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method == transport.MethodCatchUp && c.failing.Load() {
		return nil, errors.New("catch-up refused")
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

// TestAnOwnerWhoseCatchUpFailedDoesNotReleaseItsReplicas: a restarted owner
// whose catch-up did not complete has not brought its previous run's unflushed
// writes back, so it must not tell its replicas its writes are stored. A failed
// catch-up still opens readiness and the catch-up gate, which is why a separate
// condition decides this: on kind the restarted pod's catch-ups failed for
// minutes while it served.
func TestAnOwnerWhoseCatchUpFailedDoesNotReleaseItsReplicas(t *testing.T) {
	var failing atomic.Bool
	fx := newShedFixture(t, withWrappers(func(id string, tr core.Transport) core.Transport {
		if id == "n0" {
			return &catchUpFailingTransport{Transport: tr, failing: &failing}
		}
		return tr
	}, nil))
	ctx := context.Background()
	fx.catchUp(t, "n0")
	fx.catchUp(t, "n1")
	fx.write(t)
	fx.restartOwner(t)

	failing.Store(true)
	if err := fx.f.caches["n0"].CatchUp(ctx); err == nil {
		t.Fatal("CatchUp succeeded through a transport refusing every catch-up; the failure this test is about was not constructed")
	}
	// A write after the failed catch-up, flushed: its copy carries whatever
	// the owner now says about its records.
	if err := fx.f.caches["n0"].Namespace("durable").Set(ctx, "after-restart", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	flushAll(t, fx.f)
	fx.f.clk.Advance(unflushedFor + time.Second)
	if err := fx.f.caches["n0"].Namespace("durable").Set(ctx, "after-flush", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	if st := fx.overfill(t); st.ShedReplicas != 0 {
		t.Errorf("%d replicas shed after their owner's catch-up failed; its previous run's unflushed "+
			"writes are not back on it", st.ShedReplicas)
	}

	failing.Store(false)
	fx.catchUp(t, "n0")
	if lost := fx.lostFrom(t, "n0"); lost > 0 {
		t.Fatalf("%d of %d acknowledged writes lost after a failed catch-up and a successful one", lost, len(fx.keys))
	}
}

// TestAnOwnersBoundStaysBelowItsOldestUnflushedWrite: an owner whose flushes
// fail keeps writing, and the bound each write carries stays below the oldest
// write it has not stored, however long ago that was. A bound read from the
// clock alone would have released those copies a second after they were made,
// and the owner dying before the store came back would have taken them.
func TestAnOwnersBoundStaysBelowItsOldestUnflushedWrite(t *testing.T) {
	fx := newShedFixture(t)
	ctx := context.Background()
	fx.catchUp(t, "n0")
	fx.catchUp(t, "n1")

	fx.store.refusing.Store(true)
	fx.write(t)
	// Flushes fail meanwhile, and the batch goes back to pending each time.
	for i := 0; i < 3; i++ {
		fx.f.clk.Advance(time.Second)
		if err := fx.f.caches["n0"].Namespace("durable").Set(ctx, fmt.Sprintf("later-%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "a flush to fail", func() bool { return fx.f.caches["n0"].Stats().PersistFailures > 0 })
	fx.restartOwner(t) // its final flush is refused too
	fx.store.refusing.Store(false)

	fx.f.clk.Advance(unflushedFor + time.Second)
	if st := fx.overfill(t); st.ShedReplicas != 0 {
		t.Errorf("%d replicas shed of writes their owner never stored", st.ShedReplicas)
	}
	fx.catchUp(t, "n0")
	if lost := fx.lostFrom(t, "n0"); lost > 0 {
		t.Fatalf("%d of %d acknowledged writes lost: their owner's bound passed them while its flushes failed",
			lost, len(fx.keys))
	}
}
