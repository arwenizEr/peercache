package peercache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
)

// TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy is the M8 simulation's
// first real find, reduced to its mechanism.
//
// The schedule that found it (PEERCACHE_SEED=243287519653701681) partitioned a
// five-node cluster into [n0] | [n1 n2 n3 n4], advanced past the activation
// delay, wrote one key through the isolated n0 under synchronous replication at
// replication factor 3 — which returned success — and then killed n0. The key
// was gone.
//
// # Why it happens
//
// Replication targets come from the ranking over the writer's *current
// membership view*. An isolated node eventually detects every peer as failed,
// so its view shrinks to itself, it ranks itself the owner of everything, and
// the activation guard's delay expires. Synchronous replication then places
// every copy it can reach — one — and reports success, because from where it is
// standing it did exactly what was asked.
//
// # Why this matters more than the usual split-brain caveat
//
// CLAUDE.md non-negotiable 5 already says a partition can produce two
// activations and that this is narrowed, not eliminated. That is not the
// surprise. The surprise is the *acknowledgement*: README and docs/api.md both
// say synchronous replication closes the async data-loss window, and a caller
// who chose WithSyncReplication and RF=3 reasonably reads that as "when Set
// returns nil, three nodes have it". Here Set returns nil with one node holding
// it, and nothing in the response distinguishes that from the healthy case.
//
// The window is also not narrow. It lasts as long as the partition does.
//
// This test pins the behaviour as it currently stands so that a change to it is
// deliberate and visible. See docs/decisions/open.md question 37 for the options
// and the recommendation.
func TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy(t *testing.T) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("degraded",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	isolated := f.ids[0]
	rest := append([]string(nil), f.ids[1:]...)

	f.net.Partition([]string{isolated}, rest)
	t.Cleanup(func() { f.net.Heal() })

	// Long enough for the isolated node to detect every peer as failed and
	// for the activation guard's delay to expire.
	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	waitForCache(t, "the isolated node to see a cluster of one", func() bool {
		return f.caches[isolated].Stats().Members == 1
	})

	const key = "degraded/only-copy"
	err := openNS(isolated).Set(ctx, key, []byte("acknowledged"), 0)

	view := f.caches[isolated].Stats()
	t.Logf("isolated node sees %d members; Set returned %v", view.Members, err)

	if err != nil {
		t.Skipf("the isolated node refused the write (%v); the behaviour this test "+
			"pins has changed, which is the intended direction — update it and "+
			"docs/decisions/open.md question 37", err)
	}

	// It said yes. Establish that it really is the only copy by checking that
	// nobody on the majority side has it.
	f.net.Heal()
	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids))
	f.settle(t)

	holders := 0
	for _, id := range rest {
		if _, err := openNS(id).Get(ctx, key); err == nil {
			holders++
		}
	}

	t.Logf("after healing, %d of the %d majority-side nodes can serve the key",
		holders, len(rest))

	if view.Members != 1 {
		t.Fatalf("the isolated node saw %d members, so this test did not construct "+
			"the isolation it is about", view.Members)
	}

	// The finding, asserted so it cannot regress silently in either
	// direction: the write was acknowledged while the writer's own view could
	// not possibly satisfy the replication factor it was configured with.
	if view.Members >= 3 {
		t.Fatalf("the writer saw %d members, which could satisfy RF=3; this test "+
			"is not exercising the degraded case", view.Members)
	}
	// The caller cannot distinguish it. An operator now can, which is option 1
	// of question 37 and the only part of it that is decided.
	after := f.caches[isolated].Stats()
	if after.DegradedWrites == 0 {
		t.Fatalf("Set returned nil with a view of %d under RF=3 sync replication and "+
			"DegradedWrites stayed 0. The counter is the only signal that separates "+
			"this from a write that placed three copies; without it the window is "+
			"invisible to everyone, which is the state question 37 was opened about",
			view.Members)
	}
	// And it reaches the scrape output, not only Stats. Those two have drifted
	// before: StateEscapes was documented as exported from M7 and stopped at
	// the actor package's counters, which the M8 audit found.
	assertMetric(t, f.caches[isolated], "peercache_degraded_writes_total")

	t.Logf("CONFIRMED: Set returned nil under WithSyncReplication at RF=3 while the "+
		"writer's view contained %d node, and DegradedWrites counted %d. A caller "+
		"still cannot distinguish this from a write that placed three copies; an "+
		"operator can.", view.Members, after.DegradedWrites)
}

// TestDegradedWritesStayZeroWhenTheFactorIsMet is the counter's other half, and
// the one that makes it worth alerting on.
//
// A counter that rises during healthy operation is one an operator learns to
// ignore, and it would then be silent in exactly the sense question 37 says the
// window currently is. So: a settled cluster large enough for the replication
// factor, synchronous writes, and nothing counted.
//
// It also covers the two configurations that must not count. Asynchronous
// replication is momentarily below its factor on every write by design — that
// window is measured in docs/benchmarks.md, not counted here — and replication
// factor 1 asks for one copy and gets one.
func TestDegradedWritesStayZeroWhenTheFactorIsMet(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	sync3 := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("healthy",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	async3 := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("async",
			peercache.WithReplicationFactor(3))
	}
	rf1 := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("single")
	}
	for _, id := range f.ids {
		sync3(id)
		async3(id)
		rf1(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("healthy/%d", i)
		via := f.ids[i%len(f.ids)]
		if err := sync3(via).Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("sync write %s via %s on a settled five-node cluster: %v", key, via, err)
		}
		if err := async3(via).Set(ctx, "async/"+key, []byte("v"), 0); err != nil {
			t.Fatalf("async write %s via %s: %v", key, via, err)
		}
		if err := rf1(via).Set(ctx, "single/"+key, []byte("v"), 0); err != nil {
			t.Fatalf("rf=1 write %s via %s: %v", key, via, err)
		}
	}
	f.settle(t)

	for _, id := range f.ids {
		if n := f.caches[id].Stats().DegradedWrites; n != 0 {
			t.Fatalf("%s counted %d degraded writes on a settled five-node cluster at "+
				"RF=3. A counter that rises when nothing is wrong is one nobody will "+
				"act on when something is", id, n)
		}
	}
}

// TestSyncReplicationRefusesWhenReplicasAreUnreachable is the other half, and
// the reassuring one.
//
// When the writer still *sees* its replicas but cannot reach them, synchronous
// replication fails the write rather than acknowledging it. That is the
// guarantee working. The isolated case above differs only because the writer no
// longer believes those replicas exist — which is why the fix, if there is one,
// belongs at the point where the view is consulted rather than at the point
// where copies are sent.
func TestSyncReplicationRefusesWhenReplicasAreUnreachable(t *testing.T) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 30 * time.Second, // long, so the view does not shrink
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("unreachable",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	writer := f.ids[0]
	// Cut the writer off without letting it conclude the peers are gone: the
	// failure timeout is far longer than anything this test advances.
	f.net.Partition([]string{writer}, append([]string(nil), f.ids[1:]...))
	t.Cleanup(func() { f.net.Heal() })

	if got := f.caches[writer].Stats().Members; got < 3 {
		t.Skipf("the writer's view already shrank to %d; this test needs it to still "+
			"believe its replicas exist", got)
	}

	var refused, accepted int
	for i := 0; i < 20; i++ {
		key := "unreachable/" + string(rune('a'+i))
		if err := openNS(writer).Set(ctx, key, []byte("v"), 0); err != nil {
			refused++
		} else {
			accepted++
		}
	}

	t.Logf("writer still sees %d members: %d writes refused, %d accepted",
		f.caches[writer].Stats().Members, refused, accepted)

	if refused == 0 {
		t.Fatal("every synchronous write was acknowledged while the writer could reach " +
			"none of its replicas. Synchronous replication exists to place copies " +
			"before returning; if it cannot, it must say so")
	}
}
