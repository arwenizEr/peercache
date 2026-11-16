package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy is the simulation's
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
// The design already says a partition can produce two
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
	// The isolated node acquires every group when it sees its peers fail,
	// which happens during the advances above, and since question 56 its own
	// callers wait out the activation delay too. Without this the write was
	// refused as pending, the test skipped, and the degraded-write counter
	// went unchecked.
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	const key = "degraded/only-copy"
	err := openNS(isolated).Set(ctx, key, []byte("acknowledged"), 0)

	view := f.caches[isolated].Stats()
	t.Logf("isolated node sees %d members; Set returned %v", view.Members, err)

	// Refusing below the factor is the direction question 37 leaves open, so
	// that error says the pinned behaviour changed on purpose. Any other
	// error means the test is not reaching the case it is about, and a skip
	// would hide that, as it once did.
	if errors.Is(err, peercache.ErrUnderReplicated) {
		t.Skipf("the isolated node refused the write (%v); the behaviour this test "+
			"pins has changed, which is the intended direction — update it and "+
			"docs/decisions/open.md question 37", err)
	}
	if err != nil {
		t.Fatalf("the isolated node's write failed for another reason: %v", err)
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
	// before: StateEscapes was documented as exported and stopped at the
	// actor package's counters.
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

// TestRequireReplicationFactorRefusesAnIsolatedWrite is option 3 of open
// question 37, opt-in: the same isolation as
// TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy, on a namespace that asked
// for its replication factor to be honoured.
//
// The write must not be acknowledged. It must say why, distinguishably from an
// unreachable replica, so a caller can tell "this node is cut off" from "a peer
// is slow". And the value must be held locally — the same contract as every
// other synchronous failure, because there is no rollback in this system.
func TestRequireReplicationFactorRefusesAnIsolatedWrite(t *testing.T) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("strict",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithRequireReplicationFactor())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	isolated := f.ids[0]
	f.net.Partition([]string{isolated}, append([]string(nil), f.ids[1:]...))
	t.Cleanup(func() { f.net.Heal() })

	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	waitForCache(t, "the isolated node to see a cluster of one", func() bool {
		return f.caches[isolated].Stats().Members == 1
	})
	// After the view has shrunk, not before it: the delay runs from the
	// membership change, and a write through the node itself now waits it
	// out like a peer's would (open question 56).
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	const key = "strict/only-copy"
	err := openNS(isolated).Set(ctx, key, []byte("held"), 0)
	if !errors.Is(err, peercache.ErrUnderReplicated) {
		t.Fatalf("Set on a node whose view holds 1 member, under RF=3 sync with "+
			"WithRequireReplicationFactor, returned %v; want ErrUnderReplicated. "+
			"Returning nil is the acknowledgement question 37 is about", err)
	}
	if errors.Is(err, peercache.ErrNodeUnreachable) {
		t.Fatalf("Set returned %v, which also matches ErrNodeUnreachable. The two "+
			"call for different responses and must be distinguishable", err)
	}

	got, gerr := openNS(isolated).Get(ctx, key)
	if gerr != nil || string(got) != "held" {
		t.Fatalf("Get after a refused write = %q, %v; want the value held locally. "+
			"A refused synchronous write is reported, not rolled back", got, gerr)
	}

	if n := f.caches[isolated].Stats().DegradedWrites; n != 0 {
		t.Fatalf("DegradedWrites = %d after a write that was refused. The counter "+
			"means acknowledged below the factor, and this one was not acknowledged", n)
	}
	if n := f.caches[isolated].Stats().UnderReplicatedRefusals; n != 1 {
		t.Fatalf("UnderReplicatedRefusals = %d after one refused write, want 1", n)
	}
	assertMetric(t, f.caches[isolated], "peercache_under_replicated_refusals_total")

	// Deletes go through the same fan-out and carry the same promise.
	if err := openNS(isolated).Delete(ctx, key); !errors.Is(err, peercache.ErrUnderReplicated) {
		t.Fatalf("Delete on the isolated node returned %v; want ErrUnderReplicated", err)
	}
}

// TestRequireReplicationFactorRefusesThroughAForwardedWrite covers the path
// where the caller is not the owner. The refusal happens on the owner and has
// to survive the hop back, or a caller writing through any other pod would see
// a generic failure and the distinction would exist only for local writes.
//
// Two nodes cannot hold three copies however healthy they are, so this needs no
// partition at all.
func TestRequireReplicationFactorRefusesThroughAForwardedWrite(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("strict2",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithRequireReplicationFactor())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	forwarded := 0
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("strict2/%d", i)
		via := f.ids[i%2]
		before := f.caches[via].Stats().UnderReplicatedRefusals
		err := openNS(via).Set(ctx, key, []byte("v"), 0)
		if !errors.Is(err, peercache.ErrUnderReplicated) {
			t.Fatalf("Set %s via %s on a two-node cluster at RF=3 returned %v; want "+
				"ErrUnderReplicated", key, via, err)
		}
		// The refusal is counted where it happened. If the node the write
		// was sent to did not count it, the owner did, and the error
		// crossed the hop.
		if f.caches[via].Stats().UnderReplicatedRefusals == before {
			forwarded++
		}
	}
	if forwarded == 0 {
		t.Fatal("all 20 writes were refused by the node they were sent to; none was " +
			"forwarded to its owner, so the hop this test is about was never exercised")
	}
}

// TestRequireReplicationFactorAcceptsWhenTheFactorIsMet is the option's other
// half. A namespace that refuses healthy writes is one nobody will enable.
func TestRequireReplicationFactorAcceptsWhenTheFactorIsMet(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("strict-healthy",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithRequireReplicationFactor())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("strict-healthy/%d", i)
		via := f.ids[i%len(f.ids)]
		if err := openNS(via).Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set %s via %s on a settled five-node cluster at RF=3: %v", key, via, err)
		}
	}
	for _, id := range f.ids {
		if n := f.caches[id].Stats().UnderReplicatedRefusals; n != 0 {
			t.Fatalf("%s refused %d writes on a cluster large enough for its factor", id, n)
		}
	}
}
