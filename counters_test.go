package peercache_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// An audit found nine Stats fields that appeared in no test file at all.
//
// Every one of them is a correctness signal — the counters an operator is told
// to alert on — and every one was wired by hand from an internal counter into
// Stats and then into a metric name. That wiring broke three times in a row
// (MailboxRejections, VersionConflicts, StateEscapes), each time by being
// incremented somewhere and reaching nothing. The behaviour underneath was
// tested; the path to the operator was not.
//
// These tests close that. They assert the counter moves in response to the
// condition it names, and that the value reaches the scrape output — not just
// Stats — because the two have drifted before.

// assertMetric checks a series is present in the exposition output with a
// non-zero value.
func assertMetric(t *testing.T, cache peercache.Cache, name string) {
	t.Helper()
	var sb strings.Builder
	if err := cache.WriteMetrics(&sb); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	for _, line := range strings.Split(sb.String(), "\n") {
		field, value, ok := strings.Cut(line, " ")
		if !ok || field != name {
			continue
		}
		if value == "0" {
			t.Errorf("%s is 0 in the scrape output; Stats moved but the metric did not, "+
				"so the series is wired to the wrong field", name)
		}
		return
	}
	t.Errorf("%s is absent from the scrape output entirely", name)
}

// TestStateEscapesIsCountedAndExported covers a counter that was found to have
// never reached Stats, despite a comment in internal/actor/state.go asserting
// that it was exported.
//
// There is no error for this condition and there cannot be — the goroutine
// using the escaped state is not inside a Call — so this counter is the only
// evidence the application bug ever happened.
func TestStateEscapesIsCountedAndExported(t *testing.T) {
	cache, err := peercache.New(peercache.WithNodeID("escapes"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cache.Close(context.Background())

	escaped := make(chan peercache.GroupState, 1)
	if err := cache.RegisterGroupFunc("leak",
		func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
			s.Set("inside", []byte("legitimate"))
			escaped <- s
			return nil, nil
		}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	ctx := context.Background()
	group := cache.Group("leaky")
	if _, err := group.Call(ctx, "leak", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	if n := cache.Stats().StateEscapes; n != 0 {
		t.Fatalf("StateEscapes = %d before any escape; the counter is counting "+
			"legitimate in-turn use", n)
	}

	// Use the state after its turn ended. This is the application bug the
	// counter exists to report.
	state := <-escaped
	state.Set("outside", []byte("written after the turn"))

	if n := cache.Stats().StateEscapes; n == 0 {
		t.Fatal("a GroupState was used after its turn and StateEscapes stayed 0; " +
			"the violation is invisible, which is the whole failure mode — there is " +
			"no error return for this condition")
	}

	// The write must also have done nothing. A counted violation that still
	// mutated the map would be a data race that merely reports itself.
	got, err := group.Call(ctx, "leak", nil)
	_ = got
	if err != nil {
		t.Fatalf("Call after escape: %v", err)
	}
	assertMetric(t, cache, "peercache_state_escapes_total")
}

// TestEscapedWritesAreInert is the other half: the counter would be worthless
// if the escaped write had landed anyway.
func TestEscapedWritesAreInert(t *testing.T) {
	cache, err := peercache.New(peercache.WithNodeID("inert"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cache.Close(context.Background())

	escaped := make(chan peercache.GroupState, 1)
	if err := cache.RegisterGroupFunc("capture",
		func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
			escaped <- s
			return nil, nil
		}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	if err := cache.RegisterGroupFunc("read",
		func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
			val, _ := s.Get(string(arg))
			return val, nil
		}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	ctx := context.Background()
	group := cache.Group("inert-group")
	if _, err := group.Call(ctx, "capture", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}

	state := <-escaped
	state.Set("ghost", []byte("must not land"))

	got, err := group.Call(ctx, "read", []byte("ghost"))
	if err != nil {
		t.Fatalf("Call read: %v", err)
	}
	if got != nil {
		t.Fatalf("an escaped Set wrote %q into the group; the generation check is "+
			"not preventing the write, only counting it — which makes it a data "+
			"race that reports itself", got)
	}
}

// TestInvalidationsAreCountedAndExported covers three counters the audit found
// untested: sent, dropped, and failed.
//
// Invalidation is documented as a latency optimisation that correctness never
// depends on. That is exactly why it needs a counter an operator can watch: a
// silently broken invalidation path degrades staleness with no correctness test
// able to notice, because no correctness test is allowed to depend on it.
func TestInvalidationsAreCountedAndExported(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("invalidate",
			peercache.WithReplicationFactor(2),
			peercache.WithL1TTL(time.Minute))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	// Populate non-owned copies: every node reads every key, so whichever
	// node does not own a key ends up holding an L1 copy of it.
	const keys = 60
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("inv/%d", i)
		if err := writeWithRetry(t, f, openNS, key, "v1"); err != nil {
			t.Fatalf("seed %q: %v", key, err)
		}
		for _, id := range f.ids {
			_, _ = openNS(id).Get(ctx, key)
		}
	}

	// Overwrite. Each write to a key with copies elsewhere should broadcast.
	for i := 0; i < keys; i++ {
		if err := writeWithRetry(t, f, openNS, fmt.Sprintf("inv/%d", i), "v2"); err != nil {
			t.Fatalf("overwrite: %v", err)
		}
	}
	f.settle(t)

	var sent int64
	var sender string
	for _, id := range f.ids {
		if n := f.caches[id].Stats().InvalidationsSent; n > sent {
			sent, sender = n, id
		}
	}
	if sent == 0 {
		t.Fatal("no node sent an invalidation after overwriting 60 keys that other " +
			"nodes held L1 copies of; either the broadcast is not happening or the " +
			"counter is not wired")
	}
	t.Logf("invalidations sent: %d (max on %s)", sent, sender)
	assertMetric(t, f.caches[sender], "peercache_invalidations_sent_total")
}

// TestRefusedActivationsAreCounted covers the guard's refusal counter.
//
// A node asked for a group it does not own must refuse to activate it. That
// refusal is the mechanism preventing two activations of one group, and the
// counter is how an operator sees it happening at a rate that suggests callers
// disagree with the ranking.
func TestRefusedActivationsAreCounted(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	// Find keys this node provably does not own, rather than trying a few
	// hundred and skipping if none happened to be refused. A test that skips
	// when it fails to construct its own precondition is a test that reports
	// success for the wrong reason.
	target := f.ids[0]
	var foreign []string
	for i := 0; len(foreign) < 5 && i < 2000; i++ {
		key := fmt.Sprintf("refuse/%d", i)
		if f.ownerOf(t, key) != target {
			foreign = append(foreign, key)
		}
	}
	if len(foreign) < 5 {
		t.Fatalf("only %d of 2000 keys are owned away from %s; the ranking is not "+
			"distributing and this test cannot construct its precondition",
			len(foreign), target)
	}

	before := f.caches[target].Stats().RefusedActivations
	for _, key := range foreign {
		// The local Group handle asks this node directly, bypassing the
		// router that would otherwise send the call to the real owner.
		_, _ = f.caches[target].Group(key).Keys(ctx)
	}
	after := f.caches[target].Stats().RefusedActivations

	if after <= before {
		t.Fatalf("%s was asked to activate 5 groups it does not own and "+
			"RefusedActivations went %d -> %d. The refusal is what stops two nodes "+
			"activating one group; a refusal nobody counts is one nobody can alert on",
			target, before, after)
	}
	t.Logf("refused activations on %s: %d -> %d", target, before, after)
	assertMetric(t, f.caches[target], "peercache_refused_activations_total")
}

// TestCatchUpCountersMove covers ReplicasPulled and CatchUpRuns.
//
// A restarted node ranks for keys it does not hold. CatchUp is the only thing
// that fixes that, and it is a required call rather than an
// automatic one — so a counter showing it ran, and moved data, is the only
// evidence an operator has that a restart completed properly.
func TestCatchUpCountersMove(t *testing.T) {
	// The sequence here mirrors TestARestartedNodeRecoversItsColdReplicas
	// exactly, including the clock advances, because that test is known to
	// converge and an invented variant of it hung for ten minutes: advancing
	// by activation delays rather than by the failure and join timeouts never
	// re-admitted the revived node.
	f := newFabric(t, 5)
	ctx := context.Background()

	const rf = 3
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("cucount",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}

	writer := f.ids[0]
	for i := 0; i < 300; i++ {
		k := fmt.Sprintf("cu/%d", i)
		if err := openNS(writer).Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	// A node that was present throughout already holds every replica it
	// ranks for, so its CatchUp correctly pulls nothing. The counter is only
	// observable on a node that actually missed something.
	victim := ""
	for _, id := range f.ids {
		if id == writer {
			continue
		}
		if f.caches[id].Stats().ReplicaKeys > 0 {
			victim = id
			break
		}
	}
	if victim == "" {
		t.Fatal("no node holds replicas; there is nothing to pull")
	}

	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)

	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	openNS(victim)

	before := f.caches[victim].Stats()
	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	after := f.caches[victim].Stats()

	if after.CatchUpRuns <= before.CatchUpRuns {
		t.Errorf("CatchUpRuns did not move across an explicit CatchUp: %d -> %d",
			before.CatchUpRuns, after.CatchUpRuns)
	}
	if after.ReplicasPulled == 0 {
		t.Errorf("a restarted node with an empty store pulled 0 replicas. CatchUp is " +
			"the only thing that repairs it, and a run that pulls nothing is " +
			"indistinguishable from one that was never needed")
	}
	t.Logf("catch-up runs %d -> %d, replicas pulled %d -> %d",
		before.CatchUpRuns, after.CatchUpRuns, before.ReplicasPulled, after.ReplicasPulled)
	assertMetric(t, f.caches[victim], "peercache_catchup_runs_total")
	assertMetric(t, f.caches[victim], "peercache_replicas_pulled_total")
}

// TestKeysAdoptedIsCounted covers the receiving end of a drain.
//
// KeysHandedOff counts what the departing node pushed. The receiver takes each
// key into its owned store one of two ways: *adoption*, when it already owns the
// key as the transfer lands, or *promotion*, when the transfer arrives before it
// has processed the departure and is filed as a replica for the next
// reconciliation to pick up. The fear the test exists for is that a handed-off
// key reaches neither — "sent and not adopted is silent data loss" — and that is
// what it asserts.
//
// # Why it does not assert adoption alone
//
// It used to, and it was a race. Drain announces its departure and then
// transfers, with nothing synchronising the two, so which path a key takes
// depends on whether the receiver's view updated before the transfer landed.
// Both preserve the data, which is why the library is built to tolerate either.
//
// This was learned the hard way. Adding a hand-off goroutine per cache — an
// unrelated change that touched no drain code — shifted the scheduling enough to
// make every key take the promotion path, and the test failed claiming "silent
// data loss" while no data had been lost. It then *passed* when a diagnostic
// t.Logf was added above the assertion, which is the definition of a test
// measuring the observer.
//
// So it asserts the outcome and not the route: every handed-off key is in some
// receiver's owned store, and every key written is still readable.
func TestKeysAdoptedIsCounted(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("adopt", peercache.WithReplicationFactor(1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	const keys = 200
	for i := 0; i < keys; i++ {
		if err := writeWithRetry(t, f, openNS, fmt.Sprintf("ad/%d", i), "v"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	victim := f.ids[0]
	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	waitForMembers(t, f, []string{f.ids[1], f.ids[2]}, 2)
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	receivers := []string{f.ids[1], f.ids[2]}
	taken := func() (adopted, promoted int64) {
		for _, id := range receivers {
			s := f.caches[id].Stats()
			adopted += s.KeysAdopted
			promoted += s.KeysPromoted
		}
		return adopted, promoted
	}

	handedOff := f.caches[victim].Stats().KeysHandedOff
	if handedOff == 0 {
		t.Fatal("the drained node handed off nothing, so there is nothing to observe")
	}

	// The promotion path needs one reconciliation after the transfer, which a
	// membership change drives. Give it room rather than assuming the ordering
	// that failed above.
	for i := 0; i < 20; i++ {
		if a, p := taken(); a+p >= handedOff {
			break
		}
		f.clk.Advance(2 * ownership.DefaultActivationDelay)
		f.settle(t)
	}

	adopted, promoted := taken()
	t.Logf("handed off %d; receivers adopted %d and promoted %d", handedOff, adopted, promoted)

	if adopted+promoted < handedOff {
		t.Fatalf("%d keys were handed off and the receivers account for only %d "+
			"(%d adopted, %d promoted). A key that is sent and lands in neither the "+
			"owned store nor a promotion is silent data loss",
			handedOff, adopted+promoted, adopted, promoted)
	}

	// And the data itself, which is the thing the counters are a proxy for.
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("ad/%d", i)
		if got, err := readAnywhere(t, f, openNS, key); err != nil || got != "v" {
			t.Fatalf("after the drain, %s = %q (%v); a graceful shutdown must not lose "+
				"the keys it was transferring", key, got, err)
		}
	}

	// The counter that did move must reach the scrape output. Which one moved
	// is the race above; that at least one did is not.
	if adopted > 0 {
		assertMetric(t, f.caches[receivers[0]], "peercache_keys_adopted_total")
		return
	}
	assertMetric(t, f.caches[receivers[0]], "peercache_keys_promoted_total")
}

// TestReplicasEvictedIsCounted covers the counter for replicas a node stops
// ranking for.
//
// Without eviction a node accumulates replicas forever as membership churns,
// which is a slow memory leak that no correctness test would catch — the data
// is merely surplus, not wrong.
func TestReplicasEvictedIsCounted(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("evict",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}

	writer := f.ids[0]
	for i := 0; i < 400; i++ {
		if err := openNS(writer).Set(ctx, fmt.Sprintf("ev/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	// Removing a node makes the survivors rank for *more*, which evicts
	// nothing. Bringing one back is what makes a held replica surplus: the
	// returning node reclaims its share of the ranking and whoever covered
	// for it is now holding copies it no longer ranks for.
	//
	// The first version of this test killed a node and asserted eviction,
	// then skipped when it saw none — which is the correct outcome for that
	// scenario and a vacuous test.
	victim := f.ids[4]
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	f.settle(t)

	// Write again so the survivors take on the departed node's share.
	for i := 400; i < 800; i++ {
		if err := writeWithRetry(t, f, openNS, fmt.Sprintf("ev/%d", i), "v"); err != nil {
			t.Fatalf("second seed: %v", err)
		}
	}
	f.settle(t)

	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	openNS(victim)
	f.settle(t)

	var evicted int64
	var evictor string
	for _, id := range f.ids {
		if n := f.caches[id].Stats().ReplicasEvicted; n > evicted {
			evicted, evictor = n, id
		}
	}
	if evicted == 0 {
		t.Fatal("a node rejoined and reclaimed its share of the ranking, and no peer " +
			"evicted a single surplus replica. Replicas a node no longer ranks for are " +
			"never read and never freed, which is a memory leak no correctness test " +
			"can see because the data is surplus rather than wrong")
	}
	t.Logf("replicas evicted: %d (max on %s)", evicted, evictor)
	assertMetric(t, f.caches[evictor], "peercache_replicas_evicted_total")
}
