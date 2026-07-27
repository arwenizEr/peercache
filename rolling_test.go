package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/rebalance"
	"github.com/peercache/peercache/internal/testkit"
)

// M6 acceptance, from SPEC.md §4:
//
//	a simulated rolling upgrade of a 9-node cluster under sustained write
//	load completes with zero failed client requests and moves no more than
//	the theoretically required key count ±10%
//
// Two things in that sentence need pinning before the test means anything.
//
// # "Zero failed client requests" — what a client request is
//
// During a handover the old owner refuses (ErrNotOwner) and the new one defers
// (ErrActivationPending). Both are *correct*, both are retryable, and a caller
// that gives up after one attempt sees a failure. So "zero failed requests" can
// only mean zero after the retry the errors are explicitly asking for.
//
// The library already re-resolves once internally. This test adds a bounded
// retry loop on top, which is what a real client would do, and counts a request
// as failed if it exhausts that. Anything else would either be measuring the
// library's internal retry alone — too weak — or claiming an unretried request
// never fails, which is false.
//
// # "Theoretically required key count" — the denominator
//
// A rolling upgrade replaces each of nine pods in turn, and each replacement is
// two membership changes: the pod leaves, then rejoins. So the theoretical
// movement is 9 × 2 × (rf/9) of the keyspace, not one keyspace-worth. The
// figure is computed from internal/rebalance rather than asserted as a round
// number, because ±10% of an unstated quantity means nothing.

const (
	rollingNodes = 9
	rollingRF    = 3
	rollingKeys  = 3_000

	// liveKeys is how many keys the *live-cluster* test seeds and verifies,
	// as distinct from rollingKeys which the planner test uses.
	//
	// They differ because the two tests cost differently. The planner runs
	// in memory and 3,000 keys is free; the live test seeds each key through
	// a real routing path, kills and restarts nine pods around it, and probes
	// ownership per key per node. Under -race that reached three minutes and
	// climbing before this was split out.
	//
	// 600 keys keeps every assertion intact — zero failed requests, every key
	// readable, movement inside the derived bound — because all three are
	// ratios rather than counts. What it loses is resolution on the
	// percentage, which the planner test measures far more precisely anyway.
	liveKeys = 600

	// placementSample bounds the ownership probing, which is the single most
	// expensive thing here: one group activation per key per node.
	placementSample = 200
)

// TestRollingUpgradeLosesNoRequests is the M6 acceptance criterion.
func TestRollingUpgradeLosesNoRequests(t *testing.T) {
	// Detection faster than the upgrade cadence, for the reason M5 recorded:
	// a cluster that loses a node faster than it notices never converges.
	f := newFabricFull(t, rollingNodes, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("rolling",
			peercache.WithReplicationFactor(rollingRF),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	// Seed the keyspace and record where every key lives, so movement can be
	// measured rather than estimated.
	written := make(map[string]string, liveKeys)
	for i := 0; i < liveKeys; i++ {
		key := fmt.Sprintf("rolling/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := writeWithRetry(t, f, openNS, key, val); err != nil {
			t.Fatalf("seeding %q: %v", key, err)
		}
		written[key] = val
	}

	placementBefore := placementSnapshot(t, f, placementSample)

	var (
		requests   int
		failures   int
		upgraded   int
		handedOff  int64
		retryTotal int
	)

	// Replace each pod in turn, writing continuously throughout.
	for _, victim := range append([]string{}, f.ids...) {
		// Drain: mark unready, announce departure, hand off, deactivate.
		if err := f.caches[victim].Drain(ctx); err != nil {
			t.Fatalf("Drain(%s): %v", victim, err)
		}
		handedOff += f.caches[victim].Stats().KeysHandedOff

		f.hardKill(t, victim)
		f.clk.Advance(2 * f.mcfg.FailureTimeout)
		f.convergeTo(t, rollingNodes-1)
		f.settle(t)

		// Write while the cluster is one node short.
		for i := 0; i < 12; i++ {
			key := fmt.Sprintf("during/%s/%d", victim, i)
			val := fmt.Sprintf("d-%s-%d", victim, i)
			requests++
			retries, err := writeCountingRetries(t, f, openNS, key, val)
			retryTotal += retries
			if err != nil {
				failures++
				t.Errorf("write %q failed during the upgrade of %s: %v", key, victim, err)
				continue
			}
			written[key] = val
		}

		// Bring the replacement up: same identity, empty memory.
		f.revive(t, victim)
		openNS(victim)
		f.clk.Advance(2 * f.mcfg.JoinDelay)
		f.converge(t)
		f.settle(t)

		// Catch up before serving, which is the documented restart
		// procedure and the whole reason readiness exists.
		if err := f.caches[victim].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s) after restart: %v", victim, err)
		}
		f.clk.Advance(2 * ownership.DefaultActivationDelay)
		f.settle(t)

		if st := f.caches[victim].Ready(); !st.Ready {
			t.Fatalf("%s is %s after a full restart cycle", victim, st)
		}
		upgraded++
	}

	t.Logf("upgraded %d of %d pods; %d writes during the upgrade, %d failed, %d retries, "+
		"%d keys handed off", upgraded, rollingNodes, requests, failures, retryTotal, handedOff)

	if upgraded != rollingNodes {
		t.Fatalf("upgraded %d pods, want %d", upgraded, rollingNodes)
	}
	if failures != 0 {
		t.Fatalf("%d of %d client requests failed during a rolling upgrade", failures, requests)
	}

	// Every key ever written must still be readable, with the value it was
	// written with.
	live := f.live()
	for key, want := range written {
		got, err := readWithRetry(t, f, live[0], key)
		if err != nil {
			t.Fatalf("after the upgrade, %q is unreadable: %v", key, err)
		}
		if string(got) != want {
			t.Fatalf("after the upgrade, %q = %q, want %q", key, got, want)
		}
	}

	// And movement stayed within the theoretical bound.
	placementAfter := placementSnapshot(t, f, placementSample)
	moved := 0
	for key, before := range placementBefore {
		if placementAfter[key] != before {
			moved++
		}
	}

	// Each pod replacement is two membership changes, and the cluster ends
	// where it started — so a key's *final* owner should mostly be its
	// original owner. What moved is what the ranking genuinely reassigned.
	t.Logf("%d of %d keys have a different owner after the full upgrade cycle",
		moved, len(placementBefore))
	if moved > len(placementBefore)/10 {
		t.Fatalf("%d of %d keys changed owner across a round trip that ended with the same "+
			"membership; a rolling upgrade must not permanently reshuffle the keyspace",
			moved, len(placementBefore))
	}
}

// TestRollingUpgradeMovementIsBounded measures the transfer volume against the
// derived expectation, which is the second half of the criterion.
func TestRollingUpgradeMovementIsBounded(t *testing.T) {
	// Measured on the planner rather than on a live cluster: the criterion
	// is about how much *must* move, which is a property of the placement
	// function. A live measurement would also include retries and catch-up,
	// and conflating them would make the number unfalsifiable.
	keys := make([]string, rollingKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("rolling/%d", i)
	}

	full := viewOf(rollingNodes, "")
	totalMoves := 0
	for i := 0; i < rollingNodes; i++ {
		victim := fmt.Sprintf("n%d", i)
		short := viewOf(rollingNodes, victim)

		out, err := rebalance.Compute(full, short, keys, rollingRF)
		if err != nil {
			t.Fatalf("Compute (leave): %v", err)
		}
		back, err := rebalance.Compute(short, full, keys, rollingRF)
		if err != nil {
			t.Fatalf("Compute (rejoin): %v", err)
		}
		totalMoves += out.Total() + back.Total()
	}

	// Nine pods, each leaving and rejoining, at rf/9 of the keyspace per
	// change.
	perChange := rebalance.ExpectedMoveFraction(rollingNodes, rollingNodes-1, rollingRF)
	expected := perChange * float64(rollingKeys) * float64(rollingNodes) * 2
	deviation := math.Abs(float64(totalMoves)-expected) / expected

	t.Logf("rolling upgrade of %d pods at rf=%d over %d keys: %d transfers "+
		"(theoretical %.0f, %.1f%% off)",
		rollingNodes, rollingRF, rollingKeys, totalMoves, expected, deviation*100)

	if deviation > 0.10 {
		t.Fatalf("a rolling upgrade moves %d keys, theoretically %.0f (%.1f%% off, "+
			"budget 10%%)", totalMoves, expected, deviation*100)
	}
}

// writeWithRetry performs a write the way a real client would: retrying the
// errors the library explicitly marks retryable.
func writeWithRetry(t *testing.T, f *fabric, ns func(string) peercache.Namespace,
	key, val string) error {
	t.Helper()
	_, err := writeCountingRetries(t, f, ns, key, val)
	return err
}

// writeCountingRetries is writeWithRetry, reporting how many attempts it took.
//
// The retry budget is small on purpose. A client that retried indefinitely
// would make "zero failed requests" true of any system that eventually
// converges, which is not what the criterion is asking.
func writeCountingRetries(t *testing.T, f *fabric, ns func(string) peercache.Namespace,
	key, val string) (int, error) {
	t.Helper()
	const attempts = 20

	var lastErr error
	for i := 0; i < attempts; i++ {
		live := f.live()
		via := live[i%len(live)]
		err := ns(via).Set(context.Background(), key, []byte(val), 0)
		if err == nil {
			return i, nil
		}
		lastErr = err
		if !isRetryable(err) {
			return i, err
		}
		// Let the cluster make progress between attempts.
		f.settle(t)
	}
	return attempts, lastErr
}

// readWithRetry is the same for reads.
func readWithRetry(t *testing.T, f *fabric, via, key string) ([]byte, error) {
	t.Helper()
	const attempts = 20

	var lastErr error
	for i := 0; i < attempts; i++ {
		got, err := f.caches[via].Namespace("rolling").Get(context.Background(), key)
		if err == nil {
			return got, nil
		}
		lastErr = err
		if !isRetryable(err) {
			return nil, err
		}
		f.settle(t)
	}
	return nil, lastErr
}

// isRetryable reports whether an error is one the cluster is asking the caller
// to try again for.
func isRetryable(err error) bool {
	return errors.Is(err, peercache.ErrNotOwner) ||
		errors.Is(err, peercache.ErrActivationPending) ||
		errors.Is(err, peercache.ErrStaleEpoch) ||
		errors.Is(err, peercache.ErrNodeUnreachable)
}

// viewOf builds an ownership view over n0..n(size-1), optionally omitting one.
func viewOf(size int, drop string) *ownership.Table {
	tbl := ownership.NewTable()
	var nodes []core.Node
	for i := 0; i < size; i++ {
		id := fmt.Sprintf("n%d", i)
		if id == drop {
			continue
		}
		nodes = append(nodes, core.Node{ID: id})
	}
	tbl.Update(nodes, core.Epoch(size))
	return tbl
}

// placementSnapshot records which node owns each key.
func placementSnapshot(t *testing.T, f *fabric, keys int) map[string]string {
	t.Helper()
	out := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("rolling/%d", i)
		for _, id := range f.live() {
			if f.probeOwns(id, key) == nil {
				out[key] = id
				break
			}
		}
	}
	return out
}

// TestDrainSavesDataWithoutReplication is where the hand-off actually earns its
// keep, and it exists because a mutation showed that the rolling-upgrade test
// above does not.
//
// At RF=3 a graceful drain is very nearly redundant: the keys are already on
// two other nodes, promotion moves ownership for free, and catch-up restores
// the returning pod. Deleting the entire hand-off left the rolling upgrade
// passing. That is worth knowing — it means replication, not drain, is what
// makes a rolling upgrade safe — but it also means the rolling upgrade cannot
// be the test that verifies drain.
//
// At RF=1 there are no replicas. The only thing standing between a graceful
// shutdown and losing that node's entire share is the hand-off, so this is the
// configuration where it is load-bearing.
func TestDrainSavesDataWithoutReplication(t *testing.T) {
	f := newFabricTuned(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	})
	ctx := context.Background()

	// Replication factor 1: owner only, no copies anywhere.
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("solo", peercache.WithReplicationFactor(1))
	}
	for _, id := range f.ids {
		openNS(id)
	}

	const keys = 600
	written := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("solo/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := writeWithRetryNS(t, f, openNS, key, val); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
		written[key] = val
	}

	// Pick a node that owns a meaningful share.
	victim := ""
	var owned int64
	for _, id := range f.ids {
		if n := f.caches[id].Stats().OwnedKeys; n > owned {
			victim, owned = id, n
		}
	}
	if owned == 0 {
		t.Fatal("no node owns anything")
	}
	t.Logf("draining %s, which owns %d of %d keys with no replicas anywhere", victim, owned, keys)

	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	handed := f.caches[victim].Stats().KeysHandedOff
	if handed == 0 {
		t.Fatalf("a drain of a node owning %d keys handed off nothing; at RF=1 those keys "+
			"exist nowhere else and are about to be lost", owned)
	}
	t.Logf("handed off %d keys", handed)

	f.hardKill(t, victim)
	f.clk.Advance(2 * f.mcfg.FailureTimeout)
	f.convergeTo(t, 4)
	f.settle(t)

	// Every key must still be readable from a survivor.
	//
	// Through readAnywhere rather than a single Get on one node, because the
	// two errors this can legitimately return mean "ask again", not "gone". A
	// membership change starts the activation guard's delay on every group it
	// moved, and ownership may have moved off whichever survivor is first in
	// the list.
	//
	// Counting those as loss is how this test failed in a full-package run
	// while passing five times out of five alone: under load one key landed
	// inside the deferral window, the loop recorded it as lost by a graceful
	// shutdown, and no data had gone anywhere. It is the same mistake
	// readAnywhere was written to stop making, in a test that predates it.
	var lost int
	for key, want := range written {
		got, err := readAnywhere(t, f, func(id string) peercache.Namespace {
			return f.caches[id].Namespace("solo")
		}, key)
		if err != nil {
			lost++
			continue
		}
		if got != want {
			t.Fatalf("%q = %q after the drain, want %q", key, got, want)
		}
	}
	if lost > 0 {
		t.Fatalf("%d of %d keys were lost by a *graceful* shutdown at RF=1; the hand-off "+
			"is the only thing that saves them and it did not", lost, keys)
	}
}

// writeWithRetryNS is writeWithRetry for a caller-supplied namespace accessor.
func writeWithRetryNS(t *testing.T, f *fabric, ns func(string) peercache.Namespace,
	key, val string) error {
	t.Helper()
	_, err := writeCountingRetries(t, f, ns, key, val)
	return err
}

// TestDrainStopsPeersRoutingImmediately is what the departure announcement
// actually buys, as distinct from what the hand-off buys.
//
// A mutation made this distinction necessary. Deleting the Leave call left the
// data-safety test passing, because failure detection eventually notices a dead
// node and promotion covers the keys either way. So Leave is not what saves the
// data — the hand-off is.
//
// What Leave saves is *time*. Without it, peers keep routing to a draining node
// until the failure timeout expires, and every request in that window has to be
// retried. With it, ownership moves the moment the announcement lands.
func TestDrainStopsPeersRoutingImmediately(t *testing.T) {
	f := newFabricTuned(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 30 * time.Second, // deliberately long
		JoinDelay:      200 * time.Millisecond,
	})
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("routing", peercache.WithReplicationFactor(2))
	}

	victim := f.ids[0]
	observer := f.ids[1]

	// Find a key the victim owns, as seen by another node.
	var key string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("routing/%d", i)
		if f.probeOwns(victim, k) == nil {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatalf("no key of 5000 resolves to %s", victim)
	}

	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// No clock advance at all. The failure timeout is thirty seconds away,
	// so anything the peers have noticed can only be the announcement.
	//
	// The expected state is deliberately asymmetric: peers drop the victim
	// from their view while the victim still counts itself. That asymmetry
	// *is* the property — it means ownership moved on the announcement
	// rather than on a timeout.
	const maxYields = 5_000_000
	for i := 0; ; i++ {
		peersDropped := true
		for _, id := range f.ids {
			if id == victim {
				continue
			}
			if f.caches[id].Stats().Members != len(f.ids)-1 {
				peersDropped = false
				break
			}
		}
		if peersDropped {
			break
		}
		if i > maxYields {
			var seen []string
			for _, id := range f.ids {
				seen = append(seen, fmt.Sprintf("%s=%d", id, f.caches[id].Stats().Members))
			}
			t.Fatalf("peers had not dropped %s after %d yields with no clock advance and "+
				"the failure timeout %v away: %v; every request for its keys would be "+
				"routed to a node on its way out", victim, maxYields, 30*time.Second, seen)
		}
		runtime.Gosched()
	}
	f.settle(t)

	// And the key has a new owner among the survivors.
	if f.probeOwns(observer, key) != nil {
		found := false
		for _, id := range f.ids {
			if id != victim && f.probeOwns(id, key) == nil {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("no surviving node owns %q after %s drained", key, victim)
		}
	}
}

// TestAnImplicitNamespaceIsCountedAndReplicatesNothing pins the hazard that
// looked like a data-loss bug for most of a milestone.
//
// A write routed to its owner creates the namespace there if the application
// has not opened it — with default options, so replication factor 1. Options
// are fixed at creation, so that node replicates nothing for that namespace
// ever after, however it is configured later. Killing it loses its whole share
// at what the operator believes is RF=3.
//
// The library cannot refuse the write without breaking routing, so it counts
// and logs instead. This test is what stops that counter being quietly dropped.
func TestAnImplicitNamespaceIsCountedAndReplicatesNothing(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	// Open the namespace on ONE node only, which is the mistake.
	writer := f.ids[0]
	ns := f.caches[writer].Namespace("implicit",
		peercache.WithReplicationFactor(3),
		peercache.WithSyncReplication())

	const keys = 200
	for i := 0; i < keys; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("implicit/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// The other owners created the namespace implicitly and replicate
	// nothing, so the cluster holds far fewer copies than RF=3 implies.
	var implicit, sent int64
	for _, id := range f.ids {
		st := f.caches[id].Stats()
		implicit += st.ImplicitNamespaces
		sent += st.ReplicationsSent
	}
	if implicit == 0 {
		t.Fatal("no implicit namespace was counted, yet only one node opened it; the " +
			"misconfiguration that silently disables replication is invisible")
	}
	t.Logf("%d nodes created the namespace implicitly; %d replications sent "+
		"(RF=3 over %d keys would be %d)", implicit, sent, keys, keys*2)

	// The shortfall is the point: this is what "replication factor 3" is
	// actually delivering under this misconfiguration.
	if sent >= int64(keys*2) {
		t.Fatalf("%d replications sent for %d keys at RF=3; the test is not reproducing "+
			"the misconfiguration", sent, keys)
	}

	// And with every node configured properly, replication is complete.
	for _, id := range f.ids {
		f.caches[id].Namespace("proper",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	before := int64(0)
	for _, id := range f.ids {
		before += f.caches[id].Stats().ReplicationsSent
	}
	for i := 0; i < keys; i++ {
		if err := f.caches[writer].Namespace("proper").Set(
			ctx, fmt.Sprintf("proper/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	after := int64(0)
	for _, id := range f.ids {
		after += f.caches[id].Stats().ReplicationsSent
	}
	if got := after - before; got != int64(keys*2) {
		t.Fatalf("with every node configured, %d replications were sent for %d keys at "+
			"RF=3; want exactly %d", got, keys, keys*2)
	}
}
