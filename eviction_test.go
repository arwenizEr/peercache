package peercache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestEvictionDoesNotDropTheLastCopy guards an ordering hazard that the code
// does not currently protect against, and which no test had covered.
//
// # Where it came from, and what it is not
//
// The simulation lost keys after a single node destruction at RF=3
// (PEERCACHE_SEED=601996618416914900), which RF=3 should survive. Reading the
// code suggested eviction: internal/replication.Store.Evict drops every replica
// this node no longer ranks for, decided purely from the local ranking --
//
//	dropped := c.replicas.Evict(name, func(key string) bool {
//	        return c.stillReplicaFor(ns.groupKeyFor(key), rf, self)
//	})
//
// -- with nothing asking whether a node that now ranks for the key actually
// holds it. The comment above that call reasons carefully about ordering within
// one node (promote before evict) and not at all across nodes.
//
// That reasoning was right about the hazard and wrong about this schedule. The
// kill-and-rejoin churn below produces zero evictions and zero losses: a
// rejoining node does not take replica ranking away from the holder of a last
// copy when the copies were placed under the full membership in the first place.
//
// **A partition is what reaches it**, found once question 38's
// assertion was finally written. Writes accepted while one node is cut off place
// their copies at ranking positions computed *without* that node; when it
// returns, it ranks for many of those keys and holds none, and the nodes that do
// hold them stop ranking and drop them. Twenty-one replicas evicted, twenty keys
// lost — see TestMajoritySideWritesSurviveAHealAndTwoDestructions, which
// reproduces it, and handoff.go, which fixes it by moving a replica to its new
// holders before dropping the local copy.
//
// This test stays as the other half of the pair: the schedule that must keep
// costing nothing. Nothing else in the suite would notice if a future change to
// the ranking, the replication factor, or the reconciliation order made ordinary
// churn start moving data it does not need to move.
func TestEvictionDoesNotDropTheLastCopy(t *testing.T) {
	f := newFabric(t, 5)
	ctx := context.Background()

	const rf = 2
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("evictloss",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	const keys = 400
	written := make(map[string]string, keys)
	writer := f.ids[0]
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("el/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := openNS(writer).Set(ctx, key, []byte(val), 0); err != nil {
			t.Fatalf("Set(%s): %v", key, err)
		}
		written[key] = val
	}

	// Everything is readable before the churn, so a later failure is
	// attributable to the churn and not to seeding.
	for key, want := range written {
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil || got != want {
			t.Fatalf("before churn, %s = %q (%v), want %q", key, got, err, want)
		}
	}

	// One node leaves and returns empty. Its return is the interesting half:
	// it reclaims ranking for keys it does not have, and the nodes that do
	// have them stop ranking for them.
	victim := f.ids[4]
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	f.settle(t)

	f.revive(t, victim)
	openNS(victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.convergeTo(t, len(f.ids))
	f.settle(t)

	// Catch-up on every node, which is the strongest repair the design
	// offers. If a key is gone after this, it is gone.
	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.settle(t)

	var lost []string
	for key, want := range written {
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil || got != want {
			lost = append(lost, fmt.Sprintf("%s (want %q, got %q, err %v)", key, want, got, err))
		}
	}

	var evicted int64
	for _, id := range f.ids {
		evicted += f.caches[id].Stats().ReplicasEvicted
	}
	t.Logf("%d of %d keys lost; %d replicas evicted across the cluster",
		len(lost), len(written), evicted)

	if len(lost) > 0 {
		sample := lost
		if len(sample) > 5 {
			sample = sample[:5]
		}
		t.Fatalf("%d of %d keys did not survive one node leaving and rejoining at "+
			"replication factor %d.\n"+
			"One destruction; RF=%d is supposed to survive %d.\n"+
			"%d replicas were evicted cluster-wide during the churn.\n"+
			"Evict drops a replica whenever this node stops ranking for the key, "+
			"without checking that a node which now ranks for it holds a copy. "+
			"Catch-up cannot repair this: it pulls from whoever has the key, and "+
			"after the eviction nobody does.\nexamples: %v",
			len(lost), len(written), rf, rf, rf-1, evicted, sample)
	}
}

// TestEvictionKeepsTheCopyWhenNobodyElseHasIt states the property the fix must
// establish, independently of how it is implemented.
//
// It is deliberately written against behaviour rather than internals: a replica
// may be dropped only when the key is retrievable from somewhere else. Any
// implementation satisfying that — asking the new holders first, deferring
// eviction until catch-up reports success, or pushing before dropping — passes.
func TestEvictionKeepsTheCopyWhenNobodyElseHasIt(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const rf = 2
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("evictkeep",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	const keys = 200
	written := make(map[string]string, keys)
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("ek/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := writeWithRetry(t, f, openNS, key, val); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
		written[key] = val
	}

	victim := f.ids[2]
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	f.revive(t, victim)
	openNS(victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.convergeTo(t, 3)
	f.settle(t)
	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.settle(t)

	missing := 0
	for key, want := range written {
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil || got != want {
			missing++
		}
	}
	if missing > 0 {
		t.Fatalf("%d of %d keys are unreadable after one node left and rejoined. "+
			"A replica may only be dropped once the key is retrievable elsewhere",
			missing, len(written))
	}
}

// TestAReplicaInANonReplicatingNamespaceIsDroppedNotHandedOff covers the one
// place the hand-off must *not* apply, and it exists because that branch was
// written as a bug fix and then had no test.
//
// # Why the branch exists
//
// A namespace at replication factor 1 has no replica set, so `replicaTargets`
// resolves nothing for it. Handing an orphan off is therefore not merely
// pointless — it does not terminate: the placement fails, the copy is restored,
// the next reconciliation orphans it again, and the counters climb forever while
// the key sits in a store nothing reads.
//
// Copies do arrive in such a namespace. `Drain` deliberately pushes to
// `max(rf, 2)` targets so that a departing node's keys survive even at RF=1, and
// whichever target does not own the key files it as a replica.
//
// # This test usually SKIPs, and that is the honest outcome
//
// A coverage run found the branch unexercised, which is the same shape of gap as
// the mutation audit's VACUOUS verdict — a branch whose correctness is asserted
// by nothing — and it was introduced by a fix, which is the worst place for one.
//
// Constructing the condition turned out to be beyond what the public API offers
// here. A drain at RF=1 pushes to two targets, and in this harness both receivers
// adopt their own share into their owned stores; the surplus copy that would land
// in a replica store never appears, so the branch is never reached. The drain
// handler comment describes that surplus copy as a case it has seen, so the
// condition is real, but nothing in this suite produces it on demand.
//
// The test is kept skipping rather than deleted. It states the property, names
// the reason it did not run, and starts asserting the moment a change makes the
// condition reachable — which is more than a comment does, and considerably more
// than a test that quietly passes without executing the branch it names.
func TestAReplicaInANonReplicatingNamespaceIsDroppedNotHandedOff(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	// Replication factor 1: no replica set, nowhere to hand anything.
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("norf", peercache.WithReplicationFactor(1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	for i := 0; i < 150; i++ {
		if err := writeWithRetry(t, f, openNS, fmt.Sprintf("nr/%d", i), "v"); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	f.settle(t)

	// Drain pushes to two targets even at RF=1, so the node that does not own a
	// key files it as a replica. That is how a copy gets into a replica store
	// for a namespace that replicates nothing.
	victim := f.ids[0]
	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.settle(t)

	// Reconciliation runs on a membership change. Give it several, so that a
	// restore-and-re-orphan spin would be unmistakable rather than marginal.
	survivors := []string{f.ids[1], f.ids[2]}
	for i := 0; i < 3; i++ {
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		f.settle(t)
	}

	var held, evicted, retained, rehomed int64
	for _, id := range survivors {
		s := f.caches[id].Stats()
		held += s.ReplicaKeys
		evicted += s.ReplicasEvicted
		retained += s.ReplicasRetained
		rehomed += s.ReplicasRehomed
	}
	t.Logf("after the drain and reconciliation: %d replicas held, %d evicted, "+
		"%d retained, %d rehomed", held, evicted, retained, rehomed)

	// The assertion is about what reconciliation *did*, not what is left: the
	// branch under test drops the copy, so zero held is the expected outcome and
	// says nothing on its own about which path produced it.
	if evicted == 0 {
		t.Skip("the drain left no surplus replica at RF=1, so this schedule does not " +
			"reach the branch under test")
	}
	if held != 0 {
		t.Fatalf("%d replicas remain in a namespace at replication factor 1 after "+
			"%d were orphaned. There is no replica set for them to belong to, and a "+
			"copy nothing reads is a copy that only grows", held, evicted)
	}

	if retained != 0 {
		t.Fatalf("%d replicas were retained in a namespace at replication factor 1. "+
			"There is no replica set to hand them to, so a hand-off can only fail, "+
			"restore the copy, and orphan it again on the next reconciliation — an "+
			"unbounded spin over keys nothing will ever read", retained)
	}
	if rehomed != 0 {
		t.Fatalf("%d replicas were rehomed in a namespace at replication factor 1, "+
			"which has no replica set for them to be rehomed into", rehomed)
	}

	// And the keys themselves are still served, which is the point of drain
	// pushing to two targets in the first place.
	for i := 0; i < 150; i++ {
		key := fmt.Sprintf("nr/%d", i)
		if got, err := readAnywhere(t, f, openNS, key); err != nil || got != "v" {
			t.Fatalf("after the drain, %s = %q (%v); dropping the surplus replica must "+
				"not touch the copy the new owner took into its owned store", key, got, err)
		}
	}
}
