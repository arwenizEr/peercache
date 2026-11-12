package tests

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

const probation = 30 * time.Second

// newProbationFabric builds a five-node cluster with join probation and a
// replicated namespace on every node, past its cold start, with keys written.
func newProbationFabric(t *testing.T, keys int) (*fabric, func(string) peercache.Namespace) {
	t.Helper()
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithJoinProbation(probation)))
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("probation",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	// Past the cold start: every node is now older than the probation.
	f.clk.Advance(probation + time.Second)
	f.settle(t)

	ctx := context.Background()
	for i := 0; i < keys; i++ {
		if err := openNS(f.ids[i%4]).Set(ctx, fmt.Sprintf("probation/%04d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	return f, openNS
}

// waitForRawMembers waits until a node's membership authority — gossip, not the
// ranked view — lists want. Probation hides a young node from the ranking, so
// the ranked view cannot be what says it has rejoined.
func waitForRawMembers(t *testing.T, f *fabric, id string, want []string) {
	t.Helper()
	waitForCache(t, fmt.Sprintf("%s's membership to list %v", id, want), func() bool {
		got := f.memberships[id].MemberIDs()
		for _, w := range want {
			if !slices.Contains(got, w) {
				return false
			}
		}
		return true
	})
}

// TestACrashloopingNodeIsNeverRanked is the crashloop criterion: a node
// restarted every 2 seconds for 60 seconds causes no key held by the healthy
// nodes to be reported missing.
//
// Without probation each restart is a join. Every peer ranks the new node at
// once, a fifth of the keys move to a node holding nothing, and until it has
// caught up — it never does, it dies first — reads of those keys come back
// ErrNotFound for data that four healthy nodes hold between them.
func TestACrashloopingNodeIsNeverRanked(t *testing.T) {
	const keys = 300
	f, openNS := newProbationFabric(t, keys)
	survivors := []string{"n0", "n1", "n2", "n3"}
	victim := "n4"

	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 4)
	f.settle(t)

	missing := 0
	for round := 0; round < 30; round++ {
		f.revive(t, victim)
		openNS(victim)
		// Two seconds of life: long enough for gossip to admit it,
		// far short of the probation.
		f.clk.Advance(2 * time.Second)
		waitForRawMembers(t, f, survivors[0], append(slices.Clone(survivors), victim))
		// No settle here: a probationed join changes no ranked view, so
		// there is nothing to reconcile — which is the claim under test.

		if st := f.caches[victim].Stats(); st.OwnedKeys != 0 {
			t.Fatalf("round %d: the restarted node owns %d keys %v into its life", round,
				st.OwnedKeys, 2*time.Second)
		}
		for i := 0; i < keys; i++ {
			via := survivors[i%len(survivors)]
			if _, err := readWithRetryFrom(t, f, openNS, via, fmt.Sprintf("probation/%04d", i)); err != nil {
				if errors.Is(err, peercache.ErrNotFound) {
					missing++
					continue
				}
				t.Fatalf("round %d: Get %d via %s: %v", round, i, via, err)
			}
		}

		f.hardKill(t, victim)
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		// Wait for the death to be gossiped, so the next restart is a
		// fresh join rather than a node that never left.
		waitForCache(t, victim+" to leave the survivors' membership", func() bool {
			return !slices.Contains(f.memberships[survivors[0]].MemberIDs(), victim)
		})
	}
	if missing != 0 {
		t.Fatalf("a node crashlooping for 30 rounds made %d reads of keys held by the "+
			"healthy nodes come back ErrNotFound", missing)
	}
	for _, id := range survivors {
		if n := f.caches[id].Stats().MembersOnProbation; n != 0 {
			t.Fatalf("%s still counts %d members on probation after the victim died", id, n)
		}
	}
}

// TestAProbationedNodeIsRankedOnceItHasLasted is the other half: a node that
// stays up past its probation joins the ranking, and then behaves like any
// other joining node.
func TestAProbationedNodeIsRankedOnceItHasLasted(t *testing.T) {
	const keys = 300
	f, openNS := newProbationFabric(t, keys)
	ctx := context.Background()
	victim := "n4"

	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 4)
	f.settle(t)

	f.revive(t, victim)
	openNS(victim)
	f.clk.Advance(2 * time.Second)
	waitForRawMembers(t, f, "n0", []string{"n0", "n1", "n2", "n3", victim})
	f.settle(t)

	if got := f.caches["n0"].Stats().MembersOnProbation; got != 1 {
		t.Fatalf("n0 counts %d members on probation two seconds after %s rejoined, want 1", got, victim)
	}
	assertMetric(t, f.caches["n0"], "peercache_members_on_probation")
	if got := f.caches[victim].Stats().Members; got != 4 {
		t.Fatalf("the young node's own ranked view has %d members, want the 4 established "+
			"ones: it must not rank itself before its peers do", got)
	}

	f.clk.Advance(probation)
	f.convergeTo(t, 5)
	f.settle(t)

	for _, id := range f.ids {
		if got := f.caches[id].Stats().MembersOnProbation; got != 0 {
			t.Fatalf("%s counts %d members on probation after it ended", id, got)
		}
	}
	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp(%s): %v", victim, err)
	}
	f.settle(t)
	if st := f.caches[victim].Stats(); st.OwnedKeys == 0 {
		t.Fatalf("%s outlasted its probation and caught up, and owns nothing", victim)
	}
	for i := 0; i < keys; i++ {
		if _, err := openNS(f.ids[i%5]).Get(ctx, fmt.Sprintf("probation/%04d", i)); err != nil {
			t.Fatalf("Get %d after %s was ranked: %v", i, victim, err)
		}
	}
}

// TestColdStartRanksEveryNode covers the case the rule's exception exists for:
// a cluster whose members are all younger than the probation. Excluding them all
// would leave nobody to own anything, so all of them are ranked.
func TestColdStartRanksEveryNode(t *testing.T) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithJoinProbation(probation)))
	f.settle(t)

	owners := map[string]bool{}
	for i := 0; i < 200; i++ {
		owners[f.ownerOf(t, fmt.Sprintf("cold/%d", i))] = true
	}
	if len(owners) != 5 {
		t.Fatalf("at cold start, with every node younger than the probation, %d of 5 nodes "+
			"own anything", len(owners))
	}
	if n := f.caches["n0"].Stats().MembersOnProbation; n != 0 {
		t.Fatalf("cold start counts %d members on probation; the exception ranks them all", n)
	}
}

// TestJoinProbationMustNotBeNegative pins the option's validation.
func TestJoinProbationMustNotBeNegative(t *testing.T) {
	_, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithJoinProbation(-time.Second))
	if !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New with a negative probation returned %v, want ErrInvalidOption", err)
	}
}
