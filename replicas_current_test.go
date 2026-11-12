package peercache_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestCatchUpReportsAPeerItCouldNotReach pins CatchUp's error, which from M5
// until this test was written was always nil.
//
// The per-peer failure was logged and counted, and the error variable meant to
// carry it was declared and never assigned. docs/api.md has always shown
// `if err := cache.CatchUp(ctx); err != nil { log.Warn("catch-up incomplete") }`,
// and that branch could not run. A caller gating a rolling upgrade on CatchUp
// returning nil was told "done" by a node that had pulled from nobody.
func TestCatchUpReportsAPeerItCouldNotReach(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{
		FailureTimeout: 30 * time.Second, // long, so the view keeps the cut-off peer
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("catchup-err", peercache.WithReplicationFactor(2))
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	asker := f.ids[0]
	f.net.Partition([]string{asker}, []string{f.ids[1], f.ids[2]})
	t.Cleanup(func() { f.net.Heal() })

	if got := f.caches[asker].Stats().Members; got != 3 {
		t.Skipf("the asker's view already shrank to %d; this test needs it to still "+
			"list the peers it cannot reach", got)
	}

	before := f.caches[asker].Stats().CatchUpFailures
	err := f.caches[asker].CatchUp(ctx)
	if err == nil {
		t.Fatalf("CatchUp returned nil while every peer in its view was unreachable "+
			"(CatchUpFailures %d -> %d). The caller was told catch-up finished",
			before, f.caches[asker].Stats().CatchUpFailures)
	}
	if !errors.Is(err, peercache.ErrNodeUnreachable) {
		t.Fatalf("CatchUp returned %v; want it to wrap ErrNodeUnreachable so a caller "+
			"can tell an unreachable peer from a cancelled context", err)
	}
	if f.caches[asker].Stats().CatchUpFailures == before {
		t.Fatal("CatchUp returned an error but CatchUpFailures did not move")
	}
	if st := f.caches[asker].Ready(); st.ReplicasCurrent {
		t.Fatalf("ReplicasCurrent is true after a catch-up that reached nobody: %s", st)
	}
}

// TestReplicasCurrentFollowsCatchUpAndMembership is open question 40's third
// thing: a durability signal beside readiness, not inside it.
//
// Ready answers "can this node serve". ReplicasCurrent answers "has this node
// pulled, from every peer in its view, the replicas it ranks for under the
// membership it holds now". The first is true moments after a membership change;
// the second only after CatchUp has run, completely, since that change — which
// is the condition docs/api.md tells an operator to wait for before replacing
// the next pod.
func TestReplicasCurrentFollowsCatchUpAndMembership(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{},
		ownership.DefaultActivationDelay)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("current", peercache.WithReplicationFactor(2))
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	for _, id := range f.ids {
		if st := f.caches[id].Ready(); st.ReplicasCurrent {
			t.Fatalf("%s reports ReplicasCurrent before it has ever caught up: %s", id, st)
		}
	}

	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	for _, id := range f.ids {
		st := f.caches[id].Ready()
		if !st.ReplicasCurrent {
			t.Fatalf("%s reports ReplicasCurrent false after a complete catch-up under "+
				"unchanged membership: %s", id, st)
		}
		if !f.caches[id].Stats().ReplicasCurrent {
			t.Fatalf("%s: Ready says current, Stats does not", id)
		}
	}
	assertMetric(t, f.caches[f.ids[0]], "peercache_replicas_current")

	// Move the membership. Every survivor ranks differently now, so what it
	// pulled before no longer answers the question.
	survivors := []string{f.ids[0], f.ids[1]}
	f.hardKill(t, f.ids[2])
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	for _, id := range survivors {
		st := f.caches[id].Ready()
		if !st.Ready {
			t.Fatalf("precondition: %s is not ready after settling: %s", id, st)
		}
		if st.ReplicasCurrent {
			t.Fatalf("%s is ready and reports ReplicasCurrent after a member died and "+
				"before any catch-up. The two signals are the same signal again, "+
				"which is what question 40 is about: %s", id, st)
		}
	}

	for _, id := range survivors {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s) after the change: %v", id, err)
		}
		if st := f.caches[id].Ready(); !st.ReplicasCurrent {
			t.Fatalf("%s reports ReplicasCurrent false after catching up under the new "+
				"membership: %s", id, st)
		}
	}
}

// TestASingleNodeHasCurrentReplicas covers the configuration with nothing to
// pull from. It holds everything it owns, so it is current from the start, for
// the same reason it is ready from the start.
func TestASingleNodeHasCurrentReplicas(t *testing.T) {
	c, _ := newCache(t)
	if st := c.Ready(); !st.ReplicasCurrent {
		t.Fatalf("a single node reports %s; want ReplicasCurrent", st)
	}
	if !c.Stats().ReplicasCurrent {
		t.Fatal("a single node's Stats reports ReplicasCurrent false")
	}
}
