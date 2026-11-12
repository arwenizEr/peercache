package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// Replication acceptance, from docs/design.md §4:
//
//	a chaos test that kills a random node every 2 seconds for 60 seconds
//	loses no key that was written with sync replication
//
// The criterion names a cadence and a duration but not a cluster size or a
// replication factor, and those decide whether it is achievable at all. They
// are pinned here rather than chosen quietly:
//
//   - RF=3. At RF=2 a key exists on two nodes, so two kills close enough
//     together take both. That is not a defect, it is what RF=2 means — and a
//     test that passed only at RF=3 while implying RF=2 would also pass would
//     be lying by omission. RF=2 is exercised separately below and is expected
//     to lose data.
//   - Kill *and restart*. Thirty kills of a cluster that never restarts
//     anything leaves nothing alive long before sixty seconds. Kubernetes
//     reschedules; so does this.
//   - Five nodes. Enough that RF=3 has somewhere to place copies after a kill,
//     small enough to run in seconds of simulated time.
//
// Time is simulated. Sixty seconds of chaos costs milliseconds of wall clock,
// and every kill lands at an exactly reproducible instant — which is what makes
// a failure debuggable rather than a story about a bad afternoon.
const (
	chaosDuration = 60 * time.Second
	chaosInterval = 2 * time.Second
	chaosNodes    = 5
)

// TestChaosLosesNoSynchronouslyWrittenKey is the replication acceptance criterion.
func TestChaosLosesNoSynchronouslyWrittenKey(t *testing.T) {
	// Detection must be faster than the kill cadence, or the cluster never
	// converges between kills and the test measures a permanently confused
	// system rather than a recovering one. That is a real constraint the
	// criterion implies without saying: killing every two seconds requires
	// noticing in less than two seconds.
	f := newFabricTuned(t, chaosNodes, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	})
	ctx := context.Background()

	const rf = 3
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("chaos",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}

	written := make(map[string]string)
	rounds := int(chaosDuration / chaosInterval)
	killed, revived := 0, 0

	for round := 0; round < rounds; round++ {
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("chaos/%d/%d", round, i)
			val := fmt.Sprintf("r%d-i%d", round, i)
			live := f.live()
			via := live[(round+i)%len(live)]

			if err := openNS(via).Set(ctx, key, []byte(val), 0); err != nil {
				// A write that failed is not a loss: the caller
				// saw the error and did not record the key. Only
				// the errors a reconfiguring cluster legitimately
				// produces are tolerated here.
				if !errors.Is(err, peercache.ErrNotOwner) &&
					!errors.Is(err, peercache.ErrNodeUnreachable) &&
					!errors.Is(err, peercache.ErrActivationPending) &&
					!errors.Is(err, peercache.ErrStaleEpoch) {
					t.Fatalf("round %d: unexpected write error for %q: %v", round, key, err)
				}
				continue
			}
			// This write returned success under synchronous
			// replication, so the cluster promised the key is on rf
			// nodes. That promise is what the test holds it to.
			written[key] = val
		}

		live := f.live()
		if len(live) > rf {
			victim := live[round%len(live)]
			f.hardKill(t, victim)
			killed++
			// Advance past detection, then the rest of the interval.
			f.clk.Advance(f.mcfg.FailureTimeout * 2)
			f.convergeTo(t, len(f.live()))
			f.settle(t)

			f.revive(t, victim)
			revived++
			openNS(victim)
			f.clk.Advance(f.mcfg.JoinDelay * 2)
			f.converge(t)
			f.settle(t)

			// Catch up before serving. This is not test scaffolding:
			// it is the recovery procedure, and running the chaos
			// test without it is what showed why it exists.
			//
			// A restarted node reclaims ownership of keys it no
			// longer holds — the ranking says it owns them, its
			// memory says it has nothing — so it answers "not found"
			// with total confidence for data sitting on the node that
			// covered for it. 558 of 600 keys "lost" that way, none
			// of them actually gone.
			//
			// That is worse than being unavailable, and it is why
			// Cache.CatchUp exists and why README and docs/api.md
			// say a restarted pod must call it before serving.
			if err := f.caches[victim].CatchUp(ctx); err != nil {
				t.Fatalf("round %d: CatchUp(%s): %v", round, victim, err)
			}
			f.settle(t)
		} else {
			f.clk.Advance(chaosInterval)
		}
	}

	t.Logf("%d rounds: %d kills, %d restarts, %d keys written synchronously",
		rounds, killed, revived, len(written))
	if killed < rounds/2 {
		t.Fatalf("only %d kills in %d rounds; the test is not chaotic", killed, rounds)
	}
	if len(written) < 100 {
		t.Fatalf("only %d keys were written; the cluster was too broken to test", len(written))
	}

	// Through readAnywhere, not a single Get on one node.
	//
	// After sixty seconds of kills the surviving nodes are still settling:
	// ownership has moved, and a membership change starts the activation
	// guard's delay on every group it moved. Both produce errors that mean
	// "ask again" rather than "gone", and counting them here would report a
	// key as lost by chaos when it is merely deferred — a false alarm on the
	// replication acceptance criterion, which is the most credible place in the suite
	// to get one. readAnywhere retries those and only those.
	var lost []string
	for key, want := range written {
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil {
			lost = append(lost, fmt.Sprintf("%s (%v)", key, err))
			continue
		}
		if got != want {
			lost = append(lost, fmt.Sprintf("%s (value %q, want %q)", key, got, want))
		}
	}
	if len(lost) > 0 {
		show := lost
		if len(show) > 10 {
			show = show[:10]
		}
		t.Fatalf("%d of %d synchronously written keys were lost after %d kills: %v",
			len(lost), len(written), killed, show)
	}
}

// TestChaosAtReplicationFactorTwoCanLoseData is the honest companion.
//
// It exists so the criterion above cannot be read as "replication makes data
// safe". At RF=2 a key lives on two nodes; killing both loses it, and no amount
// of correct code prevents that. Writing it as a test rather than a caveat in
// prose keeps the claim true as the code changes.
//
// It asserts only that survivors stay *correct*: a key that survives has the
// value it was written with, and one that is gone is gone rather than silently
// wrong. Losing data at RF=2 is acceptable; corrupting it is not.
func TestChaosAtReplicationFactorTwoCanLoseData(t *testing.T) {
	f := newFabric(t, 4)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("rf2",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication())
	}

	written := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("rf2/%d", i)
		val := fmt.Sprintf("v%d", i)
		if err := f.caches[f.ids[0]].Namespace("rf2").Set(ctx, key, []byte(val), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		written[key] = val
	}

	// Two kills in quick succession, enough to take both holders of some
	// key at RF=2.
	f.hardKill(t, f.ids[0])
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 3)
	f.settle(t)

	f.hardKill(t, f.ids[1])
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	// Same reason as above, with one difference worth stating: this test
	// *expects* losses, because RF=2 does not survive two kills. That makes
	// distinguishing "not found" from "not yet" more important here, not less
	// — a deferral counted as either outcome corrupts the number this test
	// exists to report.
	rf2NS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("rf2")
	}
	lost, kept := 0, 0
	for key, want := range written {
		got, err := readAnywhere(t, f, rf2NS, key)
		switch {
		case errors.Is(err, peercache.ErrNotFound):
			lost++
		case err != nil:
			t.Fatalf("unexpected error reading %q: %v", key, err)
		case got != want:
			t.Fatalf("%q survived with the wrong value %q, want %q; losing data at RF=2 "+
				"is acceptable and corrupting it is not", key, got, want)
		default:
			kept++
		}
	}

	t.Logf("RF=2 after two kills: %d keys kept, %d lost of %d", kept, lost, len(written))
	if kept == 0 {
		t.Fatal("every key was lost; replication did nothing at all")
	}
}
