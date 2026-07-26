package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
)

// readyPollStep advances the fake clock past WaitReady's internal poll, which
// is measured on the injected clock like everything else in this library.
const readyPollStep = 50 * time.Millisecond

// TestASingleNodeIsReadyImmediately guards the configuration most applications
// start with. A cache with no membership authority owns everything, has nobody
// to catch up from, and nothing can change — making it wait would be a startup
// delay in exchange for nothing.
func TestASingleNodeIsReadyImmediately(t *testing.T) {
	c, _ := newCache(t)

	st := c.Ready()
	if !st.Ready {
		t.Fatalf("a single-node cache is %s, want ready at construction", st)
	}
	if !st.Joined || !st.Settled || !st.CaughtUp {
		t.Fatalf("Ready() = %+v; every condition should be trivially met", st)
	}
}

// TestANodeIsNotReadyBeforeCatchUp is the gate that resolves the M5 trap.
//
// A node becomes an owner the instant it joins, because every peer recomputes
// the ranking. It holds nothing until catch-up runs. Between those two moments
// it answers ErrNotFound for data that exists elsewhere — indistinguishable, to
// a caller, from a genuine miss. Readiness is what keeps traffic away until the
// two facts agree.
func TestANodeIsNotReadyBeforeCatchUp(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("ready",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const keys = 100
	for i := 0; i < keys; i++ {
		if err := f.caches[f.ids[0]].Namespace("ready").Set(
			ctx, fmt.Sprintf("ready/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	victim := f.ids[1]
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	f.settle(t)

	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	f.settle(t)

	// It is a member again — peers are already routing to it — and it holds
	// nothing.
	if st := f.caches[victim].Ready(); st.Ready {
		t.Fatalf("a restarted node reports %s before catching up; traffic would reach a "+
			"node that owns keys it does not have", st)
	} else if st.CaughtUp {
		t.Fatalf("Ready() = %s; CaughtUp must be false before CatchUp runs", st)
	}

	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	f.settle(t)

	if st := f.caches[victim].Ready(); !st.Ready {
		t.Fatalf("a node that has caught up reports %s, want ready", st)
	}
}

// TestANodeIsNotReadyWhileMembershipMoves covers the second condition. A node
// mid-change is one whose ownership answers are about to change, and routing to
// it means routing to an answer with a short shelf life.
//
// The unsettled state is created deliberately — by killing a node — rather than
// hoped for at construction. An earlier version checked whether the cluster
// happened to be unsettled just after forming and skipped when it was not,
// which meant it skipped every time and asserted nothing. A test that can skip
// itself into vacuity is worse than no test, because the suite still shows a
// line for it.
func TestANodeIsNotReadyWhileMembershipMoves(t *testing.T) {
	// A real activation delay, so settling is observable. The rest of the
	// suite disables it because those tests are about routing rather than
	// about the handover window.
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{},
		ownership.DefaultActivationDelay)
	ctx := context.Background()

	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	survivors := []string{f.ids[0], f.ids[1]}
	for _, id := range survivors {
		if st := f.caches[id].Ready(); !st.Ready {
			t.Fatalf("precondition: %s reports %s, want ready before the change", id, st)
		}
	}

	// Now move the membership. Every survivor's view changes, so none of
	// them has settled.
	f.hardKill(t, f.ids[2])
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)

	unsettled := 0
	for _, id := range survivors {
		st := f.caches[id].Ready()
		if st.Settled {
			continue
		}
		unsettled++
		if st.Ready {
			t.Fatalf("%s reports ready while its view is still moving: %s", id, st)
		}
	}
	if unsettled == 0 {
		t.Fatal("no node reported an unsettled view after a member was killed; the " +
			"settling condition is not observable and therefore not tested")
	}

	// And they settle once the delay elapses.
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	for _, id := range survivors {
		if st := f.caches[id].Ready(); !st.Ready {
			t.Fatalf("%s reports %s after the activation delay elapsed", id, st)
		}
	}
}

// TestDrainingIsPermanentlyNotReady is the shutdown half. A pod on its way out
// should leave the service before it stops being able to serve, not after.
func TestDrainingIsPermanentlyNotReady(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	victim := f.ids[0]
	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	if st := f.caches[victim].Ready(); !st.Ready {
		t.Fatalf("precondition: %s", st)
	}

	if err := f.caches[victim].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	st := f.caches[victim].Ready()
	if st.Ready {
		t.Fatal("a draining node reports ready; Kubernetes would keep sending it traffic " +
			"while it hands its keys away")
	}
	if !st.Draining {
		t.Fatalf("Ready() = %s, want Draining", st)
	}

	// And it does not come back. A node that flickered ready again mid-drain
	// would be added back to the service it is trying to leave.
	f.clk.Advance(time.Hour)
	if st := f.caches[victim].Ready(); st.Ready {
		t.Fatal("a draining node became ready again")
	}
}

// TestWaitReadyReturnsWhenTheContextEnds covers the caller's escape hatch. A
// startup path that blocks forever on readiness is a pod that never reports
// anything at all, which is worse than one that reports a failure.
func TestWaitReadyReturnsWhenTheContextEnds(t *testing.T) {
	f := newFabric(t, 3)

	// Never caught up, so never ready.
	victim := f.ids[0]

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.caches[victim].WaitReady(ctx) }()

	cancel()

	// Bounded by yields rather than by a wall-clock timer. The library
	// forbids time.After outside internal/clock precisely so that a test
	// cannot wait on real time, and the discipline test caught this one —
	// which is the rule working.
	const maxYields = 5_000_000
	for i := 0; ; i++ {
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("WaitReady = %v, want a context error", err)
			}
			return
		default:
		}
		if i > maxYields {
			t.Fatalf("WaitReady had not returned after %d yields following cancellation; "+
				"a startup path that blocks forever is a pod that never reports at all",
				maxYields)
		}
		// The poll is on the injected clock, so nudge it along.
		f.clk.Advance(readyPollStep)
		runtime.Gosched()
	}
}

// TestReadyStateExplainsItself checks the diagnostic, which is the whole reason
// this returns a struct rather than a bool. An operator reading a probe failure
// needs to know which condition is unmet, because the remedies differ.
func TestReadyStateExplainsItself(t *testing.T) {
	f := newFabric(t, 3)

	st := f.caches[f.ids[0]].Ready()
	if st.Ready {
		t.Skip("node is already ready; nothing to explain")
	}
	if st.Reason == "" {
		t.Fatal("an unready node gave no reason; a probe failure would say only 'not ready'")
	}
	if s := st.String(); s == "" || s == "not ready: " {
		t.Fatalf("String() = %q, want a usable diagnostic", s)
	}
}
