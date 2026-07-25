package ownership_test

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
)

func newGuard(t *testing.T, self core.Node, delay time.Duration) (*ownership.Guard, *ownership.Table, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	tbl := ownership.NewTable()
	g, err := ownership.NewGuard(ownership.GuardOptions{
		Self:            self,
		Table:           tbl,
		Clock:           fc,
		ActivationDelay: delay,
	})
	if err != nil {
		t.Fatalf("NewGuard: %v", err)
	}
	t.Cleanup(g.Close)
	return g, tbl, fc
}

// ownedBy returns a key this table assigns to the named node.
func ownedBy(t *testing.T, tbl *ownership.Table, id string) string {
	t.Helper()
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("probe:%d", i)
		node, _, err := tbl.Owner(key)
		if err != nil {
			t.Fatalf("Owner: %v", err)
		}
		if node.ID == id {
			return key
		}
	}
	t.Fatalf("no key among 100k resolves to %s", id)
	return ""
}

func TestGuardRefusesGroupsItDoesNotOwn(t *testing.T) {
	members := nodes(5)
	g, tbl, fc := newGuard(t, members[0], time.Second)
	g.Update(members, 1)
	// Learning the membership for the first time is itself a change: until
	// the window closes, every group looks newly acquired, because as far as
	// this node knows it might be.
	fc.Advance(time.Second)

	mine := ownedBy(t, tbl, "n0")
	if err := g.CanActivate(mine); err != nil {
		t.Fatalf("CanActivate on an owned group = %v, want nil", err)
	}

	theirs := ownedBy(t, tbl, "n3")
	err := g.CanActivate(theirs)
	if !errors.Is(err, core.ErrNotOwner) {
		t.Fatalf("CanActivate on another node's group = %v, want ErrNotOwner", err)
	}
	// The message must name the actual owner: a caller that has to guess
	// where to retry will guess wrong.
	if got := err.Error(); got == "" || !contains(got, "n3") {
		t.Fatalf("error %q does not name the owner", got)
	}
	if g.Stats().Refusals == 0 {
		t.Fatal("Stats().Refusals did not count the refusal")
	}
}

// TestGuardWithNoMembershipOwnsNothing is the safe default. A pod that has just
// started and heard from nobody must not conclude it owns the keyspace.
func TestGuardWithNoMembershipOwnsNothing(t *testing.T) {
	g, _, _ := newGuard(t, core.Node{ID: "n0"}, time.Second)
	if err := g.CanActivate("anything"); !errors.Is(err, core.ErrNotOwner) {
		t.Fatalf("CanActivate with no membership = %v, want ErrNotOwner", err)
	}
}

// TestGuardDefersOnlyNewlyAcquiredGroups is the core of the activation guard.
//
// After a membership change, a group this node has just acquired is deferred —
// the previous owner may not have noticed yet. A group it has held all along is
// not: that group cannot be active anywhere else, and deferring it would turn
// every membership change into a brownout across the whole keyspace rather than
// across the part that actually moved.
func TestGuardDefersOnlyNewlyAcquiredGroups(t *testing.T) {
	const delay = 3 * time.Second

	all := nodes(5)
	g, tbl, fc := newGuard(t, all[0], delay)

	// Start with every node present, and settle past the initial window.
	g.Update(all, 1)
	fc.Advance(delay)

	held := ownedBy(t, tbl, "n0")
	if err := g.CanActivate(held); err != nil {
		t.Fatalf("CanActivate on a settled owned group = %v, want nil", err)
	}

	// A key currently owned by n4, which n0 will inherit when n4 leaves.
	victimKey := ownedBy(t, tbl, "n4")

	// n4 leaves.
	g.Update(all[:4], 2)

	newOwner, _, err := tbl.Owner(victimKey)
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	if newOwner.ID != "n0" {
		// The key went to another survivor; find one that came to us.
		victimKey = ""
		for i := 0; i < 100_000 && victimKey == ""; i++ {
			key := fmt.Sprintf("probe:%d", i)
			after, _, err := tbl.Owner(key)
			if err != nil {
				t.Fatalf("Owner: %v", err)
			}
			if after.ID != "n0" {
				continue
			}
			// Was it n4's before?
			before := ownershipUnder(t, all, key)
			if before == "n4" {
				victimKey = key
			}
		}
		if victimKey == "" {
			t.Skip("no key moved from n4 to n0 in this hash; the property is covered by the other assertions")
		}
	}

	// Newly acquired: deferred.
	if err := g.CanActivate(victimKey); !errors.Is(err, core.ErrActivationPending) {
		t.Fatalf("CanActivate on a newly acquired group = %v, want ErrActivationPending", err)
	}
	// Held all along: permitted, even inside the window.
	if err := g.CanActivate(held); err != nil {
		t.Fatalf("CanActivate on a group held throughout = %v, want nil", err)
	}

	// The delay is not over one nanosecond early.
	fc.Advance(delay - time.Nanosecond)
	if err := g.CanActivate(victimKey); !errors.Is(err, core.ErrActivationPending) {
		t.Fatalf("the deferral lifted early: %v", err)
	}

	fc.Advance(time.Nanosecond)
	if err := g.CanActivate(victimKey); err != nil {
		t.Fatalf("CanActivate after the delay = %v, want nil", err)
	}
	if g.Stats().Deferrals == 0 {
		t.Fatal("Stats().Deferrals did not count the deferral")
	}
}

// ownershipUnder resolves a key against an ad-hoc membership.
func ownershipUnder(t *testing.T, members []core.Node, key string) string {
	t.Helper()
	tbl := ownership.NewTable()
	tbl.Update(members, 1)
	node, _, err := tbl.Owner(key)
	if err != nil {
		t.Fatalf("Owner: %v", err)
	}
	return node.ID
}

func TestGuardNegativeDelayDisablesDeferral(t *testing.T) {
	all := nodes(3)
	g, tbl, _ := newGuard(t, all[0], -1)
	g.Update(all, 1)

	// Even immediately after a change, nothing is deferred.
	g.Update(all, 2)
	mine := ownedBy(t, tbl, "n0")
	if err := g.CanActivate(mine); err != nil {
		t.Fatalf("CanActivate with deferral disabled = %v, want nil", err)
	}
	if g.Stats().Deferrals != 0 {
		t.Fatal("a deferral was counted with the delay disabled")
	}
}

func TestNewGuardValidatesOptions(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	tbl := ownership.NewTable()

	tests := map[string]ownership.GuardOptions{
		"no self":  {Table: tbl, Clock: fc},
		"no table": {Self: core.Node{ID: "n0"}, Clock: fc},
		"no clock": {Self: core.Node{ID: "n0"}, Table: tbl},
	}
	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := ownership.NewGuard(opts); !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("NewGuard = %v, want ErrInvalidOption", err)
			}
		})
	}
}

// TestPartitionedMinorityStopsActivating is the third M3 acceptance criterion:
// a partitioned minority stops activating new groups within activationDelay.
//
// It runs on the testkit harness, so the partition, the failure detection and
// the activation delay are all driven by one fake clock — the whole scenario is
// exact rather than timing-dependent.
func TestPartitionedMinorityStopsActivating(t *testing.T) {
	const delay = 2 * time.Second

	c := testkit.NewCluster(t, testkit.WithNodes(5))
	majority := []string{"n0", "n1", "n2"}
	minority := []string{"n3", "n4"}

	// One guard per node, each watching its own membership view.
	guards := map[string]*ownership.Guard{}
	tables := map[string]*ownership.Table{}
	for _, id := range c.IDs() {
		node := c.Node(id)
		tbl := ownership.NewTable()
		g, err := ownership.NewGuard(ownership.GuardOptions{
			Self:            node.Info,
			Table:           tbl,
			Clock:           c.Clock,
			ActivationDelay: delay,
		})
		if err != nil {
			t.Fatalf("NewGuard: %v", err)
		}
		g.Watch(node.Membership)
		t.Cleanup(g.Close)
		guards[id] = g
		tables[id] = tbl
	}

	// Everyone sees everyone, and the initial window has passed.
	c.Advance(delay)
	for _, id := range c.IDs() {
		if got := guards[id].Stats().Members; got != 5 {
			t.Fatalf("%s sees %d members before the partition, want 5", id, got)
		}
	}

	// Split. Both sides keep their old view until the failure detector fires.
	c.Net.Partition(majority, minority)
	c.Advance(testkit.DefaultFailureTimeout)

	// Wait for every guard to have applied the new view: the watcher runs on
	// its own goroutine, so membership changing and the table being updated
	// are different moments.
	waitForGuards(t, "the minority to shrink its view", func() bool {
		return guards["n3"].Stats().Members == 2 && guards["n0"].Stats().Members == 3
	})

	// Inside the delay, the minority defers every group it just acquired.
	// It cannot know the majority has also reassigned them.
	deferred := 0
	refused := 0
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("probe:%d", i)
		err := guards["n3"].CanActivate(key)
		switch {
		case errors.Is(err, core.ErrActivationPending):
			deferred++
		case errors.Is(err, core.ErrNotOwner):
			refused++
		case err == nil:
			// Held throughout the change: legitimate.
		default:
			t.Fatalf("CanActivate = %v", err)
		}
	}
	if deferred == 0 {
		t.Fatal("the partitioned minority deferred nothing; it is activating groups the majority also owns")
	}
	t.Logf("minority node n3 inside the window: %d deferred, %d refused, of 500 probes", deferred, refused)

	// The window closes and the minority resumes — it is AP, so it keeps
	// serving its own view. The deferral bought time, it did not prevent
	// divergence, and CLAUDE.md decision 5 says so explicitly.
	c.Advance(delay)
	resumed := 0
	for i := 0; i < 500; i++ {
		if err := guards["n3"].CanActivate(fmt.Sprintf("probe:%d", i)); err == nil {
			resumed++
		}
	}
	if resumed == 0 {
		t.Fatal("the minority never resumed activating after the delay")
	}
	t.Logf("after the delay the minority activates %d of 500 probes: the window narrows, it does not close", resumed)

	// Heal, and both sides converge on one view again.
	c.Net.Heal()
	c.Advance(testkit.DefaultJoinDelay)
	waitForGuards(t, "the cluster to reconverge", func() bool {
		for _, id := range c.IDs() {
			if guards[id].Stats().Members != 5 {
				return false
			}
		}
		return true
	})
}

func waitForGuards(t *testing.T, what string, cond func() bool) {
	t.Helper()
	const yields = 5_000_000
	for i := 0; !cond(); i++ {
		if i > yields {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestStaleEpochIsRefused covers the half of SPEC.md §4 (M3) that the
// activation delay does not: "a node refuses to activate a group whose epoch is
// stale".
//
// The caller resolved ownership under a view that has since been replaced. Its
// conclusion may still be right, but it was not reached from the facts this
// node now holds, and a caller that cannot tell the difference will keep
// arriving at the wrong node.
func TestStaleEpochIsRefused(t *testing.T) {
	self := core.Node{ID: "a"}
	g, tbl, _ := newGuard(t, self, -1) // deferral off; the epoch is the subject
	nodes := []core.Node{self, {ID: "b"}, {ID: "c"}}
	g.Update(nodes, 7)

	key := ownedBy(t, tbl, "a")

	if err := g.CanActivateAt(key, 7); err != nil {
		t.Fatalf("CanActivateAt at the current epoch: %v; a caller in step with this "+
			"node must be allowed to activate", err)
	}
	err := g.CanActivateAt(key, 6)
	if !errors.Is(err, core.ErrStaleEpoch) {
		t.Fatalf("CanActivateAt at epoch 6 with the node at 7 = %v; want ErrStaleEpoch", err)
	}
	if got := g.Stats().StaleEpochs; got != 1 {
		t.Fatalf("Stats().StaleEpochs = %d, want 1; the refusal must be countable", got)
	}
}

// TestFutureEpochDefersRatherThanRefusing is the other direction, and it is a
// different problem with the opposite remedy.
//
// A caller ahead of this node is not wrong; this node is behind. Answering
// ErrStaleEpoch would send it away from the node that is about to be able to
// serve it, so it gets ErrActivationPending — retry here, shortly.
func TestFutureEpochDefersRatherThanRefusing(t *testing.T) {
	self := core.Node{ID: "a"}
	g, tbl, _ := newGuard(t, self, -1)
	g.Update([]core.Node{self, {ID: "b"}, {ID: "c"}}, 7)

	key := ownedBy(t, tbl, "a")

	err := g.CanActivateAt(key, 9)
	if !errors.Is(err, core.ErrActivationPending) {
		t.Fatalf("CanActivateAt at epoch 9 with the node at 7 = %v; want ErrActivationPending, "+
			"because this node is behind rather than the caller being stale", err)
	}
	if errors.Is(err, core.ErrStaleEpoch) {
		t.Fatal("a caller ahead of this node was told its epoch was stale; that sends it " +
			"away from the node that is about to be able to serve it")
	}
}

// TestNotOwnerBeatsStaleEpoch pins the order of the two checks.
//
// When both are wrong, "go elsewhere" is the more useful answer: it is the
// action the caller would take after re-resolving anyway, delivered one round
// trip sooner.
func TestNotOwnerBeatsStaleEpoch(t *testing.T) {
	self := core.Node{ID: "a"}
	g, tbl, _ := newGuard(t, self, -1)
	g.Update([]core.Node{self, {ID: "b"}, {ID: "c"}}, 7)

	other := ownedBy(t, tbl, "b")

	err := g.CanActivateAt(other, 6)
	if !errors.Is(err, core.ErrNotOwner) {
		t.Fatalf("CanActivateAt(%q, 6) = %v; want ErrNotOwner even though the epoch is "+
			"also stale, because the ownership answer is the actionable one", other, err)
	}
}

// TestCanActivateUsesTheCurrentEpoch proves the no-epoch form is exactly the
// epoch-carrying form at the current view, so that adding the epoch check did
// not quietly change local activation.
func TestCanActivateUsesTheCurrentEpoch(t *testing.T) {
	self := core.Node{ID: "a"}
	g, tbl, _ := newGuard(t, self, -1)

	for _, epoch := range []core.Epoch{1, 2, 50, 1 << 40} {
		g.Update([]core.Node{self, {ID: "b"}, {ID: "c"}}, epoch)
		key := ownedBy(t, tbl, "a")
		if err := g.CanActivate(key); err != nil {
			t.Fatalf("CanActivate at epoch %d: %v; a local activation resolves under the "+
				"current view and can never be stale", epoch, err)
		}
	}
	if got := g.Stats().StaleEpochs; got != 0 {
		t.Fatalf("Stats().StaleEpochs = %d after only local activations, want 0", got)
	}
}
