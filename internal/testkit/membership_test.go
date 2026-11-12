package testkit_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// wantMembers asserts one node's view, by ID, in sorted order.
func wantMembers(t *testing.T, c *testkit.Cluster, id string, want ...string) {
	t.Helper()
	got := c.Node(id).Membership.MemberIDs()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("%s sees %v, want %v", id, got, want)
	}
}

func TestMembershipConvergesOnStart(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(5))
	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2", "n3", "n4")
	}
}

// TestMembershipAdmissionWaitsForJoinDelay shows that convergence is a timed
// event rather than something NewCluster fakes.
func TestMembershipAdmissionWaitsForJoinDelay(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3), testkit.WithoutConverge())
	for _, id := range c.IDs() {
		wantMembers(t, c, id, id)
	}

	c.Advance(testkit.DefaultJoinDelay - time.Nanosecond)
	wantMembers(t, c, "n0", "n0")

	c.Advance(time.Nanosecond)
	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2")
	}
}

// TestMembershipPartitionDetectedAfterFailureTimeout is the core failure case:
// each side of a split converges on its own half, and not one instant before
// the failure timeout.
func TestMembershipPartitionDetectedAfterFailureTimeout(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(5))
	majority := []string{"n0", "n1", "n2"}
	minority := []string{"n3", "n4"}

	c.Net.Partition(majority, minority)

	// Nothing has been concluded yet: a partition is indistinguishable from
	// a slow network until the detector says otherwise.
	wantMembers(t, c, "n0", "n0", "n1", "n2", "n3", "n4")

	c.Advance(testkit.DefaultFailureTimeout - time.Nanosecond)
	wantMembers(t, c, "n0", "n0", "n1", "n2", "n3", "n4")

	c.Advance(time.Nanosecond)
	for _, id := range majority {
		wantMembers(t, c, id, majority...)
	}
	for _, id := range minority {
		wantMembers(t, c, id, minority...)
	}
}

func TestMembershipHealRestoresTheView(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(5))
	before := c.Node("n0").Membership.Epoch()

	c.Net.Partition([]string{"n0", "n1", "n2"}, []string{"n3", "n4"})
	c.Advance(testkit.DefaultFailureTimeout)
	wantMembers(t, c, "n0", "n0", "n1", "n2")

	split := c.Node("n0").Membership.Epoch()
	if split <= before {
		t.Fatalf("epoch did not advance across a partition: %d then %d", before, split)
	}

	c.Net.Heal()
	c.Advance(testkit.DefaultJoinDelay)
	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2", "n3", "n4")
	}
	if healed := c.Node("n0").Membership.Epoch(); healed <= split {
		t.Fatalf("epoch did not advance across a heal: %d then %d", split, healed)
	}
}

// TestMembershipGracefulLeaveIsImmediate is the difference a PreStop hook buys:
// no failure timeout, because the departing node said so.
func TestMembershipGracefulLeaveIsImmediate(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	if err := c.GracefulKill(context.Background(), "n2"); err != nil {
		t.Fatalf("GracefulKill: %v", err)
	}

	for _, id := range []string{"n0", "n1"} {
		wantMembers(t, c, id, "n0", "n1")
	}
	if live := c.Live(); len(live) != 2 {
		t.Fatalf("Live() = %v, want two nodes", live)
	}
}

// TestMembershipHardKillWaitsForTheTimeout is the same event without the
// announcement, and it must cost the full detection delay.
func TestMembershipHardKillWaitsForTheTimeout(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	c.HardKill("n2")

	wantMembers(t, c, "n0", "n0", "n1", "n2")

	c.Advance(testkit.DefaultFailureTimeout)
	for _, id := range []string{"n0", "n1"} {
		wantMembers(t, c, id, "n0", "n1")
	}
}

// TestMembershipGracefulLeaveIsNotHeardAcrossAPartition is the case that makes
// graceful shutdown insufficient on its own: a minority that cannot hear the
// announcement still has to time the node out.
func TestMembershipGracefulLeaveIsNotHeardAcrossAPartition(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	c.Net.Partition([]string{"n0", "n1"}, []string{"n2"})
	c.Advance(testkit.DefaultFailureTimeout)

	// n2 already believes it is alone, and n0/n1 have already timed it out.
	wantMembers(t, c, "n2", "n2")

	if err := c.GracefulKill(context.Background(), "n0"); err != nil {
		t.Fatalf("GracefulKill: %v", err)
	}
	// n1 heard it; n2 could not have.
	wantMembers(t, c, "n1", "n1")
	wantMembers(t, c, "n2", "n2")
}

// TestMembershipRevivedNodeRejoinsWithAnEmptyView models a pod restarting: same
// identity, no memory. A node that came back believing its old view would be a
// split brain with extra steps.
func TestMembershipRevivedNodeRejoinsWithAnEmptyView(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	c.HardKill("n2")
	c.Advance(testkit.DefaultFailureTimeout)
	wantMembers(t, c, "n0", "n0", "n1")

	revived := c.Revive("n2")
	if got := revived.Membership.MemberIDs(); len(got) != 1 || got[0] != "n2" {
		t.Fatalf("revived node starts with view %v, want only itself", got)
	}

	c.Advance(testkit.DefaultJoinDelay)
	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2")
	}
}

func TestMembershipRevivedNodeRejoinsAfterGracefulLeave(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	if err := c.GracefulKill(context.Background(), "n2"); err != nil {
		t.Fatalf("GracefulKill: %v", err)
	}
	wantMembers(t, c, "n0", "n0", "n1")

	c.Revive("n2")
	c.Advance(testkit.DefaultJoinDelay)
	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2")
	}
}

// TestMembershipFlapIsNotApplied covers the bug this design most invites: a
// transition armed by a condition that stopped being true before the timer
// fired. Applying it anyway turns one flapping link into a permanently wrong
// view.
func TestMembershipFlapIsNotApplied(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	before := c.Node("n0").Membership.Epoch()

	c.Net.Isolate("n2")
	c.Advance(testkit.DefaultFailureTimeout / 2)
	c.Net.Rejoin("n2")
	c.Advance(testkit.DefaultFailureTimeout)

	wantMembers(t, c, "n0", "n0", "n1", "n2")
	if got := c.Node("n0").Membership.Epoch(); got != before {
		t.Fatalf("epoch moved from %d to %d for a flap that changed nothing", before, got)
	}
}

func TestMembershipWatchDeliversChanges(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	changes := c.Node("n0").Membership.Watch()

	c.HardKill("n2")
	c.Advance(testkit.DefaultFailureTimeout)

	select {
	case change := <-changes:
		if change.Kind != core.ChangeFail {
			t.Fatalf("Kind = %v, want fail", change.Kind)
		}
		if change.Node.ID != "n2" {
			t.Fatalf("Node = %v, want n2", change.Node)
		}
		if change.Epoch != c.Node("n0").Membership.Epoch() {
			t.Fatalf("change epoch %d does not match the view epoch %d",
				change.Epoch, c.Node("n0").Membership.Epoch())
		}
	default:
		t.Fatal("no change delivered")
	}
}

func TestMembershipWatchIsClosedOnClose(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	changes := c.Node("n1").Membership.Watch()

	c.HardKill("n1")
	if _, open := <-changes; open {
		t.Fatal("watch channel delivered after Close instead of closing")
	}
}

// TestMembershipSelfIsAlwaysInTheView is a small invariant with large
// consequences: a node that can time itself out would refuse to own anything.
func TestMembershipSelfIsAlwaysInTheView(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	c.Net.Partition([]string{"n0"}, []string{"n1"}, []string{"n2"})
	c.Advance(24 * time.Hour)
	for _, id := range c.IDs() {
		wantMembers(t, c, id, id)
	}
}

func TestMembershipLeaveAfterCloseReportsClosed(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	m := c.Node("n1").Membership
	c.HardKill("n1")
	if err := m.Leave(context.Background()); err == nil {
		t.Fatal("Leave on a closed membership returned nil")
	}
}
