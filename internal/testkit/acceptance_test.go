package testkit_test

import (
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// acceptanceBudget is the harness wall-clock ceiling from docs/design.md section 4.
const acceptanceBudget = 50 * time.Millisecond

// TestM0Acceptance is the harness acceptance criterion, verbatim: create a
// five-node in-process cluster, partition it three/two, advance a fake clock by
// an hour, heal the partition, and assert on membership — in under 50ms of wall
// clock and with zero sleeps.
//
// The wall-clock assertion is the interesting half. Simulated time being free
// is not a convenience, it is the property that decides whether the rest of the
// suite can afford to test its timeouts at all. An implementation that
// polled would pass every other assertion here and quietly cost seconds.
//
// Wall time is read through clock.Real so that every time access in the
// repository still goes through internal/clock, including this one.
func TestM0Acceptance(t *testing.T) {
	realClock := clock.Real()
	started := realClock.Now()

	c := testkit.NewCluster(t, testkit.WithNodes(5))

	majority := []string{"n0", "n1", "n2"}
	minority := []string{"n3", "n4"}
	c.Net.Partition(majority, minority)

	c.Advance(time.Hour)

	for _, id := range majority {
		wantMembers(t, c, id, majority...)
	}
	for _, id := range minority {
		wantMembers(t, c, id, minority...)
	}

	c.Net.Heal()
	c.Advance(time.Hour)

	for _, id := range c.IDs() {
		wantMembers(t, c, id, "n0", "n1", "n2", "n3", "n4")
	}

	elapsed := realClock.Since(started)
	t.Logf("simulated 2h across 5 nodes in %v of wall clock", elapsed)
	if elapsed > acceptanceBudget {
		t.Fatalf("took %v of wall clock, budget is %v", elapsed, acceptanceBudget)
	}
}

// TestM0AcceptanceEpochsAdvanceOnBothSides checks the half of the criterion
// that "assert on membership" leaves implicit: both sides of a split must
// notice, and each must stamp its conclusion with a new epoch. A minority that
// silently kept the old epoch would look, to every other test, exactly
// like a minority that never partitioned.
func TestM0AcceptanceEpochsAdvanceOnBothSides(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(5))

	before := make(map[string]uint64)
	for _, id := range c.IDs() {
		before[id] = uint64(c.Node(id).Membership.Epoch())
	}

	c.Net.Partition([]string{"n0", "n1", "n2"}, []string{"n3", "n4"})
	c.Advance(time.Hour)

	for _, id := range c.IDs() {
		if got := uint64(c.Node(id).Membership.Epoch()); got <= before[id] {
			t.Fatalf("%s epoch stayed at %d across a partition", id, got)
		}
	}
}
