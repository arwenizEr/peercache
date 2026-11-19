package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestAForkWrittenOnBothSidesOfAPartitionIsCounted is open question 66. A node
// cut off from the cluster promotes its replica of a key once its activation
// delay passes and takes a write there, while the majority's owner takes
// others. Decision 5 allows the two activations and says their conflicting
// writes are detected and counted, not merged. Before 66 the minority's value
// was demoted at the heal with no writer and no predecessor, the owned store
// keeping neither, and a copy without them is never judged: the conflict was
// resolved by order, silently, and no node counted it.
func TestAForkWrittenOnBothSidesOfAPartitionIsCounted(t *testing.T) {
	f := partitionFabric(t)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("forked",
			peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	// A key owned in the majority with a replica on the node that will be
	// cut off.
	isolated := f.ids[0]
	var key string
	for i := 0; key == ""; i++ {
		k := fmt.Sprintf("k%d", i)
		d, err := f.caches[isolated].DescribeKey("forked", k)
		if err != nil {
			t.Fatal(err)
		}
		if d.Owner != isolated && len(d.Holders) > 2 && (d.Holders[1] == isolated || d.Holders[2] == isolated) {
			key = k
		}
	}
	if _, err := open(f.ids[1]).Increment(ctx, key, -1, 0); err != nil {
		t.Fatal(err)
	}
	majority := append([]string(nil), f.ids[1:]...)
	f.net.Partition([]string{isolated}, majority)
	t.Cleanup(func() { f.net.Heal() })
	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	waitForCache(t, "the isolated node to see itself alone", func() bool {
		return f.caches[isolated].Stats().Members == 1
	})
	waitForCache(t, "the majority to see four members", func() bool {
		return f.caches[majority[0]].Stats().Members == len(majority)
	})
	f.settle(t)
	// Past the activation delay: decision 5's minority resumes.
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	minorityWrote, majorityWrote := 0, 0
	for i := 0; i < 3; i++ {
		if _, err := open(isolated).Increment(ctx, key, -1, 0); err == nil {
			minorityWrote++
		} else {
			t.Logf("minority write: %v", err)
		}
		if _, err := open(majority[0]).Increment(ctx, key, -1, 0); err == nil {
			majorityWrote++
		}
	}
	if minorityWrote == 0 || majorityWrote == 0 {
		t.Fatalf("writes on the minority %d, on the majority %d: both sides must write for a fork", minorityWrote, majorityWrote)
	}

	f.net.Heal()
	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids))
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)
	waitForHandoffsToDrain(t, f)
	for _, id := range f.ids {
		_ = f.caches[id].CatchUp(ctx)
	}
	f.settle(t)

	var forks int64
	for _, id := range f.ids {
		forks += f.caches[id].Stats().SplitBrainDetections
	}
	if forks == 0 {
		t.Fatalf("both sides of a partition wrote %s (%d on the minority, %d on the majority) and no "+
			"node counted a fork; decision 5 says conflicting writes are detected and counted",
			key, minorityWrote, majorityWrote)
	}
}
