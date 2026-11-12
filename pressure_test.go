package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/testkit"
)

// pressureSlack is how far past its budget a cache may be observed between two
// checks: the inline check runs every peercache.PressureCheckInterval writes, so
// that many entries of the largest size a test writes can land before it runs.
func pressureSlack(valueSize int) int64 {
	return int64(peercache.PressureCheckInterval) * int64(valueSize+256)
}

// TestMemoryBudgetMustNotBeNegative pins the option's validation.
func TestMemoryBudgetMustNotBeNegative(t *testing.T) {
	_, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithMemoryBudget(-1))
	if !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New with a negative budget returned %v, want ErrInvalidOption", err)
	}
}

// TestASingleNodeStaysWithinItsMemoryBudget writes many times the budget into a
// single node and checks the accounted size never gets away from it.
//
// A single node has no L1 and no replicas, so the owned store is the only tier
// there is to shed — the last resort, reached first here because nothing else
// exists. The point is the bound, not the order.
func TestASingleNodeStaysWithinItsMemoryBudget(t *testing.T) {
	const (
		budget = 256 << 10
		size   = 1 << 10
	)
	c, _ := newCache(t, peercache.WithMemoryBudget(budget))
	ns := c.Namespace("bulk")
	ctx := context.Background()

	slack := pressureSlack(size)
	peak := int64(0)
	for i := 0; i < 4000; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("k%05d", i), make([]byte, size), 0); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		used := c.Stats().MemoryUsed
		if used > peak {
			peak = used
		}
		if used > budget+slack {
			t.Fatalf("after %d writes of %d bytes the cache accounts %d bytes against a "+
				"budget of %d (allowed overshoot %d)", i+1, size, used, budget, slack)
		}
	}

	st := c.Stats()
	t.Logf("budget %d, peak %d, final %d, shed owned %d", budget, peak, st.MemoryUsed, st.ShedOwned)
	if st.MemoryBudget != budget {
		t.Fatalf("Stats().MemoryBudget = %d, want %d", st.MemoryBudget, budget)
	}
	if st.ShedOwned == 0 {
		t.Fatal("4 MB written into a 256 KB budget and nothing was shed")
	}
	if st.ShedL1 != 0 || st.ShedReplicas != 0 {
		t.Fatalf("a single node shed L1 %d and replicas %d; it has neither", st.ShedL1, st.ShedReplicas)
	}
	// The series that moved here; the other two tiers are checked where
	// they move, in TestSheddingTakesL1AndReplicasBeforeOwnedData.
	for _, name := range []string{"peercache_memory_used_bytes", "peercache_memory_budget_bytes",
		"peercache_shed_owned_total"} {
		assertMetric(t, c, name)
	}
}

// TestNoBudgetMeansNoShedding keeps the default what it was: unbounded, with the
// accounting still reported so an operator can size a budget from it.
func TestNoBudgetMeansNoShedding(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("bulk")
	for i := 0; i < 500; i++ {
		if err := ns.Set(context.Background(), fmt.Sprintf("k%d", i), make([]byte, 1024), 0); err != nil {
			t.Fatal(err)
		}
	}
	st := c.Stats()
	if st.ShedOwned != 0 || st.MemoryBudget != 0 {
		t.Fatalf("no budget configured, yet MemoryBudget %d and ShedOwned %d", st.MemoryBudget, st.ShedOwned)
	}
	if st.MemoryUsed < 500*1024 {
		t.Fatalf("MemoryUsed = %d after 500 KiB of values; the accounting is not being reported", st.MemoryUsed)
	}
}

// TestSheddingTakesL1AndReplicasBeforeOwnedData checks the order of the tiers.
//
// Three nodes at RF=2 each own about a third of the data and replicate another
// third. The budget is set at half the data per node: owned data alone fits,
// owned plus replicas does not. Replicas must go and owned data must not. Then
// one node reads everything, filling its L1, and the next check must shed L1
// before it touches anything else.
func TestSheddingTakesL1AndReplicasBeforeOwnedData(t *testing.T) {
	const (
		keys = 600
		size = 512
	)
	perEntry := int64(size + 160) // value, key, and the larger of the tier overheads
	data := keys * perEntry
	budget := data / 2

	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithMemoryBudget(budget)))
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("tiers",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication(),
			peercache.WithL1TTL(time.Minute))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	for i := 0; i < keys; i++ {
		via := f.ids[i%len(f.ids)]
		if err := openNS(via).Set(ctx, fmt.Sprintf("tiers/%04d", i), make([]byte, size), 0); err != nil {
			t.Fatalf("Set %d via %s: %v", i, via, err)
		}
	}
	// One tick runs the periodic check everywhere.
	f.clk.Advance(time.Second)
	f.settle(t)

	slack := pressureSlack(size)
	shedReplicas := int64(0)
	for _, id := range f.ids {
		st := f.caches[id].Stats()
		t.Logf("%s: used %d of %d, owned %d keys, replicas %d keys, shed replicas %d owned %d",
			id, st.MemoryUsed, budget, st.OwnedKeys, st.ReplicaKeys, st.ShedReplicas, st.ShedOwned)
		if st.ShedOwned != 0 {
			t.Fatalf("%s shed %d owned keys while it still held replicas it could have shed "+
				"instead; owned data is the last resort", id, st.ShedOwned)
		}
		if st.MemoryUsed > budget+slack {
			t.Fatalf("%s accounts %d bytes against a budget of %d", id, st.MemoryUsed, budget)
		}
		shedReplicas += st.ShedReplicas
	}
	if shedReplicas == 0 {
		t.Fatal("owned plus replicas exceeded every node's budget and no replica was shed; " +
			"the test did not construct the pressure it is about")
	}

	// Shedding stopped at the low-water mark, so every node now sits just
	// above it. Delete a fifth of the keys to bring them well below, so that
	// the L1 about to be filled is, by itself, more than the excess — which is
	// the only condition under which "L1 first" can be told apart from
	// "everything at once".
	const deleted = keys / 5
	for i := 0; i < deleted; i++ {
		if err := openNS(f.ids[i%len(f.ids)]).Delete(ctx, fmt.Sprintf("tiers/%04d", i)); err != nil {
			t.Fatalf("Delete %d: %v", i, err)
		}
	}
	f.settle(t)

	// Fill n0's L1 by reading everything left through it.
	reader := f.ids[0]
	for i := deleted; i < keys; i++ {
		if _, err := openNS(reader).Get(ctx, fmt.Sprintf("tiers/%04d", i)); err != nil &&
			!errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("Get %d via %s: %v", i, reader, err)
		}
	}
	before := f.caches[reader].Stats()
	f.clk.Advance(time.Second)
	f.settle(t)
	after := f.caches[reader].Stats()

	t.Logf("%s L1 tick: shed L1 %d -> %d, replicas %d -> %d, owned %d -> %d", reader,
		before.ShedL1, after.ShedL1, before.ShedReplicas, after.ShedReplicas,
		before.ShedOwned, after.ShedOwned)
	if after.ShedL1 == before.ShedL1 {
		t.Fatalf("%s read every key into its L1, went over budget, and shed no L1 entries", reader)
	}
	assertMetric(t, f.caches[reader], "peercache_shed_l1_entries_total")
	assertMetric(t, f.caches[reader], "peercache_shed_replicas_total")
	if after.ShedReplicas != before.ShedReplicas || after.ShedOwned != before.ShedOwned {
		t.Fatalf("%s shed replicas (%d -> %d) or owned data (%d -> %d) while L1 alone could "+
			"cover the excess", reader, before.ShedReplicas, after.ShedReplicas,
			before.ShedOwned, after.ShedOwned)
	}
}

// TestAShedKeyIsNeverServedAtAnOlderValue is M9's correctness criterion.
//
// An owner that sheds a key and is then asked to write it again starts that
// key's version sequence over (open question 18). If the shed had dropped only
// the owner's copy, the replica would still hold the old value at a version the
// rewrite cannot beat — the rewrite is refused there as a retransmission — and a
// failover would promote the old value over the one the caller last wrote.
//
// So: two nodes, a group owned by n0, a budget on n0 only so that the replica
// on n1 is never shed for pressure of its own. Write, flood until n0 sheds,
// rewrite what it shed, kill n0, and read the rewritten keys from n1.
func TestAShedKeyIsNeverServedAtAnOlderValue(t *testing.T) {
	const (
		budget = 192 << 10
		filler = 2 << 10
	)
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n0", peercache.WithMemoryBudget(budget)))
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("resurrect",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication(),
			peercache.WithPartitioner(peercache.PartitionByPrefix(":")))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	// A group n0 owns, so every key in it is owned on n0 and replicated on n1.
	prefix := ""
	for i := 0; i < 64 && prefix == ""; i++ {
		// The partitioner maps "g3:anything" to group "g3".
		if p := fmt.Sprintf("g%d", i); f.ownerOf(t, p) == "n0" {
			prefix = p
		}
	}
	if prefix == "" {
		t.Fatal("no group among 64 is owned by n0")
	}
	key := func(i int) string { return fmt.Sprintf("%s:k%03d", prefix, i) }

	const watched = 100
	for i := 0; i < watched; i++ {
		if err := openNS("n0").Set(ctx, key(i), []byte(fmt.Sprintf("old-%d", i)), 0); err != nil {
			t.Fatalf("Set %s: %v", key(i), err)
		}
	}
	for j := 0; j < 2000 && f.caches["n0"].Stats().ShedOwned < watched; j++ {
		if err := openNS("n0").Set(ctx, fmt.Sprintf("%s:fill%04d", prefix, j), make([]byte, filler), 0); err != nil {
			t.Fatalf("filler %d: %v", j, err)
		}
	}

	var shed []int
	for i := 0; i < watched; i++ {
		if _, err := openNS("n0").Get(ctx, key(i)); errors.Is(err, peercache.ErrNotFound) {
			shed = append(shed, i)
		}
	}
	if len(shed) == 0 {
		t.Fatalf("n0 shed %d owned keys and none of the %d watched ones; the test cannot "+
			"say anything about them", f.caches["n0"].Stats().ShedOwned, watched)
	}

	// Let the replicated removals land: n1 replicates exactly what n0 owns.
	waitForCache(t, "n1's replicas to match n0's owned keys", func() bool {
		return f.caches["n1"].Stats().ReplicaKeys == f.caches["n0"].Stats().OwnedKeys
	})

	for _, i := range shed {
		if err := openNS("n0").Set(ctx, key(i), []byte(fmt.Sprintf("new-%d", i)), 0); err != nil {
			t.Fatalf("rewrite %s: %v", key(i), err)
		}
	}

	f.hardKill(t, "n0")
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 1)
	f.settle(t)

	served := 0
	for _, i := range shed {
		got, err := openNS("n1").Get(ctx, key(i))
		if errors.Is(err, peercache.ErrNotFound) {
			// Shed again after the rewrite, with its removal replicated.
			// Lossy, and allowed.
			continue
		}
		if err != nil {
			t.Fatalf("Get %s from n1: %v", key(i), err)
		}
		if strings.HasPrefix(string(got), "old-") {
			t.Fatalf("%s was shed by its owner, rewritten as new-%d, and after the owner "+
				"died the replica served %q. The shed dropped the owner's copy and left the "+
				"replica's, which the rewrite could not displace", key(i), i, got)
		}
		served++
	}
	t.Logf("%d watched keys shed and rewritten; %d served at the new value after failover",
		len(shed), served)
	if served == 0 {
		t.Fatal("no rewritten key survived to be read from n1, so the property was not exercised")
	}
}

// TestCatchUpStopsAtTheMemoryBudget checks that a returning node too small for
// its share stops pulling rather than pulling and shedding in a loop, and says
// so: CatchUp reports ErrOverMemoryBudget and ReplicasCurrent stays false.
func TestCatchUpStopsAtTheMemoryBudget(t *testing.T) {
	const (
		keys = 600
		size = 512
	)
	budget := int64(keys) * (size + 160) / 4 // a quarter of the data
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n2", peercache.WithMemoryBudget(budget)))
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("pull",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	for i := 0; i < keys; i++ {
		if err := openNS(f.ids[i%2]).Set(ctx, fmt.Sprintf("pull/%04d", i), make([]byte, size), 0); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}

	f.hardKill(t, "n2")
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)
	f.revive(t, "n2")
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.convergeTo(t, 3)
	openNS("n2")
	f.settle(t)

	err := f.caches["n2"].CatchUp(ctx)
	st := f.caches["n2"].Stats()
	t.Logf("n2 catch-up: err=%v, pulled %d, used %d of %d", err, st.ReplicasPulled, st.MemoryUsed, budget)

	if !errors.Is(err, peercache.ErrOverMemoryBudget) {
		t.Fatalf("CatchUp on a node with a quarter of the data's budget returned %v; want "+
			"ErrOverMemoryBudget", err)
	}
	if st.ReplicasPulled == 0 {
		t.Fatal("CatchUp stopped before pulling anything; the budget was not what stopped it")
	}
	if ready := f.caches["n2"].Ready(); ready.ReplicasCurrent {
		t.Fatalf("ReplicasCurrent is true after a catch-up stopped by the budget: %s", ready)
	}
	// It stopped near the budget, not after pulling everything and shedding it.
	if st.ShedReplicas > int64(catchUpPageAllowance) {
		t.Fatalf("n2 shed %d replicas during catch-up; it should have stopped pulling instead "+
			"of pulling and shedding", st.ShedReplicas)
	}
}

// catchUpPageAllowance is how many replicas a stopped catch-up may have shed:
// at most one page lands between the check before it and the check after.
const catchUpPageAllowance = 512

// TestAReplicaOnlyNodeStaysWithinItsBudget covers the inflow a failover
// actually produces: copies arriving from peers, with no local writes to
// trigger a check and no clock tick in between. n1 owns nothing in the group
// being written and holds only its replicas.
func TestAReplicaOnlyNodeStaysWithinItsBudget(t *testing.T) {
	const (
		budget = 128 << 10
		size   = 1 << 10
	)
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n1", peercache.WithMemoryBudget(budget)))
	ctx := context.Background()

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("inflow",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication(),
			peercache.WithPartitioner(peercache.PartitionByPrefix(":")))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	prefix := ""
	for i := 0; i < 64 && prefix == ""; i++ {
		if p := fmt.Sprintf("g%d", i); f.ownerOf(t, p) == "n0" {
			prefix = p
		}
	}
	if prefix == "" {
		t.Fatal("no group among 64 is owned by n0")
	}

	slack := pressureSlack(size)
	for i := 0; i < 1000; i++ {
		if err := openNS("n0").Set(ctx, fmt.Sprintf("%s:%04d", prefix, i), make([]byte, size), 0); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
		if used := f.caches["n1"].Stats().MemoryUsed; used > budget+slack {
			t.Fatalf("after %d replicated writes n1 accounts %d bytes against a budget of %d; "+
				"copies arriving from a peer are not being checked", i+1, used, budget)
		}
	}
	if f.caches["n1"].Stats().ShedReplicas == 0 {
		t.Fatal("1 MB of replicas into a 128 KB budget and none were shed")
	}
}
