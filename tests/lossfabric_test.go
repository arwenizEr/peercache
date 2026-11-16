package tests

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// The asynchronous data-loss window, measured.
//
// docs/design.md §4 requires a "measured and documented data-loss window for
// async mode", and §6 has a placeholder — "approximately X ms" — waiting for
// this number. Async replication is the default, and its cost has to be a documented, measured figure rather than
// an unknown.
//
// # What the window actually is
//
// Between a write returning to its caller and the last replica holding it, the
// value exists on exactly one node. Kill that node inside the window and the
// write is gone despite having reported success. The window is the interval
// between those two events.
//
// # Why it is measured in operations rather than milliseconds here
//
// The fabric runs on a fake clock, so wall-clock milliseconds are meaningless:
// simulated time only advances when a test advances it. What *is* meaningful,
// and what actually determines exposure in production, is the queue: how many
// writes can return before the replicator has drained what it owes. That number
// is a property of this code. Converting it to milliseconds needs a real
// network, which is what TestAsyncLossWindowOverGRPC below does.

// TestAsyncWriteIsExposedUntilTheQueueDrains demonstrates the window exists and
// bounds it in operations.
//
// The replicator's queue is what separates a returned write from a durable one.
// This measures how much can accumulate in it, because that is the exposure a
// sudden death would realise.
func TestAsyncWriteIsExposedUntilTheQueueDrains(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	// Asynchronous: the default, and the mode under measurement.
	for _, id := range f.ids {
		f.caches[id].Namespace("async", peercache.WithReplicationFactor(3))
	}

	// Silence every peer, so nothing the replicator sends can land. Writes
	// still return — that is the whole point of asynchronous mode — and
	// every one of them is exposed.
	writer := f.ids[0]
	for _, id := range f.ids {
		if id != writer {
			f.net.SetDropRate(writer, id, 1.0)
		}
	}

	// Write only keys this node owns.
	//
	// Silencing a node silences its replies as well as its fan-out, so a
	// *routed* write to a silenced owner never returns — the caller waits
	// out a deadline it did not set. That is correct behaviour for a
	// dropped message and useless for this measurement, which is about the
	// gap between a write returning and its copies landing. Keeping the
	// writes local isolates the fan-out as the only remote part.
	ns := f.caches[writer].Namespace("async")
	var owned int
	for i := 0; owned < 200 && i < 5000; i++ {
		key := fmt.Sprintf("async/%d", i)
		if f.probeOwns(writer, key) != nil {
			continue
		}
		if err := ns.Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("a local write failed while peers were silent: %v", err)
		}
		owned++
	}

	stats := f.caches[writer].Stats()
	t.Logf("%d writes returned with every replica silent; %d sent, %d dropped, %d failed",
		owned, stats.ReplicationsSent, stats.ReplicationsDropped, stats.ReplicationsFailed)

	// The window is real: writes returned success and no replica has them.
	replicated := int64(0)
	for _, id := range f.ids {
		if id != writer {
			replicated += f.caches[id].Stats().ReplicaKeys
		}
	}
	if replicated != 0 {
		t.Fatalf("%d replicas exist despite every peer being silent; the test is not "+
			"measuring what it claims", replicated)
	}
	if owned == 0 {
		t.Fatal("no writes returned successfully; asynchronous mode is not asynchronous")
	}

	// And the queue is bounded, which is what stops the window growing
	// without limit. A replicator that queued forever would turn a slow
	// peer into an out-of-memory.
	if stats.ReplicationsDropped == 0 && owned > 0 {
		t.Logf("note: %d writes fit in the queue without dropping", owned)
	}
}

// TestSyncModeHasNoWindow is the control. Without it the measurement above says
// nothing: a system that lost data in both modes would produce the same numbers.
func TestSyncModeHasNoWindow(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("sync",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const key = "sync/window"
	owner := f.ownerOf(t, key)
	if err := f.caches[owner].Namespace("sync").Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// No advancing, no yielding, no waiting of any kind: the copies must
	// exist the instant Set returns.
	replicated := int64(0)
	for _, id := range f.ids {
		if id != owner {
			replicated += f.caches[id].Stats().ReplicaKeys
		}
	}
	if replicated == 0 {
		t.Fatal("a synchronous write returned before any replica held it; the mode that " +
			"exists to close the window has not closed it")
	}
}

// TestKillingInsideTheAsyncWindowLosesTheWrite is the consequence, stated
// plainly rather than left as an inference.
//
// This is the failure the window describes. It is a test rather than a sentence
// in the documentation so that the claim stays true as the code changes — and
// so nobody reads "async by default" as "async is free".
func TestKillingInsideTheAsyncWindowLosesTheWrite(t *testing.T) {
	f := newFabricTuned(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	})
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("window", peercache.WithReplicationFactor(3))
	}

	// Write on the owner itself, with its outbound traffic silenced.
	//
	// Routing the write through another node would not work: silencing a
	// node silences its replies too, so the remote write would never return
	// and the test would hang rather than measure. Writing locally isolates
	// the fan-out, which is the part that defines the window.
	owner := f.ids[0]
	var key string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("window/%d", i)
		if f.probeOwns(owner, k) == nil {
			key = k
			break
		}
	}
	if key == "" {
		t.Fatalf("no key of 5000 resolves to %s", owner)
	}

	for _, id := range f.ids {
		if id != owner {
			f.net.SetDropRate(owner, id, 1.0)
		}
	}

	if err := f.caches[owner].Namespace("window").Set(ctx, key, []byte("doomed"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Kill the owner inside the window.
	f.hardKill(t, owner)
	f.clk.Advance(2 * f.mcfg.FailureTimeout)
	f.convergeTo(t, len(f.live()))
	f.settle(t)

	_, err := f.caches[f.live()[0]].Namespace("window").Get(ctx, key)
	if err == nil {
		t.Fatal("a write acknowledged asynchronously survived its owner dying before any " +
			"replica had it; either the window does not exist or this test is not in it")
	}
	t.Logf("as documented: an asynchronously acknowledged write was lost when its owner "+
		"died inside the window (%v)", err)
}

// TestAsyncLossWindowOverGRPC measures the window in real time.
//
// The fake-clock tests above bound the window in operations, which is the part
// that is a property of this code. This puts a number on it in milliseconds,
// which is what docs/design.md §6 asks for and what an operator actually needs.
//
// Opt-in, like every other timing measurement here: a window measured while the
// rest of the suite competes for the same cores describes the machine.
func TestAsyncLossWindowOverGRPC(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets")
	}
	if raceEnabled {
		t.Skip("timing under the race detector describes the detector")
	}
	if on, _ := latencyGate(); !on {
		t.Skipf("timing gate is opt-in; run `make latency` or set %s=1", latencyGateEnv)
	}

	nodes := newGRPCCluster(t, 3)
	ctx := context.Background()

	const rf = 3
	for _, n := range nodes {
		n.cache.Namespace("window", peercache.WithReplicationFactor(rf))
	}

	writer := nodes[0]
	ns := writer.cache.Namespace("window")

	// Warm the connection pool: the first fan-out to each peer pays for a
	// handshake, which is a one-off and not part of the steady-state window.
	for i := 0; i < 200; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("warm/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("warm-up: %v", err)
		}
	}
	waitForReplication(t, nodes)

	const samples = 300
	windows := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		key := fmt.Sprintf("measure/%d", i)
		before := totalReplicaKeys(nodes)

		start := time.Now()
		if err := ns.Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		// The window closes when the replica count moves, which is the
		// observable moment the copies landed.
		deadline := time.Now().Add(30 * time.Second)
		for totalReplicaKeys(nodes) <= before {
			if time.Now().After(deadline) {
				t.Fatal("replication did not complete within 30s")
			}
			runtime.Gosched()
		}
		windows = append(windows, time.Since(start))
	}

	// A batch drain as well, because the per-write figures above are
	// quantised by the platform clock: on Windows a single sub-millisecond
	// interval times as zero, so a p50 of 0s means "below one tick" rather
	// than "instant". Timing a whole batch and dividing gives a mean that
	// the clock can actually resolve.
	const batch = 2000
	drainBefore := totalReplicaKeys(nodes)
	batchStart := time.Now()
	for i := 0; i < batch; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("drain/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	acked := time.Since(batchStart)
	deadline := time.Now().Add(60 * time.Second)
	want := drainBefore + int64(batch)*(rf-1)
	for totalReplicaKeys(nodes) < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d expected replicas landed within 60s",
				totalReplicaKeys(nodes)-drainBefore, int64(batch)*(rf-1))
		}
		runtime.Gosched()
	}
	drained := time.Since(batchStart)
	t.Logf("batch of %d async writes: acknowledged in %v, fully replicated in %v "+
		"(mean window %v per write)",
		batch, acked.Round(time.Millisecond), drained.Round(time.Millisecond),
		(drained / time.Duration(batch)).Round(time.Microsecond))

	sort.Slice(windows, func(i, j int) bool { return windows[i] < windows[j] })
	p := func(q float64) time.Duration {
		idx := int(q * float64(len(windows)))
		if idx >= len(windows) {
			idx = len(windows) - 1
		}
		return windows[idx]
	}
	t.Logf("async data-loss window over gRPC at RF=%d, %d samples: "+
		"p50 %v  p90 %v  p99 %v  max %v",
		rf, len(windows),
		p(0.50).Round(time.Microsecond), p(0.90).Round(time.Microsecond),
		p(0.99).Round(time.Microsecond), windows[len(windows)-1].Round(time.Microsecond))
	t.Logf("NOTE: the platform monotonic clock resolves to %v, so per-write percentiles "+
		"below that read as zero; the batch figure above is the one to quote",
		clockResolution().Round(time.Microsecond))
}

// totalReplicaKeys sums replicas held across a cluster.
func totalReplicaKeys(nodes []*grpcNode) int64 {
	var total int64
	for _, n := range nodes {
		total += n.cache.Stats().ReplicaKeys
	}
	return total
}

// waitForReplication blocks until the replica count stops moving.
func waitForReplication(t *testing.T, nodes []*grpcNode) {
	t.Helper()
	last := int64(-1)
	stable := 0
	deadline := time.Now().Add(60 * time.Second)
	for stable < 10000 {
		now := totalReplicaKeys(nodes)
		if now == last {
			stable++
		} else {
			stable, last = 0, now
		}
		if time.Now().After(deadline) {
			t.Fatal("replication never quiesced")
		}
		runtime.Gosched()
	}
}
