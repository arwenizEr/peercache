package tests

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/membership"
	"github.com/arwenizEr/peercache/transport"
)

// Remote-read acceptance, from docs/design.md §4:
//
//	remote Get p99 under 2ms and p999 under 10ms at 50k rps in a 3-node
//	real-network test
//
// "Real-network" is read here as three cache instances in one process talking
// over real gRPC on loopback TCP: real sockets, real serialisation, real
// deadlines, real connection pool. What it does not include is three OS
// processes on three machines, which would measure a network this test cannot
// provision and cannot make reproducible.
//
// Like the local latency gate, this is opt-in. Measuring a 2ms budget while the
// rest of the suite competes for the same cores reports the machine's load
// rather than the cache's latency, and CI runs it as its own step.

const m4RemoteBudgetP99 = 2 * time.Millisecond
const m4RemoteBudgetP999 = 10 * time.Millisecond

// sharedRunnerFactor scales the remote budgets under PEERCACHE_LATENCY_GATE=shared.
// Five times the criterion leaves ~2.7x headroom over the 3.7ms p99 a
// GitHub-hosted runner measured, and still fails on the order-of-magnitude
// regression the step exists to catch.
const sharedRunnerFactor = 5

// m4TargetRPS is the offered load the criterion names.
const m4TargetRPS = 50_000

// m4ThroughputFloor is a regression floor, not the acceptance criterion.
//
// Set well below the 45k-48k range measured in docs/benchmarks.md so runner
// noise cannot trip it. Its job is to catch a collapse, which is what the
// unasserted criterion above could not do.
const m4ThroughputFloor = 20_000

// grpcNode is one cache serving real gRPC on loopback.
type grpcNode struct {
	id     string
	info   core.Node
	gossip *membership.Gossip
	cache  peercache.Cache
}

// newGRPCCluster builds n caches wired together by real gossip and real gRPC.
func newGRPCCluster(t *testing.T, n int) []*grpcNode {
	t.Helper()

	nodes := make([]*grpcNode, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("pod-%d", i)

		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		info := core.Node{ID: id, Addr: l.Addr().String()}

		tr, err := transport.New(transport.Options{Self: info})
		if err != nil {
			t.Fatalf("transport.New(%s): %v", id, err)
		}

		// Through RetryBind, because this host reserves UDP ports the OS will
		// still hand out. See open question 27: `netsh interface ipv4 show
		// excludedportrange protocol=udp` reports 600 contiguous ports
		// excluded here, and binding port 0 lands inside that band often
		// enough to fail this test most times it is run.
		//
		// This is the third caller to need the fix and the second to
		// rediscover the bug rather than inherit it. The membership tests
		// were fixed first, then cluster_test.go's own helper, and this one
		// built a third — so it kept
		// failing, invisibly, because the test is behind an opt-in latency
		// gate that only CI's Linux runner sets. Question 27's closing
		// sentence is about exactly this: a correct fix applied to some of the
		// places that need it reads, in the commit log, like a complete one.
		g, err := testkit.RetryBind(func() (*membership.Gossip, error) {
			return membership.New(membership.Options{
				Self:   info,
				Config: testkit.LocalGossipConfig(id),
			})
		})
		if err != nil {
			t.Fatalf("membership.New(%s): %v", id, err)
		}

		c, err := peercache.New(
			peercache.WithNodeID(id),
			peercache.WithClock(clock.Real()),
			peercache.WithMembership(g),
			peercache.WithTransport(tr),
			peercache.WithListener(l),
			// Deferral off: this measures throughput, not the
			// ownership handover window, which has its own tests.
			peercache.WithActivationDelay(-1),
		)
		if err != nil {
			t.Fatalf("peercache.New(%s): %v", id, err)
		}

		node := &grpcNode{id: id, info: info, gossip: g, cache: c}
		nodes = append(nodes, node)

		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_ = c.Close(ctx)
			_ = g.Close()
		})

		if i > 0 {
			if err := g.Join([]string{nodes[0].gossip.LocalAddr()}); err != nil {
				t.Fatalf("%s joining: %v", id, err)
			}
		}
	}

	// Wait for every node to see the whole cluster before measuring
	// anything. A node that has not converged owns a different share of the
	// keyspace, which would make the measurement a mixture of two systems.
	deadline := time.Now().Add(60 * time.Second)
	for {
		converged := true
		for _, node := range nodes {
			if node.cache.Stats().Members != n {
				converged = false
				break
			}
		}
		if converged {
			break
		}
		if time.Now().After(deadline) {
			var seen []string
			for _, node := range nodes {
				seen = append(seen, fmt.Sprintf("%s=%d", node.id, node.cache.Stats().Members))
			}
			t.Fatalf("gossip did not converge in 60s: %v (want %d each)", seen, n)
		}
		runtime.Gosched()
	}
	return nodes
}

// TestRemoteGetThroughputAndLatency is the remote-read acceptance criterion.
func TestRemoteGetThroughputAndLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and drives 50k rps")
	}
	if raceEnabled {
		t.Skip("latency budgets are meaningless under the race detector")
	}
	on, shared := latencyGate()
	if !on {
		t.Skipf("latency gate is opt-in; run `make latency` or set %s=1", latencyGateEnv)
	}

	nodes := newGRPCCluster(t, 3)
	ctx := context.Background()

	// The namespace is created with the non-owned tier *disabled*, before
	// anything is written to it. Both halves matter.
	//
	// Namespaces are created once and their options are fixed then, so
	// asking for the namespace first and configuring it later silently kept
	// the default five-second L1 — every read after the first was a local
	// cache hit and the run reported twelve million rps, which is not a
	// number any gRPC stack produces.
	//
	// Disabling the tier outright rather than giving it a one-nanosecond
	// bound also matters. A nanosecond bound still stores each value and
	// expires it on the next read, so the load generator spent real CPU on
	// cache churn — on a box where the generator and all three servers share
	// twenty cores, that is throughput taken directly from the thing being
	// measured.
	const nsName = "bench-remote"
	for _, node := range nodes {
		node.cache.Namespace(nsName, peercache.WithL1TTL(-1))
	}
	reader := nodes[0].cache.Namespace(nsName)

	const keys = 20_000
	value := make([]byte, 256)
	for i := range value {
		value[i] = byte(i)
	}
	all := make([]string, 0, keys)
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("bench/%d", i)
		all = append(all, k)
		if err := reader.Set(ctx, k, value, 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	// Keep only the keys that are genuinely remote from the reader, so the
	// sample is not diluted by local hits.
	remote := make([]string, 0, keys)
	for _, k := range all {
		if _, err := nodes[0].cache.Group(k).Keys(ctx); errors.Is(err, peercache.ErrNotOwner) {
			remote = append(remote, k)
		}
	}
	if len(remote) < keys/4 {
		t.Fatalf("only %d of %d keys are remote from the reader; the keyspace is not "+
			"distributed and this would not measure the remote path", len(remote), keys)
	}
	t.Logf("%d of %d keys are remote from the reader", len(remote), keys)

	// Windows cannot time a single sub-millisecond operation: its monotonic
	// clock resolves to roughly half a millisecond, so individual deltas
	// come back as zero. Measure in batches large enough that a batch is
	// comfortably longer than the resolution, exactly as the local gate does,
	// and divide.
	//
	// The cost is real and stated rather than hidden: a batch mean smooths
	// isolated outliers, so the percentiles below are a lower bound on the
	// true per-operation percentiles. They still catch sustained stalls,
	// which is what a p99 budget is for.
	resolution := clockResolution()
	perOp := resolution <= 100*time.Nanosecond
	batch := 1
	if !perOp {
		// ~20x the resolution, so quantisation is a few percent.
		batch = 64
	}
	t.Logf("platform monotonic clock resolution: %v (per-operation timing: %v, batch: %d)",
		resolution, perOp, batch)

	// Worker count is chosen to *offer* roughly the target rate, not to
	// saturate the system, and that distinction is the whole measurement.
	//
	// The criterion asks for "p99 under 2ms at 50k rps": a latency at a given
	// offered load. An earlier version ran 64 workers flat out, which reached
	// 56k rps but with a p50 of 1.2ms — almost all of it queueing behind the
	// load generator's own concurrency, since 64 workers against a ~175µs
	// service time means each request waits behind dozens of others. That
	// measured queue depth, not service time, and it was unstable: two runs
	// of the same binary gave p99 1.737ms and 2.882ms, a 66% swing, because
	// saturation amplifies any scheduling noise on the machine.
	//
	// A serial round trip costs ~175µs (see BenchmarkInvoke), so a worker
	// sustains ~5,700 rps and twelve of them offer ~68k — comfortably past
	// the target without driving the system into its queue.
	//
	// The constant below is 36, not the twelve that paragraph reasons to. The
	// prose was left behind by a change to the number and a review found the two
	// disagreeing, which is worth a sentence rather than a silent correction:
	// a comment that argues carefully to a value the code does not use is
	// worse than no comment, because it is read as evidence.
	//
	// Thirty-six is kept. A sweep measured 110k rps at
	// twelve workers and 235k at sixty-four, with p99 rising 180µs → 334µs
	// across that range — so the queueing the paragraph above warns about does
	// not begin anywhere near here, and offering ~4x the target rather than
	// ~1.4x leaves the criterion asserted with margin rather than by a
	// whisker.
	const (
		workers  = 36
		duration = 5 * time.Second
		warmup   = 500 * time.Millisecond
	)

	var (
		wg        sync.WaitGroup
		completed atomic.Int64
		failed    atomic.Int64
	)
	samples := make([][]time.Duration, workers)

	// Warm the connection pool first. The first request to each peer pays
	// for a TCP handshake and an HTTP/2 preface, and a one-off 1.4s outlier
	// in the tail says nothing about steady-state latency.
	warmDeadline := time.Now().Add(warmup)
	for i := 0; time.Now().Before(warmDeadline); i++ {
		if _, err := reader.Get(ctx, remote[i%len(remote)]); err != nil {
			t.Fatalf("warm-up read: %v", err)
		}
	}

	start := time.Now()
	stop := start.Add(duration)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Per-worker sample slices, merged afterwards: sharing one
			// would put a mutex on the measurement path and measure
			// the mutex.
			local := make([]time.Duration, 0, 1<<14)
			i := w
			for time.Now().Before(stop) {
				t0 := time.Now()
				for b := 0; b < batch; b++ {
					k := remote[i%len(remote)]
					i += workers
					if _, err := reader.Get(ctx, k); err != nil {
						failed.Add(1)
					}
				}
				elapsed := time.Since(t0)
				completed.Add(int64(batch))
				local = append(local, elapsed/time.Duration(batch))
			}
			samples[w] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	if n := failed.Load(); n > 0 {
		t.Fatalf("%d reads failed", n)
	}

	merged := make([]time.Duration, 0, len(samples)*64)
	for _, s := range samples {
		merged = append(merged, s...)
	}
	if len(merged) == 0 {
		t.Fatal("no samples collected")
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })

	rps := float64(completed.Load()) / elapsed.Seconds()
	p := func(q float64) time.Duration {
		idx := int(q * float64(len(merged)))
		if idx >= len(merged) {
			idx = len(merged) - 1
		}
		return merged[idx]
	}

	t.Logf("achieved %.0f rps over %v with %d workers (%d reads, %d samples)",
		rps, elapsed.Round(time.Millisecond), workers, completed.Load(), len(merged))
	t.Logf("per-operation, derived from batches of %d: p50 %v  p90 %v  p99 %v  p99.9 %v  max %v",
		batch,
		p(0.50).Round(time.Microsecond), p(0.90).Round(time.Microsecond),
		p(0.99).Round(time.Microsecond), p(0.999).Round(time.Microsecond),
		merged[len(merged)-1].Round(time.Microsecond))

	// The latency budgets are the criterion and are asserted unconditionally —
	// scaled on a shared runner, where they are a regression ceiling instead.
	budgetP99, budgetP999, what := m4RemoteBudgetP99, m4RemoteBudgetP999, "budget"
	if shared {
		budgetP99 *= sharedRunnerFactor
		budgetP999 *= sharedRunnerFactor
		what = "shared-runner ceiling"
	}
	if got := p(0.99); got > budgetP99 {
		t.Errorf("p99 %v exceeds the %v %s", got.Round(time.Microsecond), budgetP99, what)
	}
	if got := p(0.999); got > budgetP999 {
		t.Errorf("p99.9 %v exceeds the %v %s", got.Round(time.Microsecond), budgetP999, what)
	}

	// The rate is reported rather than asserted, and deliberately so. 50k rps
	// is a property of the machine offering the load as much as of the cache;
	// failing a correctness suite because a laptop is busy would make the
	// number a nuisance rather than a signal. It is recorded in
	// docs/benchmarks.md with the machine it was measured on, which is where
	// a throughput claim belongs.
	if rps < m4TargetRPS {
		t.Logf("NOTE: achieved %.0f rps, below the %d rps the criterion names. "+
			"The latency budgets above still hold at this rate; see docs/benchmarks.md.",
			rps, m4TargetRPS)
	}

	// A regression floor, which the paragraph above deliberately does not
	// provide.
	//
	// An audit found that this test could not fail on throughput at all:
	// the criterion's number was logged and nothing was asserted, so a change
	// that cut the rate by a factor of ten would have passed silently while
	// the file still claimed to cover a 50k rps criterion. Reporting a number
	// nobody checks is indistinguishable from not measuring it.
	//
	// The floor is set well below the measured range (45k-48k on the machine
	// in docs/benchmarks.md) precisely so that runner noise cannot trip it.
	// It is not the criterion and does not pretend to be — it catches the
	// order-of-magnitude regression that the criterion, being unasserted,
	// could not.
	if rps < m4ThroughputFloor {
		t.Errorf("achieved %.0f rps, below the %d rps regression floor. This is not "+
			"the %d rps criterion — it is the floor that catches a collapse. Either "+
			"the transport regressed badly or this machine is too loaded to measure on",
			rps, m4ThroughputFloor, m4TargetRPS)
	}
}
