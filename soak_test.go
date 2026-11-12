package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// docs/design.md asks for a "load and soak test: 24h at target throughput, memory flat".
//
// # What this actually runs, and why the distinction matters
//
// Twenty-four hours is not a thing a test suite can do, and a test that claimed
// to have run for a day after five minutes would be the worst kind of false
// evidence — the kind that makes every other number in the project suspect.
//
// So this is the *harness*, with the duration configurable and the duration it
// actually ran reported in the output. CI runs it for seconds as a regression
// gate. To run it for real:
//
//	PEERCACHE_SOAK=24h go test . -run TestSoak -timeout 25h
//
// The measured result is recorded in docs/benchmarks.md with the duration it
// was measured over. If that says thirty seconds, thirty seconds is what was
// run.
//
// # What "memory flat" means here
//
// Not "allocates nothing" — the write path allocates by design, values are
// copied on Set. Flat means the *live heap* does not grow across the run once
// the working set is established. A leak shows as monotonic growth in
// HeapAlloc after a GC; churn does not.
//
// The check is deliberately loose on absolute size and strict on trend. A cache
// holding a million keys uses memory; a cache whose memory climbs forever with
// a bounded key set has a leak.

// soakDuration is how long the soak runs. Short by default so the suite stays
// usable; override with PEERCACHE_SOAK.
func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv("PEERCACHE_SOAK")
	if raw == "" {
		return 3 * time.Second
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("PEERCACHE_SOAK=%q: %v", raw, err)
	}
	return d
}

// TestSoakMemoryStaysFlat drives sustained traffic against a live cluster and
// asserts the live heap does not trend upward.
func TestSoakMemoryStaysFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}

	duration := soakDuration(t)
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)
	ctx := context.Background()

	// Every node opens the namespace, which is a correctness
	// requirement rather than tidiness.
	for _, id := range f.ids {
		f.caches[id].Namespace("soak",
			peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication(),
			peercache.WithL1TTL(2*time.Second))
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	// A bounded key set. An unbounded one would grow the heap legitimately
	// and the test could not tell that from a leak.
	const keys = 2_000
	value := make([]byte, 256)

	// Establish the working set before measuring, so the baseline is the
	// steady state rather than an empty cache.
	writer := f.ids[0]
	ns := func(id string) peercache.Namespace { return f.caches[id].Namespace("soak") }
	for i := 0; i < keys; i++ {
		if err := ns(writer).Set(ctx, soakKey(i), value, 0); err != nil {
			t.Fatalf("warm-up Set: %v", err)
		}
	}

	baseline := liveHeap()
	start := time.Now()
	deadline := start.Add(duration)

	var ops, failures int64
	var samples []uint64
	lastSample := time.Now()

	for i := 0; time.Now().Before(deadline); i++ {
		key := soakKey(i % keys)
		node := f.ids[i%len(f.ids)]

		var err error
		if i%4 == 0 {
			err = ns(node).Set(ctx, key, value, 0)
		} else {
			_, err = ns(node).Get(ctx, key)
		}
		if err != nil {
			// Retryable errors are the cluster working, not failing.
			if !errors.Is(err, peercache.ErrNotOwner) &&
				!errors.Is(err, peercache.ErrActivationPending) &&
				!errors.Is(err, peercache.ErrStaleEpoch) {
				failures++
			}
		}
		ops++

		// Sample the heap periodically. Sampling every operation would
		// make the measurement dominate the workload.
		if time.Since(lastSample) > duration/10 || len(samples) == 0 {
			samples = append(samples, liveHeap())
			lastSample = time.Now()
			// Move simulated time so TTLs expire and the sweeper runs;
			// a soak that never ages anything exercises no reclamation.
			f.clk.Advance(time.Second)
		}
	}
	elapsed := time.Since(start)
	final := liveHeap()

	t.Logf("soak ran %v: %d operations, %d failures, %.0f ops/sec",
		elapsed.Round(time.Millisecond), ops, failures, float64(ops)/elapsed.Seconds())
	t.Logf("live heap: baseline %.1f MiB, final %.1f MiB, samples %v",
		mib(baseline), mib(final), mibAll(samples))

	if failures > 0 {
		t.Fatalf("%d of %d operations failed with a non-retryable error", failures, ops)
	}
	if ops < 100 {
		t.Fatalf("only %d operations in %v; the soak did no work", ops, elapsed)
	}

	// The trend, not the absolute. Doubling the live heap over a run with a
	// bounded key set is a leak; some growth is GC timing.
	if final > baseline*2 && final-baseline > 32<<20 {
		t.Fatalf("live heap grew from %.1f MiB to %.1f MiB over %v with a fixed %d-key "+
			"working set; that is a leak, not churn",
			mib(baseline), mib(final), elapsed, keys)
	}
}

// TestSoakGoroutinesStayFlat is the other half. A goroutine leak is invisible
// in heap terms until it is severe, and this project has shipped one before —
// the gossip event loop, found only because it slowed an unrelated latency
// test.
func TestSoakGoroutinesStayFlat(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}

	duration := soakDuration(t)
	f := newFabric(t, 3)
	ctx := context.Background()

	for _, id := range f.ids {
		f.caches[id].Namespace("goroutines", peercache.WithReplicationFactor(2))
	}

	// Settle before the baseline: a cluster still starting has goroutines in
	// flight that are not leaks.
	f.settle(t)
	runtime.GC()
	baseline := runtime.NumGoroutine()

	deadline := time.Now().Add(duration)
	for i := 0; time.Now().Before(deadline); i++ {
		key := soakKey(i % 500)
		node := f.ids[i%len(f.ids)]
		if i%3 == 0 {
			_ = f.caches[node].Namespace("goroutines").Set(ctx, key, []byte("v"), 0)
		} else {
			_, _ = f.caches[node].Namespace("goroutines").Get(ctx, key)
		}
	}

	f.settle(t)
	runtime.GC()
	final := runtime.NumGoroutine()

	t.Logf("goroutines: baseline %d, final %d after %v", baseline, final, duration)

	// A fixed workload against a fixed cluster should not accumulate
	// goroutines. The allowance covers GC workers and the runtime's own,
	// which move without anything leaking.
	if final > baseline+20 {
		t.Fatalf("goroutines grew from %d to %d over a fixed workload; something is "+
			"started per operation and never stops", baseline, final)
	}
}

func soakKey(i int) string { return "soak/" + strconv.Itoa(i) }

// liveHeap returns bytes of live heap after a collection.
//
// The GC is forced so the number is the live set rather than whatever has not
// been collected yet. Without it the measurement reports allocation rate, which
// is the thing this test is trying *not* to be alarmed by.
func liveHeap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

func mib(b uint64) float64 { return float64(b) / (1 << 20) }

func mibAll(bs []uint64) string {
	out := "["
	for i, b := range bs {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprintf("%.1f", mib(b))
	}
	return out + "]"
}
