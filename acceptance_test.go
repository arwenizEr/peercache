package peercache_test

import (
	"context"
	"math"
	"os"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
)

// Local-read acceptance parameters, from SPEC.md section 4.
// latencyGateEnv opts a run into the latency assertion. See the skip in
// TestLocalGetP99AtOneMillionKeys for why it is not on by default.
const latencyGateEnv = "PEERCACHE_LATENCY_GATE"

const (
	acceptanceKeys      = 1_000_000
	acceptanceValueSize = 64
	acceptanceP99Budget = time.Microsecond

	// batchOps is how many reads make up one sample when the platform clock
	// is too coarse to time a single one. Sized so that a batch runs for
	// roughly twenty times the clock's resolution, which keeps the
	// quantisation error near five percent of the per-operation figure.
	batchOps = 30_000
	// batchCount is how many such samples are taken. Four hundred puts the
	// p99 four samples from the worst, so a single descheduled batch -
	// which measures the operating system, not the cache - does not decide
	// the result.
	batchCount = 400
)

// percentiles summarises a latency sample. The slice is sorted in place.
type percentiles struct {
	p50, p90, p99, p999, max time.Duration
}

func summarise(samples []time.Duration) percentiles {
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	// Nearest-rank: the p-th percentile is the smallest sample at or above
	// which p of the data lies, which for a zero-indexed sorted slice is
	// ceil(q*n) - 1. Using floor(q*n) instead — the obvious version, and the
	// one this had — shifts every figure one sample high.
	at := func(q float64) time.Duration {
		idx := int(math.Ceil(q*float64(len(samples)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}
	return percentiles{
		p50:  at(0.50),
		p90:  at(0.90),
		p99:  at(0.99),
		p999: at(0.999),
		max:  samples[len(samples)-1],
	}
}

// clockResolution returns the smallest non-zero interval the platform's
// monotonic clock can report.
//
// This is not a formality. Go's monotonic clock on Windows is driven by the
// system interrupt timer and resolves to roughly 500 microseconds, so timing a
// single sub-microsecond operation there returns zero every time - and a test
// that reported a p99 of "0s" would look like a spectacular pass rather than a
// broken measurement.
func clockResolution() time.Duration {
	const probes = 2_000_000
	realClock := clock.Real()

	smallest := time.Hour
	seen := 0
	for i := 0; i < probes && seen < 50; i++ {
		start := realClock.Now()
		d := realClock.Since(start)
		if d > 0 {
			seen++
			if d < smallest {
				smallest = d
			}
		}
	}
	if seen == 0 {
		return time.Hour
	}
	return smallest
}

// TestLocalGetP99AtOneMillionKeys is the local latency acceptance criterion: local
// Get p99 under one microsecond at a million keys.
//
// Two things about the criterion need stating rather than quietly resolving.
//
// First, it does not say whether the tail includes garbage collection. It turns
// out not to matter: the read path allocates nothing, which
// [TestGetIsAllocationFree] asserts directly, and measuring with the collector
// disabled produced the same distribution within noise. So this measures the
// collector in its normal state, which is both the honest configuration and the
// stricter one.
//
// An earlier version of this test measured both and gated on the
// collector-disabled number. That was wrong in an instructive way: whichever
// measurement ran *second* was consistently slower, because a desktop CPU under
// several seconds of sustained load drops clock speed. The test was measuring
// thermal drift and attributing it to the garbage collector.
//
// Second, the criterion assumes a clock that can time a single operation. On a
// platform whose monotonic clock resolves to half a millisecond that is
// impossible, so the test measures batches instead and says so. A batch mean
// hides within-batch variance but still catches a sustained stall - a lock
// convoy, a pathological hash, a collector that will not yield - because those
// inflate whichever batch they land in.
func TestLocalGetP99AtOneMillionKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates roughly 230 MB; skipped under -short")
	}
	if raceEnabled {
		// Measured at roughly four times the cost under -race, which
		// tells you about the detector and nothing about the cache. The
		// CI job that runs without the detector is the one that gates
		// this criterion.
		t.Skip("latency budgets are meaningless under the race detector")
	}
	if os.Getenv(latencyGateEnv) != "1" {
		// The same argument as the one above, for the same reason.
		//
		// `go test ./...` runs packages concurrently. That means
		// this budget is measured while gRPC servers, memberlist
		// clusters and loopback sockets in other packages are competing
		// for the same cores, and the p99 it reports is a fact about the
		// machine rather than about the cache: 478ns idle, 1.87µs
		// co-tenant, with nothing in this package changed between them.
		//
		// The budget is not relaxed — it is measured where it means
		// something. `make latency` runs this alone, and CI runs it as
		// its own step.
		t.Skipf("latency gate is opt-in; run `make latency` or set %s=1", latencyGateEnv)
	}

	resolution := clockResolution()
	perOp := resolution <= 100*time.Nanosecond
	t.Logf("platform monotonic clock resolution: %v (per-operation timing: %v)", resolution, perOp)

	c, _ := newCache(t)
	ns := c.Namespace("bench")
	ctx := context.Background()

	value := make([]byte, acceptanceValueSize)
	keys := make([]string, acceptanceKeys)
	for i := range keys {
		keys[i] = key(i)
	}
	for _, k := range keys {
		if err := ns.Set(ctx, k, value, 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if got := c.Stats().OwnedKeys; got != acceptanceKeys {
		t.Fatalf("OwnedKeys = %d, want %d", got, acceptanceKeys)
	}

	var mem runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mem)
	t.Logf("resident heap with %d keys of %d bytes: %.1f MiB",
		acceptanceKeys, acceptanceValueSize, float64(mem.HeapAlloc)/(1<<20))

	// Access order is shuffled deterministically so the measurement is not a
	// sequential walk over a freshly built map, which would flatter the
	// cache hierarchy in a way no real workload does.
	order := shuffledIndices(len(keys))

	measure := measureBatched
	method := "per-operation cost derived from batches of " + itoa(batchOps) +
		"; batch means smooth single-operation outliers, so this p99 is a lower bound " +
		"on the true per-operation p99 and detects sustained stalls rather than isolated ones"
	if perOp {
		measure = measurePerOp
		method = "per-operation, directly timed"
	}

	// A warm-up pass that is measured and discarded, so that the recorded
	// distribution is of a steady-state cache rather than of one whose
	// translation buffers are still cold.
	measure(t, ns, ctx, keys, order)
	got := measure(t, ns, ctx, keys, order)

	t.Logf("method: %s", method)
	t.Logf("p50 %v  p90 %v  p99 %v  p99.9 %v  max %v", got.p50, got.p90, got.p99, got.p999, got.max)

	if got.p99 >= acceptanceP99Budget {
		t.Fatalf("p99 %v exceeds the %v budget at %d keys",
			got.p99, acceptanceP99Budget, acceptanceKeys)
	}
}

// TestBothMeasurementPathsWork exercises measurePerOp and measureBatched
// regardless of which one this platform's clock selects.
//
// Without it, the per-operation path is dead code on Windows and runs for the
// very first time inside CI on Linux — where a bug in it fails the acceptance
// criterion and looks like a performance regression. A measurement harness that
// is only exercised on the platform where it is not used is not a harness.
func TestBothMeasurementPathsWork(t *testing.T) {
	const keys = 2000

	c, _ := newCache(t)
	ns := c.Namespace("measure")
	ctx := context.Background()

	value := make([]byte, acceptanceValueSize)
	ks := make([]string, keys)
	for i := range ks {
		ks[i] = key(i)
		if err := ns.Set(ctx, ks[i], value, 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	order := shuffledIndices(keys)

	for name, measure := range map[string]func(*testing.T, peercache.Namespace, context.Context, []string, []int) percentiles{
		"per operation": measurePerOp,
		"batched":       measureBatched,
	} {
		t.Run(name, func(t *testing.T) {
			got := measure(t, ns, ctx, ks, order)

			// The distribution must at least be ordered. On a coarse
			// clock every figure may legitimately be zero, so this
			// asserts consistency rather than magnitude.
			if got.p50 > got.p90 || got.p90 > got.p99 || got.p99 > got.p999 || got.p999 > got.max {
				t.Fatalf("percentiles are not monotonic: %+v", got)
			}
			if got.max < 0 {
				t.Fatalf("negative duration in %+v", got)
			}
		})
	}
}

// TestSummariseOrdersCorrectly pins the percentile arithmetic itself. The
// acceptance criterion is only as trustworthy as this function.
func TestSummariseOrdersCorrectly(t *testing.T) {
	samples := make([]time.Duration, 1000)
	for i := range samples {
		// Deliberately reverse order, so a missing sort shows up.
		samples[i] = time.Duration(1000-i) * time.Nanosecond
	}

	got := summarise(samples)
	want := percentiles{
		p50:  500 * time.Nanosecond,
		p90:  900 * time.Nanosecond,
		p99:  990 * time.Nanosecond,
		p999: 999 * time.Nanosecond,
		max:  1000 * time.Nanosecond,
	}
	if got != want {
		t.Fatalf("summarise = %+v, want %+v", got, want)
	}

	single := summarise([]time.Duration{7 * time.Nanosecond})
	if single.p50 != 7*time.Nanosecond || single.max != 7*time.Nanosecond {
		t.Fatalf("single-sample summarise = %+v", single)
	}
}

// TestGetIsAllocationFree is the assertion that makes the garbage collector
// irrelevant to the latency criterion above.
//
// A read that allocates cannot have a bounded tail: sooner or later one of them
// triggers a collection and inherits the pause. Asserting zero allocations is a
// far stronger and far cheaper statement than measuring with the collector
// switched off, and it fails loudly the day someone adds a fmt.Errorf to the
// read path - which is exactly how the miss path came to allocate 128 bytes.
func TestGetIsAllocationFree(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("alloc")
	ctx := context.Background()

	if err := ns.Set(ctx, "hit", []byte("value"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	hits := testing.AllocsPerRun(1000, func() {
		if _, err := ns.Get(ctx, "hit"); err != nil {
			t.Fatalf("Get: %v", err)
		}
	})
	if hits != 0 {
		t.Errorf("a cache hit allocated %.1f times per operation, want 0", hits)
	}

	// A miss is routine, not exceptional: it is most of what GetOrLoad and
	// BatchGet see. It must be free too.
	misses := testing.AllocsPerRun(1000, func() {
		if _, err := ns.Get(ctx, "absent"); err == nil {
			t.Fatal("expected a miss")
		}
	})
	if misses != 0 {
		t.Errorf("a cache miss allocated %.1f times per operation, want 0", misses)
	}

	exists := testing.AllocsPerRun(1000, func() {
		if _, err := ns.Exists(ctx, "hit"); err != nil {
			t.Fatalf("Exists: %v", err)
		}
	})
	if exists != 0 {
		t.Errorf("Exists allocated %.1f times per operation, want 0", exists)
	}
}

// measurePerOp times every read individually. Only valid where the platform
// clock can resolve a single operation.
func measurePerOp(t *testing.T, ns peercache.Namespace, ctx context.Context, keys []string, order []int) percentiles {
	t.Helper()
	realClock := clock.Real()
	samples := make([]time.Duration, len(order))

	for i, idx := range order {
		start := realClock.Now()
		val, err := ns.Get(ctx, keys[idx])
		samples[i] = realClock.Since(start)
		if err != nil {
			t.Fatalf("Get(%q): %v", keys[idx], err)
		}
		if len(val) != acceptanceValueSize {
			t.Fatalf("Get returned %d bytes, want %d", len(val), acceptanceValueSize)
		}
	}
	return summarise(samples)
}

// measureBatched times batches of reads and reports the per-operation cost of
// each batch. Each batch is sized so that its duration exceeds the platform's
// clock resolution by a wide margin; the resulting quantisation error is a few
// nanoseconds per operation.
func measureBatched(t *testing.T, ns peercache.Namespace, ctx context.Context, keys []string, order []int) percentiles {
	t.Helper()
	realClock := clock.Real()
	samples := make([]time.Duration, 0, batchCount)

	next := 0
	for b := 0; b < batchCount; b++ {
		start := realClock.Now()
		for i := 0; i < batchOps; i++ {
			idx := order[next%len(order)]
			next++
			val, err := ns.Get(ctx, keys[idx])
			if err != nil {
				t.Fatalf("Get(%q): %v", keys[idx], err)
			}
			if len(val) != acceptanceValueSize {
				t.Fatalf("Get returned %d bytes, want %d", len(val), acceptanceValueSize)
			}
		}
		samples = append(samples, realClock.Since(start)/batchOps)
	}
	return summarise(samples)
}

// itoa avoids pulling strconv into this file's import list for one call.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// shuffledIndices returns 0..n-1 in a fixed pseudo-random order. The shuffle is
// deterministic so the measurement is reproducible without a seed.
func shuffledIndices(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	state := uint64(0x9E3779B97F4A7C15)
	for i := n - 1; i > 0; i-- {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		j := int(state % uint64(i+1))
		out[i], out[j] = out[j], out[i]
	}
	return out
}
