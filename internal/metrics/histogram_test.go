package metrics_test

import (
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/peercache/peercache/internal/metrics"
)

// TestQuantilesBoundTheTruth is the property that makes a bucketed histogram
// usable: the reported percentile is an upper bound on the real one, never an
// under-estimate.
//
// Over-reporting a p99 makes an operator investigate a latency that was
// actually better. Under-reporting one lets a breach pass unnoticed, which is
// the failure that matters.
func TestQuantilesBoundTheTruth(t *testing.T) {
	var h metrics.Histogram

	// A known distribution: 990 samples at 300ns, 10 at 3ms.
	for i := 0; i < 990; i++ {
		h.Observe(300)
	}
	for i := 0; i < 10; i++ {
		h.Observe(3_000_000)
	}

	if got := h.Count(); got != 1000 {
		t.Fatalf("Count = %d, want 1000", got)
	}

	// The true p50 is 300ns; the bucket containing it has an upper bound of
	// 500ns, so that is what must be reported.
	if p50 := h.Quantile(0.50); p50 < 300 || p50 > 500 {
		t.Errorf("p50 = %dns; the true value is 300ns and the containing bucket "+
			"ends at 500ns", p50)
	}
	// The true p99 is still 300ns — only 1% are slow, and rank 990 of 1000
	// is the last fast sample.
	if p99 := h.Quantile(0.99); p99 > 500 {
		t.Errorf("p99 = %dns; 990 of 1000 samples were 300ns so the p99 is in the "+
			"fast bucket", p99)
	}
	// The true p999 is 3ms.
	if p999 := h.Quantile(0.999); p999 < 3_000_000 {
		t.Errorf("p999 = %dns, which under-reports the 3ms tail; an under-reported "+
			"percentile lets a breach pass unnoticed", p999)
	}
}

// TestEmptyHistogramReportsZero distinguishes "nothing measured" from "measured
// as instant". No operation takes zero nanoseconds, so zero is unambiguous.
func TestEmptyHistogramReportsZero(t *testing.T) {
	var h metrics.Histogram
	if got := h.Quantile(0.99); got != 0 {
		t.Errorf("an empty histogram reported p99 = %d, want 0", got)
	}
	if got := h.Count(); got != 0 {
		t.Errorf("Count = %d, want 0", got)
	}
}

// TestSumAndCountTrackObservations covers the two series Prometheus uses to
// compute an average, independently of the buckets.
func TestSumAndCountTrackObservations(t *testing.T) {
	var h metrics.Histogram
	for i := 1; i <= 100; i++ {
		h.Observe(uint64(i) * 1000)
	}
	if got := h.Count(); got != 100 {
		t.Fatalf("Count = %d, want 100", got)
	}
	// 1000 + 2000 + ... + 100000
	if want := uint64(1000 * 100 * 101 / 2); h.Sum() != want {
		t.Fatalf("Sum = %d, want %d", h.Sum(), want)
	}
}

// TestBucketsAreCumulative pins the format Prometheus requires. A histogram
// with exclusive buckets parses fine and produces nonsense quantiles in every
// dashboard that reads it.
func TestBucketsAreCumulative(t *testing.T) {
	var h metrics.Histogram
	h.Observe(300)
	h.Observe(3_000_000)

	buckets, count, _ := h.Snapshot()
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	var last uint64
	for _, b := range buckets {
		if b.Count < last {
			t.Fatalf("bucket counts decreased (%d after %d); Prometheus buckets "+
				"must be cumulative", b.Count, last)
		}
		last = b.Count
	}
	if final := buckets[len(buckets)-1]; final.UpperBound != math.MaxUint64 {
		t.Error("the last bucket is not +Inf")
	} else if final.Count != 2 {
		t.Errorf("the +Inf bucket holds %d, want every sample (2)", final.Count)
	}
}

// TestConcurrentObserveLosesNothing is the reason the counters are atomic. A
// histogram that dropped samples under load would under-report exactly when the
// system was busiest.
func TestConcurrentObserveLosesNothing(t *testing.T) {
	var h metrics.Histogram
	const writers, each = 32, 1000

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				h.Observe(300)
			}
		}()
	}
	wg.Wait()

	if got, want := h.Count(), uint64(writers*each); got != want {
		t.Fatalf("Count = %d after %d concurrent observations, want %d", got, want, want)
	}
	buckets, _, _ := h.Snapshot()
	if final := buckets[len(buckets)-1].Count; final != uint64(writers*each) {
		t.Fatalf("+Inf bucket holds %d, want %d", final, writers*each)
	}
}

// TestHistogramTextFormat checks the exposition output, including that bounds
// are emitted in seconds — the unit every dashboard assumes for a duration.
func TestHistogramTextFormat(t *testing.T) {
	var h metrics.Histogram
	h.Observe(300)
	h.Observe(1_500_000)

	var sb strings.Builder
	metrics.WriteHistogram(&sb, "peercache_remote_duration_seconds", "Remote latency.", &h)
	out := sb.String()

	for _, want := range []string{
		"# TYPE peercache_remote_duration_seconds histogram",
		`peercache_remote_duration_seconds_bucket{le="+Inf"} 2`,
		"peercache_remote_duration_seconds_count 2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// 500ns must appear as 5e-07 seconds, not as 500.
	if !strings.Contains(out, `le="5e-07"`) {
		t.Errorf("bucket bounds are not in seconds:\n%s", out)
	}
}

// TestQuantileRoundsUp pins the rounding, which a mutation showed the tests
// above could not distinguish.
//
// With a rank that lands between samples, rounding down picks the faster one
// and under-reports the percentile. Under-reporting is the direction that
// matters: it lets a latency breach pass unnoticed, whereas over-reporting only
// costs somebody an investigation.
//
// Three samples make the difference visible. The p50 has rank ceil(0.5*3) = 2,
// not 1, so it must report the middle sample's bucket rather than the fastest.
func TestQuantileRoundsUp(t *testing.T) {
	var h metrics.Histogram
	h.Observe(300)        // bucket ends at 500ns
	h.Observe(3_000)      // bucket ends at 5us
	h.Observe(30_000_000) // bucket ends at 50ms

	if p50 := h.Quantile(0.50); p50 <= 500 {
		t.Fatalf("p50 = %dns for samples {300ns, 3us, 30ms}; rank ceil(0.5*3) is 2, "+
			"so the p50 is the 3us sample and rounding down under-reports it", p50)
	}
	if p99 := h.Quantile(0.99); p99 < 30_000_000 {
		t.Fatalf("p99 = %dns; rank ceil(0.99*3) is 3, so it must reach the 30ms "+
			"sample", p99)
	}
}
