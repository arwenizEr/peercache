package metrics

import (
	"math"
	"strconv"
	"strings"
	"sync/atomic"
)

// Histogram records a latency distribution.
//
// # Why a histogram rather than a running average
//
// An average hides the thing you need. A cache that answers in 200 ns for
// 99 requests and 200 ms for the hundredth has a 2 ms average and a serious
// problem, and the average is the only number that looks fine. Every latency
// claim in this project is a percentile for that reason.
//
// # Why fixed buckets rather than a sketch
//
// Bucketed counters are what Prometheus consumes, they are lock-free to update,
// and the error is bounded and knowable — a sample lands in a bucket and the
// reported percentile is the bucket's upper bound. A t-digest or HDR histogram
// would be more accurate and would need either a lock or a per-goroutine
// structure to merge, on a path where the whole operation costs 500 ns.
//
// The buckets below are chosen for this system's actual numbers: sub-microsecond
// local reads (M1 measured p99 at 515 ns), millisecond remote ones (M4 measured
// p99 at 1.7 ms). They run from 100 ns to 10 s.
//
// A Histogram is safe for concurrent use and its zero value is ready.
type Histogram struct {
	// buckets are cumulative counts indexed by bucketBounds. Atomic rather
	// than mutex-guarded because this is on the read path and a mutex there
	// would serialise every reader through one cache line.
	buckets [len(bucketBounds) + 1]atomic.Uint64
	count   atomic.Uint64
	sum     atomic.Uint64 // nanoseconds
}

// bucketBounds are the upper bounds in nanoseconds, ascending.
//
// Roughly 2.5x apart, which keeps the count small while bounding the reported
// percentile's error to that factor. Denser around the microsecond and
// millisecond marks, because that is where this system's decisions live: "is a
// local read still sub-microsecond" and "is a remote read still under 2 ms" are
// the two questions an operator asks.
var bucketBounds = [...]uint64{
	100, 250, 500, // sub-microsecond: the local read path
	1_000, 2_500, 5_000, // single-digit microseconds
	10_000, 25_000, 50_000, // tens of microseconds
	100_000, 250_000, 500_000, // sub-millisecond
	1_000_000, 2_000_000, 5_000_000, // the remote read budget lives here
	10_000_000, 50_000_000, // tens of milliseconds
	100_000_000, 1_000_000_000, // slow, and very slow
	10_000_000_000, // something is wrong
}

// Observe records one measurement in nanoseconds.
//
// It is three atomic adds and a linear scan of at most 21 comparisons, with no
// allocation. The scan is linear rather than binary because at this length the
// branch predictor beats the extra arithmetic, and because most samples land in
// the first few buckets — the common case exits early.
func (h *Histogram) Observe(nanos uint64) {
	h.count.Add(1)
	h.sum.Add(nanos)
	for i, bound := range bucketBounds {
		if nanos <= bound {
			h.buckets[i].Add(1)
			return
		}
	}
	h.buckets[len(bucketBounds)].Add(1)
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Sum returns the total observed nanoseconds.
func (h *Histogram) Sum() uint64 { return h.sum.Load() }

// Quantile estimates a percentile in nanoseconds.
//
// The result is a bucket's upper bound, so it overestimates by at most the
// bucket width. That is stated rather than hidden: a p99 reported as 500 ns
// means "at most 500 ns", and the true value is somewhere above 250.
//
// Returns 0 when nothing has been observed, which is distinguishable from a
// genuine zero because no operation takes zero nanoseconds.
func (h *Histogram) Quantile(q float64) uint64 {
	total := h.count.Load()
	if total == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	} else if q >= 1 {
		q = 1
	}

	// The rank of the sample we want, 1-based.
	want := uint64(math.Ceil(q * float64(total)))
	if want == 0 {
		want = 1
	}

	var cumulative uint64
	for i, bound := range bucketBounds {
		cumulative += h.buckets[i].Load()
		if cumulative >= want {
			return bound
		}
	}
	// Everything above the last bound. Reported as the last bound rather
	// than infinity: a number an operator can act on beats a symbol.
	return bucketBounds[len(bucketBounds)-1]
}

// Snapshot returns the cumulative bucket counts, as Prometheus expects them.
//
// Prometheus histogram buckets are cumulative — each le bucket counts every
// sample at or below its bound — whereas the internal counters are exclusive.
// Converting here rather than storing cumulatively keeps Observe to a single
// increment.
func (h *Histogram) Snapshot() (buckets []Bucket, count, sum uint64) {
	var cumulative uint64
	buckets = make([]Bucket, 0, len(bucketBounds)+1)
	for i, bound := range bucketBounds {
		cumulative += h.buckets[i].Load()
		buckets = append(buckets, Bucket{UpperBound: bound, Count: cumulative})
	}
	cumulative += h.buckets[len(bucketBounds)].Load()
	buckets = append(buckets, Bucket{UpperBound: math.MaxUint64, Count: cumulative})
	return buckets, h.count.Load(), h.sum.Load()
}

// Bucket is one cumulative histogram bucket.
type Bucket struct {
	// UpperBound is in nanoseconds; MaxUint64 marks the +Inf bucket.
	UpperBound uint64
	// Count is every sample at or below UpperBound.
	Count uint64
}

// WriteHistogram renders a histogram in the Prometheus text exposition format.
//
// The bounds are emitted in *seconds*, because that is the unit Prometheus
// conventions expect for durations and the one every dashboard assumes. The
// internal representation stays in nanoseconds because integers are exact and
// this system's numbers are small.
func WriteHistogram(sb *strings.Builder, name, help string, h *Histogram) {
	buckets, count, sum := h.Snapshot()

	sb.WriteString("# HELP ")
	sb.WriteString(name)
	sb.WriteString(" ")
	sb.WriteString(help)
	sb.WriteString("\n# TYPE ")
	sb.WriteString(name)
	sb.WriteString(" histogram\n")

	for _, b := range buckets {
		sb.WriteString(name)
		sb.WriteString(`_bucket{le="`)
		if b.UpperBound == math.MaxUint64 {
			sb.WriteString("+Inf")
		} else {
			sb.WriteString(strconv.FormatFloat(float64(b.UpperBound)/1e9, 'g', -1, 64))
		}
		sb.WriteString(`"} `)
		sb.WriteString(strconv.FormatUint(b.Count, 10))
		sb.WriteString("\n")
	}

	sb.WriteString(name)
	sb.WriteString("_sum ")
	sb.WriteString(strconv.FormatFloat(float64(sum)/1e9, 'g', -1, 64))
	sb.WriteString("\n")
	sb.WriteString(name)
	sb.WriteString("_count ")
	sb.WriteString(strconv.FormatUint(count, 10))
	sb.WriteString("\n")
}
