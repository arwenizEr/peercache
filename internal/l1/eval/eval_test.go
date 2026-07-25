package eval

import (
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
)

// The L1 workload, as concretely as it can be stated before M4 exists:
// small values, a keyspace far larger than the capacity, reads dominating
// writes, and a skewed access distribution.
const (
	capacity  = 100_000
	keyspace  = 1_000_000
	valueSize = 64
	// zipfS controls skew. 1.1 is the usual stand-in for web-cache access
	// patterns: a small hot set and a very long tail.
	zipfS = 1.1
	// evalSeed keeps every candidate on identical access sequences. A
	// comparison where each library saw different keys would be worthless.
	evalSeed = 0x5EED
)

func value() []byte { return make([]byte, valueSize) }

// keys precomputes the key strings once, so that strconv does not end up
// dominating a benchmark that is supposed to measure a cache.
func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "user:" + strconv.Itoa(i) + "/session"
	}
	return out
}

// zipfSequence returns n key indices drawn from a Zipf distribution over the
// keyspace, deterministically.
func zipfSequence(n int) []int {
	rng := rand.New(rand.NewSource(evalSeed))
	z := rand.NewZipf(rng, zipfS, 1, uint64(keyspace-1))
	out := make([]int, n)
	for i := range out {
		out[i] = int(z.Uint64())
	}
	return out
}

// BenchmarkGetHit measures the read path with everything resident. This is the
// L1 tier's dominant operation.
func BenchmarkGetHit(b *testing.B) {
	ks := keys(capacity)
	for _, f := range Factories() {
		b.Run(f.Name, func(b *testing.B) {
			c := f.New(capacity)
			defer c.Close()
			for _, k := range ks {
				c.Put(k, value())
			}
			// Without this, a candidate with buffered writes is
			// measured on misses rather than hits, and misses are an
			// order of magnitude cheaper.
			c.Settle()

			var hits, total atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i, localHits, localTotal := 0, int64(0), int64(0)
				for pb.Next() {
					if _, ok := c.Get(ks[i%len(ks)]); ok {
						localHits++
					}
					localTotal++
					i++
				}
				hits.Add(localHits)
				total.Add(localTotal)
			})
			b.StopTimer()

			// The reported hit ratio is data, not just a guard:
			// inserting exactly `capacity` distinct keys and getting
			// less than 100% back is a real property of an admission
			// policy that may reject a new key even when not full.
			//
			// The threshold only catches a broken harness — a
			// candidate whose writes never landed reads as ~0%, which
			// is how two earlier bugs in this file were found.
			if total.Load() > 0 {
				ratio := float64(hits.Load()) / float64(total.Load()) * 100
				b.ReportMetric(ratio, "%hit")
				if ratio < 50 {
					b.Errorf("only %.1f%% of reads hit; the harness is not measuring the hit path", ratio)
				}
			}
		})
	}
}

// BenchmarkMixed90Read measures the realistic ratio: nine reads per write, over
// a keyspace ten times the capacity, so eviction is continuous.
func BenchmarkMixed90Read(b *testing.B) {
	ks := keys(keyspace)
	seq := zipfSequence(1 << 16)

	for _, f := range Factories() {
		b.Run(f.Name, func(b *testing.B) {
			c := f.New(capacity)
			defer c.Close()
			val := value()

			// Warm to steady state, so the measurement is of a cache
			// doing its job rather than of one filling up.
			for _, idx := range seq {
				c.Put(ks[idx], val)
			}
			c.Settle()

			var hits, total atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i, localHits, localTotal := 0, int64(0), int64(0)
				for pb.Next() {
					k := ks[seq[i%len(seq)]]
					if i%10 == 0 {
						c.Put(k, val)
					} else {
						if _, ok := c.Get(k); ok {
							localHits++
						}
						localTotal++
					}
					i++
				}
				hits.Add(localHits)
				total.Add(localTotal)
			})
			b.StopTimer()
			if total.Load() > 0 {
				b.ReportMetric(float64(hits.Load())/float64(total.Load())*100, "%hit")
			}
		})
	}
}

// BenchmarkPut measures the write path in isolation, which is where the
// asynchronous candidate is expected to look best and behave worst: its Set is
// an enqueue, not an insert, so this measures buffering rather than storing.
func BenchmarkPut(b *testing.B) {
	ks := keys(keyspace)
	for _, f := range Factories() {
		b.Run(f.Name, func(b *testing.B) {
			c := f.New(capacity)
			defer c.Close()
			val := value()

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					c.Put(ks[i%len(ks)], val)
					i++
				}
			})
		})
	}
}

// TestReportHitRatio measures what the L1 tier actually cares about. A miss in
// L1 is a network round trip to the key's owner, so a percentage point of hit
// ratio is worth more than any plausible difference in nanoseconds per
// operation.
//
// It reports rather than asserts: this is an evaluation, and a threshold picked
// to make one library pass would be a conclusion disguised as a test.
func TestReportHitRatio(t *testing.T) {
	const operations = 2_000_000

	ks := keys(keyspace)
	seq := zipfSequence(operations)

	t.Logf("capacity %d, keyspace %d, %d operations, Zipf s=%.1f",
		capacity, keyspace, operations, zipfS)

	for _, f := range Factories() {
		c := f.New(capacity)
		val := value()
		hits, misses := 0, 0

		for _, idx := range seq {
			k := ks[idx]
			if _, ok := c.Get(k); ok {
				hits++
				continue
			}
			misses++
			c.Put(k, val)
		}
		c.Settle()

		ratio := float64(hits) / float64(hits+misses) * 100
		t.Logf("%-24s hit ratio %6.2f%%  (hits %d, misses %d, resident %d)",
			f.Name, ratio, hits, misses, c.Size())
		c.Close()
	}
}

// TestReportWriteVisibility measures whether a Put is visible to the very next
// Get.
//
// For the L1 tier this is not a performance question. A namespace that writes a
// value and then reads it back must not observe a miss: a miss is
// indistinguishable from "the owner has newer data" and would send the caller
// on a pointless round trip, or worse, cause it to overwrite fresh data with
// stale.
func TestReportWriteVisibility(t *testing.T) {
	const samples = 10_000

	ks := keys(samples)
	for _, f := range Factories() {
		c := f.New(capacity)
		val := value()
		invisible := 0

		for _, k := range ks {
			c.Put(k, val)
			if _, ok := c.Get(k); !ok {
				invisible++
			}
		}

		t.Logf("%-24s %d of %d writes were not visible to the next read (declared async: %v)",
			f.Name, invisible, samples, c.SettlesAsynchronously())
		c.Close()
	}
}

// TestReportEvictionAccuracy checks that a candidate respects its capacity. A
// library that overshoots badly turns a memory bound into a suggestion, and the
// L1 tier shares a process with the owned store.
func TestReportEvictionAccuracy(t *testing.T) {
	const smallCapacity = 10_000
	ks := keys(smallCapacity * 10)

	for _, f := range Factories() {
		c := f.New(smallCapacity)
		val := value()
		for _, k := range ks {
			c.Put(k, val)
		}
		// Every candidate does some eviction work lazily; compare them
		// settled, or the numbers measure scheduling luck.
		c.Settle()
		t.Logf("%-24s resident %d after inserting %d with capacity %d (%+.1f%%)",
			f.Name, c.Size(), len(ks), smallCapacity,
			float64(c.Size()-smallCapacity)/float64(smallCapacity)*100)
		c.Close()
	}
}

// TestCandidateCountMeetsSpec pins SPEC.md's "at least two candidates".
func TestCandidateCountMeetsSpec(t *testing.T) {
	seen := map[string]bool{}
	for _, f := range Factories() {
		if seen[f.Name] {
			t.Fatalf("duplicate candidate %q", f.Name)
		}
		seen[f.Name] = true

		c := f.New(16)
		if c.Name() != f.Name {
			t.Errorf("factory %q builds a candidate calling itself %q", f.Name, c.Name())
		}
		c.Close()
	}
	if len(seen) < 2 {
		t.Fatalf("SPEC.md requires at least two candidates; found %d", len(seen))
	}
	t.Logf("evaluated %d candidates: %v", len(seen), seen)
}
