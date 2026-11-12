# 0005 — L1 uses maypok86/otter v1

**Status:** accepted, M1
**Affects:** `internal/l1`

docs/design.md section 4 (M1) requires evaluating at least two candidate cache
libraries under this workload and recording the comparison, rather than
assuming the first one is best. Four were measured.

## The workload

The L1 tier holds copies of keys this node does *not* own. Reads dominate,
values are small, the keyspace is far larger than the capacity, and access is
skewed. So the measurements are: throughput on the hit path, throughput under a
mixed load, hit ratio under a Zipf distribution, whether a write is visible to
the next read, and whether capacity is respected.

TTL and versioning are deliberately **not** delegated to any candidate. Every
one of them implements expiry against the wall clock, which cannot be driven by
a fake clock and would violate the no-`time.Now` rule. The library provides
capacity-bounded eviction; expiry is computed in `internal/l1` from the injected
clock and enforced on read.

## Harness

`internal/l1/eval`, a nested module so that three losing dependencies never
enter the shipping dependency graph.

```
cd internal/l1/eval
go test -run TestReport -v
go test -run '^$' -bench . -benchmem -count 5
```

Machine A (i7-12700K, Windows 11, go1.26.5). Capacity 100,000; keyspace
1,000,000; 64-byte values; Zipf s=1.1.

## Results

Throughput, median of five, `ns/op` (lower is better):

| Candidate | GetHit | Mixed 90% read | Put | Go floor |
|---|---:|---:|---:|---|
| hashicorp/golang-lru v2 | 206.5 | 238.3 | **325.7** | 1.18 |
| **maypok86/otter v1** | **33.5** | **70.2** | 580.3 | **1.19** |
| maypok86/otter v2 | 36.9 | 70.7 | 371.7 | 1.24 |
| dgraph-io/ristretto v2 | 43.8 | 113.8 | 83.2 (buffered) | 1.24 |

Behaviour:

| Candidate | Zipf hit ratio | Writes visible to next read | Capacity respected |
|---|---:|---|---|
| hashicorp/golang-lru v2 | 87.30% | yes | exact |
| maypok86/otter v1 | 87.34% | yes | exact once settled |
| maypok86/otter v2 | 87.18% | yes | exact |
| dgraph-io/ristretto v2 | 87.00% | **no — 10,000 of 10,000** | exact |

## Reasoning

**Hit ratio does not decide it.** All four land within 0.34 percentage points.
That was the metric expected to matter most, and it turned out to separate
nothing; recording that is the point of measuring rather than assuming.

**Write visibility disqualifies ristretto outright.** Its `Set` is buffered and
applied by a background goroutine, so every one of ten thousand writes was
invisible to the immediately following read. For L1 that is not a performance
characteristic, it is a correctness one: a namespace that writes a value and
reads it back must not observe a miss, because a miss is indistinguishable from
"the owner has newer data" and sends the caller on a pointless round trip — or
causes it to overwrite fresh data with stale. Its fast `Put` is an enqueue, not
a store, and comparing it to a synchronous write is comparing different
operations.

**Reads dominate, so read throughput decides it.** otter serves the hit path
about six times faster than an exact LRU behind a single mutex. Under
parallelism that gap widens, because golang-lru's single mutex serialises reads
that otter handles without one.

**otter v2 requires Go 1.24; CLAUDE.md fixes the floor at 1.23.** v1 is the same
S3-FIFO-derived design from before the module raised its floor, needs Go 1.19,
and is marginally *faster* on the read path. Adopting v2 would mean raising the
floor for every consumer of this library to satisfy an internal implementation
detail. v1 was added to the evaluation specifically because a candidate the
project cannot adopt is not a candidate.

**Decision: maypok86/otter v1.2.4.**

## Costs, honestly

- **Put is the slowest of the four** (580 ns against golang-lru's 326 ns). L1
  writes happen on a miss, which already cost a network round trip, so this is
  off the path that matters. If that stops being true, revisit.
- **Eviction is asynchronous**, so residency can exceed capacity during a write
  burst — 40.9% over in the harness, settling back to exact. For a tier sharing
  a process with the owned store that is a real, bounded transient, and it is
  why the L1 capacity should be set with headroom.
- **v1 is a maintenance branch.** The upgrade to v2 is a small diff, gated only
  on the project's Go floor moving to 1.24.

## Two harness bugs worth recording

Both would have produced a confident, wrong recommendation.

1. **Measuring misses and calling them hits.** The first run showed ristretto at
   0.28 ns/op — faster than a single memory access, which is the tell. Its
   buffered writes had not landed, so `BenchmarkGetHit` was measuring the miss
   path. Fixed by adding `Settle()` to the candidate interface and asserting a
   minimum hit ratio inside the benchmark.
2. **Reusing a closed instance.** Candidates were built once and `defer
   c.Close()` sat inside the benchmark body, so the second and later calibration
   passes ran against a closed cache. Ristretto's `Close` is permanent, so it
   silently scored 0% hits. Fixed by handing out factories instead of instances.

A third issue was configuration rather than harness: ristretto adds its own
per-item storage overhead to the cost passed to `Set` unless
`IgnoreInternalCost` is set, so a `MaxCost` of 100,000 held roughly a thousand
entries. Corrected before the numbers above were taken.

## Reversal

Cheap. `internal/l1` uses four methods of the library (`Get`, `Set`, `Delete`,
`Size`, `Clear`, `Close`) behind its own API, and the eval module makes
re-running the comparison a single command.
