# Benchmarks

Numbers without a machine are noise, so every table below records the machine
it was measured on. Benchmarks are re-run in CI for regression; the tracked
numbers here come from a known machine because a shared CI runner is too noisy
to compare against.

Reproduce with:

```
make bench                     # every benchmark, 5 runs
go test ./internal/clock -run '^$' -bench . -benchmem -count 5
```

---

## Machine A

| | |
|---|---|
| CPU | 12th Gen Intel Core i7-12700K (12 physical / 20 logical cores) |
| RAM | 31.8 GB |
| OS | Windows 11 Pro 10.0.26200 |
| Go | go1.26.5 windows/amd64, GOAMD64=v1 |
| Flags | `-benchtime 1s -count 5`, no `-race` |

---

## M0 — clock and harness

Measured on Machine A. Each figure is the median of five runs.

### internal/clock

| Benchmark | ns/op | B/op | allocs/op | What it tells you |
|---|---:|---:|---:|---|
| `BenchmarkFakeAdvanceIdleHour` | 31 | 0 | 0 | Skipping an hour with nothing armed is constant time. This is the number the whole harness rests on. |
| `BenchmarkFakeAdvanceWithArmedTimers` | 43 | 0 | 0 | An advance that fires nothing but inspects a 1000-entry heap. |
| `BenchmarkFakeTimerChurn` | 116 | 80 | 1 | Arm then cancel — the shape of a per-request deadline. |
| `BenchmarkFakeFireTimers` | 252,329 | 0 | 0 | Firing 1000 timers in one advance: ~252 ns per timer fired. |
| `BenchmarkRealNow` | 6.1 | 0 | 0 | Baseline: the real clock. |
| `BenchmarkFakeNow` | 17.8 | 0 | 0 | The fake takes a mutex on every read; ~12 ns is the price of injectability. |

Range across the five runs: `FakeAdvanceIdleHour` 22.9–38.4 ns, `FakeNow`
17.6–18.2 ns, `FakeTimerChurn` 109–132 ns. The idle-hour spread is measurement
noise on a desktop, not a property of the duration — advancing an hour and
advancing a millisecond cost the same, which is the point.

### internal/testkit

| Benchmark | ns/op | B/op | allocs/op | What it tells you |
|---|---:|---:|---:|---|
| `BenchmarkClusterPartitionHeal` | 264,542 | 278,802 | 585 | The full M0 acceptance scenario: build 5 nodes, split 3/2, skip an hour, heal, skip another hour, tear down with leak checks. |
| `BenchmarkClusterStartup` | 332,582 | 272,275 | 469 | Fixed overhead every test pays. Most of `PartitionHeal` is this. |
| `BenchmarkNetworkRoundTrip` | 1,327 | 336 | 6 | One in-process request/response with no faults. Bounds how much of a later milestone's benchmark is harness overhead. |
| `BenchmarkMembershipRefresh` | 2,514 | 264 | 4 | Re-evaluating 8 peers, which happens on every reachability change. |

Range across the five runs: `ClusterPartitionHeal` 181–324 µs,
`ClusterStartup` 309–390 µs, `NetworkRoundTrip` 1.21–1.43 µs. Cluster
construction dominates and is where the allocations are; the partition, the two
hour-long advances and the heal together cost roughly 30 µs on top of startup.

### Acceptance measurement

The M0 acceptance criterion caps the scenario at 50 ms of wall clock.
`TestM0Acceptance` measures itself through `clock.Real` and logs the result:

```
simulated 2h across 5 nodes in 1.4957ms of wall clock
```

Observed 0–1.5 ms under `-race`, against a 50 ms budget — roughly 30x headroom.
Note that the criterion asks for one hour; the test advances two, because it
skips an hour on each side of the heal.

---

## M1 — store, L1, codec, namespace

Measured on Machine A. Each figure is the median of three to five runs.

### Acceptance: local Get p99 at 1M keys

The criterion is a p99 under 1 µs at a million keys.

```
platform monotonic clock resolution: 502.3us (per-operation timing: false)
resident heap with 1000000 keys of 64 bytes: 227.4 MiB
p50 354ns  p90 417ns  p99 478ns  p99.9 618ns  max 618ns
```

**Pass**, with roughly 2x headroom.

Two caveats, both in the test's own documentation:

- **Windows cannot time a single operation.** Go's monotonic clock there is
  driven by the system interrupt timer and resolves to ~500 µs, so per-operation
  deltas read as zero. The test measures the resolution, reports it, and falls
  back to batches of 30,000 reads — each batch about twenty times the clock
  resolution, so quantisation is a few nanoseconds per operation. A batch mean
  smooths isolated outliers, so this p99 is a *lower bound* on the true
  per-operation p99; it still catches sustained stalls. On a platform with a
  fine clock (Linux CI) the test detects that and times each operation directly.
- **The garbage collector turned out not to matter**, because the read path
  allocates nothing — asserted directly by `TestGetIsAllocationFree` rather than
  inferred. An earlier version measured with the collector disabled and gated on
  that; it was measuring thermal drift, since whichever run went second on a
  loaded desktop CPU was consistently slower.

The criterion is not measured under `-race`, which costs roughly 4x and would
say nothing about the cache.

### Namespace (root package)

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkNamespaceGetSmall` | 71.8 | 0 | 0 | 1024 keys: the code path, with the working set in cache |
| `BenchmarkNamespaceGet` | 365.6 | 0 | 0 | 1M keys, shuffled: the memory hierarchy dominates |
| `BenchmarkNamespaceGetParallel` | 108.1 | 0 | 0 | 1M keys across 20 threads |
| `BenchmarkNamespaceMiss` | 44.5 | 0 | 0 | was 492 ns and 4 allocs before the miss path stopped wrapping the sentinel |
| `BenchmarkNamespaceSet` | 238.2 | 67 | 1 | the allocation is the value copy |
| `BenchmarkNamespaceSetParallel` | 99.9 | 65 | 1 | |
| `BenchmarkGetOrLoadHit` | 72.7 | 0 | 0 | resident path through singleflight |
| `BenchmarkBatchGet` | 18,873 | 5464 | 4 | 100 keys, so ~189 ns/key; the allocations are the result map |
| `BenchmarkNamespaceGetCompressed/plain` | 72.2 | 0 | 0 | 4 KiB value |
| `BenchmarkNamespaceGetCompressed/lz4` | 5023 | 4864 | 1 | same value, 70x — this is why compression is opt-in |

### internal/store

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkGetHot` | 29.1 | 0 | 0 | 1024 keys |
| `BenchmarkGetHotCopied` | 75.0 | 64 | 1 | what copy-on-read would cost; see decision 0007 |
| `BenchmarkGetCold` | 321.2 | 0 | 0 | 1M keys, shuffled |
| `BenchmarkGetWithTTL` | 71.5 | 0 | 0 | the clock read costs ~42 ns, which is why keys without a TTL skip it |
| `BenchmarkGetMiss` | 35.2 | 0 | 0 | |
| `BenchmarkGetParallel` | 88.3 | 0 | 0 | 1M keys across 20 threads |
| `BenchmarkSet` | 238.2 | 67 | 1 | |
| `BenchmarkExists` | 53.2 | 0 | 0 | |
| `BenchmarkSweepClean` | 49,422 | 0 | 0 | 8 shards of a 100k-key store with nothing to reclaim |

**Shard count, under parallel writes.** This is the measurement that justifies
the default of 256:

| Shards | ns/op | vs. 1 shard |
|---:|---:|---:|
| 1 | 339.6 | 1.0x |
| 8 | 145.1 | 2.3x |
| 64 | 99.3 | 3.4x |
| **256** | **88.4** | **3.8x** |
| 1024 | 89.2 | 3.8x |

256 is the knee. 1024 buys nothing and costs memory.

### internal/l1

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkGetHit` | 124.5 | 0 | 0 | includes the TTL check this package owns |
| `BenchmarkGetHitParallel` | 62.3 | 0 | 0 | otter's read path scales |
| `BenchmarkGetMiss` | 37.3 | 0 | 0 | |
| `BenchmarkPut` | 665.5 | 149 | 2 | includes the version check and the stripe lock |
| `BenchmarkInvalidateOlderThan` | 199.0 | 0 | 0 | |

Candidate library comparison: `docs/decisions/0005-l1-cache-library.md`.

### internal/singleflight

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkUncontended` | 1051 | 256 | 4 |
| `BenchmarkDistinctKeys` | 898 | 254 | 3 |
| `BenchmarkOneHotKey` | 540 | 86 | 1 |

Collapsing is *cheaper* than not collapsing, because 999 of 1000 callers
allocate nothing. The uncontended cost is a goroutine and a channel, paid so
that a leader never blocks past its own deadline; see decision 0006.

---

## Method notes

- `-count 5` is the minimum for a median worth quoting. Single-run numbers are
  not recorded here.
- Benchmarks are run without `-race`. The race detector changes throughput by
  an order of magnitude and comparing across it is meaningless.
- `BenchmarkClusterPartitionHeal` calls `Close` explicitly inside the loop.
  Leaving teardown to `b.Cleanup` would keep every iteration's cluster alive
  and measure the garbage collector instead.
