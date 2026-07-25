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

## Method notes

- `-count 5` is the minimum for a median worth quoting. Single-run numbers are
  not recorded here.
- Benchmarks are run without `-race`. The race detector changes throughput by
  an order of magnitude and comparing across it is meaningless.
- `BenchmarkClusterPartitionHeal` calls `Close` explicitly inside the loop.
  Leaving teardown to `b.Cleanup` would keep every iteration's cluster alive
  and measure the garbage collector instead.
