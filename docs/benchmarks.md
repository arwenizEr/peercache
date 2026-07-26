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

## M2 — group actor runtime

Measured on Machine A. Medians of three.

### Acceptance: memory at 1M addressable / 20k active

SPEC.md requires this "measured and recorded" and does not state a budget, so
these are reported rather than asserted against an invented number.

```
baseline heap:                         0.5 MiB
1000000 addressable, 19997 active:   159.5 MiB (167 B per addressable group)
goroutines: 19999
evictions: 980003
```

Isolating an activation's cost:

```
20000 active groups: 21.8 MiB heap (1144 B each), 20000 goroutines
note: goroutine stacks are not counted in HeapAlloc; at 8 KiB initial stack
      that is roughly 156 MiB more
```

So the split that the design rests on holds by a wide margin:

| | Cost |
|---|---:|
| Addressable group (record: identity, state, timer schedule) | **167 B** |
| Active group (adds mailbox, goroutine, idle timer) | **~1.1 KiB heap + ~8 KiB stack** |

An activation is roughly **fifty times** the heap of a record, and the goroutine
stack dwarfs both. That ratio is why records and activations are separate types:
a million addressable groups cost 160 MiB, while a million *active* ones would
cost around 9 GiB of stack alone. `MaxActiveGroups` is the knob that keeps the
second number hypothetical.

The test asserts one bound rather than a total: an addressable group must stay
under 1 KiB. If it ever approached a goroutine's stack, the separation would
have silently collapsed.

### Throughput

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkCallHot` | 1243 | 232 | 4 | one turn on an activated group: mailbox round trip and nothing else |
| `BenchmarkCallHotParallel` | 815 | 232 | 4 | many callers, one group — bounded by the single turn goroutine *by design* |
| `BenchmarkCallAcrossGroups` | 608 | 232 | 4 | 1024 groups: parallelism comes from having many groups, never from one |
| `BenchmarkActivation` | 11,378 | 33,853 | 16 | the cold path — what an eviction costs the next caller |
| `BenchmarkStateAccess/Get` | 1190 | 232 | 4 | |
| `BenchmarkStateAccess/Set` | 1334 | 242 | 6 | the two extra allocations are the value copy and the event |

Two things worth reading off this table.

**A group call costs about 1.2 µs, against ~70 ns for a namespace read.** That
is the price of serialization: a channel send, a scheduler hop to the turn
goroutine, and a reply. It is the right trade when the alternative is shipping
state to the caller, and the wrong one for a plain key lookup — which is why
namespaces and groups are separate APIs rather than one with a flag.

**Activation costs ~11 µs and 33 KB**, dominated by the goroutine and the
mailbox. That is what a caller pays after its group has been evicted, and it is
the number to weigh when setting `MaxActiveGroups`: too low and the working set
thrashes through this path.

### Fuzzing

The frame parser is the only code here that interprets bytes it did not write.
From M4 those bytes arrive over the network, and "no panic in library paths" is
a claim that needs stronger evidence than a handful of hand-written corrupt
inputs.

| Fuzz target | Executions | Result |
|---|---:|---|
| `FuzzUnframe` | 692,030 in 30s | no crash, no unbounded expansion |
| `FuzzFrameRoundTrip` | 2,350,582 in 45s | no round-trip mismatch |

CI runs each for 60s as a regression gate. Any crasher found is committed to
`testdata/` and becomes a permanent case.

---

## M3 — membership and ownership

Measured on Machine A. Medians of five.

### Acceptance: distribution and movement

SPEC.md §4 (M3) requires that keys distribute evenly and that removing a node
moves only that node's share. Both are asserted in
`internal/ownership/table_test.go` rather than merely measured.

| Property | Setup | Result | Budget |
|---|---|---:|---|
| Uniformity | 100,000 keys, 7 nodes | max deviation **1.44%** | 5% |
| Movement on removal | 7 → 6 nodes, 100,000 keys | **14.27%** moved | 14.29% ideal |
| Keys traded between survivors | same | **0** | 0 |
| Movement on addition | 7 → 8 nodes | only keys taken *from* others | no reshuffle |

The zero is the number that matters. A modulo hash would move roughly 6/7 of the
keyspace for the same change; rendezvous hashing moves the departed node's share
and nothing else, which is what makes a rolling deploy survivable.

The three-node example reproduces it end to end: 1020 of 3000 keys changed owner
when `pod-2` left, which is exactly the set `pod-2` held, and 0 keys moved
between `pod-0` and `pod-1`.

### Acceptance: a partitioned minority stops activating

`TestPartitionedMinorityStopsActivating` splits a five-node cluster 3/2 and
probes 500 keys from the minority side. Inside the activation window the
minority deferred **160 of 500**; after the window closed it resumed and served
**256 of 500**. Both numbers are the point: it stops, and it starts again.

### Ownership lookup

`Owner` runs on every activation and, from M4, on every routed call. It is
allocation-free at every size.

```
BenchmarkOwner/nodes=3          16.1 ns/op      0 B/op   0 allocs/op
BenchmarkOwner/nodes=7          48.9 ns/op      0 B/op   0 allocs/op
BenchmarkOwner/nodes=16         85.2 ns/op      0 B/op   0 allocs/op
BenchmarkOwner/nodes=64        246.2 ns/op      0 B/op   0 allocs/op
BenchmarkOwner/nodes=256       767.2 ns/op      0 B/op   0 allocs/op
BenchmarkOwnerParallel          12.5 ns/op      0 B/op   0 allocs/op
BenchmarkOwnerDuringUpdate      75.6 ns/op      0 B/op   0 allocs/op
```

Three things to read off this.

**It is linear in members, at about 3 ns each.** That is inherent: the owner is
the maximum score over the whole view, so every member is scored. At the cluster
sizes SPEC.md targets this is tens of nanoseconds and irrelevant; at 256 nodes it
is 768 ns and would start to matter. Recorded as open question 23 rather than
optimised speculatively.

**The parallel number is lower than the serial one**, 12.5 ns against 48.9 ns at
7 nodes, because `RunParallel` spreads the work across 20 logical cores. What
matters is that it does not *degrade* under contention: the read is an atomic
pointer load against an immutable snapshot, so there is no shared cache line
being written and nothing to serialise on. A `RWMutex` here would show the
opposite shape.

**Lookups during continuous membership churn cost 75.6 ns**, against 48.9 ns
quiescent — a 55% penalty from cache pressure as the writer publishes new
snapshots, not from blocking. Readers never wait for the writer. The `51-58 B/op`
the benchmark reports is the writer goroutine's allocation being attributed to
the reader's iterations; the reader itself allocates nothing, which is why the
allocation *count* stays at zero.

### Membership change and replica ranking

Paid once per gossip event and once per M5 replica resolution respectively, not
per request.

```
BenchmarkUpdate/nodes=7        754 ns/op       504 B/op    5 allocs/op
BenchmarkUpdate/nodes=256    21,247 ns/op    13,720 B/op    5 allocs/op
BenchmarkReplicas/nodes=7      840 ns/op       600 B/op    5 allocs/op
BenchmarkReplicas/nodes=64   9,333 ns/op     3,448 B/op    5 allocs/op
```

`Replicas` sorts the entire view per call — 9.3 µs at 64 nodes, seventeen times
what `Owner` costs at the same size. That is the price of a full ranking rather
than a maximum, and it is the reason M3's hot path takes the head directly
instead of calling `Replicas(key, 1)`. M5 will need a partial selection rather
than a full sort if replica resolution ends up on a request path.

`Update` allocates a fresh snapshot every time by design; it is what lets readers
run without a lock. Twenty microseconds per membership change at 256 nodes is
not a number worth optimising.

### Activation guard

```
BenchmarkCanActivate           445 ns/op       136 B/op    3 allocs/op
```

Six keys in seven are not this node's in a 7-node cluster, so this benchmark is
dominated by the `ErrNotOwner` path, and the allocation is the formatted error
message. The guard is consulted once per activation rather than once per
operation, so this is not on a hot path. Open question 24 records what would
change that.

Re-measured after the stale-epoch check was added, and unchanged within noise
(460 ns before, 445 ns after). That is not evidence the check is free: it sits
*after* the ownership test, so six iterations in seven return before reaching
it. Two integer comparisons on the remaining seventh are below what this
benchmark can resolve, which is the honest statement — a targeted benchmark of
the owned path would be needed to say more, and there is nothing yet worth
saying.

---

## M4 — transport and remote routing

Measured on Machine A.

### Acceptance: remote Get at load

SPEC.md §4 asks for "remote Get p99 under 2ms and p999 under 10ms at 50k rps in
a 3-node real-network test". Three cache instances in one process over real gRPC
on loopback TCP: real sockets, real serialisation, real deadlines, real
connection pool.

| | Measured | Budget |
|---|---:|---:|
| Throughput | **56,009 rps** | 50,000 rps |
| p50 | 1.223 ms | — |
| p90 | 1.459 ms | — |
| **p99** | **1.737 ms** | 2 ms |
| **p99.9** | **2.047 ms** | 10 ms |
| max | 2.165 ms | — |

64 workers, 5 seconds, 283,392 reads, 13,287 of 20,000 keys genuinely remote
from the reader.

Four things about this number are worth stating, because each one was a way to
get it wrong.

**The non-owned tier is disabled for the measurement.** With the default 5s L1
bound, a remote Get is a local map read after the first one — the first version
of this test reported **12 million rps**, which is not a number any gRPC stack
produces. The namespace is created with a one-nanosecond L1 bound *before*
anything is written to it, because namespace options are fixed at creation and
configuring one afterwards is silently ignored.

**The percentiles are a lower bound.** Windows' monotonic clock resolves to
~506 µs, so a single sub-millisecond operation times as zero. Latency is
measured in batches of 64 and divided, which smooths isolated outliers. It still
catches sustained stalls, which is what a p99 budget is for.

**The connection pool is warmed first.** The first request to each peer pays a
TCP handshake and an HTTP/2 preface; an unwarmed run put a 1.4 s outlier in the
tail that said nothing about steady state.

**Throughput is reported, not asserted.** 50k rps is as much a property of the
machine offering the load as of the cache, and failing a correctness suite
because a laptop was busy would make the number a nuisance rather than a signal.
The latency budgets are asserted; the rate is recorded here.

### Router batching

SPEC.md §4 also asks that "a router submitting 10k messages across 3 owners
issues at most 3 RPCs per flush window".

| Messages | Groups | Owners | Remote requests |
|---:|---:|---:|---:|
| 10,000 | 500 | 3 | **2** |

Two rather than three because the sender applies its own share in-process. The
count is taken from an instrumented transport rather than inferred from timing:
"it was fast" is not evidence of batching.

---

## Method notes

- `-count 5` is the minimum for a median worth quoting. Single-run numbers are
  not recorded here.
- Benchmarks are run without `-race`. The race detector changes throughput by
  an order of magnitude and comparing across it is meaningless.
- `BenchmarkClusterPartitionHeal` calls `Close` explicitly inside the loop.
  Leaving teardown to `b.Cleanup` would keep every iteration's cluster alive
  and measure the garbage collector instead.
