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
| `BenchmarkGetOrLoadHitWithRefreshAhead` | 99.1 | 0 | 0 | the same hit in a namespace with `WithRefreshAhead`, keys an hour from expiry: the window check and the clock read an expiring key needs. BenchmarkGetOrLoadHit, keys without TTL, ran 37–85 (median 65) in the same sitting. Decision 0036, 2026-11-14 |
| `BenchmarkGetOrLoadNegativeHit` | 95.3 | 0 | 0 | a missing key answered from its remembered absence (decision 0034); spread 56–121 ns. Measured 2026-11-14 |
| `BenchmarkGetOrLoadNotFound` | 3,423 | 945 | 18 | the same probe without negative caching, through a loader that does no work; a real one adds a database round trip |
| `BenchmarkBatchGet` | 18,873 | 5464 | 4 | 100 keys, so ~189 ns/key; the allocations are the result map |
| `BenchmarkBatchGetOrLoadWarm` | 9,796 | 5272 | 14 | 50 resident keys, ~196 ns/key: nothing loads. Measured 2026-11-14 (BatchGet ran 24,578 ns / 100 keys in the same sitting) |
| `BenchmarkBatchGetOrLoadCold/batch` | 34,355 | 16930 | 92 | 50 missing keys, one batch loader call that does no work |
| `BenchmarkBatchGetOrLoadCold/perkey` | 156,971 | 51438 | 771 | the same 50 through the per-key loader: 4.6x the cache's own cost, before the 49 extra queries a real loader makes (decision 0035) |
| `BenchmarkNamespaceGetCompressed/plain` | 72.2 | 0 | 0 | 4 KiB value |
| `BenchmarkNamespaceGetCompressed/lz4` | 5023 | 4864 | 1 | same value, 70x — this is why compression is opt-in |

Negative caching added a nil-map check to every store write. Medians of five
runs on Machine A, main against the branch, 2026-11-14 (the kind cluster idle
in the background): `BenchmarkNamespaceSet` 441 → 458 ns, spreads 426–478 and
412–518; `BenchmarkGetOrLoadHit` 82.9 → 84.7 ns; `BenchmarkNamespaceGet` 450 →
437 ns. All within run-to-run noise.

**Running digests (decision 0039, 2026-11-14).** Every write now adds one key
hash and one or two atomic adds to keep the store's digest. Medians of five
runs, Machine A, main against the branch: replica `BenchmarkPutDistinctKeys`
136 → 163 ns; store `BenchmarkSet` 245 → 284 ns, where both sides spread over
~100 ns between runs. Reading the digests costs 64 pairs per store, whatever
it holds.
**Paging a whole store (2026-11-14).** `BenchmarkSnapshotFullPass`, 512-entry
pages, Machine A:

| Keys | `Snapshot` (re-sort per page) | `SnapshotFrom` (sorted once per pass) |
|---:|---:|---:|
| 10,000 | 22 ms, 7.3 MB | 2.1 ms, 1.6 MB |
| 100,000 | 2.3 s, 823 MB | 30 ms, 18.5 MB |
| 400,000 | 57 s, 13.7 GB | 172 ms, 74 MB |
| 1,000,000 | — | 677 ms, 184 MB |

### internal/store

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `BenchmarkGetHot` | 29.1 | 0 | 0 | 1024 keys |
| `BenchmarkGetHotCopied` | 75.0 | 64 | 1 | what copy-on-read would cost; see decision 0007 |
| `BenchmarkGetCold` | 321.2 | 0 | 0 | 1M keys, shuffled |
| `BenchmarkGetWithTTL` | 71.5 | 0 | 0 | the clock read costs ~42 ns, which is why keys without a TTL skip it |
| `BenchmarkGetMiss` | 35.2 | 0 | 0 | |
| `BenchmarkSetWithTTLJitter` | 254.9 | 67 | 1 | a TTL'd Set with jitter 0.1: the key hash and multiply. BenchmarkSet ran 178–239 (median 235) in the same sitting, 2026-11-14; decision 0037 |
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

**Bounded in bytes (decision 0033, 2026-11-14).** The tier's bound became a
total cost, each entry costing its charged bytes. Medians of five runs, Machine
A, main against the branch in one sitting:

| Benchmark | entry-count bound | byte bound | Note |
|---|---:|---:|---|
| `BenchmarkPut` ns/op | 666.9 | 680.8 | +2%: the cost function and otter's cost-tracking node |
| `BenchmarkPut` B/op | 165 | 181 | +16 B per entry, the node's cost field |
| `BenchmarkGetHit` ns/op | 143.7 | 151.6 | within noise: both sides spread 84–157 across runs |
| `BenchmarkGetHitParallel` ns/op | 61.2 | 63.1 | within noise |
| `BenchmarkGetMiss` ns/op | 34.8 | 34.8 | |

Reads are unchanged; a Put, which follows a network round trip, costs about
14 ns more.

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

docs/design.md requires this "measured and recorded" and does not state a budget, so
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

docs/design.md §4 (M3) requires that keys distribute evenly and that removing a node
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
sizes docs/design.md targets this is tens of nanoseconds and irrelevant; at 256 nodes it
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

docs/design.md §4 asks for "remote Get p99 under 2ms and p999 under 10ms at 50k rps in
a 3-node real-network test". Three cache instances in one process over real gRPC
on loopback TCP: real sockets, real serialisation, real deadlines, real
connection pool.

**M4 verdict: the latency criterion is met, the rate criterion is not.**
**M8 re-measurement contradicts the rate half — see below the sweep.**

| | Measured (5 consecutive runs) | Budget | Verdict |
|---|---|---:|---|
| Throughput | 45,168 – 48,035 rps | 50,000 rps | **short by 4–10%** |
| p50 | 756 – 812 µs | — | |
| p90 | 984 – 1,027 µs | — | |
| **p99** | **1,356 – 1,520 µs** | 2 ms | **pass, 5/5** |
| **p99.9** | **1,766 – 1,874 µs** | 10 ms | **pass, 5/5** |

36 workers, 5 seconds, ~230,000 reads per run, 13,287 of 20,000 keys genuinely
remote from the reader, non-owned tier disabled.

**Why the rate falls short, and why that is a measurement limit rather than a
cache limit.** The load generator and all three servers share twenty logical
cores in one process. Every request the client issues takes CPU away from the
three servers answering it. A worker sweep shows the system running out of
machine, not out of headroom:

| Workers | Rate | p99 |
|---:|---:|---:|
| 12 | 36–38k | 654 µs |
| 20 | 41.8k | 891 µs |
| 36 | 45–48k | 1.4–1.5 ms |
| 64 | 56k | 1.7–2.9 ms, unstable |

Throughput flattens between 45k and 56k while p99 degrades by a factor of three.

#### M8 re-measurement: the rate criterion is met, by 4x

This test had never executed on Machine A. It built its gossip nodes through a
third helper that never received open question 27's port fix, so every run failed
at the bind, and it sits behind an opt-in gate that only CI's Linux runner sets —
so the failure was invisible and the table above cannot have come from this
machine in this configuration. Fixed in M8; the sweep below is Machine A.

| Workers | Rate | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 12 | 110,494 | 97 µs | 180 µs | 376 µs |
| 24 | 164,782 | 143 µs | 211 µs | 222 µs |
| 36 | **191,153** | 189 µs | **249 µs** | 292 µs |
| 64 | 235,361 | 270 µs | 334 µs | 373 µs |

At the same 36 workers the table above records, the rate is four times higher and
p99 is six times lower. Throughput does not flatten anywhere in the range, and
the criterion — 50k rps, p99 under 2 ms — is met at every point of it, with the
lowest point offering more than twice the required rate at a ninth of the budget.

Four consecutive runs at 36 workers: 195,015 / 193,199 / 195,324 / 194,865 rps,
p99 248 / 249 / 244 / 245 µs. It is stable.

**What changed is not established**, and this records both sets rather than
replacing one with the other. The worker count is identical, so that is not the
explanation. `BenchmarkInvoke` re-measures at 70–177 µs against the ~175 µs this
document cites, and `BenchmarkInvokeParallel` at 12–15 µs against 28 µs — both
consistent with the whole remote path having become faster, and neither
establishing why. See open question 29.
Reaching 50k with the latency budget intact needs the load offered from
somewhere that is not competing for the same cores — a separate process, or the
three machines "3-node real-network" literally implies. That is out of scope for
a test that has to be reproducible on one developer machine, and it is stated
here rather than worked around.

**Three earlier versions of this measurement were wrong**, each in a way worth
recording because each looked plausible.

*Twelve million rps.* The namespace was asked for the non-owned tier to be
bounded at one nanosecond — after it had already been created with defaults.
Namespace options are fixed at creation and silently ignored afterwards, so
every "remote" read was a local cache hit. No gRPC stack produces that number,
which is the only reason it was caught.

*A 66% swing between identical runs.* 64 workers drove the system into
saturation, so p50 measured queue depth behind the load generator's own
concurrency rather than service time. Two runs of the same binary gave p99
1.737 ms and 2.882 ms. The criterion asks for latency *at* a rate, not latency
under saturation; the worker count now offers load rather than maximising it.

*Cache churn charged to the server.* Bounding the tier at one nanosecond still
stores every value and expires it on the next read. On a box where the client
and the servers share cores, that overhead comes straight out of the thing being
measured. `WithL1TTL` now accepts a negative duration meaning "no non-owned
tier", which is a capability worth having anyway — a namespace whose values must
never lag wants exactly that.

**The percentiles are a lower bound.** Windows' monotonic clock resolves to
~506 µs, so a single sub-millisecond operation times as zero. Latency is
measured in batches of 64 and divided, which smooths isolated outliers. It still
catches sustained stalls, which is what a p99 budget is for.

**The connection pool is warmed first.** The first request to each peer pays a
TCP handshake and an HTTP/2 preface; an unwarmed run put a 1.4 s outlier in the
tail that said nothing about steady state.

**Throughput is reported, not asserted.** The latency budgets are asserted
unconditionally and fail the build. The rate is logged, because failing a
correctness suite on a busy laptop would train people to ignore it — and because
the shortfall above is a fact about this machine that a green tick would hide.

### internal/wire — message codec

Paid twice per remote operation: once to encode the request, once to decode the
response.

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `EncodeGetRequest` | 206 | 224 | 4 | |
| `DecodeGetRequest` | 327 | 336 | 6 | |
| `EncodeGetResponse/64B` | 382 | 608 | 6 | 167 MB/s |
| `EncodeGetResponse/1KiB` | 422 | 1376 | 5 | 2.4 GB/s |
| `EncodeGetResponse/16KiB` | 2038 | 18656 | 5 | 8.0 GB/s |
| `DecodeBatchGetResponse/10` | 6039 | 3725 | 39 | |
| `DecodeBatchGetResponse/100` | 57,840 | 32,856 | 312 | |
| `DecodeBatchGetResponse/1000` | 673,067 | 282,241 | 3015 | |
| `EncodeSubmitRequest` | 106,715 | 262,246 | 15 | 100 groups x 20 messages |
| `StampNewerThan` | 0.35 | 0 | 0 | |

The envelope costs a few hundred nanoseconds and stops mattering above about a
kilobyte, which is the right shape: a remote read should be dominated by the
network and the payload, not by the framing.

`DecodeBatchGetResponse` is ~670 ns per entry and allocates three times per
entry. That is the price of the bounded decoder, which appends as elements
arrive rather than sizing the slice from a declared length — a trusted length
would save the reallocations and reintroduce the remote out-of-memory that
`FuzzDecode` found in two seconds. The trade is deliberate and cheap at any
realistic batch size.

#### Frame validation (M8)

Every decode now walks the frame first and checks that no declared length exceeds
the bytes behind it. Decision 0019 has the defect this closes — five bytes could
turn any message with a `[]byte` field into a 4 GiB allocation, because msgpack
v5's byte-string path bypasses its own allocation limit.

| Benchmark | ns/op | B/op | allocs/op | Notes |
|---|---:|---:|---:|---|
| `Validate/GetRequest` | 25 | 0 | 0 | 1.95 GB/s |
| `Validate/GetResponse` | 51 | 0 | 0 | 256-byte value, 6.7 GB/s |
| `Validate/BatchGetResponse/100` | 4130 | 0 | 0 | 1.89 GB/s |
| `Validate/SubmitRequest/10x10` | 365 | 0 | 0 | 10 groups x 10 messages |

Zero allocations, by construction: the walk carries offsets and nothing else, and
`TestValidateAllocatesNothing` fails if that changes.

Against decoding the same message it is 8% on a `GetRequest` and 7% on a
hundred-entry `BatchGetResponse`. Against the 175 µs remote round trip it
defends, it is three orders of magnitude below the noise. Dispatch checks the
four inline-length msgpack families first, by high bits, because between them
they cover almost every byte this protocol emits; ordering them behind the
twenty-odd equality tests for the rare shapes measured meaningfully worse.

### internal/transport — gRPC round trip

An echo handler, no store and no ownership: the floor under every remote
operation, and the number to subtract when reading the end-to-end figures above.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Invoke/0B` | 175,511 | 10,254 | 156 |
| `Invoke/64B` | 253,254 | 11,060 | 169 |
| `Invoke/1KiB` | 213,687 | 16,077 | 163 |
| `Invoke/16KiB` | 298,451 | 65,016 | 178 |
| `InvokeParallel` | 28,476 | 12,665 | 153 |
| `DialPooled` | 33.2 | 0 | 0 |

**A serial round trip costs ~175-300 µs and a concurrent one ~28 µs.** The gap
is the point: a single caller spends nearly all of that blocked on syscalls, and
loopback gRPC on Windows is syscall-heavy. Under concurrency the same connection
multiplexes and per-operation cost drops by six times, which is why the
end-to-end acceptance run reaches 56k rps with a p50 of 1.2 ms — 64 workers each
seeing roughly 64x the uncontended service time in queueing.

**`DialPooled` is 33 ns and allocation-free**, which matters because the data
path dials per request rather than holding a client. If the pool lookup were not
essentially free, every remote operation would pay for it.

### Router batching

docs/design.md §4 also asks that "a router submitting 10k messages across 3 owners
issues at most 3 RPCs per flush window".

| Messages | Groups | Owners | Remote requests |
|---:|---:|---:|---:|
| 10,000 | 500 | 3 | **2** |

Two rather than three because the sender applies its own share in-process. The
count is taken from an instrumented transport rather than inferred from timing:
"it was fast" is not evidence of batching.

---

## M5 — replication

Measured on Machine A.

### The asynchronous data-loss window

docs/design.md §4 requires this be "measured and documented" rather than an unknown,
and §6 has a placeholder waiting for it.

**What it is.** Between an asynchronous write returning to its caller and the
last replica holding it, the value exists on exactly one node. Kill that node
inside the window and the write is gone despite having reported success.

| Measurement | Value |
|---|---:|
| **Mean window per write** | **≈101 µs** |
| Batch of 2000 writes: acknowledged | 201 ms |
| Batch of 2000 writes: fully replicated | 203 ms |
| Per-write p90 | 506 µs |
| Per-write p99 | 1.46 ms |
| Per-write max | 7.55 ms |

Three nodes over real gRPC on loopback, RF=3, 300 individual samples plus a
2000-write batch.

**Read the batch figure, not the percentiles.** Windows' monotonic clock
resolves to ~498 µs, so a single sub-millisecond window times as zero — the
reported p50 of 0s means "below one tick", not "instant", and the p90 of 506 µs
is one tick rather than a measurement. Timing a whole batch and dividing gives a
mean the clock can actually resolve, which is where 101 µs comes from.

**The 7.55 ms maximum is the honest tail.** It is a scheduling outlier rather
than a queue: the batch shows the replicator drains 2000 writes within 2 ms of
the last acknowledgement, so the queue is not the constraint at this rate.

**Synchronous mode has no window at all**, by construction — the write does not
return until the replicas hold it. `TestSyncModeHasNoWindow` asserts that with
no waiting of any kind: the copies must exist the instant `Set` returns.

### internal/replication — the replica store

On the write path of every replicated namespace: an owner's fan-out becomes one
`Put` per replica, so this cost is paid per key per replica.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Put` (one hot key) | 54.3 | 0 | 0 |
| `PutDistinctKeys` (4096 keys) | 62.7 | 0 | 0 |
| `PutRejected` (stale copy) | 48.8 | 0 | 0 |
| `Get` | 37.6 | 0 | 0 |
| `StampComparison` | 0.12 | 0 | 0 |

**The write path is allocation-free**, which matters because it runs once per
replica per write: at RF=3 a single application write becomes two of these.

**Rejecting a stale copy is cheaper than accepting a fresh one** — 48.8 ns
against 54.3. That is the right way round. Asynchronous replication reorders,
so out-of-order arrivals are routine rather than exceptional, and a system where
the disorderly case cost more would degrade exactly when the network was already
struggling.

Reconciliation is paid per membership change rather than per request, which is
why it is allowed to be four orders of magnitude more expensive:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Drain` 1,000 keys | 22,214 | 21,240 | 11 |
| `Drain` 100,000 keys | 2,677,751 | 2,886,920 | 81 |
| `Evict` 1,000 keys | 29,892 | 0 | 0 |
| `Evict` 100,000 keys | 7,930,219 | 0 | 0 |

**A node holding 100,000 replicas spends ~2.7 ms promoting half of them and
~7.9 ms evicting half.** Both run on the membership watcher, so that is added
latency on the *next* view change rather than on any request — but it bounds how
large a replica set can get before reconciliation becomes visible. At a million
keys it would be tens of milliseconds, which is worth knowing before it is
discovered.

`Evict` allocates nothing because it only deletes; `Drain` builds the map it
hands to promotion, which is the allocation.

### Chaos

docs/design.md §4: "a chaos test that kills a random node every 2 seconds for 60
seconds loses no key that was written with sync replication."

| Configuration | Result |
|---|---|
| 5 nodes, RF=3, sync, kill+restart every 2 s for 60 s | **30 kills, 30 restarts, 600 keys, 0 lost** |
| 4 nodes, RF=2, sync, two kills in succession | 83 kept, 17 lost, **0 corrupted** |

The second row is deliberate. At RF=2 a key lives on two nodes and killing both
loses it; a suite that only demonstrated RF=3 would imply a guarantee
replication does not make. What is asserted at RF=2 is that survivors are
*correct* — losing data there is acceptable, corrupting it is not.

Detection is configured faster than the kill cadence (300 ms against 2 s). A
cluster that kills every two seconds while taking three to notice never
converges, and the test would measure permanent confusion rather than recovery.

---

## M6 — rebalancing

Measured on Machine A.

### internal/rebalance — move planning

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `ComputeRemoval` 1,000 keys | 1,573,722 | 1,393,516 | 10,013 |
| `ComputeRemoval` 100,000 keys | 214,843,400 | 142,598,096 | 1,000,031 |
| `ComputeByClusterSize` 3 nodes, 10k keys | 8,697,977 | 6,880,090 | 100,000 |
| `ComputeByClusterSize` 9 nodes, 10k keys | 18,547,776 | 14,041,829 | 100,018 |
| `ComputeByClusterSize` 32 nodes, 10k keys | 54,324,683 | 38,362,840 | 100,014 |
| `ComputeNoChange` 10k keys | 26,178,133 | 13,923,856 | 100,020 |
| `ExpectedMoveFraction` | 0.38 | 0 | 0 |

**Read these knowing what the planner is.** `rebalance.Compute` is not called by
anything at runtime — the promoter reconciles incrementally, per key, using
`Table.Owner` and `Table.Replicas` directly. Planning is an analysis API: it is
what lets a test assert that a membership change moves only the share the
ranking reassigns, and what `examples/rolling-upgrade` uses to show the figure. So 215 ms for
100,000 keys does not delay a node's reaction to a failure, because no node does
this when a peer dies.

**It is nonetheless expensive** — roughly 10 allocations per key, because
`Replicas` builds and sorts a fresh ranking per call and clones each node. Two
calls per key (before and after) explains the shape. If planning ever moves onto
a runtime path, that is the thing to fix first: a ranking that could be computed
without allocating would take an order of magnitude off it.

**Cost scales with cluster size**, roughly 3x from 3 nodes to 32, which is
rendezvous hashing working as designed: placement is a maximum over all members,
so every member is scored.

**An unchanged view costs nearly as much as a changed one** (26 ms against
18 ms for 10k keys). The planner still ranks every key twice to discover that
nothing moved. That is acceptable for an analysis API and would not be for a
runtime one.

### The rolling-upgrade acceptance

docs/design.md §4: "a simulated rolling upgrade of a 9-node cluster under sustained
write load completes with zero failed client requests and moves no more than the
theoretically required key count ±10%."

| | Measured | Budget |
|---|---|---|
| Pods upgraded | 9 of 9 | 9 |
| Client requests failed | **0** of 360 | 0 |
| Retries needed | 149 | — |
| Keys handed off | 4,732 | — |
| Keys permanently rehomed | **0** of 3,000 | — |
| Transfers vs theoretical | **18,000 vs 18,000** | ±10% |

"Zero failed requests" is measured after the retry the errors explicitly ask
for. During a handover the old owner returns `ErrNotOwner` and the new one
`ErrActivationPending`; both are retryable and a caller that gives up after one
attempt sees a failure. The 149 retries are the honest cost.

The theoretical figure is derived rather than assumed: nine pods, each leaving
and rejoining, at rf/9 of the keyspace per change.

---

## M7 — observability and soak

Measured on Machine A.

### The soak

docs/design.md §4 asks for "24h at target throughput, memory flat".

**This was run for 2 minutes, not 24 hours.** The harness takes any duration —
`PEERCACHE_SOAK=24h go test ./tests -run TestSoak -timeout 25h` — and CI runs it for
seconds as a regression gate. What is recorded here is what was actually
executed, because a 24-hour claim from a 2-minute run would make every other
number in this file suspect.

| | Measured |
|---|---|
| Duration | 2m0s |
| Operations | **20,105,305** |
| Throughput | 167,544 ops/sec |
| Non-retryable failures | **0** |
| Live heap, baseline | 4.1 MiB |
| Live heap, final | 5.6 MiB |
| Live heap samples | 4.1, then 5.6 for all nine subsequent samples |
| Goroutines | **23 → 23** |

Three nodes, RF=2 synchronous, 2,000-key working set, 2s L1 TTL, mixed 25%
writes.

**"Flat" means the live heap, after a forced collection.** The distinction
matters: this system allocates on the write path by design — `Set` copies the
value — so allocation *rate* is high and says nothing about leaks. What a leak
looks like is monotonic growth in `HeapAlloc` across samples with a bounded key
set, and there is none: 5.6 MiB from the second sample to the tenth.

**The goroutine count is the stricter check.** A goroutine leak stays invisible
in heap terms until it is severe, and this project has shipped one — M3's gossip
event loop, found only because it slowed an unrelated latency test. Exactly 23
before and after twenty million operations.

### The soak, an hour (after M8)

Re-run on Machine A after M8 and the question 37 change, for an hour split
between the two soak tests and run one after the other — the first local run
longer than two minutes. **An hour, not twenty-four, and that is now a decision
rather than a gap:** 728 million operations with the heap flat is where a leak
per operation would show, and a leak that grows with wall time instead — a
timer, a ticker, a gossip goroutine — would show in the hour the nightly job
runs. Twenty-four hours would test neither more.

| | `TestSoakMemoryStaysFlat` | `TestSoakGoroutinesStayFlat` |
|---|---|---|
| `PEERCACHE_SOAK` | 50m | 10m |
| Operations | **728,173,249** | — |
| Throughput | 242,724 ops/sec | — |
| Non-retryable failures | **0** | — |
| Live heap, baseline → final | 5.2 MiB → 6.9 MiB | — |
| Live heap samples | 5.2, 6.8 ×4, 6.9 ×5 | — |
| Goroutines | — | **23 → 23** |

Same workload as above: three nodes, RF=2 synchronous, 2,000-key working set.
That is 36 times as many operations as the two-minute run. The heap stays flat in
the sense defined above: after the first sample it moves from 6.8 to 6.9 MiB over
forty-five minutes and does not move again. A leak proportional to operations
would have shown as tens of megabytes at this count. Throughput is higher than in
M7 — 243k against 168k ops/sec — on the same machine. That was not investigated
and is not claimed as an improvement: a 50-minute run averages over
machine-level noise that a 2-minute one does not.

The split exists because `PEERCACHE_SOAK` is per test, and `-run TestSoak` selects
both tests. The nightly job had been sized as if it selected one, and could
never have finished; see `TestNightlySoakFitsItsTimeouts`.

### internal/metrics — instrumentation cost

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Observe` | 15.81 | 0 | 0 |
| `ObserveParallel` | 36.88 | 0 | 0 |
| `ObserveFastPath` | 15.64 | 0 | 0 |
| `ObserveSlowPath` | 15.82 | 0 | 0 |
| `Quantile` | 7.31 | 0 | 0 |

**These numbers decided the design.** 36.9 ns under contention against M1's
515 ns local read is ~7%, and on a platform whose monotonic clock resolves to
~500 µs a sub-microsecond operation times as zero regardless — so instrumenting
the local path would pay 7% to record nothing. `WithLatencyHistograms` is
therefore opt-in and instruments the remote path only, where a 1.7 ms round trip
makes the observation free by comparison.

The fast and slow paths costing the same (15.64 vs 15.82 ns) says the linear
bucket scan is not the cost — the atomics are. That is why a finer bucket set
would be nearly free and a lock-based sketch would not.

---

## M9 — memory budget

Measured on Machine A, `-count 5`, medians. "Before" is `38e3a94`, the last commit
before M9, built in a separate worktree on the same machine in the same session.

### internal/store — what byte accounting costs the write path

| Benchmark | before (ns/op) | after (ns/op) | runs, after |
|---|---:|---:|---|
| `BenchmarkSet` | 126.5 | 124.8 | 124.4, 124.8, 125.3, 127.8, 131.0 |
| `BenchmarkSetParallel` | 21.6 | 20.9 | 20.72, 20.87, 20.89, 21.19, 21.84 |
| `BenchmarkGetHot` | — | 25.6 | unchanged code path; recorded as a reference |

**No measurable cost.** Set now reads the resident entry it was already
replacing, adds one atomic to the shard's byte counter, and compares against the
shard's shed floor — all inside a lock it already held. The difference is inside
run-to-run noise in both directions. `BenchmarkSet` reads 124.8 ns here against
238.2 ns in the M1 table above; that table was recorded on an earlier toolchain
and is not comparable, which is why "before" was re-measured rather than quoted.

The read path is unchanged by design: shedding is sampled rather than LRU so
that Get records nothing.

## M10 — typed namespaces

Machine A, `-count 5`, medians. The value is a four-field struct (two strings, a
string slice, two integers), about 50 bytes in MessagePack.

| Benchmark | ns/op | B/op | allocs/op | What it is |
|---|---:|---:|---:|---|
| `BenchmarkTypedGet` | 486.8 | 376 | 8 | typed `Get` |
| `BenchmarkTypedGetBytePath` | 36.6 | 0 | 0 | `Raw().Get` of the same stored bytes |
| `BenchmarkTypedDecodeOnly` | 553.5 | 376 | 8 | one MessagePack decode, alone |
| `BenchmarkTypedSet` | 841.4 | 381 | 7 | typed `Set` |
| `BenchmarkTypedSetBytePath` | 351.0 | 85 | 2 | `Raw().Set` of pre-encoded bytes |
| `BenchmarkTypedEncodeOnly` | 456.3 | 288 | 5 | one MessagePack encode, alone |

docs/design.md's criterion is that the typed path costs the byte path plus one encode
or decode. For `Get`, 486.8 against 36.6 + 553.5 = 590.1: inside it. For `Set`,
841.4 against 351.0 + 456.3 = 807.3: 4% over, within the run-to-run spread of
either term (the decode-only runs ranged 469–1015 ns). Allocations agree
exactly: the typed `Get` allocates what the decode does and nothing more.

**The codec is the cost, by an order of magnitude.** A typed read is about 13×
the byte read it wraps. That is MessagePack's price for reflection-driven
decoding of a struct, not the view's; a hot path that cannot afford it keeps the
byte API.

## M12 — atomic key operations

Machine A, `go test -run '^$' -bench 'BenchmarkNamespace(Set|Increment|CompareAndSwap)$' -benchmem -count 5 .`,
median of five, a 64-byte value or a small counter, 65,536 keys.

| Benchmark | ns/op | B/op | allocs/op | What it measures |
|---|---:|---:|---:|---|
| `BenchmarkNamespaceSet` | 411.2 | 85 | 2 | the baseline: one local `Set` |
| `BenchmarkNamespaceIncrement` | 491.8 | 38 | 3 | parse, add, format, store — one locked update |
| `BenchmarkNamespaceCompareAndSwap` | 303.7 | 80 | 2 | a successful swap of one key |

Decision 0027's criterion is a local `Increment` within twice a local `Set`. It
is 1.20×. The extra allocation is the formatted integer; the bytes per op are
lower because a counter's value is a few digits rather than 64 bytes.

`CompareAndSwap` being cheaper than `Set` is a single-key effect, not a property
of the path: the benchmark swaps one key back and forth, so its shard and map
slot stay hot, where `Set` walks 65,536 keys.

## M13 — hot-key reads

Machine A, `-benchmem -count 5`, medians. The remote benchmarks run on the
in-process fake network in `tests/`, so a round trip is a few microseconds of
scheduling rather than a network; their point is the request counts, which do
not depend on the transport.

### One hot key, every goroutine on one node, L1 disabled

`go test ./tests/ -run '^$' -bench BenchmarkRemoteGetOneHotKey -benchmem -count 5`.
"Before" is `854a771`, the commit before coalescing, with the same benchmark file
copied into a worktree.

| | ns/op | rpcs/op | B/op | allocs/op |
|---|---:|---:|---:|---:|
| before | 2,495 | **1.000** | 2,904 | 34 |
| after | 1,683 | **0.181** | 958 | 9 |

`rpcs/op` is requests the owner received per read. Before, every read was one;
after, concurrent misses share a fetch and it ranged from 0.077 to 0.196 across
the five runs, depending on how many goroutines overlapped. The acceptance test
pins the deterministic case: a thousand concurrent misses, one request.

### Cold remote reads, five nodes, RF 3

`go test ./tests/ -run '^$' -bench BenchmarkColdRemoteGet -benchmem -count 5`.
Every read is of a distinct key owned elsewhere, so every read is an L1 miss.
Invalidations from the setup writes are drained and the heap collected before
the timer starts; left in flight they were a tenth of the timed loop's CPU.

| Mode | ns/op | owner RPCs/op | replica RPCs/op | B/op | allocs/op |
|---|---:|---:|---:|---:|---:|
| owner only | 10,401 | 1.000 | 0 | 3,540 | 41 |
| `WithReplicaReads` | 12,789 | **0.333** | 0.501 | 3,889 | 40 |

Two thirds of the reads left the owners: half to another replica, a sixth served
by the reader's own replica copy with no request at all. Total requests per read
fell from 1.0 to 0.83.

The first version allocated 58 times per read here, not 40. Profiling put it in
two calls to `Table.Replicas` per read — one on the reader to pick a holder, one
on the replica to check it still ranks — each paying a copy of the membership, a
reflective sort and a clone per member, plus a string concatenation per holder
in the pick. `Table.Holders` replaced both:

| `internal/ownership` | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkReplicas/nodes=7` | 394 | 600 | 5 |
| `BenchmarkHolders/nodes=7` | 137 | 0 | 0 |
| `BenchmarkReplicas/nodes=64` | 13,032 | 3,448 | 5 |
| `BenchmarkHolders/nodes=64` | 524 | 0 | 0 |

The replica-read path is still about 1.2x the owner path in wall-clock time on
this transport. It is not CPU in the read path: profiled, the replica leg costs
about 0.7 µs of CPU per call against the owner leg's 2 µs, and the reader's
selection is 5% of `fetchHolder`. What remains is scheduling in the in-process
network, which a real round trip — hundreds of microseconds — makes noise. It is
recorded rather than explained away; a real-network measurement would settle it.

The write path ranked replicas with `Table.Replicas` on every write too. Moving
it to `Holders` (open question 46), `BenchmarkReplicatedSet` — an asynchronous
RF 3 write on its owner, medians of five:

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| 7 nodes, before | 1,608 | 3,655 | 32 |
| 7 nodes, after | 748 | 2,224 | 19 |
| 32 nodes, before | 3,737 | 7,278 | 60 |
| 32 nodes, after | 898 | 2,378 | 21 |

`Replicas` cloned every member, so the old write cost grew with the cluster;
the new one barely does.

## M14 — opt-in persistence

Machine A, `go test . -run '^$' -bench 'BenchmarkNamespace(Set|Get)$|BenchmarkPersistentSet$' -benchmem -count 5`,
medians. "Before" is `c33d0fd`, the last commit before any M14 code, built in a
worktree in the same session.

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `BenchmarkNamespaceGet`, before | 420.0 | 0 | 0 |
| `BenchmarkNamespaceGet`, after | 439.3 | 0 | 0 |
| `BenchmarkNamespaceSet`, before | 512.1 | 86 | 2 |
| `BenchmarkNamespaceSet`, after | 429.6 | 86 | 2 |
| `BenchmarkPersistentSet` | 851.1 | 906 | 4 |

A namespace without persistence is within noise: the five `Get` runs span
387–436 ns before and 417–463 after, the allocations are identical, and `Set`
came out faster, which is noise in the other direction. The added work on the
path is a nil check.

A persistent write is about twice a plain one. The time is the clock read and
the record; the bytes are mostly the dirty map, which is replaced by a fresh one
at every flush and regrown, amortised over the writes between.

**Then the dirty map was recycled.** Profiled after decision 0030 (705 B and 4
allocations a write by then), a third of the bytes were a heap record per write
and most of the rest the map regrowing at every flush. Records are now held by
value, and a flush clears and keeps the map and its record slice for the next
one — unless its batch is under a quarter of the map's high-water mark, so a
burst's capacity is released instead of held. Same benchmark, medians of five:

| `BenchmarkPersistentSet` | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| M14b | 851 | 906 | 4 |
| after 0030 | 672 | 705 | 4 |
| recycled | 594 | 437 | 3 |

Holding records by value alone made it worse — 849 B — because a map of
96-byte entries copies more each time it grows; the gain is entirely in not
growing it again.

### M14c — the loss window on `persist.Dir`

Machine A, an NVMe SSD under NTFS. `go test . -run '^$' -bench 'BenchmarkPersistLossWindow' -benchtime 200x -count 3`
and `go test ./persist -run '^$' -bench BenchmarkDirPut -benchmem -count 5`.

| | p50 | p99 | max |
|---|---:|---:|---:|
| write acknowledged → durable on disk, worst phase | 98.6 ms | 102–110 ms | 139 ms |

Each iteration writes just after a flush finished, so it waits nearly a whole
interval: this is the bound, not the average. A write at a random moment waits
half an interval on average, plus the write. The I/O term:

| | ns/op | MB/s |
|---|---:|---:|
| `BenchmarkDirPut`, 4 KiB, one at a time | ~2.9 ms | 1.4 |
| `BenchmarkDirPut`, 256 KiB, one at a time | ~3.1 ms | 84 |
| `BenchmarkDirPutParallel`, 4 KiB, 20 goroutines | ~1.1 ms per Put | — |

A durable write is about 3 ms, almost all of it the fsync, and they overlap only
about threefold on this disk. That decided two things. A flush writes one
object per partition it touches, and written one after another, a flush
touching all of 256 partitions took **0.9–1.1 s** (`BenchmarkPersistFlushSpread`,
4,096 keys) — ten times the interval it is meant to fit inside. Written sixteen
at a time it took 40–600 ms; with the partition count cut to 64, about **5 ms**
(16 partitions: 1.5 ms; not chosen, because each restore would read four times
as much). Both changes are in M14c, and open question 48 records what the same
arithmetic costs on an object store.

**Then decision 0030 made a flush one object.** The same benchmarks after it:
`BenchmarkPersistFlushSpread` is one tick plus one durable write — about
100 ms of waiting for the tick and ~3 ms of writing, whatever the partitions —
and the loss window is p50 93 ms, p99 104–107 ms, max 107 ms (50 iterations, three
runs). Objects per flush are counted rather than benchmarked:
`TestAFlushWritesOneObject` makes one request for 1,000 keys over every
partition, where before 0030 the same flush made 64.

## What the test suite itself costs

Measured on Machine A, because the CI timeout is set from it and a timeout
without the measurement behind it is a number somebody will halve.

| | Wall clock |
|---|---:|
| `go test .` (root package, no race) | ~25 s |
| `go test . -race` | ~208 s |
| `go test . -race -count=2` | ~442 s |
| `go test ./... -race -count=2` (the CI gate) | **527 s** |

The last row is a re-measurement and the reason the table now marks it as the
gate. It was 448 s when this section was written, mid-M8. Every test the rest of
that milestone added lands in the root package, and the same command now measures
**527.3 s, exit 0, every package green** — 18% more, for tests that did not exist
when the budget was set.

That is not a regression; it is what a milestone that added twenty-odd tests costs
under the detector. It is recorded because the 900 s timeout was chosen against
the 448 s figure and the margin has quietly gone from 2.0x to 1.7x. A timeout is a
deadlock detector, not a performance budget, so 1.7x is still the right side of
the line — but the direction is one-way, and the next milestone should re-measure
rather than assume.

Three tests are **81%** of a race pass:

| Test | `-race` | no race | multiplier |
|---|---:|---:|---:|
| `TestRollingUpgradeLosesNoRequests` | 67.1 s | 3.8 s | 17.5x |
| `TestSimulation` | 57.0 s | 4.3 s | 13.2x |
| `TestChaosLosesNoSynchronouslyWrittenKey` | 44.2 s | 2.8 s | 15.7x |
| everything else, together | ~40 s | ~12 s | ~3x |

The multiplier is the interesting column. Everything else pays roughly 3x for the
race detector; these three pay 13-17x, because all three run many nodes doing
concurrent work and that is exactly what the detector instruments. The cost is
inherent to what they test rather than an inefficiency in how they test it.

Two of the three are acceptance criteria — M6's rolling upgrade and M5's chaos
run — so trimming them means weakening the Definition of Done rather than
speeding up a test.

**The one safe lever**, if the gate ever needs to fit a smaller budget:
`TestSimulation`'s seed count is environment-driven (`PEERCACHE_SIM_SEEDS`,
default 8) and the nightly workflow already runs 2000. Halving the default saves
roughly 28 s a pass without changing what the per-commit gate proves, because the
seeds that find things are in the nightly sweep either way.

---

## Method notes

- `-count 5` is the minimum for a median worth quoting. Single-run numbers are
  not recorded here.
- Benchmarks are run without `-race`. The race detector changes throughput by
  an order of magnitude and comparing across it is meaningless.
- `BenchmarkClusterPartitionHeal` calls `Close` explicitly inside the loop.
  Leaving teardown to `b.Cleanup` would keep every iteration's cluster alive
  and measure the garbage collector instead.
