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

**The latency criterion is met. The rate criterion is not met on this machine.**

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

SPEC.md §4 also asks that "a router submitting 10k messages across 3 owners
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

SPEC.md §4 requires this be "measured and documented" rather than an unknown,
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

SPEC.md §4: "a chaos test that kills a random node every 2 seconds for 60
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
ranking reassigns, and what `examples/m6` uses to show the figure. So 215 ms for
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

SPEC.md §4: "a simulated rolling upgrade of a 9-node cluster under sustained
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

## Method notes

- `-count 5` is the minimum for a median worth quoting. Single-run numbers are
  not recorded here.
- Benchmarks are run without `-race`. The race detector changes throughput by
  an order of magnitude and comparing across it is meaningless.
- `BenchmarkClusterPartitionHeal` calls `Close` explicitly inside the loop.
  Leaving teardown to `b.Cleanup` would keep every iteration's cluster alive
  and measure the garbage collector instead.
