# Open questions

Things guessed at, in the order they will start to hurt. Each has a
provisional answer that is cheap to reverse; none of them are silent.

Format: what is unresolved, what was picked for now, and what would force a
change.

---

## 1. The shape of `PeerClient` and `Handler` — M0, confirmed by M4

SPEC.md section 3.6 names `Transport.Dial(node) (PeerClient, error)` and
`Serve(l net.Listener, h Handler)` but never defines either type.

**Picked:** a byte-oriented request/response pair.

```go
type PeerClient interface {
    Invoke(ctx context.Context, method string, req []byte) ([]byte, error)
    Close() error
}

type Handler interface {
    Handle(ctx context.Context, from Node, method string, req []byte) ([]byte, error)
}
```

**Why:** gRPC implements it, an in-process fake implements it in a few hundred
lines, and adding an RPC in M4 or M5 does not widen the interface.

**What would force a change:** streaming. M6's resumable partition transfer
wants a stream, not a unary call, and M5's replication fan-out might too. The
likely resolution is an additional `InvokeStream` method rather than reshaping
`Invoke`, but that is not decided.

**Confirmed in M4.** The gRPC transport implements this shape unchanged, and so
does the in-process fake, which is the evidence the interface was worth having:
the same seven RPCs run over sockets and over a fault-injecting fabric with no
difference above the transport. Adding `Load` and `Submit` during M4 widened the
protocol without touching this interface at all, which was the property it was
chosen for.

---

## 2. Whether `Serve` should take a `net.Listener` — M0, deferred

The signature is fixed by SPEC.md section 3.6 and it suits gRPC. It suits an
in-process transport badly: there is nothing to listen on.

**Picked:** the fake transport accepts a nil listener and ignores it.

**What would force a change:** nothing urgent. If a second non-socket transport
appears, replacing the parameter with an opaque `ListenSpec` would be tidier.

---

## 3. The contents of `Stats` — M0, provisional

SPEC.md section 3.1 returns `Stats` and never says what is in it.

**Picked:** a plain struct with self, member count, epoch, key and group counts,
and hit/miss counters. A struct, not an interface, so adding a field is not a
breaking change.

**What would force a change:** M7, which adds the correctness counters
(split-brain detections, stale-epoch rejections, version conflicts, mailbox
rejections, transfer failures). Those are additive.

---

## 4. Module path — M0, placeholder

**Picked:** `github.com/peercache/peercache`.

**What would force a change:** the repository actually being published
somewhere. This is a single `go mod edit -module` plus a find-and-replace, and
it should happen before the first tag, not after.

---

## 5. `Watch()` semantics under a slow consumer — M0, provisional

SPEC.md section 3.5 gives `Watch() <-chan MembershipChange` with no buffering
or fan-out rule.

**Picked:** one channel per call, buffered at 64, non-blocking send. A watcher
that stops reading loses changes rather than stalling membership.

**Why:** the alternative is letting a slow consumer block the failure detector,
which converts a slow watcher into a cluster-wide outage.

**What would force a change:** a consumer that genuinely cannot miss a change.
That consumer should be reading `Members()` after each wake-up rather than
treating the channel as a log, and if one appears the fix is to document that,
not to make the channel lossless.

---

## 6. Real-clock tests wait on real timers — M0, accepted

CLAUDE.md forbids `time.Sleep` in tests. `internal/clock/real_test.go` blocks on
a real 1 ms timer, which is a wait by another name.

**Picked:** allow it, only in the tests for the real clock itself, only on
timers of 1 ms, and nowhere else.

**Why:** there is no way to test that the real clock's timers fire without
letting one fire. The rule's purpose — that no test waits on a timeout that
belongs to the code under test — is intact everywhere else, and
`TestNoSleepAnywhere` enforces the literal rule across the whole tree.

---

## 7. Detection latency defaults — M0, arbitrary

`DefaultFailureTimeout` is 3 s and `DefaultJoinDelay` is 500 ms in the harness.
They are distinct so that a test asserting on one cannot pass by accidentally
satisfying the other.

**What would force a change:** M3, where the real gossip layer has its own
timings and the harness should probably default to matching them.

---

## 8. `activationDelay` interacts with join delay — M3, resolved

SPEC.md section 4 (M3) requires a node to wait `activationDelay` after a
membership change before activating groups it newly owns. The harness's
`JoinDelay` is a different thing — admission latency, not activation latency —
and the two will need to be reasoned about together.

Recording it now because getting them confused is the sort of mistake that
produces a test proving the wrong property.

**Resolved in M3.** They are independent and never composed. `JoinDelay` belongs
to `internal/testkit` and controls when a simulated node is admitted to a
simulated view; `ActivationDelay` belongs to `ownership.Guard` and is measured
from the moment the local view changed, whatever caused it. The guard never
reads the harness, and the harness never activates anything.

The confusion this note anticipated did nearly happen, in a different form: the
first end-to-end cluster test set `ActivationDelay` to 100ms and observed no
deferral at all, because waiting for both nodes to *report* convergence took
longer than the window. The window has to outlast detection to be observable at
all, which is the real relationship between the two delays and the reason
`DefaultActivationDelay` is seconds rather than milliseconds.

---

## 9. `BlockUntil` counts every timer, not the interesting ones — M0, known sharp edge

`Fake.BlockUntil(n)` parks until *n* timers are armed on the clock. It cannot
distinguish yours from anyone else's. In a test where a component already holds
a timer, `BlockUntil(1)` returns immediately and synchronises with nothing —
the test then passes for the wrong reason, which is worse than failing.

**Picked:** document it, and keep the harness quiescent in the steady state so
the common case is safe. `testkit.Membership` arms timers only during
transitions, so a converged cluster has zero pending and `BlockUntil(1)` means
what it looks like it means.

**What would force a change:** M2's idle-deactivation sweep, which will hold a
timer per active group. At that point `BlockUntil` needs either a baseline
(`Pending()` before, then block on baseline+n) or a tagged variant.

---

## 10. Network change watchers accumulate — M0, bounded

`Network.OnChange` registrations cannot be removed, so each
`Cluster.Revive` adds a watcher that outlives the membership it belongs to. A
closed membership's `Refresh` returns immediately, so the cost is a slice entry
and a function call per revival.

**Picked:** leave it. Bounded by the length of one test.

**What would force a change:** M5's chaos test, which kills and revives a random
node every two seconds for sixty seconds. Thirty stale watchers is still
nothing, so this is being recorded rather than fixed — but if a soak test ever
revives thousands of times, `OnChange` needs to return a deregistration
function.

---

## 11. `goleak` and parallel tests — M0, constraint not yet violated

`Cluster.Close` runs `goleak.VerifyNone` with `IgnoreCurrent` captured at
construction. That correctly ignores goroutines that existed beforehand, but it
cannot distinguish another test's goroutines started afterwards.

**Picked:** no test in the repository calls `t.Parallel`, so this is currently
sound.

**What would force a change:** the first parallel test. The fix is to move the
leak check to `TestMain` for parallel packages, and it should happen before, not
after, someone adds `t.Parallel` and starts seeing phantom leaks.

---

## 12. There is no typed namespace API — M1, needs a decision

SPEC.md section 2 describes the namespace as "typed Get/Set/GetOrLoad". Section
3.1 defines it entirely in terms of `[]byte`, and no typed API appears anywhere
in section 3.

**Picked:** the `[]byte` API exactly as section 3.1 specifies. The codec
registry is built and tested as a standalone component, and `WithCodec`
validates its argument at construction so an unregistered name fails at startup.

**The consequence:** `WithCodec` currently has nothing to encode. The registry
is used by nothing on the data path.

**What would force a change:** a decision to add `Namespace[T]`, which would be
a genuine API addition rather than an implementation detail, or M4, where the
codec is needed for RPC payloads regardless.

---

## 13. The L1 tier is not reachable from a namespace — M1, resolved in M4

`internal/l1` is complete and tested, and nothing calls it. On a single node
every key is owned locally, so there is no non-owned copy to cache.

**Picked:** build and test it standalone, as SPEC.md section 4 (M1) requires,
and wire it in at M4 when remote reads exist.

Recorded so that it is not mistaken for dead code at review.

**Resolved in M4.** Namespaces build an L1 cache whenever a membership authority
is present, and every non-owned read populates it. The wiring also changed the
tier's comparison: it stamps entries with `(epoch, version)` rather than a bare
version, because question 18 below turned out to matter exactly here — a key
that moves to a new owner is usually reconstructed there at version 1, and a
version-only comparison would have rejected that fresh value as stale until the
TTL expired.

---

## 14. Sub-microsecond latency cannot be measured on Windows — M1, worked around

Go's monotonic clock on Windows is driven by the system interrupt timer and
resolves to roughly 500 microseconds, so `time.Since` across a single 300 ns
operation returns zero.

**Picked:** the acceptance test measures the platform's resolution, reports it,
and switches method — direct per-operation timing where the clock is fine
enough (Linux CI), batches of 30,000 reads where it is not. The batched p99 is a
lower bound on the true per-operation p99 and is labelled as such.

**What would force a change:** nothing urgent. The Linux CI job measures the
real per-operation distribution; the Windows figure is a development-time
approximation.

---

## 15. otter v1 is a maintenance branch — M1, accepted

`internal/l1` uses `maypok86/otter` v1 rather than v2 because v2 requires Go
1.24 and CLAUDE.md fixes the floor at 1.23. v1 measured marginally faster on the
read path, so nothing was given up on performance.

**What would force a change:** the project's Go floor moving to 1.24, at which
point v2 is a small diff and worth taking for the maintained branch.

---

## 16. L1 eviction overshoots capacity transiently — M1, bounded

otter evicts asynchronously, so a write burst can leave residency above the
configured capacity — measured at 40.9% over before settling back to exact.

**Picked:** documented, and the L1 capacity should be configured with headroom.
The tier shares a process with the owned store, so this is real memory.

**What would force a change:** a memory-constrained deployment where the
transient matters, which would need either a hard admission gate or a different
library.

---

## 17. Configured options that do nothing yet — M1, tracked

Three namespace options are accepted and validated at construction, and are not
consulted by any data path:

| Option | Inert until | Why it cannot act sooner |
|---|---|---|
| `WithPartitioner` | M3 | Grouping decides which node owns a key. One node, no ownership, nothing to decide. |
| `WithReplicationFactor` | M5 | There is nowhere to put a replica. |
| `WithCodec` | M4 | SPEC.md section 3.1 defines a `[]byte` API; there is nothing to encode. See question 12. |

**Updated in M4.** `WithPartitioner` now acts: `namespace.groupKeyFor` consults
it on every routed operation, so keys grouped under one group key share an owner
and travel together in a batch. `WithReplicationFactor` remains inert until M5.
`WithCodec` still only validates — the protocol's own encoding is fixed
(`internal/wire`), and choosing a codec for application *values* is question 12,
which is still open.

**Picked:** accept and validate them now. An unregistered codec or an
out-of-range replication factor fails at startup, which is the point of naming
them in configuration, and changing a partitioner later moves keys between
owners — a decision better made before there is data.

**The risk being managed:** an option that silently does nothing is a lie by
omission. `TestPartitionerHasNoEffectYet` pins the current behaviour so that
wiring it up in M3 is a deliberate change to a failing assertion rather than a
silent one, and the examples say plainly that these are inert.

---

## 18. A key's version is not monotonic across its lifetime — M1, an M4 constraint

Versions live with the entry, so reclaiming an entry restarts the sequence at 1.
An expired key that no sweep has reached yet is still resident, and overwriting
it continues from the old version; the same key, once swept, starts again.

**Whether a version restarts therefore depends on sweeper timing.** Two nodes
running identical workloads can hold different versions for the same key, and a
key's version can decrease.

Found by `TestStoreMatchesReferenceModel`, which disagreed with its reference
model for exactly this reason. Nobody reasoned their way to it; the model did.
Pinned by `TestVersionRestartsOnlyIfTheEntryWasReclaimed`.

**Why it is survivable:** the L1 TTL is a hard bound on staleness precisely
because version comparison cannot be trusted as a total order. `internal/l1`
already treats an expired entry as replaceable by *any* version, which is what
stops a restarted sequence from wedging a key until its TTL runs out.

**What M4 must not do:** treat a version as a cluster-wide monotonic counter, or
use "version is lower" alone as proof that an incoming value is stale. If M4
needs a real ordering it has to carry the owner's epoch alongside the version.

---

## 20. One optimisation has no deterministic test — M1, known and unresolved

`Namespace.GetOrLoad` re-checks the store *after* winning the singleflight, in
case a previous load populated the key between the outer lookup and the flight.

**It has no test that would fail if it were deleted.** That is not for lack of
trying: the window is the gap between two adjacent statements, so hitting it
requires pausing a goroutine between them, and there is no hook to do that. A
test was written that appeared to cover it and was found to be vacuous by
mutation — it passed with the re-check removed, because its callers hit the
outer lookup and never reached the flight.

**Picked:** keep the optimisation, delete the false test, record this.

The optimisation is cheap and obviously correct by inspection — a store lookup
that returns the value already there. What it is not is *verified*, and a
comment claiming a test covers it would have been worse than this paragraph.

**What would resolve it:** a test hook that blocks a caller between the store
lookup and `flight.Do`. That is real machinery for a marginal optimisation, so
it is not worth building today. If M4 ever makes redundant loads expensive
enough to matter — a load then costs a network round trip, not a local map
read — build the hook and test it properly, or remove the optimisation.

**The general lesson**, which cost the most to learn here: a passing test is not
evidence. Every test written to cover a specific fix should be run once against
the un-fixed code. Three fixes in this milestone were verified that way and
held; this one was not, and did not.

---

## 21. A node that loses a group keeps serving it — M3, an M6 constraint

The guard runs on activation, not on every turn. Once a group is active, the
runtime does not re-check ownership, so a node that loses a group to a
membership change carries on serving the copy it already has until the group
goes idle and deactivates.

**Picked:** leave it. The alternative available in M3 — checking ownership per
turn — would put a table lookup on the hot path to fix half a problem, because
stopping the losing node does not by itself get the state to the winning node.

**What resolves it:** M6's graceful handoff, which stops the group on the old
owner *and* moves its state. Until then the honest description is that ownership
changes are eventually consistent for already-active groups, and the activation
guard narrows the window in which a *new* activation is the second one, not the
window in which an *old* one is still running.

This is the largest gap between what M3 does and what a reader might assume it
does, which is why it is stated in `internal/ownership/doc.go` as well.

---

## 22. `DeferredActivations` counts events, not deferred groups — M3, wrong shape

`Stats().DeferredActivations` and `RefusedActivations` are monotonic counters
incremented once per refused call. A caller that retries in a tight loop
inflates them without bound: the M3 example, polling during a one-second window,
drove the deferral count past 1.7 million across three nodes. The number
describes the client's retry policy, not the cluster's health.

**Picked:** keep the counters, and do not publish them in the example. They are
still useful as a "did this ever happen" signal and as a rate once M7 exports
them properly.

**What would resolve it:** a gauge — the number of groups currently deferred —
alongside the counter, which is what an operator actually wants to alert on. It
needs the guard to track deferred keys rather than just count decisions, which
is a small map and a real cost on the activation path. Deferred to M7, where the
metrics surface is designed rather than accumulated.

---

## 23. Ownership lookup is linear in cluster size — M3, bounded and measured

Rendezvous hashing scores every member on every lookup, by construction: the
owner is the maximum. `BenchmarkOwner` on Machine A measures 16ns at 3 nodes,
49ns at 7, 85ns at 16, 246ns at 64 and 768ns at 256 — about 3ns per member,
allocation-free.

**Picked:** accept it. SPEC.md targets clusters in the tens of nodes, where the
cost is tens of nanoseconds and is dwarfed by the network hop M4 adds behind it.

**What would force a change:** a cluster in the hundreds, where 768ns per
resolution starts to matter against a sub-microsecond local read. The fix is
known — highest-random-weight with a skip list, or consistent hashing with
virtual nodes — and both trade the "a departure moves only the departed node's
keys" property or the memory for it. Not worth paying before the size demands
it.

---

## 24. `CanActivate` allocates on the refusal path — M3, accepted

`BenchmarkCanActivate` reports 136 B/op and 3 allocs/op over a 7-node cluster,
where six keys in seven are not this node's. The allocation is the formatted
`ErrNotOwner`, which names the group and the node that does own it.

**Picked:** keep the message. The guard is consulted once per *activation*, not
once per operation, so this is not a hot path; and an ownership error that does
not say who owns it instead is an error that costs an engineer more than the
allocation costs the process.

**What would force a change:** M4 routing a request per call through the same
check. If that happens, the fix is to return a sentinel and attach the detail at
the boundary that logs it, not to make the message worse.

---

## 25. Goroutine leaks are caught by accident, not by construction — M3, a gap

`internal/membership` leaked one goroutine per node until an unrelated latency
budget in another package noticed. Open question 11 records that `goleak` is not
wired in because parallel tests would make it noisy. That reasoning still holds,
but the cost of it is now measured rather than theoretical.

**Picked:** a targeted leak test per package that starts goroutines
(`TestCloseLeavesNoGoroutines`), rather than a global `TestMain` check. It is
narrower than `goleak` and catches the case that actually occurred: a `Close`
that does not close.

**What would resolve it:** `goleak.VerifyTestMain` in each package that owns
goroutines, once the tests in that package are known not to run in parallel with
anything that outlives them. Worth doing in M7 alongside the metrics work, when
every long-lived goroutine in the system exists and can be enumerated.

---

## 26. Namespace options are fixed at creation, silently — M4, a sharp edge

`Namespace(name, opts...)` returns the existing namespace if there is one, and
the options are ignored. There is no error and no warning.

This cost an afternoon during M4: the acceptance benchmark asked for a
one-nanosecond L1 bound on a namespace that had already been created with
defaults, got the default five seconds, and measured twelve million requests per
second — because every "remote" read was a local cache hit. No gRPC stack
produces that number, which is the only reason it was caught.

**Picked:** leave the behaviour, document it. Returning an error would break the
common and reasonable pattern of calling `Namespace("x")` from several places
with the same options, and comparing option sets for equality means comparing
function values, which Go cannot do meaningfully for `Loader` or `Partitioner`.

**What would resolve it:** a `Stats` field, or a debug-level log line, when a
namespace is requested with options that cannot be applied. That needs the
comparison problem solved for the fields where it is possible (TTLs, shard
counts, codec names) and skipped for the ones where it is not.

---

## 27. Windows reserves ephemeral ports, so socket tests flake — M4, handled in M6, completed in M7

`internal/membership` and the gRPC transport tests bind port 0 and let the OS
choose. On this Windows host the choice sometimes lands in a range Hyper-V has
reserved, and the bind fails with "An attempt was made to access a socket in a
way forbidden by its access permissions". Observed roughly once in twenty runs;
three consecutive re-runs passed.

**Picked in M4:** nothing, on the grounds that a retry loop would hide a real
bind failure.

**Revised in M6.** The reserved ranges turned out to be far larger than the
"once in twenty runs" first observed suggested — `netsh interface ipv4 show
excludedportrange protocol=udp` reports 600 contiguous UDP ports reserved on
this machine (50510-51109), and a single race run failed three tests at once
inside that band.

A gate that fails for environmental reasons is a gate people learn to ignore,
which is worse than either outcome it was protecting against. The gossip tests
now retry construction up to twelve times, and *only* when the error is the OS
declining a port it just handed out. Any other bind failure still fails
immediately, so the original objection is met: a real problem is not hidden.

Retrying is also what a real deployment does, so this is not test-only sleight
of hand.

**What would resolve it:** running the socket tests on Linux CI, where the
ephemeral range is not carved up. Recorded here so that a future flake is
recognised rather than investigated twice.

---

**M7 addendum.** The M6 fix was applied to `internal/membership`'s tests only.
The root package's `cluster_test.go` builds gossip nodes through its own helper
and kept failing — latent for a milestone, surfaced by M7's `-count=2` gate.
Both now call `testkit.RetryBind` and `testkit.IsPortRefused`, one exported
implementation, so a third caller inherits the fix instead of rediscovering the
bug. Recorded because the failure mode is general: a correct fix applied to some
of the places that need it reads, in the commit log, exactly like a complete one.

## 28. `EpochSkew` is observed but nothing acts on it — M4, deliberate

The peer handler counts requests whose caller resolved ownership under a
different epoch, and does nothing else with the number.

That is the correction of an earlier mistake rather than an oversight. The first
implementation *refused* those requests, which looked principled and was wrong:
epochs count one node's observed transitions, so two nodes that agree exactly
about membership can hold different epochs, and demanding equality fails
requests between nodes that agree — permanently, once the counters have drifted.
`core.Epoch`'s own documentation says comparing epochs "establishes staleness,
never consensus"; the handler was treating it as consensus.

**Picked:** the ownership check (`assertOwned`) is the safety mechanism, because
"do I own this key under my own current view" is answerable locally and is the
only question that matters. The epoch is carried for stamping and counted for
visibility.

**What would resolve it into something actionable:** M7's metrics work, where a
sustained skew rate under stable membership is a useful alert — it means views
are not settling — and a transient spike during churn is not.

---

## 29. The 50k rps acceptance figure is not reachable in-process — M4, unmet

SPEC.md §4 asks for "remote Get p99 under 2ms and p999 under 10ms at 50k rps in
a 3-node real-network test". The latency half is met with 5/5 reliability. The
rate half is not: five consecutive runs reached 45,168 to 48,035 rps.

**Why.** The load generator and all three servers share twenty logical cores in
one process, so every request the client issues takes CPU from the servers
answering it. A worker sweep shows throughput flattening between 45k and 56k
while p99 degrades threefold — the machine runs out, not the design.

**Picked:** report the shortfall rather than tune around it. The latency budgets
are asserted and fail the build; the rate is logged with the sweep behind it in
`docs/benchmarks.md`. Picking the worker count that happened to pass would have
turned a measurement into a decoration.

**What would resolve it:** offering the load from outside the process under
measurement — a separate load generator, or the three machines that "3-node
real-network" literally implies. Neither belongs in a test that must be
reproducible on one developer machine, so this needs a benchmark harness that
CI runs on dedicated hardware. Worth building alongside M7's observability work,
when there is something to point a load generator at in a real cluster.

**What it does not mean.** Nothing here suggests the cache cannot serve 50k rps;
it suggests this machine cannot both generate and serve it. The per-operation
figures — a 175 µs serial round trip, 28 µs under concurrency — are the numbers
to reason from, and they are in `docs/benchmarks.md`.

---

## 30. A restarted node owns keys it does not have — M5, mitigated not solved

`Cache.CatchUp` exists and works, and calling it is a documented requirement of
the restart procedure. What is *not* solved is that a node which skips it
answers `ErrNotFound` for data that exists elsewhere — confidently, and
indistinguishably from a genuine miss.

The window is real even when catch-up *is* called, because the node becomes an
owner the moment it joins while catch-up runs afterwards.

**Picked:** document it loudly — README guarantees, `docs/api.md`, decision 0015
— and leave the behaviour. The alternatives available in M5 are worse: refusing
to serve until catch-up completes needs a readiness concept the library does not
have, and consulting replicas on every miss puts a second lookup on the common
path to paper over the rare case.

**What would resolve it:** M6's readiness gate. A node that has joined but not
settled should fail readiness, so traffic does not reach it until it holds what
it owns. Kubernetes already has the vocabulary for this; the library does not.

---

## 31. The chaos test cannot distinguish a failed write from a lost one — M5, accepted

`TestChaosLosesNoSynchronouslyWrittenKey` records a key as written only when
`Set` returns nil under synchronous replication, which is a precise promise and
the one the test holds the cluster to. Writes that fail mid-reconfiguration are
skipped, and the test cannot tell a write that legitimately failed from one that
was accepted and then lost in a way that also produced an error.

**Picked:** accept it. The synchronous promise — a returned write is on RF nodes
— is exactly what is being verified. Verifying the failures too would require
the test to know which of them were "real", which is the thing under test.

**What would resolve it:** per-write outcome logging, which is M7 territory if
it happens at all.

---

## 32. An implicitly created namespace silently disables replication — M6, resolved

**Symptom, as first seen.** Hard-killing one node of five at replication factor
3 with synchronous replication left 53 of 300 keys permanently unreachable —
`ErrNotFound`, not deferred. A node failure at RF=3 should be survivable by
definition, so this looked like a serious defect in replication.

**Cause.** A write routed to its owner creates the namespace there if that
node's application has not opened it, using *default* options — replication
factor 1. Namespace options are fixed at creation and silently ignored
afterwards (question 26), so such an owner replicates nothing for that
namespace, permanently, however the application configures it later.

In the failing example only one node had opened the namespace. The other four
created it implicitly on their first routed write, and the cluster placed 68 of
the 400 copies that RF=3 implies — 17%. Every counter said the writes succeeded,
because they had.

**Resolved by** making the hazard visible rather than silent.
`Stats().ImplicitNamespaces` counts namespaces created by an incoming peer write
rather than by the local application, and each one logs a warning naming the
namespace and the replication factor it is stuck with. Any non-zero value is a
misconfiguration: the node is running with options its application never chose.
`TestAnImplicitNamespaceIsCountedAndReplicatesNothing` pins both the shortfall
and the correct behaviour when every node is configured.

The creation itself still happens. Refusing would break routed writes to a node
that has not yet opened the namespace, which is a legitimate startup ordering.

**The operational rule**, now stated in the deploy README and the guarantees:
open every namespace, with the options you want, on every node, before serving
traffic. The library assumes every pod runs the same binary; the corollary is
that every pod must configure its namespaces identically.

**What this cost, and why it is worth recording.** The diagnosis took far longer
than the fix. Three things went wrong in the process and all three are
repeatable mistakes:

- I hypothesised the correct cause early, "fixed" it, saw no change, and
  discarded it. The fix had silently not applied — a `str.replace` whose anchor
  did not match is a no-op, not an error. It is the third time in this project
  that has happened, and the second time it sent an investigation down a wrong
  path. Every generated edit now gets verified by reading the file back.
- I announced the root cause twice before confirming it.
- The measurement that finally settled it — printing the replication counters
  per node — took one command. It came after a long run of reading code and
  reasoning about what *should* happen. Bisecting the failing case against a
  passing one worked immediately every time it was actually used.

---

## 33. Deliberately deferred, per SPEC.md section 5

Not open questions. Listed so that nobody reopens them by accident:

- Persistence: WAL, snapshots, object-storage backup.
- Cross-group transactions.
- Multi-region and multi-cluster.
- Raft-backed membership. The interface exists; the implementation does not.
- Group sharding for hot keys. The limitation is documented instead.
- Protobuf and CBOR codecs beyond the registry hook.
- Snappy and Zstd beyond the compression interface.

---

## 34. A nil context panicked at every API boundary — M7, resolved

Every context-taking method in the public API began by calling `ctx.Err()`.
`context.Context` is an interface, so `Close(nil)` was a nil-pointer dereference
inside the actor runtime — a panic crossing an API boundary, which CLAUDE.md
forbids without carving out an exception for caller error.

It was found by accident: a throwaway program written to count exported metric
series used `defer c.Close(nil)` and died. Eighteen exported methods had the
same hole. No test had ever passed a nil context, because nobody writes that on
purpose — which is precisely why it survived seven milestones.

**Resolved** by normalising nil to `context.Background()` at each of the
eighteen, with the reasoning in `context.go` and `TestNilContextDoesNotPanic`
covering all of them exhaustively rather than representatively.

The judgement call worth recording is *Background* over *a returned error*. Go's
own guidance is not to pass nil, so this is a caller bug either way. But a nil
context carries no deadline, no cancellation and no values, which is exactly
what Background is — and returning an error would have made `Close(nil)` a
no-op that leaves goroutines running, converting a cosmetic mistake into a leak.
The bug is still the caller's to find; it just does not take the process with
it. Reversible if it ever hides something real.

---

## 35. The mutation audit silently skipped an entry for two milestones — M7, resolved

`scripts/mutation-audit.sh` contained an orphaned argument list — a `mutate`
entry whose `mutate` line had been lost in an edit, leaving its arguments as a
top-level command. The script runs under `set -u` but not `set -e`, so bash
reported "command not found" on a stderr the run discards and carried on. The
entry sat in the file looking audited and never ran. It was M6's acceptance
criterion.

Reconstructing it made things worse before better: the criterion could not have
caught either mutation anyway. `TestRollingUpgradeLosesNoRequests` runs at
replication factor 2 with synchronous replication, so deleting `Drain`'s handoff
entirely — and separately, reversing the step order that drain.go's comment
calls "the whole design" — both left it passing. The replicas held every key.
The test proved replication worked and said nothing about drain.

**Resolved** by `TestDrainHandsOffKeysNothingElseWouldSave` and
`TestDrainAnnouncesDepartureBeforeTransferring`, both at replication factor 1
where the handoff is the only copy, and both confirmed to catch their mutations.

The guard against a recurrence went through three attempts, which is the part
worth recording:

1. `command_not_found_handle` — bash runs it in a context where `exit` does not
   stop the script. Verified not to work.
2. The same handler signalling the parent with `kill -TERM $$` — the existing
   cleanup trap absorbed it and execution continued. Also verified not to work.
3. A static check that runs before anything is mutated, joining backslash
   continuations into logical lines and requiring every line carrying a
   `'TestSomething'` argument to begin with `mutate`.

The third needed a real quote-aware scan rather than a regex, because most
arguments here are multi-line Go snippets inside single quotes and a naive
line-joiner flags fifty-nine false positives. A first version checked only the
shape of the last line, which missed the case where the `mutate` line alone is
deleted and the continuation chain starts at the filename — bash runs that just
as silently. Both variants are now confirmed detected.

**The lesson is the general one.** A verification tool that fails open is worse
than no verification tool: it produces the paperwork of coverage without the
coverage. This is the second time this script has done real damage — the first
was a crashed run leaving a mutant in the tree that shipped for three commits
(question 32) — and both failures were silence rather than error. Anything that
exists to catch mistakes must be loud when it cannot.
