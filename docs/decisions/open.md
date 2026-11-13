# Open questions

Things guessed at, in the order they will start to hurt. Each has a
provisional answer that is cheap to reverse; none of them are silent.

Format: what is unresolved, what was picked for now, and what would force a
change.

Numbers are permanent once assigned, because the code cites them by hand:
`examples/rolling-upgrade/main.go` refers to question 26, `tests/acceptance_remote_test.go` to questions
27 and 29, and question 38 is named fourteen times in `.go` and `.sh` files alone. There is no question 19 —
the gap predates this record's current form — and renumbering to close it would
invalidate every one of those references for no gain.

---

## 1. The shape of `PeerClient` and `Handler` — M0, confirmed by M4

docs/design.md section 3.6 names `Transport.Dial(node) (PeerClient, error)` and
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

The signature is fixed by docs/design.md section 3.6 and it suits gRPC. It suits an
in-process transport badly: there is nothing to listen on.

**Picked:** the fake transport accepts a nil listener and ignores it.

**What would force a change:** nothing urgent. If a second non-socket transport
appears, replacing the parameter with an opaque `ListenSpec` would be tidier.

---

## 3. The contents of `Stats` — M0, provisional

docs/design.md section 3.1 returns `Stats` and never says what is in it.

**Picked:** a plain struct with self, member count, epoch, key and group counts,
and hit/miss counters. A struct, not an interface, so adding a field is not a
breaking change.

**What would force a change:** M7, which adds the correctness counters
(split-brain detections, stale-epoch rejections, version conflicts, mailbox
rejections, transfer failures). Those are additive.

---

## 4. Module path — M0, placeholder; resolved before the first tag

**Picked:** `github.com/peercache/peercache`.

**What would force a change:** the repository actually being published
somewhere. This is a single `go mod edit -module` plus a find-and-replace, and
it should happen before the first tag, not after.

**Resolved.** The module is `github.com/arwenizEr/peercache`, changed before any
tag. Earlier records that quote compiler output keep the old path, because they
are quotations.

---

## 5. `Watch()` semantics under a slow consumer — M0, provisional

docs/design.md section 3.5 gives `Watch() <-chan MembershipChange` with no buffering
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

docs/design.md forbids `time.Sleep` in tests. `internal/clock/real_test.go` blocks on
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

docs/design.md section 4 (M3) requires a node to wait `activationDelay` after a
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

## 12. There is no typed namespace API — M1, resolved in M10

docs/design.md section 2 describes the namespace as "typed Get/Set/GetOrLoad". Section
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

**Resolved in M10.** `NamespaceOf[T]` returns a `TypedNamespace[T]` over the
byte namespace, `WithTypedLoader[T]` supplies a typed loader, and `WithCodec` now
chooses the encoding, with MessagePack as the default. See
`docs/decisions/0025-typed-namespaces.md`.

---

## 13. The L1 tier is not reachable from a namespace — M1, resolved in M4

`internal/l1` is complete and tested, and nothing calls it. On a single node
every key is owned locally, so there is no non-owned copy to cache.

**Picked:** build and test it standalone, as docs/design.md section 4 (M1) requires,
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
1.24 and docs/design.md fixes the floor at 1.23. v1 measured marginally faster on the
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
| `WithCodec` | M4 | docs/design.md section 3.1 defines a `[]byte` API; there is nothing to encode. See question 12. |

**Updated in M4.** `WithPartitioner` now acts: `namespace.groupKeyFor` consults
it on every routed operation, so keys grouped under one group key share an owner
and travel together in a batch. `WithReplicationFactor` remains inert until M5.
`WithCodec` still only validates — the protocol's own encoding is fixed
(`internal/wire`), and choosing a codec for application *values* is question 12,
which is still open. **Updated in M10:** `WithCodec` now acts — it is the
encoding of a `NamespaceOf` view. All three options in this table do something.

**Picked:** accept and validate them now. An unregistered codec or an
out-of-range replication factor fails at startup, which is the point of naming
them in configuration, and changing a partitioner later moves keys between
owners — a decision better made before there is data.

**The risk being managed:** an option that silently does nothing is a lie by
omission. A test pinned the inert behaviour so that wiring it up in M3 was a
deliberate change rather than a silent one; the partitioner now routes, and
`TestAPartitionerDoesNotMergeKeys` pins what it does not do.

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

**Narrowed in M9 and after M10.** A deleted or shed entry no longer restarts the
sequence: the shard keeps a floor at the highest version it removed that way,
because a replica that missed the removal still holds that version (questions
42 and M9's shedding). Only an entry the sweeper reclaims restarts at 1 now.

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

**Picked:** accept it. docs/design.md targets clusters in the tens of nodes, where the
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
The root package's `tests/cluster_test.go` builds gossip nodes through its own helper
and kept failing — latent for a milestone, surfaced by M7's `-count=2` gate.
Both now call `testkit.RetryBind` and `testkit.IsPortRefused`, one exported
implementation, so a third caller inherits the fix instead of rediscovering the
bug. Recorded because the failure mode is general: a correct fix applied to some
of the places that need it reads, in the commit log, exactly like a complete one.

**M8 addendum. There was a third caller, and the sentence above was right.**

`tests/acceptance_remote_test.go` builds gossip nodes through a helper of its own and never
got the fix. It failed on every attempt here — `bind: An attempt was made to
access a socket in a way forbidden by its access permissions`, on a port inside
the reserved band — which means the M4 acceptance criterion had not executed on
this machine since it was written.

It stayed invisible because the test sits behind an opt-in latency gate that only
CI's Linux runner sets, where the reserved band does not exist. It passed in the
one place it ran and could not run in the one place it failed.

**The first fix was also wrong**, and the reason is the useful part: wrapping the
existing `BindPort = 0` in `RetryBind` still failed twelve times out of twelve.
`RetryBind` is the backstop; the defence is `LocalGossipConfig`, which *probes* a
bindable port before memberlist is given it. Re-rolling into a 600-port band and
hoping is not a strategy, and the retry loop's own doc comment says so — "It is
not the primary defence; the probe is." All three callers now use the same helper.

**What would stop a fourth.** Nothing structural does today. The exported helpers
exist and are documented, and three separate call sites have now each written
their own `memberlist.Config` by hand before someone noticed. A `testkit`
constructor that returns a started `*membership.Gossip` — leaving no config for a
caller to assemble — would make the correct path the only path. That is a small
change and it is not made here, because the fourth caller does not exist yet and
inventing an API for a hypothetical one is how the first three got written.

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

## 29. The 50k rps acceptance figure is not reachable in-process — M4, met in M8

docs/design.md §4 asks for "remote Get p99 under 2ms and p999 under 10ms at 50k rps in
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

**M8 addendum: re-measured, and the numbers do not agree with the ones above.**

This test could not run on Machine A at all. It built its gossip nodes through a
third helper that never got question 27's port fix, so every attempt failed at
the bind, and it is behind an opt-in latency gate that only CI sets — so nobody
saw it. Fixed (the helper now uses `testkit.LocalGossipConfig` and `RetryBind`
like the other two callers), and it now runs here.

Four consecutive runs on Machine A:

| | rps | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| 1 | 195,015 | 186 µs | 248 µs | 304 µs |
| 2 | 193,199 | 187 µs | 249 µs | 284 µs |
| 3 | 195,324 | 186 µs | 244 µs | 257 µs |
| 4 | 194,865 | 187 µs | 245 µs | 261 µs |

That is four times the 45,168–48,035 rps this question records, against the same
criterion, with p99 seven times inside its 2 ms budget. The per-operation latency
*agrees* with the recorded 175 µs serial round trip; it is only the rate that
differs, and 36 workers at 186 µs each is 193k by arithmetic, so the figure is
internally consistent.

**Not marked resolved, because the disagreement is unexplained.** Either the
earlier figures came from a configuration this one does not reproduce — a
different worker count, or a run on different hardware, since the test could not
have executed on Machine A as recorded — or something between M4 and M8 made the
remote path substantially faster. Both are plausible and neither is established,
and rewriting a shortfall into a success on an unexplained 4× is exactly the kind
of decoration this question was opened to avoid.

**The sweep was re-run, and it does not flatten.**

| workers | rps | p50 | p99 | p99.9 |
|---:|---:|---:|---:|---:|
| 12 | 110,494 | 97 µs | 180 µs | 376 µs |
| 24 | 164,782 | 143 µs | 211 µs | 222 µs |
| 36 | 191,153 | 189 µs | 249 µs | 292 µs |
| 64 | 235,361 | 270 µs | 334 µs | 373 µs |

Throughput scales monotonically across the whole range. This question records
sixty-four workers reaching 56k rps at a p50 of 1.2 ms; sixty-four workers now
reach 235k at a p50 of 270 µs — four times the rate at a quarter of the latency,
at the same concurrency on the same machine.

**The criterion is met, with margin.** docs/design.md §4 asks for p99 under 2 ms and
p999 under 10 ms at 50k rps. The *lowest* point of the sweep offers 110k rps at
p99 180 µs, eleven times inside the budget. There is no worker count in this
range at which the criterion fails.

**Why the old figures differ is still not established**, and this is recorded as
met rather than explained. Two concrete discrepancies turned up while measuring
and neither accounts for a 4x:

- The worker constant is 36 while the comment above it reasons carefully to 12.
  The prose was left behind by a change to the number. That explains part of the
  *offered load* gap between the recorded runs and the first M8 run, and none of
  the per-operation improvement.
- The measured p50 at twelve workers is 97 µs, against the ~175 µs serial round
  trip this question cites. That looked like a contradiction and is not:
  re-running `BenchmarkInvoke` gives 70 µs to 177 µs across three counts at the
  same payload size, so 97 µs sits inside the benchmark's own spread. The 175 µs
  figure is the top of a noisy range being quoted as if it were a point, which
  is a reporting flaw rather than a stale number — and `BenchmarkInvokeParallel`
  has moved too, from the recorded 28 µs to 12-15 µs.

**One clue that narrows it.** The M1 local-read acceptance test, which touches no
networking at all, is *also* faster than its recorded figures: p50 203 ns and p99
318 ns now, against the 354 ns and 478 ns in `docs/benchmarks.md`. A local
`Store.Get` does not decode a frame, dial a peer or cross a socket, so whatever
moved cannot live in the remote path. That points at the environment or the
toolchain rather than at any commit in this repository — which is consistent with
both tables being honest measurements of different machines-in-time.

It does not close the question. Local is ~1.6x faster and remote ~4x, and one
factor does not explain the other; a toolchain that made everything 1.6x faster
would leave the remote figure needing a further 2.5x. Recorded because it is the
first evidence that rules a region *out*.

The honest summary is that the acceptance criterion this question was opened
about is satisfied and reproducibly so, and that the numbers it was opened
*against* describe a system this machine no longer reproduces. Marking it met on
the evidence, and leaving the archaeology recorded rather than tidied away.

**How to find out, when somebody wants to.** The acceptance test itself cannot be
bisected: it could not bind a socket on this machine until M8 fixed the third
copy of the port-retry helper, so at every earlier commit it fails before
measuring anything. `BenchmarkInvokeParallel` can. It was added in the M4 commit
that introduced the transport benchmarks, so it spans the whole range, and being
per-operation it is unaffected by the load-generator worker count that made the
first comparison confusing. It is also the figure that moved: 28 µs recorded,
12–15 µs measured.

    git bisect start HEAD 8577cc7
    git bisect run sh -c '
      go test ./internal/transport -run "^$" -bench BenchmarkInvokeParallel         -benchtime 2000x -count 3 2>&1 |
      awk "/BenchmarkInvokeParallel/ {s+=\$3; n++}
           END {exit (n && s/n < 20000) ? 0 : 1}"'

The 20 µs threshold sits between the recorded figure and the measured one, so
"good" means fast. Three counts and a mean, because a single benchmark run on a
desktop is noise. The probe was dry-run on the current tree and reports 12,426
ns/op, verdict good — so it classifies correctly at the known-fast end.

**It needs a clean working tree.** `git bisect start` will begin and then fail its
first checkout with "your local changes would be overwritten", leaving a
half-started bisect to reset. Commit or stash first. This was established by
doing it.

Two outcomes, and the second is as informative as the first. If it lands on a
commit, that commit is the explanation and the recorded numbers were simply
superseded. If it finds nothing — every commit fast, or every commit slow — then
the change is not in this repository, and the Go toolchain is the first place to
look: `docs/benchmarks.md` records go1.26.5 for the current figures and the M4
table predates it. Run the machine otherwise idle either way.

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

## 33. Deliberately deferred, per docs/design.md section 5

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
inside the actor runtime — a panic crossing an API boundary, which docs/design.md
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

**M8 addendum: the guard had a hole, and an M8 entry fell straight through it.**

The static check matches a test argument with `'Test[A-Za-z0-9_|]*'` — every
character a bare name or an alternation needs, and no anchors. M8 added an entry
passing `'TestMajoritySideWritesSurviveAHeal$'`, anchored because another test's
name extends it. That argument does not match the pattern, so the check could not
see the entry, and deleting its `mutate` line was **silently undetected** — the
run proceeded exactly as it did before M6's original incident.

Confirmed rather than reasoned: the `mutate` line was removed and the script ran
on, then removed again after widening the character class to include anchors and
the other regex metacharacters a `-run` argument may legitimately contain. It is
now caught, naming the line.

**Nothing else was affected, and that was checked rather than hoped.** Scanning
all 107 entries for arguments the old pattern could not match returns exactly one
real entry: the M8 one that exposed the hole. The other two hits are the
detector's own comment and its own regex source. Every pre-existing entry uses a
plain test name, so the guard was doing its job for all of them — the hole opened
the moment an entry needed an anchor, and the first entry that needed one made it.

The shape of this is worth more than the fix. The guard was written to stop an
entry going silently unrun, and it was defeated by the *contents of the argument*
it inspects rather than by the line structure it was built to check — by a new
entry, added by someone who had read the guard and believed it was covering them.
A checker with a hole is more dangerous than none, because the paperwork it
produces is what people rely on instead of looking.

**The lesson is the general one.** A verification tool that fails open is worse
than no verification tool: it produces the paperwork of coverage without the
coverage. This is the second time this script has done real damage — the first
was a crashed run leaving a mutant in the tree that shipped for three commits
(question 32) — and both failures were silence rather than error. Anything that
exists to catch mistakes must be loud when it cannot.

---

## 36. ErrStateEscaped is exported and unreachable — M8, removed before v1

`ErrStateEscaped` is returned by nothing, and nothing can return it: the
goroutine using a stale `GroupState` is not inside a `Call`, so there is no
caller to hand an error to. The method no-ops and `Stats().StateEscapes` counts
it.

Both doc comments claimed otherwise until M8 — `internal/core/group.go` said the
implementation "turns a violation into [ErrStateEscaped] in the calling layer",
which is exactly what `internal/actor/state.go` explains it cannot do.

Kept rather than deleted: removing a sentinel breaks anyone matching on it, and
it is the canonical name for a condition the documentation needs to refer to.
Both comments now say it is unreachable. Reversible if a future API ever has a
caller to return it to.

**Removed before v1.** The reason for keeping it was compatibility with callers
that matched on it, and before the first public release there are none. A
sentinel nothing can return is a promise the API cannot keep; the counter is the
signal, and `docs/api.md` names the condition without it.

---

## 37. Synchronous replication acknowledges writes below the replication factor — M8, resolved post-M8

Found by the deterministic simulation, seed `243287519653701681`, reduced to
`TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy`.

An isolated node eventually detects every peer as failed. Its view shrinks to
itself, it ranks itself owner of everything, the activation guard's delay
expires, and synchronous replication at RF=3 then places every copy it can
reach — one — and returns nil.

**The write is acknowledged with a single copy and the caller cannot tell.**

The neighbouring behaviour is correct and worth stating, because it locates the
gap precisely: when the writer still *sees* its replicas but cannot reach them,
sync replication fails the write.
`TestSyncReplicationRefusesWhenReplicasAreUnreachable` shows 20 of 20 writes
refused. The difference is entirely whether the failure detector has already
removed the peers from the view.

This is not simply the split-brain caveat. design principle 5 (docs/design.md) already
says a partition can produce two activations. What is new is the
*acknowledgement*: README and `docs/api.md` both say synchronous replication
closes the async data-loss window, and a caller who asked for RF=3 sync
reasonably reads a nil return as "three nodes have this". The window also lasts
as long as the partition, so "narrowed" does not describe it.

**Options, in increasing order of change:**

1. **Count it.** A `DegradedWrites` counter incremented when a sync write places
   fewer copies than the namespace's replication factor, exported as
   `peercache_degraded_writes_total`. Additive, no semantic change, and it makes
   the window visible to an operator for the first time. Recommended regardless
   of what else is chosen.
2. **Report it.** Return a typed error, or a sentinel wrapped so `errors.Is`
   distinguishes "written, but below RF" from "not written". Callers that do not
   check keep working; callers that care can. This changes no data path.
3. **Refuse it.** Fail the write when RF copies cannot be placed. This is the
   strongest reading of what `WithSyncReplication` promises and it is *not*
   quorum — it is honouring a configured replication factor — but it converts an
   available system into an unavailable one during a partition, which is a real
   AP/CP choice and not mine to make silently.

**Option 1 is done.** `Stats().DegradedWrites` counts a synchronous write
acknowledged holding fewer copies than its namespace asked for, and
`peercache_degraded_writes_total` exports it. The first occurrence on a node logs
a warning naming the namespace, the factor, the copies placed and the view size;
subsequent ones do not, because a partition degrades every write for as long as
it lasts and a line per write is an outage of its own. `docs/api.md` lists it
among the metrics worth alerting on, and it is the only signal that separates a
degraded acknowledgement from a healthy one.

Only synchronous writes are counted. Asynchronous replication promises nothing
about when a copy lands, so counting those would produce a number equal to the
write rate; that window is measured in `docs/benchmarks.md` instead.

The partial-failure case does not reach the counter, and that is worth stating
because it locates what is left: under synchronous replication a target that
refuses or cannot be reached makes the write *fail*, so it is never
acknowledged. The only route to an acknowledgement below the factor is for the
targets to be missing from the ranking in the first place — the shrunken-view
case this question is about, and nothing else.

**Options 2 and 3 are still open, deliberately.** Both change what a caller sees,
and the choice between them is an AP/CP decision: option 3 converts an available
system into an unavailable one during a partition. docs/design.md's rule is to pick
the simplest reversible option and not to guess silently on anything touching
correctness under failure — counting is reversible, refusing is not, and the
counter now makes the window visible enough to decide on evidence rather than on
argument. What would settle it is a deployment's `degraded_writes` rate: if it is
zero outside genuine partitions, option 3 costs nothing anyone will notice.

**Resolved: options 2 and 3, opt-in per namespace.** See
`docs/decisions/0021-refusing-writes-below-the-factor-is-opt-in.md`.

Reading the write path showed the two options are one. A synchronous write is
stored on the owner before it fans out and nothing rolls it back — an unreachable
replica already makes `Set` fail with the value held locally — so "refuse" can
only mean "report as not confirmed". What was left to decide was the default.

`WithRequireReplicationFactor()` makes a synchronous write below the factor fail
with `ErrUnderReplicated`, counted in `Stats().UnderReplicatedRefusals` and
`peercache_under_replicated_refusals_total`. Without it, behaviour is unchanged
and `DegradedWrites` counts as before. It is off by default because the same
check, on everywhere, refuses every write on a cluster smaller than the factor —
including a single development node and a pod that has not yet joined — and a
node cannot tell those from isolation.

Pinned by `TestRequireReplicationFactorRefusesAnIsolatedWrite`,
`TestRequireReplicationFactorRefusesThroughAForwardedWrite`,
`TestRequireReplicationFactorAcceptsWhenTheFactorIsMet`, the
`ErrUnderReplicated` case of `TestSentinelErrorsSurviveTheHop`, and
`ExampleWithRequireReplicationFactor`. The pre-existing
`TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy` still pins the default.

**What would reopen it:** evidence that `degraded_writes` is non-zero only during
genuine partitions, which would make flipping the default cheap.

---

**Option 2 is done too — found by M16's real-world run (2026-11-13).** The
partial-failure case above was where it hurt. A shop demo under load had a node
killed with SIGKILL; for the seconds before the others noticed, synchronous
writes to keys it replicated failed, *after* being applied on their owners. The
application could not tell an applied `Increment` from a refused one, and its
stock counts drifted both ways. The error also lost its identity: a write from a
non-owner reached its owner over gRPC, and `ErrNodeUnreachable` did not survive
the way back, so the application saw an opaque failure.

Now a synchronous write applied on its owner whose replication fails for any
reason returns an error wrapping `ErrUnderReplicated` — "written, held by fewer
nodes than its factor" — and is counted in `DegradedWrites`, the same as the
shrunken-view case. The transport checks it before deadline and cancellation,
so a replication that ran out of time still says "held". Option 3 — refusing —
remains what `WithRequireReplicationFactor` is for.
`TestAFailedSyncReplicationReportsTheWriteAsHeld` covers both a direct and a
forwarded write.

## 38. A simulation seed still loses keys after a single destruction — M8, resolved

Seed `601996618416914900` lost two keys acknowledged under synchronous
replication at RF=3 after one node was destroyed. RF=3 is meant to survive two.

**The cause is not established, and this records that honestly.** Reading the
code suggested replica eviction: `Store.Evict` drops every replica this node no
longer ranks for, decided from the local ranking alone, with nothing checking
that a node which now ranks for the key holds a copy — and catch-up cannot
repair what nobody holds. `TestEvictionDoesNotDropTheLastCopy` was written to
reproduce that and does not: the kill-and-rejoin churn produces zero evictions
and zero losses. The hazard is real; it was not this.

The remaining explanation, and the one the schedule supports, is the documented
AP window. Writes acknowledged while the cluster was split are not promised to
survive the heal on either side — design principle 5 (docs/design.md) says conflicting
writes are detected, not merged, so the losing side's acknowledged write is
discarded by design. Filtering only on the writer's *view size* was not enough,
because a writer on the larger side of a split sees enough members to satisfy RF
and still loses its copies when the sides reconcile.

The simulation now excludes writes acknowledged during a partition, and 300
seeds pass. That is the correct scope — the guarantee is not claimed across a
partition — but it means this specific seed's loss is explained by inference
rather than by demonstration.

**What would close it:** an assertion that a write acknowledged on the *majority*
side of a split survives the heal. If that holds, the losses were minority-side
and everything is as designed. If it does not, there is a real defect in
post-partition reconciliation and question 37's option 3 becomes considerably
more attractive.

**Closed, and not the way the paragraph above expected.**

The assertion it asks for now exists. The simulation attributes
every acknowledged write to a *side* of the split rather than merely to "during a
partition": minority-side writes are excused, majority-side writes are recorded
and asserted to be readable after the heal, and the two are reported separately
so a failure names which promise broke. Splits are generated strictly uneven —
`cut` is drawn from `[1, (live-1)/2]` — because an even 2|2 split has no majority
and the assertion would otherwise be asserting about nothing.

**It did not hold, and the other branch was the right one.** Seed
`5593891418832731417` lost 5 of 12 majority-side keys. The note said what that
would mean — "there is a real defect in post-partition reconciliation" — and
there was.

**The cause is the eviction hazard this question originally proposed and then
discarded.** The discarding was correct about the evidence available at the time
and wrong about the conclusion: `TestEvictionDoesNotDropTheLastCopy` reproduces
kill-and-rejoin churn and observes zero evictions, because when copies are placed
under the full membership a rejoining node does not take ranking away from the
holder of a last copy.

A partition changes that. Writes accepted while one node is cut off place their
copies at ranking positions computed *without* the absent node. When it returns,
the ranking moves underneath every one of those keys: the rejoining node ranks
for many of them and holds nothing, and the nodes that do hold them stop ranking
and drop them. The copies do not move — they evaporate. Catch-up cannot repair
it, because it pulls from whoever has the key and after the eviction nobody does.

Reduced to `TestMajoritySideWritesSurviveAHeal` (which passes: the heal alone
loses nothing) and `TestMajoritySideWritesSurviveAHealAndTwoDestructions` (which
lost 20 of 94). The counters made the mechanism plain rather than plausible:
twenty-one replicas evicted cluster-wide, twenty keys lost.

**Resolved** by handing off before evicting. A replica this node has stopped
ranking for is sent to the nodes that now rank for it and dropped only once one
has accepted it; every failure path keeps the copy instead.
`Stats().ReplicasRehomed` and `ReplicasRetained` account for both outcomes. See decision
0020 for why this rather than a smarter eviction test, and why it needs no new
RPC.

**The evidence, measured.** Both reduced tests pass; the seed that found it
(`5593891418832731417`) replays clean; and two 300-seed sweeps pass, one of them
from the base seed that produced the original failure. The second sweep reports
300 of 300 schedules asserting at least one key, so the result is not an artefact
of everything having been excused. Two seeds in the first sweep *were* vacuous,
which is what prompted the guard rework above — well inside the quarter the
corpus gate permits.

**One diagnostic step is worth recording**, because it nearly produced a wrong
conclusion. More than half of the first run's "lost" keys were not lost: they
returned `ErrActivationPending`, a retryable "ask again shortly", and
`readAnywhere` retried three times without letting the fake clock advance — so it
asked again at the same instant and got the same refusal. A helper that loops
without waiting reports a deferral as a loss. It now advances the clock, and the
remaining failures were all genuine not-founds. The difference between "deferred"
and "not found" is the difference between a working cluster and a defect, and a
test helper that cannot tell them apart will eventually claim both.

**A side effect worth recording**, because it is the same failure mode as the
mutation audit's: splitting majority-side writes into their own pool left the
simulation's vacuity guard reading only the general pool, and it started failing
schedules that had asserted a dozen keys. The guard exists so a green run cannot
be hollow — a seed that excused every key it acknowledged proved nothing — and a
guard that fires on runs which *did* prove something teaches people to ignore it.

It now counts both pools, and a schedule that genuinely excused everything is
**skipped** rather than failed: that is a sampling outcome, not a defect. What is
failed is the *corpus* — more than a quarter of a sweep proving nothing means
either the fault rate has outrun what RF=3 survives or the excusal rule has grown
too generous, and both make a green sweep meaningless. The excusal rule is
deliberately conservative (it counts every destruction in the run, not the
destruction of a key's own holders), so on long schedules with many kills some
seeds will always land there.

**What is not settled.** Seed `601996618416914900` from the original report is not
replayed under the new attribution: the schedule generator changed when even
splits stopped being produced, so that integer no longer denotes the same
schedule. The property it was evidence about is now asserted directly, which is
the better outcome, but it does mean the original run was never explained on its
own terms.

---

## 39. Ten messages could be turned into a 4 GiB allocation by five bytes — M8, resolved

`internal/wire` interprets bytes it did not write, and from M4 those bytes arrive
from anything that can reach the port. M4 found that msgpack pre-allocates a
collection from its declared length and fixed it with bounded decoders, fuzzing
five message types as a representative sample.

M8 found two things wrong with that.

**The sample missed one.** `SubmitResponse` carried the only slice of structs with
no custom decoder. `91 dd ff ff ff ff` — six bytes — asks for 4,294,967,295
`GroupFailure` values, 137 GB.

**The library has a second hole that bounded decoders cannot reach.** msgpack v5's
allocation limit lives in `Decoder.readN`; its byte-string paths call the
package-level `readN` instead, which allocates the full declared length up front.
Every `[]byte` field in every message — ten of the twenty — turns
`c6 ff ff ff ff` into a 4 GiB allocation.

**Resolved** by validating the frame's structure in `Decode` before the decoder
sees it: every declared length must be no larger than the bytes behind it. See
decision 0019 for why that is one function rather than eight more encoder pairs,
and for the measured cost (25 ns on a `GetRequest`, zero allocations).

**What is worth carrying forward** is not the bug but how it survived. M4's fuzz
target covered a *sample* of message types, chosen when the sample was most of
them. The protocol grew to twenty; the sample stayed at five; and the defect the
sample existed to catch lived, for two milestones, in the fifteen it did not
look at. The replacement decodes every input into all twenty
(`FuzzDecodeEveryMessage`), because "no input from a peer may panic a node" has
the word "no" in it.

The second half is worse and more general: the M4 fix was correct, tested, and
*aimed at the wrong layer*. Bounding the decoders this package owns could never
have found a hole in the library's byte-string path, and no amount of care
applied to `bounded.go` would have gotten there. It took pointing a fuzzer at the
boundary rather than at the code someone had already thought about.

---

## 40. Readiness answers availability, not durability — M8, resolved post-M8

`ReadyState.CaughtUp` is a one-shot flag. It is set the first time `CatchUp`
completes and never cleared, so a node that joined, caught up, and then lived
through a partition heal still reports `Ready: true` while the cluster is below
its replication factor.

M8 established that the two are different conditions. A key is *readable* the
moment one node holds it and *survives a failure* only when RF nodes do, and
after any membership change the second is restored over time by catch-up while
the first is true almost immediately. `TestMajoritySideWritesSurviveAHealAndTwoDestructions`
lost a key about one run in seven until it called `CatchUp` before destroying
anything — the cache was correct, the schedule was not.

**The question this raises is not obviously answerable, which is why it is
here.** Readiness means "should this node receive traffic". A node inside the
settling window can serve traffic correctly: reads succeed, writes succeed,
nothing is wrong with any individual operation. Gating readiness on the cluster
being back at RF would take a healthy, serving node out of rotation because a
*different* node is still copying — which is a durability signal wearing an
availability gate, and it would make a rolling upgrade slower for a property no
single request depends on.

The opposite is equally arguable. An operator draining a second pod because the
first went ready is exactly the sequence that walks an RF=3 cluster down to RF=1,
and `deploy/README.md` now warns about it in prose. A warning in prose is what
this project calls a lie by omission when a counter would do.

**Picked for now:** neither. The behaviour is documented — README, `docs/api.md`
and the deploy guide all state that readiness does not imply the replication
factor is restored, and that `Stats().ReplicasPulled` going flat is the signal
that it is.

**What would resolve it:** a third thing, most likely, rather than either of the
two above. A separate `Cache.Replicated()` or a `ReplicationRestored` field on
`Stats` would let an operator gate a rolling upgrade on durability without
conflating it with whether a pod can serve. That is an API addition and belongs
in a milestone that is designing one, not bolted onto the end of this one.

**What would force it sooner:** anybody actually running a rolling upgrade at
RF=2. At RF=3 the window costs a copy and the cluster still tolerates a failure;
at RF=2 it costs the only redundancy there is, and the difference between "ready"
and "safe to replace the next pod" stops being academic.

**Resolved: the third thing.** `ReadyState.ReplicasCurrent`, mirrored in
`Stats().ReplicasCurrent` and the gauge `peercache_replicas_current`, reports
whether this node has completed a catch-up — every peer in its view, no failure —
that began under the membership epoch it holds now. Readiness is unchanged: it
still answers whether the node can serve, and a ready node may report
`ReplicasCurrent` false.

The epoch is read *before* the run. A change while pages are in flight means some
were chosen under a ranking that no longer holds, so that run does not count.
Concurrent catch-ups record whichever finishes last, which can understate
currency but never overstate it.

It is a per-node fact, and says so. The cluster is back at its factor when every
node reports it under the same view; a rollout gates on the minimum across pods.
It does not account for asynchronous replication still in flight — that window
is measured separately in `docs/benchmarks.md`.

**Building it found a bug underneath.** `CatchUp` had returned `nil` since M5
whatever happened: the per-peer errors were logged and counted, and the variable
meant to carry them was declared and never assigned. A signal defined as "the
last catch-up succeeded" would have inherited that and reported success after
reaching nobody. `CatchUp` now joins every peer's error, and
`TestCatchUpReportsAPeerItCouldNotReach` pins it. No existing test depended on the
old behaviour: every call site that fails on the error calls it after the view
has converged.

Pinned by `TestReplicasCurrentFollowsCatchUpAndMembership`,
`TestCatchUpReportsAPeerItCouldNotReach` and `TestASingleNodeHasCurrentReplicas`.

**What would reopen it:** a deployment that needs the cluster-wide answer from a
single node. That needs every node's epoch and catch-up state gossiped, and
membership is AP, so the answer would be one node's belief about the others —
weaker than the per-node fact, and not obviously worth a protocol change.

---

## 41. The JSON codec silently corrupts invalid UTF-8 — M8, documented; no longer the default since M10

`internal/codec`'s `Codec` interface has said since M1 that "for any v that the
codec accepts, `Unmarshal(Marshal(v))` reproduces v", with the sentence beneath
it arguing that "a codec that silently drops fields is worse than one that
errors". The JSON codec — the default, chosen because an operator can read it in
a debug dump — does not hold the first and is an instance of the second.

`encoding/json` replaces every byte of an invalid UTF-8 sequence in a string with
U+FFFD and returns no error. A value written through the JSON codec and read back
is not the value that went in, and nothing in the round trip says so:

    in.S  = "\xff\xfe"
    out.S = "��"     // Marshal returned nil error

`FuzzCodecRoundTrip` found it on its first run. `TestCodecRoundTrip` had existed
since M1 and does not catch it: it compares three hand-written values and, for
the composite fields, compares only their lengths.

**Scope, which is narrower than it first looks.** Only Go `string` fields are
affected. A `[]byte` field survives JSON because it is carried as base64, and
MsgPack — which `internal/wire` uses for the protocol itself — carries the same
bytes through unharmed. `WithCodec` is also still inert (question 17): the
public API is `[]byte`-valued, so no application value passes through a codec
today. This is a hole in a contract that has no caller yet, which is the cheapest
moment to find one and the easiest moment to leave it.

**Picked for now: document and pin, do not work around.** The substitution
happens inside `encoding/json`; no option disables it, and it cannot be detected
from the output, which is valid UTF-8 by the time it exists. The only detection
available is to walk `v` by reflection before every marshal and reject invalid
strings — a full traversal of every cached value on the encode path, to catch an
input class most applications never produce, in a library whose whole argument is
that it is faster than a network hop to Redis.

So: the limitation is documented on `jsonCodec` and on the `Codec` interface
itself, `TestJSONCodecReplacesInvalidUTF8` pins the behaviour and fails the day it
changes, and `FuzzCodecRoundTrip` excludes that one input class for JSON by name
rather than exempting the codec. The property is still enforced for JSON
everywhere else and for MsgPack everywhere.

**What would resolve it:** answering question 12. A typed namespace API is what
puts application values through a codec, and it is the point at which the choice
has to be made explicitly — most likely by making MsgPack the default for typed
values and leaving JSON as the readable opt-in, which costs nothing and moves the
sharp edge to the codec somebody names deliberately.

**What would force it sooner:** any value whose strings can hold arbitrary bytes
— a key derived from a byte slice, a field carrying a protocol identifier, an
encrypted or compressed blob stored as a `string` rather than a `[]byte`. For
those, name `msgpack` today; it is one option and it has no such rule.

**Updated in M10.** Question 12 was answered the way this predicted: the typed
view's default codec is MessagePack, and JSON is the opt-in. The hole is unchanged
and now sits behind a name somebody has to choose.
`TestInvalidUTF8SurvivesTheDefaultCodec` pins both halves — the default
round-trips invalid UTF-8 intact, and JSON named explicitly still does not.

---

## 42. A dropped asynchronous Delete can resurrect an older value — M9, resolved after M10

Found while designing M9's shedding, which has the same shape and is protected
against it; ordinary deletion is not.

Under asynchronous replication a `Delete` sends its replicas a tombstone through
the bounded replication queue. If the queue is full the tombstone is dropped and
counted (`ReplicationsDropped`). The owner has removed the key; the replica still
holds it at, say, version 5. A later `Set` of the key on the owner starts again at
version 1 (question 18), the replica refuses it as older, and if the owner then
dies the replica is promoted holding the value that was deleted *and then
overwritten*. The caller's last write is lost and an older one served in its
place.

It needs three things at once — a full queue, a rewrite of the deleted key, and
a failover before the key is written again — so it is rare. It is also exactly
what design principle 6 (docs/design.md) rules out: correctness depending on a message arriving.

**The fix is the mechanism M9 already built.** `store.Shed` raises a per-shard
version floor so a rewrite outranks any copy a replica may hold. Raising the same
floor in `store.Delete` would close this too. It was not done in M9 because it
changes question 18's documented behaviour for deletions, and
`TestARewriteAfterShedIsVersionedAboveWhatWasShed` currently pins that Delete
does not raise it.

**Picked for now:** recorded, not fixed.

**What would force it:** any deployment running asynchronous replication with a
non-zero `peercache_replications_dropped_total` and deletes followed by rewrites.

**Resolved after M10.** `store.Delete` now raises the shard's version floor, as
`store.Shed` does, so a rewrite of a deleted key starts above any version a
replica that missed the tombstone can hold. `TestALostDeleteDoesNotResurrectAnOlderValue`
reproduced it first — a key written five times, deleted while its owner was cut
off from its replica, rewritten as "new", and served as "old-5" by the replica
promoted after the owner died — and passes now. Question 18's restart remains
for an entry the sweeper reclaims, where every holder expires it alike.

---

## 43. EventEvict is exported, documented, and never emitted — post-M9, removed before v1

Found by the first test of `WithEventHandler`, which no test had configured.
`EventEvict` was documented as "emitted when a key is removed by TTL or capacity
pressure rather than by a caller". Nothing emits it. Events describe *group*
state — the keys a registered function sets on a `GroupState` — and group state
has no TTL and no capacity limit. M9's shedding removes namespace data, which
emits no events at all.

**Picked:** the same treatment as question 36's `ErrStateEscaped`. The constant
stays, because removing an exported constant breaks any caller that switched on
it; its documentation now says it is reserved and never emitted.
`TestEventHandlerSeesEveryLifecycleEvent` asserts the exact sequence a group's
life produces, so an emission added later is a visible change to that test.

**What would force a change:** group state gaining a TTL or a limit, at which
point the event has something to mean; or a request for events on namespace
data, which would be a new event source rather than this constant.

**Removed before v1**, with question 36's sentinel and for the same reason: the
compatibility it was kept for has no one to protect before the first public
release. An eviction event, if group state ever gains a TTL, is a new constant.

---

## 44. External persistence — post-M10, designed, not built

Asked whether PeerCache should write through to an external resilient store.
design principle 4 (docs/design.md) rules out writing to disk in v1 and allows persistence
hooks to be designed for; docs/design.md section 5 defers WAL, snapshots and backup.

**Picked:** a design, `docs/decisions/0026-external-persistence-design.md`, and
no code. **Superseded on 2026-11-13:** the project owner made persistence opt-in
with pluggable backends; `0029-persistence-as-a-segment-log.md` is the design
being built. It proposes a per-namespace `Persister` interface with write-behind as
the default and a key-coalescing queue, drivers in separate modules, and a
per-key order that survives ownership changes — because the in-memory `Stamp`
compares per-node epochs and cannot order writes across owners in a store that
outlives them.

**What would force building it:** a namespace whose data has no other source of
truth and cannot tolerate the loss window M5 measured — or a deployment where
M9's shedding of owned data, which persistence would turn into a reloadable miss,
is costing more than a database write per key.

---

## 45. A failed synchronous write skipped its invalidation — found in M13, resolved

`namespace.localSet` stores the value, fans out, and enqueues the invalidation
only if the fan-out succeeded. A synchronous write that cannot reach a replica
returns the error with the value already stored on the owner — by design, there
is no rollback — and no invalidation is sent, so other pods keep their L1 copy
of the old value until its TTL.

**Why it is survivable:** the TTL bounds it, as it bounds every lost
invalidation; correctness never depended on one arriving.

**Why it is still wrong:** the write happened. Skipping the hint makes a
degraded write staler than an asynchronous one, which is backwards. The fix is
to enqueue the invalidation before returning the fan-out error, in `localSet`,
the atomic operations and `Delete` alike, with a test that a failed synchronous
write still invalidates. Found while writing
`TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded`, which waited for an
invalidation that never came.

**Resolved:** all three now enqueue the invalidation before returning the
fan-out error. `TestAFailedSyncWriteStillInvalidates` covers `Set`, `Delete`
and `Increment` — the last standing for every atomic operation, which share one
write-out path — with an L1 TTL the test never reaches, so only the
invalidation can show the reader the new value. Each subtest failed before the
fix and has its own mutation entry.

---

## 46. The write path ranked replicas with an allocating sort — found in M13, resolved

`replicaTargets` calls `Table.Replicas` on every replicated write: a copy of the
membership, a reflective `sort.Slice` and a clone per member, measured at 394 ns
and 5 allocations at 7 nodes and 13 µs at 64 (docs/benchmarks.md, M13).
`Table.Holders`, added for replica reads, gives the same ranking for RF ≤ 3 in
140 ns and 525 ns with no allocation, and is tested to agree. Moving the write
path to it is a contained change with a measurable benefit at large
memberships; it was left out of M13 to keep that milestone to reads.

**Resolved:** `replicaTargets` uses `Holders` and clones only the targets it
keeps. `BenchmarkReplicatedSet` (tests/, asynchronous RF 3, medians of five):

| | before | after |
|---|---:|---:|
| 7 nodes | 1,608 ns, 3,655 B, 32 allocs | 748 ns, 2,224 B, 19 allocs |
| 32 nodes | 3,737 ns, 7,278 B, 60 allocs | 898 ns, 2,378 B, 21 allocs |

More than the ranking alone accounts for, and the reason is visible in the
allocation count: `Replicas` cloned every member, so the old path's cost grew
with the cluster; the new one is nearly flat. Catch-up, promotion and the
rebalance planner still call `Replicas`. They run per membership change rather
than per write, and catch-up takes its replication factor from the wire, which
`Holders` would clamp.

---

## 47. An internal sentinel escaped `Do` under concurrent refusals — found in M13, resolved

`actor.Runtime.Do` retried once when the record it looked up had been removed
by a refused activation on another goroutine. The fresh record could be removed
the same way before the retry reached it, and then `errRecordRemoved` — an
internal "look again" signal — was returned to the caller.
`TestRecordRemovalIsSafeUnderConcurrentCallers` caught it about twice in two
hundred race-detector runs, and the M13 race gate is where it first surfaced.

**Fix:** the retry is a loop, bounded by the caller's context. Each pass means
another caller's activation was refused, so it is progress, not a spin. A
thousand runs under `-race` pass where two hundred failed twice. There is no
mutation entry: reverting to a single retry fails one run in a hundred, and an
audit entry that is caught by chance is the vacuous kind the audit exists to
expose.

---

## 48. Objects per flush, and what an object store charges for them — found in M14c, resolved

A flush writes one object per partition it touches. M14c measured what that
costs on a disk: durable writes overlap only about threefold on Machine A, so a
flush touching all of 256 partitions took 40–600 ms, past its own 100 ms
interval. The partition count was cut to 64 (about 5 ms), and a flush's segments
are now written sixteen at a time instead of one after another.

On an object store the same arithmetic is a bill. A loaded node touching every
partition each flush makes 64 PUTs per flush, ten flushes a second: 640 requests
a second per node, which at S3's published price per thousand PUTs is real money
for a cache. Fewer partitions trade restore granularity; a longer interval
trades the loss window.

**Picked for now:** nothing, because the disk backend does not have the problem.
**Must be decided before M14d ships:** a per-namespace flush interval (an object
store wants seconds, not milliseconds), and possibly one object per flush holding
every partition, indexed, instead of one per partition. Measure the PUT rate of a
loaded node before choosing.

**Resolved by decision 0030,** the project owner's choice: a flush writes one
log object whatever it touches, a per-namespace splitter moves logs into
partition segments every ten seconds, and the flush interval is a namespace
option. Measured: a thousand keys over every partition flush as one request
(`TestAFlushWritesOneObject`); a split writes at most one segment per partition
(`TestASplitMovesLogsIntoPartitions`).

## 49. A node restarted faster than failure detection — found in M16

**Found** by M16's kill -9 on kind. A SIGKILLed pod was back in two seconds,
same name, same address; the failure detector never fired, so no peer's epoch
moved and nothing was promoted. The restarted owner came back empty to keys it
still owned, and `CatchUp` asked each peer only for the keys *that peer* owns.
The copies of the restarted node's own keys sat in its peers' replica stores,
unasked for. 60 acknowledged orders were missing or older after the run, at
synchronous RF 3 with persistence — and without persistence it would have been
every key the node owned.

**Resolved, part one:** catch-up now makes a second pass per peer for its
replica copies of keys the asker owns (`CatchUpRequest.Replicas`). They arrive
through the replica store's stamp check and are promoted by the ordinary
reconciliation, which refuses anything older than what the node already holds
— so a value restored from persistence, or written since the restart, is not
overwritten by an older copy. `TestANodeRestartedFasterThanDetectionRecoversWhatItOwns`.

**Open, part two: the window before catch-up.** Between the restart and the end
of its first catch-up, the node owns its keys and its peers route to it — the
readiness gate keeps *clients* away, not peers forwarding writes. A write in
that window lands on an empty or persisted-but-stale base: an `Increment` counts
from the wrong value, a `SetIfAbsent` of an existing key succeeds again. And a
version restarted from the store's floor can be older than the replica copy
catch-up then brings, so promotion discards the acknowledged write instead.

Options:

1. **Refuse owner operations until the first catch-up completes**, with
   `ErrActivationPending`, which every caller already retries. Correct, and the
   readiness gate already waits for the same event; but a node whose
   application never calls `CatchUp` would refuse its keys forever.
2. **Make the restart visible**: a per-process boot ID in gossip metadata, so a
   peer that sees the same name with a new boot ID treats it as a departure and
   an arrival — epochs move, the activation delay applies, promotion runs.
3. **Seed versions from the replicas** on first touch. Fixes ordering, not the
   stale base.

Leaning towards 1, measured before deciding: M16's next kill -9 run says how
often the window is hit under load.
