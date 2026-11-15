# Verification record

docs/design.md's Definition of Done requires that each milestone's acceptance
criteria be "covered by a named test that would fail if the behavior
regressed."

A test that exists is not evidence of that. A test can pass because the
behaviour is correct, or because it never really looked. The only way to tell
them apart is to break the behaviour on purpose and check that the test
notices — and to record the result, so the claim is auditable rather than
asserted.

This file records those checks. Each mutation was applied, the test run, and the
mutation reverted; `git diff` was used to confirm nothing was left behind.

---

## M0

| Criterion | Test | Mutation | Result |
|---|---|---|---|
| 5 nodes, 3/2 partition, +1h, heal, assert membership, <50ms, zero sleeps | `TestM0Acceptance` | — | see below |
| Harness leaks no timers | `Cluster.Close` guard | `Membership.Close` no longer stops transition timers | **caught** — `timer leak: 4 timer(s) still armed after teardown` |
| Delivery timers do not outlive the network | `TestNetworkCloseRacesWithDelayedDelivery` | hand-over `t.Stop()` removed from `Network.schedule` | **caught** on iteration 0, every run |

---

## M1

| Criterion | Test | Mutation | Result |
|---|---|---|---|
| **Local Get p99 under 1 µs at 1M keys** | `TestLocalGetP99AtOneMillionKeys` | ~1 µs of busy work added to `Store.Get` | **caught** — `p99 3.316µs exceeds the 1µs budget`, p50 rose 338 ns → 2.703 µs |
| **Correct TTL expiry under a fake clock** | `TestTTLExpiryUnderFakeClock`, `TestExpiryIsObservedOnRead` | read-path expiry check disabled | **caught** by both — `the key survived its deadline` |
| **1000 concurrent misses invoke the loader exactly once** | `TestGetOrLoadCollapsesConcurrentMisses` | singleflight bypassed; loader called directly | **caught** — `1000 callers entered GetOrLoad but only 0 are inside a single flight` |

Supporting invariants, same method:

| Invariant | Test | Mutation | Result |
|---|---|---|---|
| A concurrent `Put` never leaves an older version resident | `TestConcurrentPutsNeverLeaveAnOlderVersion` | L1 stripe lock removed from `Put` | **caught** within 3 rounds |
| Expiry-delete does not drop a fresh `Put` | `TestExpiryDeleteDoesNotDropAFreshPut` | stripe re-check removed from the expiry path | **caught** within ~10 rounds |

---

## What the third M1 mutation exposed

The singleflight bypass initially made
`TestGetOrLoadCollapsesConcurrentMisses` **hang for the full 300-second test
timeout** rather than fail.

The test spun waiting for `Stats().InflightLoads` to reach 1000. With
collapsing bypassed that counter never moves, so the spin never ended. The test
did technically fail — by timing out — but a timeout is close to the worst
failure mode available: it costs the entire timeout budget, blocks every test
after it, and reports nothing about which invariant broke.

Fixed by separating two conditions that had been conflated:

- **Callers have entered `GetOrLoad`** — always reachable, since it is only
  goroutine start-up. Spun on without a bound.
- **Callers are inside a single flight** — the property under test. Spun on with
  a bound, and a diagnostic on failure.

The same mutation now fails in 23 seconds with:

```
1000 callers entered GetOrLoad but only 0 are inside a single flight
after 5000000 yields; concurrent misses are not collapsing
```

This is worth stating plainly: the mutation was aimed at the production code and
found a defect in the test instead. That is a normal outcome and an argument for
doing it at all — an acceptance test whose failure mode is a hang is a test
nobody will trust at three in the morning.

---

## M3 — what running the audit twice exposed

Two consecutive runs of the same script against the same tree disagreed:
`caught: 32  vacuous: 3`, then `caught: 33  vacuous: 2`. A mutation audit that
is not deterministic is not evidence, so the disagreement was the finding.

**Two tests were flaky, in the same way.** Both asserted state immediately after
`fc.Advance(...)`. Advancing a fake clock only *delivers* a tick; the turn
goroutine acts on it afterwards, so reading `Stats()` on the next line samples
whichever moment the scheduler happened to pick. Under the unmutated code they
passed every time, because the state they sampled was correct either way. Under
the mutation they passed roughly a third of the time.

`TestReschedulingATimerReplacesIt` was fixed by round-tripping a call through
the mailbox, which proves the goroutine has been round its loop.

`TestDeactivationPreservesPendingTimers` needed more than that, and the reason
is worth recording. A round trip does not order *after* the idle tick: the turn
loop selects between a ready mailbox and a ready idle timer, and Go leaves that
choice unspecified. So the fix was to stop trying to observe an absence. The
runtime now counts idle deactivations it declines because a timer was pending —
`Stats().TimerHeldOpen` — and the test waits for that counter, or for
`Active == 0`, whichever comes first. Both outcomes are positive signals, so the
test resolves deterministically and reports rather than timing out.

The general shape: **"X did not happen" is not observable by sampling.** A group
that has not deactivated looks exactly like a group that is about to. Count the
decision instead of looking for its absence.

**One mutation was wrong, and the code comment was wronger.** "Membership order
leaks into ownership" mutated away the sort in `Table.Update` and expected
`TestOwnerIsDeterministicAcrossMemberOrder` to fail. It did not, correctly:
`owner()` takes a maximum, which is order-independent, so nodes agree whatever
order members arrive in. The sort buys a deterministic tie-break and a stable
`Replicas` ranking, nothing more — but the doc comment on `Update` claimed the
sort was what made two nodes agree.

The mutation was aimed at production code, found a false statement in a comment,
and was replaced by one that targets what the sort actually does. Comments are
not tested by anything else.

### A goroutine leak that only one test could see

`Gossip.consume` ranged over the channel memberlist delivers node events on.
memberlist never closes that channel — not on `Shutdown`, not ever — so the
goroutine waited forever. One leak per node, for the life of the process.

Every membership test missed it, and they missed it structurally rather than by
oversight: each constructs a node, asserts something, and exits. A goroutine
that leaks *after* the assertion cannot fail the assertion.

What found it was the M1 p99 acceptance test, which has nothing to do with
membership. Adding `tests/cluster_test.go` to the root package put four gossip nodes
in the same binary; under `-count=2`, the goroutines leaked by the first
iteration pushed the second iteration's p99 from 0.5µs to 1.475µs and broke the
1µs budget.

Two things are worth taking from that.

**The `-count=2` gate earned its place.** docs/design.md requires it to catch state
leaking between runs, and this is exactly that: a defect visible only on the
second pass, in a package that did not contain it.

**A budget in an unrelated test is a real detector.** The p99 assertion was not
written to find goroutine leaks. It found one because a leak is a resource cost,
and a tight latency budget is sensitive to resource costs whatever their origin.
That is an argument for asserting on numbers rather than logging them.

The fix signals a stop channel and *waits* for the goroutine to close a done
channel, so that when `Close` returns the goroutine is provably gone rather than
merely asked to leave. `TestCloseLeavesNoGoroutines` now guards it, and was
itself verified by reverting the wait: it fails, taking 30 seconds to do so, and
reports rather than hanging.


---

## M4 — what the fuzzer and the first real network found

M4 added the first code that reads bytes another machine wrote, and the first
tests that use real sockets. Both classes found defects that nothing else could
have.

### A length prefix that bought an allocation

`FuzzDecode` was written for the new message parser and failed in **under two
seconds**: two bytes decoded into ten entries.

msgpack writes an array as a length followed by its elements, and the decoder
allocated the whole slice from that length before reading a single element. A
probe confirmed the scale — a **six-byte** input wedged the process for the full
two-minute test timeout, trying to allocate two billion structs. Over a network
that is an unauthenticated remote out-of-memory: one corrupt frame, every node
that receives it.

The library has a guard for exactly this and it cannot be reached.
`decode_slice.go` computes `noLimit := d.flags&disableAllocLimitFlag != 1`, and
since that flag is not bit zero the expression is true whatever the flag is set
to — so `DisableAllocLimit` cannot switch the limit on, and maps and byte
strings are protected while slices of structs are not.

Rather than wait for that to be fixed upstream, the decoders in
`internal/wire/bounded.go` append as elements arrive. A declared length is
treated as a statement of intent, never as an instruction to allocate, so the
cost of a malformed message is proportional to the bytes actually sent.

The regression test asserts the *allocation*, not just the error: a decoder that
rejected the message after allocating would still be exploitable. Its cases are
an ordered slice with the modest one first, because reverting the fix makes that
case fail in milliseconds having allocated four megabytes — which reports —
while the enormous cases would thrash for minutes, which does not. The crasher
`` is a permanent corpus entry and `FuzzDecode` runs 60s in CI.

### A best-effort hint that had become the write path's hardest dependency

Invalidation was broadcast inline, on the writing caller's context. On the test
fabric a dropped message looks like silence rather than an error
(`docs/decisions/0004`), so an unreachable peer did not fail the send — it
simply never replied. A caller passing `context.Background()`, which is the
ordinary thing to do, blocked in `Set` **forever**.

A latency optimisation had become an unbounded dependency of every write:
precisely the inversion design principle 6 (docs/design.md) exists to prevent, on a path
that could block past any deadline because it had none.

It is now a bounded queue. Enqueueing never blocks and never fails; a full queue
drops the hint and counts it. The property is structural rather than
aspirational — correctness cannot depend on something the writer never waits
for.

Fixing it exposed a second defect immediately: `Close` waited on the delivery
goroutine, whose per-peer timeout is measured on the *injected* clock, which
under a fake clock never advances. Signalling was not enough; `Close` now
cancels a parent context so an in-flight send returns at once.

### An epoch check that could never have worked

The handler originally refused any request whose caller quoted a different
membership epoch. It looked principled. A three-node test then failed with a
reader at epoch 6 refused by an owner at epoch 4 — **and both were right**.

An epoch counts the transitions *one node* has observed. `core.Epoch`'s own
documentation says epochs are per-view and that comparing them "establishes
staleness, never consensus". Two nodes that agree exactly about who is in the
cluster can hold different epochs, because they counted different numbers of
intermediate views getting there. Demanding equality across a hop therefore
fails requests between nodes that agree, and once the counters have drifted it
fails them permanently.

The check is now advisory and the skew is counted. Safety comes from
`assertOwned`, which asks a question answerable locally: does *this* node own
this key under *its own* current view.

### Two sockets, one address

The first real-network run failed with `error reading server preface: EOF` —
what a protocol looks like when it reaches the wrong listener.

`membership.New` overwrote `Node.Addr` with memberlist's own address, so the
ownership table told the data path to dial the SWIM port expecting gRPC. The
gossip socket and the data socket are different sockets; the RPC address now
travels in gossip metadata beside the epoch, with a fallback to the gossip
address so a rolling upgrade from a build that does not advertise one still
works.

Nothing in the deterministic suite could have caught this: the in-process fabric
gives each node exactly one address, so the distinction does not exist there.
That is the argument for keeping a small number of real-socket tests despite
their cost.

### An mTLS test that proved nothing

Recorded in full under M3's heading in spirit, but it happened here. The first
"rejects an unknown CA" test passed even with the server downgraded to
`RequireAnyClientCert`. In TLS 1.3 the server's CertificateRequest advertises
its trusted CA names and Go's client will not send a certificate that cannot
chain to one, so the handshake died at "client didn't provide a certificate"
before the server formed an opinion. A test named for server-side verification
was measuring client-side reticence.

It now uses an impostor CA carrying the *same subject name* as the real one,
which forces the certificate onto the wire so the server must reject it on the
signature — also the realistic attack, since a CA's distinguished name is not a
secret. The behaviour the old test accidentally measured is pinned separately
and named for what it is.

---

## M5 — three bugs the tests found and review would not have

M5 added replication, and every defect in it was found by a test failing rather
than by reading the code. Two of them were invisible to any single-node test and
one was invisible to any test that did not kill a process.

### Remote writes were never replicated

The peer handler wrote straight to the store, bypassing fan-out and invalidation
entirely. Only keys written *through their own owner* got copies — in a
five-node cluster, roughly a fifth of them.

`TestKillingAnOwnerPromotesAReplica` found it by failing to read back two thirds
of the keyspace after a kill. The fix was to route a remote write through the
same `localSet` a local write uses, which is the general lesson: two write paths
that are supposed to behave identically will not, and the one used less is the
one that breaks.

Worth noting how nearly this was missed. The first version of that test killed
the node it had written *through*, whose keys were written locally and therefore
replicated correctly. A mutation exposed that: disabling remote replication left
the test passing. The test now kills a non-writer.

### The promoter raced the ownership table

The promoter watched membership on its own, alongside the guard, and two
independent watchers of one authority have no ordering between them. The
promoter could reconcile before the table held the new view: it saw the dead
node still owning its keys, promoted nothing, and then waited forever for an
event that had already been delivered.

The symptom was a test that hung. The cause was a design that assumed the
scheduler would be kind. Promotion now runs from `Guard.OnUpdate`, after the
table is current by construction.

### A restarted node owns keys it does not have

The worst of the three, and the one with operational consequences rather than
merely correctness ones.

Replication is a stream of writes, not a state transfer. A node that restarts
receives only what is written after it returns — while the ownership ranking
hands it back its keys immediately. It then answers `ErrNotFound`, with total
confidence, for data sitting safely on the node that covered for it.

In the chaos run that presented as **558 of 600 keys lost, none of them actually
gone**. A caller cannot tell that apart from a genuine miss, which makes it
worse than the node being unavailable.

`Cache.CatchUp` already existed and nothing called it. What the chaos test
established is that calling it is a *required* step in the restart procedure
rather than an optimisation — a distinction that only shows up when processes
actually die. It is now stated in the README guarantees, `docs/api.md`, decision
0015, and demonstrated in `examples/replication`.

### And two tests that were wrong rather than the code

An eviction test asserted that killing a node causes survivors to drop replicas.
It never happens: removing a node only moves keys *up* the ranking, so survivors
gain copies or get promoted and none are pushed out. Eviction happens on *join*.
The test was replaced with one asserting the invariant that does hold.

A promotion test claimed to check that a promoted key keeps its version
sequence, while only comparing values. A mutation restarting the sequence at 1
passed it. The test was renamed for what it can actually see, and the version
property is asserted where it is visible — at the store level, where the
mutation is caught.

### The audit deleted a guarantee and shipped it

The worst thing that happened in M5 was done by the verification tooling itself.

`internal/actor/record.go` was committed carrying the audit's own mutant for "a
full mailbox blocks instead of rejecting". The real code refuses work when a
group's mailbox is full and returns `ErrGroupOverloaded`; the mutant blocks
instead, which converts one slow group into a cluster-wide stall — the exact
failure the bounded mailbox exists to prevent.

An audit run died between applying that mutation and reverting it. A later
`git add -A` swept the mutant into a commit, and it survived **three commits**
before anything noticed.

**What caught it was `go test -race -count=2` hanging for 1201 seconds.** Not
review — the diff looked like a plausible simplification. Not the audit, which
reported 77 of 77 caught and a clean tree, because it reverts each mutation
before evaluating the next and so cannot see one it left behind earlier.

Three things follow, and they are worth more than the bug was.

**The script now reverts on any exit.** A `trap cleanup EXIT INT TERM` restores
every file it has touched, whether the run finishes, is interrupted, or crashes.
`git checkout --` on a clean tree is a no-op, so the guard costs nothing.
Verified by killing a run mid-mutation and confirming a clean tree afterwards.

**Every deletion since the previous milestone was audited by hand.** The stray
mutant was the only one; the rest are accounted for as deliberate changes.

**A tool that rewrites source is a hazard, not merely a convenience.** This one
has found a hung test, a vacuous test, two flaky tests, a bad mutation, an
incorrect comment, dead code, and now it has silently removed a guarantee from
shipping code. It stays — the evidence it produces is worth the risk — but the
risk is real and is written down here rather than remembered.

---

## M6 — a bug that was not in the library

The hardest defect of the milestone took most of a day and turned out not to be
a defect in the code at all. It is recorded because the *shape* of it recurs:
silent under-configuration that reports success.

### The symptom

`examples/rolling-upgrade` hard-killed one node of five at replication factor 3 with
synchronous replication, and 53 of 300 keys became permanently unreachable —
`ErrNotFound`, not deferred. A node failure at RF=3 is supposed to be
survivable by definition, so this read as a serious replication bug.

### The cause

A write routed to its owner creates the namespace there if that node's
application has not opened it, using *default* options — replication factor 1.
Namespace options are fixed at creation and silently ignored afterwards, so such
an owner replicates nothing for that namespace, permanently.

In the example, one of five nodes had opened the namespace. The other four
created it implicitly on their first routed write. The cluster placed **68 of the
400 copies RF=3 implies — 17%** — and every write returned success, because
every write *had* succeeded.

### How it was eventually found

By bisecting, after a long detour. What settled it was printing the replication
counters per node: only one node had sent anything, which took one command and
was unambiguous. Everything before that was reading code and reasoning about
what should happen.

### Three process failures worth more than the bug

**A fix that silently did not apply.** The correct cause was identified early, a
fix was written, the behaviour did not change, and the hypothesis was discarded.
The fix had never been applied — a string replacement whose anchor does not
match is a no-op, not an error. This is the third time in this project and the
second time it sent an investigation down a wrong path. Every generated edit is
now verified by reading the file back before drawing any conclusion from the
result.

**Announcing a root cause before confirming it.** Twice. Both times the next
measurement contradicted it.

**Reasoning where measuring was available.** The empirical bisect — remove one
step, re-run, see if the symptom survives — worked immediately every single time
it was used, and was reached for last rather than first.

### The resolution

The creation still happens; refusing it would break routed writes to a node
during legitimate startup ordering. What changed is that it is now *visible*:
`Stats().ImplicitNamespaces` counts it and each occurrence logs the namespace
and the replication factor it is stuck with.
`TestAnImplicitNamespaceIsCountedAndReplicatesNothing` asserts both the
shortfall under the misconfiguration and the exact expected count when every
node is configured.

The operational rule — open every namespace, with the options you want, on every
node, before serving — is now in the README guarantees and is the first of the
things `deploy/README.md` says must be right.

---

## M7 — the audit had been lying, and a panic nobody had looked for

M7's own code was the least interesting thing M7 found. Three of the four
findings below are in work that was already called complete.

### The mutation audit had silently skipped an entry since M6

`scripts/mutation-audit.sh` contained an orphaned argument list: a `mutate`
entry whose `mutate` line had been lost in an edit, leaving the arguments as a
top-level command of their own. The script runs under `set -u` but not `set -e`,
so bash printed "command not found" to a stderr the run discards and carried on
to the next entry. The audit reported its usual clean summary. The entry was
M6's acceptance criterion.

This is the second time this script has caused real damage, and both failures
were silence rather than error — the first was a crashed run leaving a mutant in
the tree that shipped for three commits. A verification tool that fails open
produces the paperwork of coverage without the coverage.

### And the entry would not have caught anything anyway

Reconstructing the missing mutation was worse news than losing it. Two separate
mutations to `Drain` both left `TestRollingUpgradeLosesNoRequests` passing:

| Mutation | Result |
|---|---|
| Delete the handoff from `Drain` entirely | **VACUOUS** — test passed |
| Transfer *before* announcing departure, reversing the order drain.go's comment calls "the whole design" | **VACUOUS** — test passed |

The reason is the test's own configuration. It runs at replication factor 2 with
synchronous replication, so every key already has a second copy and losing the
transfer costs it nothing it measures. It is a good test of replication. It was
never a test of `Drain`, and it had been standing in for one.

Fixed by two tests at replication factor 1, where the handoff is the only copy:

- `TestDrainHandsOffKeysNothingElseWouldSave` — 300 keys, no replicas, drain the
  node that owns a third of them, then read every key from a survivor.
- `TestDrainAnnouncesDepartureBeforeTransferring` — records the order of the two
  calls `Drain` makes to two different collaborators. Replication factor 1 does
  double duty here: it means the *only* `Replicate` calls the cluster ever makes
  are the handoff's, so the recorder cannot confuse them with ordinary writes.

Both were confirmed to fail against their mutations before being committed. The
ordering assertion needed the fabric to grow transport and membership wrappers,
because "which of these two happened first" is not recoverable from any state
either collaborator holds afterwards.

### A nil context panicked at all eighteen API boundaries

Found by accident, by a throwaway program that wrote `defer c.Close(nil)` and
died inside the actor runtime. Every context-taking method in the public API
opens by calling `ctx.Err()`; `context.Context` is an interface, so a nil one
makes that first call a nil-pointer dereference.

Eighteen exported methods, seven milestones, no test had ever passed a nil
context — because nobody writes that on purpose. docs/design.md forbids panics in
library code paths and makes no exception for caller error, which is the right
call for something embedded in someone else's process.

Resolved by normalising nil to `context.Background()` at each of the eighteen.
`TestNilContextDoesNotPanic` covers all of them exhaustively rather than
representatively: a guard applied to seventeen of eighteen is not a guard.
`TestNilContextBehavesLikeBackground` is the other half — it checks the
normalisation is transparent rather than merely non-fatal, because a guard that
swallowed the operation would also stop the panic and would be far worse.

### A histogram test that proved less than it looked

`TestQuantilesBoundTheTruth` never observes a sample above the top bucket bound,
so deleting the overflow bucket from `Observe` left it passing. `Quantile`
returns the last bound whether it reached it through the buckets or by falling
off the end of them, which is why the gap was invisible.

The difference shows in `Snapshot`. Prometheus requires the `+Inf` bucket to
equal the observation count, and a scraper seeing otherwise rejects the series
as malformed — so a lost overflow bucket does not merely omit a sample, it
invalidates the whole histogram. `TestTheInfBucketMatchesTheCount` asserts that,
and that the last finite bucket has not swallowed the overflow instead.

### A fix applied to one package and not the other, latent since M6

The `-count=2` gate failed on `TestTwoNodesAgreeOnOwnership` and
`TestJoiningNodeDefersNewlyAcquiredGroups`:

```
Failed to start UDP listener on "127.0.0.1" port 51029: bind: An attempt was
made to access a socket in a way forbidden by its access permissions.
```

That is the Windows reserved ephemeral range (50510-51109 on this machine),
which M6 already diagnosed and fixed — in `internal/membership`, whose tests got
a bounded retry. The root package's `tests/cluster_test.go` builds its gossip nodes
through its own helper and never received it. Two tests, binding real sockets
through a second code path, latent for a milestone.

This is the same shape as the nil-context finding two sections up, and the same
shape as the `//nolint` comments in the first draft of its test: a correct fix
applied to some of the places that needed it. The remedy is not more care. It is
one exported helper — `testkit.RetryBind` and `testkit.IsPortRefused` — that
both packages now call, so the next caller inherits the fix rather than
rediscovering the bug.

### And the M7 example was reporting two numbers it had not earned

`examples/observability` printed `peercache_replications_sent_total 573` next to the
explanation "RF=2 over 200 keys wants 200". The counter is cumulative across
every namespace, so it was including the healthy namespace's traffic in a figure
presented as the broken one's shortfall. Measuring the delta gives 73 of 200,
which matches M6's finding.

It also printed `p50 100ns p99 100ns p99.9 100ns` as though that were a
measurement. The example runs on a simulated clock that only moves when the code
moves it, so every operation appears to take no time and lands in the first
bucket. The example now says so and points at this document's benchmark figures
for real ones.

Neither was a library defect. Both would have been read as evidence.

---

## Automated audit

`scripts/mutation-audit.sh` applies each mutation below, runs only the test that
claims to guard it, and restores the file. It refuses to start on a dirty tree —
it reverts with `git checkout --`, which would otherwise discard uncommitted
work — and fails if any mutation is left behind or any test turns out to be
vacuous.

Latest run, all ninety-nine invariants:

```
caught   Set no longer copies the caller's buffer                           -- TestSetCopiesTheCallersBuffer
caught   Get copies instead of aliasing                                     -- TestGetReturnsTheStoredSlice
caught   read-path expiry disabled                                          -- TestExpiryIsObservedOnRead
caught   shard selection collapses to one shard                             -- TestKeysSpreadAcrossShards
caught   TTL deadline overflow reintroduced                                 -- TestUnrepresentableTTLDoesNotExpireImmediately
caught   L1 accepts an older version                                        -- TestOlderVersionIsRejected
caught   L1 stops clamping TTL to MaxTTL                                    -- TestEveryEntryIsTTLBounded
caught   L1 write stripe removed                                            -- TestConcurrentPutsNeverLeaveAnOlderVersion
caught   frame parser accepts an unknown code                               -- TestUnframeRejectsCorruptInput
caught   namespaces share one store                                         -- TestNamespacesAreIsolated
caught   singleflight bypassed                                              -- TestGetOrLoadCollapsesConcurrentMisses
caught   store version stops incrementing                                   -- TestVersionsIncrementPerKey
caught   L1 invalidation ignores the resident version                       -- TestInvalidateOlderThanKeepsNewerCopies
caught   loader panic is no longer contained                                -- TestPanickingLoaderBecomesAnError
caught   completed flights are never released                               -- TestSequentialCallsDoNotCollapse
caught   fake clock drops the creation-order tie-break                      -- TestFakeEqualDeadlinesFireInCreationOrder
caught   namespace skips the context check                                  -- TestCancelledContextIsReportedFirst
caught   loader runs without the namespace deadline                         -- TestLoaderTimeoutIsTheNamespacesNotTheCallers
caught   escaped group state is honoured instead of ignored                 -- TestEscapedStateIsInertAndCounted
caught   a full mailbox blocks instead of rejecting                         -- TestFullMailboxRejectsRatherThanBlocking
caught   a group function panic is no longer contained                      -- TestPanickingGroupFuncDoesNotWedgeTheGroup
caught   deactivation discards group state                                  -- TestDeactivationPreservesState
caught   deactivation discards the timer schedule                           -- TestDeactivationPreservesPendingTimers
caught   a group with pending timers is idle-deactivated                    -- TestDeactivationPreservesPendingTimers
caught   the active-group cap is ignored                                    -- TestActiveCapEvictsLeastRecentlyActivated
caught   rescheduling a timer name accumulates duplicates                   -- TestReschedulingATimerReplacesIt
caught   the Items snapshot aliases group state                             -- TestItemsSnapshotDoesNotAliasGroupState
caught   ownership ignores the node identity                                -- TestOwnershipIsUniform
caught   ownership hashes with a bare XOR                                   -- TestOwnershipIsUniform
caught   the replica ranking is unsorted                                    -- TestReplicasAreARankedPrefix
caught   an unknown membership claims everything                            -- TestGuardWithNoMembershipOwnsNothing
caught   the guard activates groups it does not own                         -- TestGuardRefusesGroupsItDoesNotOwn
caught   newly acquired groups activate immediately                         -- TestGuardDefersOnlyNewlyAcquiredGroups
caught   the guard compares against the previous view, not the settled one  -- TestPartitionedMinorityStopsActivating
caught   a stale epoch is accepted                                          -- TestStaleEpochIsRefused
caught   a caller ahead of this node is not deferred                        -- TestFutureEpochDefersRatherThanRefusing
caught   the epoch is checked before ownership                              -- TestNotOwnerBeatsStaleEpoch
caught   local activation does not use the current epoch                    -- TestCanActivateUsesTheCurrentEpoch
caught   gossip metadata updates advance the epoch                          -- TestEpochDoesNotRunAway
caught   the gossip event goroutine is never released                       -- TestCloseLeavesNoGoroutines
caught   the guard is built but never consulted                             -- TestTwoNodesAgreeOnOwnership
caught   the guard never learns the membership                              -- TestJoiningNodeDefersNewlyAcquiredGroups
caught   a length prefix buys an allocation                                 -- TestALengthPrefixCannotBuyAnAllocation
caught   the L1 tier compares versions, ignoring the epoch                  -- TestANewOwnerSupersedesAHigherVersion
caught   invalidation compares versions, ignoring the epoch                 -- TestInvalidationRespectsTheEpoch
caught   the epoch does not dominate the version                            -- TestStampOrdering
caught   the transport codec drops the body                                 -- TestRoundTripOverLoopback
caught   a caller does not identify itself                                  -- TestRoundTripOverLoopback
caught   sentinel errors do not survive the hop                             -- TestSentinelErrorsSurviveTheHop
caught   Unavailable is read as a deferred activation                       -- TestUnavailableDefaultsToUnreachable
caught   connections are not pooled                                         -- TestDialPoolsConnections
caught   the server does not verify client certificates                     -- TestMutualTLSRejectsAnImpostorCA
caught   the client does not verify the server                              -- TestMutualTLSRejectsAnUntrustedServer
caught   the TLS floor drops to 1.0                                         -- TestTLSFloorIsEnforced
caught   the router batches per group rather than per owner                 -- TestRouterIssuesOneRequestPerOwner
caught   a group batch is applied one message at a time                     -- TestRouterSubmit
caught   every node runs its own loader                                     -- TestTheLoaderRunsOnlyOnTheOwner
caught   gossip advertises the SWIM port as the RPC address                 -- TestGossipAdvertisesTheRPCAddress
caught   enqueueing an invalidation blocks when the queue is full           -- TestInvalidationBacklogIsDroppedNotQueued
caught   an older replica copy overwrites a newer one                       -- TestAnOlderCopyIsRejected
caught   the replica store compares versions, ignoring the epoch            -- TestALaterEpochWinsWhateverTheVersion
caught   a fork is not counted                                              -- TestTwoWritersFromOneBaseIsAFork
caught   a handover is counted as a fork                                    -- TestAHandoverIsNotAFork
caught   a rolling upgrade's handovers are counted as forks                 -- TestAGracefulRollingUpgradeCountsNoFork
caught   the L1 bound counts entries, not bytes                             -- TestMaxBytesIsBounded
caught   a namespace's L1 bound never reaches the tier                      -- TestTheL1TierIsBoundedInBytes
caught   an L1 bound of zero is accepted                                    -- TestAnL1BoundMustBePositive
caught   a remembered absence is never consulted                            -- TestARememberedAbsenceSavesTheLoad
caught   every loader error is remembered as an absence                     -- TestOnlyAnAbsenceIsRemembered
caught   a Delete does not forget an absence                                -- TestEveryWriteForgetsAnAbsence
caught   a Set does not forget an absence                                   -- TestEveryWriteForgetsAnAbsence
caught   an atomic update does not forget an absence                        -- TestEveryWriteForgetsAnAbsence
caught   an absence is recorded over a write that landed during the load    -- TestAWriteDuringTheLoadIsNotHiddenByItsAbsence
caught   absences grow without bound                                        -- TestAbsencesAreBoundedPerShard
caught   an owner does not load the keys a batch is missing                 -- TestABatchLoadCostsOneLoaderCallPerOwner
caught   the batch loader is never called                                   -- TestABatchLoadCostsOneLoaderCallPerOwner
caught   a load request goes out without load                               -- TestABatchLoadCostsOneLoaderCallPerOwner
caught   a batch load stores over a write that landed during it             -- TestAWriteDuringTheBatchLoadWins
caught   a key the batch loader omitted is stored empty                     -- TestKeysTheBatchLoaderOmitsAreAbsent
caught   a failed batch load is reported as success                         -- TestAFailingOwnersShareIsMissing
caught   a misrouted key in a load looks like a missing one                 -- TestAMisroutedLoadNamesTheKeys
caught   a batch get from the earlier release cannot be read                -- TestABatchLoadRequestCrossesARollingUpgrade
caught   a batch load asks for keys remembered as absent                    -- TestABatchLoadRemembersAndSkipsAbsences
caught   a batch load does not remember the keys it omitted                 -- TestABatchLoadRemembersAndSkipsAbsences
caught   a demoted owner keeps its keys in the owned store                  -- TestADemotedOwnerDoesNotResurrectAnOverwrittenValue
caught   a namespace with a loader is not marked                            -- TestLoadedKeysAreMarkedNotMissing
caught   an owner's operations are not counted                              -- TestTheHottestKeyIsReportedFirst
caught   hot-key counts never decay                                         -- TestHotKeysDecay
caught   a new key does not inherit the replaced count                      -- TestAKeyThatTurnsHotLateIsFound
caught   estimates are not scaled by the sample rate                        -- TestTheHottestKeyIsReportedFirst
caught   hot-key tracking accepts any bound                                 -- TestHotKeysAreOffByDefaultAndBounded
caught   a restore past its deadline installs the key                       -- TestRestoreAtInstallsTheDeadlineAsGiven
caught   a restore holds the flush lock while it reads                      -- TestAFlushIsNotHeldUpByARestore
caught   a restore ignores what was flushed while it read                   -- TestADeleteFlushedDuringARestoreIsNotUndone
caught   a flush during a restore keeps nothing for it                      -- TestADeleteFlushedDuringARestoreIsNotUndone
caught   a restore ignores the batch being flushed                          -- TestADeleteBeingFlushedDuringARestoreIsNotUndone
caught   a compaction holds up the flusher                                  -- TestAFlushIsNotHeldUpByACompaction
caught   a budget without a memory limit is not warned about            -- TestABudgetWithoutAMemoryLimitWarns
caught   a drained node applies its own callers' writes                     -- TestADrainedNodeAppliesNoWrite
caught   a local caller skips the activation delay                          -- TestANewOwnerCountsFromItsPromotedCopy
caught   a restore gives up on a log that moved                             -- TestARestoreOutlastsASplitterDeletingItsLogs
caught   an unrestored partition answers absent keys from memory            -- TestAnUnavailableStoreDegradesToMemory
caught   an unrestored partition refuses blind writes                       -- TestAnUnavailableStoreDegradesToMemory
caught   a drained node applies a peer's write                              -- TestADrainedNodeRefusesAPeerThatMissedTheDeparture
caught   a store delete leaves its digest behind                            -- TestTheRunningDigestFollowsEveryWritePath
caught   a store overwrite is counted as a new key                          -- TestTheRunningDigestFollowsEveryWritePath
caught   a replica delete leaves its digest behind                          -- TestTheReplicaDigestFollowsEveryWritePath
caught   a replica's missing copies are not reported                        -- TestAReplicaThatMissedWritesIsReported
caught   an in-step partition is reported as out of step                    -- TestInStepReplicasMatchTheirOwners
caught   a cluster with a member unreachable is reported as agreeing        -- TestAnUnreachableMemberIsReported
caught   a silent member holds DescribeCluster forever                      -- TestAMemberThatNeverAnswersCostsOnlyTheTimeout
caught   every page rebuilds and re-sorts the key list                      -- TestAPassSortsOnceAndCoversEveryKey
caught   a finished pass keeps its key list                                 -- TestAPassSortsOnceAndCoversEveryKey
caught   passes are not bounded                                             -- TestPassesAreBounded
caught   a page emptied by removals ends the pass                           -- TestAPassSkipsKeysRemovedMidPassWithoutStopping
caught   a pass sends keys the asker stopped being entitled to              -- TestAPassReappliesItsFilter
caught   an L1 copy ignores its key's remaining lifetime                    -- TestANonOwnedCopyDoesNotOutliveItsKey
caught   the owner's Get grants no remaining lifetime                       -- TestANonOwnedCopyDoesNotOutliveItsKey
caught   the owner's load grants no remaining lifetime                      -- TestALoadedCopyDoesNotOutliveItsKey
caught   a batch entry grants no remaining lifetime                         -- TestABatchReadCopyDoesNotOutliveItsKey
caught   the serving node does not continue the caller's trace              -- TestARemoteReadIsOneTraceAcrossTwoNodes
caught   the caller does not send its trace context                         -- TestARemoteReadIsOneTraceAcrossTwoNodes
caught   trace headers are not sent over gRPC                               -- TestTraceHeadersCrossTheHop
caught   trace headers are not read back on the server                      -- TestTraceHeadersCrossTheHop
caught   a load runs outside a span                                         -- TestAForwardedLoadIsTracedOnTheOwner
caught   the OpenTelemetry adapter propagates nothing                       -- TestARemoteGetIsOneOpenTelemetryTrace
caught   a hit inside the window does not refresh                           -- TestARefreshReloadsAKeyNearItsExpiry
caught   every hit refreshes, whatever its remaining lifetime               -- TestARefreshReloadsAKeyNearItsExpiry
caught   each hit in the window starts its own refresh                      -- TestConcurrentHitsStartOneRefresh
caught   a refresh stores over a write that landed during it                -- TestAWriteDuringARefreshWins
caught   Close does not wait for a refresh in flight                        -- TestCloseWaitsForARefresh
caught   refresh-ahead is accepted without a loader                         -- TestRefreshAheadNeedsALoader
caught   a namespace's jitter never reaches its store                       -- TestTTLJitterSpreadsAWarmUp
caught   the store ignores its jitter                                       -- TestJitterShortensByAnAmountFixedPerKey
caught   jitter lengthens a TTL                                             -- TestJitterShortensByAnAmountFixedPerKey
caught   jitter is drawn afresh on every write                              -- TestJitterShortensByAnAmountFixedPerKey
caught   an atomic update's TTL is not jittered                             -- TestTTLJitterSpreadsAWarmUp
caught   a restored deadline is jittered again                              -- TestRestoreKeepsTheDeadlineItWasGiven
caught   a jitter of one is accepted                                        -- TestTTLJitterMustBeAFraction
caught   catch-up ignores its rate                                          -- TestCatchUpPullsAtTheConfiguredRate
caught   a negative catch-up rate is accepted                               -- TestCatchUpRateMustNotBeNegative
caught   Kill does not wait out failure detection                           -- TestAKilledOwnersKeyIsServedByItsReplica
caught   Restart does not rejoin the membership                             -- TestADrainedNodeRestartsIntoTheCluster
caught   Heal leaves the partition in place                                 -- TestAPartitionSplitsTheMembershipAndHealsBack
caught   a partition is not applied to the network                          -- TestAPartitionSplitsTheMembershipAndHealsBack
caught   a restarted node loses its own options                             -- TestNodeOptionsReachOneNode
caught   Drain leaves a stranded copy behind                                -- TestDrainTakesOnlyWhatItClaims
caught   promotion restarts the version sequence                            -- TestRestoreContinuesTheVersionSequence
caught   Restore overwrites a newer value                                   -- TestRestoreRefusesAnOlderVersion
caught   snapshot ignores its page limit                                    -- TestSnapshotPagesInKeyOrder
caught   snapshot ignores the paging cursor                                 -- TestSnapshotPagesInKeyOrder
caught   snapshot hands over expired keys                                   -- TestSnapshotSkipsExpiredEntries
caught   sync replication does not wait                                     -- TestSyncReplicationPlacesCopiesBeforeReturning
caught   async replication waits for its replicas                           -- TestAsyncReplicationDoesNotBlockTheWrite
caught   replication does nothing at all                                    -- TestChaosLosesNoSynchronouslyWrittenKey
caught   deletions are not replicated                                       -- TestADeletionReplicates
caught   a remote write skips replication                                   -- TestKillingAnOwnerPromotesAReplica
caught   promotion drains nothing                                           -- TestKillingAnOwnerPromotesAReplica
caught   catch-up sends whatever is asked for                               -- TestCatchUpOnlySendsKeysTheAskerRanksFor
caught   catch-up does not reconcile what it pulled                         -- TestARestartedNodeRecoversItsColdReplicas
caught   readiness ignores catch-up                                         -- TestANodeIsNotReadyBeforeCatchUp
caught   readiness ignores whether the view has settled                     -- TestANodeIsNotReadyWhileMembershipMoves
caught   a draining node still reports ready                                -- TestDrainingIsPermanentlyNotReady
caught   Settled always says yes                                            -- TestANodeIsNotReadyWhileMembershipMoves
caught   drain does not hand off its keys                                   -- TestDrainSavesDataWithoutReplication
caught   drain does not announce its departure                              -- TestDrainStopsPeersRoutingImmediately
caught   handed-off keys are not adopted by their new owner                 -- TestDrainSavesDataWithoutReplication
caught   an implicitly created namespace is not counted                     -- TestAnImplicitNamespaceIsCountedAndReplicatesNothing
caught   the plan counts promotions as moves                                -- TestRemovingANodeMovesOnlyItsShare
caught   an empty view plans an evacuation                                  -- TestAnEmptyNewViewPlansNothing
caught   unchanged keys are never counted                                   -- TestNoChangeMeansNoMoves
caught   drain hands off nothing                                            -- TestDrainHandsOffKeysNothingElseWouldSave
caught   drain transfers before announcing departure                        -- TestDrainAnnouncesDepartureBeforeTransferring
caught   quantile rounds down instead of up                                 -- TestQuantileRoundsUp
caught   snapshot buckets are exclusive, not cumulative                     -- TestBucketsAreCumulative
caught   the overflow bucket is dropped                                     -- TestTheInfBucketMatchesTheCount
caught   histogram bounds emitted in nanoseconds, not seconds               -- TestHistogramTextFormat
caught   a correctness counter is exported as a gauge                       -- TestCorrectnessMetricsAreCounters
caught   an out-of-order copy is not counted as a version conflict          -- TestVersionConflictsAreCountedButRetransmissionsAreNot
caught   a retransmission is counted as a version conflict                  -- TestVersionConflictsAreCountedButRetransmissionsAreNot
caught   mailbox rejections never reach Stats                               -- TestMailboxRejectionsAreCounted
caught   a stats field is exported under the wrong name                     -- TestEveryStatsFieldIsExported

caught: 99   vacuous: 0   skipped: 0
tree clean
```

They are chosen to be the invariants whose quiet failure would be worst:
the aliasing contract, every rule that bounds staleness, both halves of the
version discipline, the panic containment that keeps application bugs local, and
the deadline rules. Not everything is audited — the p99 criterion is mutated by
hand because a single run costs a minute, and the reference-model and fuzz tests
audit themselves by construction.

Two of them are a different kind of mutation, added in M3, and worth calling
out: "the guard is built but never consulted" and "the guard never learns the
membership". Every other entry breaks logic inside a package and is caught by
that package's own tests. Those two break *wiring* — they leave
`internal/ownership` untouched and correct, and only stop `peercache.New` from
connecting it to the membership authority or to activation. Nothing in
`internal/ownership` notices, because from its point of view nothing is wrong.

They are the reason `tests/cluster_test.go` exists. Before it, thirteen ownership
invariants were verified inside a package the library could have stopped calling
without a single test failing.

Two entries had to be *repaired* in M4 rather than added, and the repair is
worth recording. Changing the L1 tier to compare `(epoch, version)` moved the
lines two M1 mutations targeted, and the audit reported them as `SKIP` rather
than silently passing.

That is the behaviour the script was built for. A mutation whose pattern no
longer matches is verifying nothing, so `SKIP` is counted as a failure and exits
non-zero — otherwise a refactor would quietly retire coverage and the total
would keep looking healthy. Both were rewritten against the new code, keeping
their original intent (an older value must not displace a newer resident one),
and both were re-verified individually before being trusted.

It is deliberately **not** run by CI. It rewrites source files, and a mutation
left behind by a crashed run would cost far more than the coverage it buys. Run
it by hand when adding a test that guards a specific invariant.

A `SKIP` result means the code moved and the script needs updating — which is a
failure, not a pass, because a mutation that no longer applies is verifying
nothing.

## A test that proved nothing

Not every mutation confirms a test. `TestGetOrLoadRechecksAfterWinningTheFlight`
was written to cover the in-flight store re-check, looked reasonable, and
**passed with the re-check deleted**.

Its callers hit the outer store lookup and returned before ever reaching the
flight, so the code under test never ran. The window the re-check closes is the
gap between two adjacent statements, and nothing in the test could land a
goroutine inside it.

The test was renamed to `TestGetOrLoadServesFromTheStoreAfterALoad`, which is
what it actually asserts, and the untested optimisation is recorded in
`docs/decisions/open.md` question 20 rather than covered by a comment claiming
otherwise.

This is the strongest argument in this file for the method. Four fixes in M1
were verified by mutation; three held and one turned out to be guarded by a test
that could not fail. Without running the mutation, all four would have looked
identical.

---

## M8 — what the verification tools found, including about themselves

M8's tools were a linearizability checker, a deterministic simulation, an
exhaustive fuzz target and the mutation audit. Each found something the milestone
that built the code had missed, and in the serious cases the earlier milestone's
fix had been correct — just aimed at a subset of the problem.

This section grew past its original title, which promised two out-of-memories and
a partition. What follows is longer than that because the tools kept finding
things after the defects ran out, and a record that stops at the interesting bugs
is a record that hides the boring ones. It divides into four:

**Defects in the library** — a remote out-of-memory reachable through every
message with a `[]byte` field, a second one in the only unguarded slice of
structs, a partition heal that discarded replicas rather than moving them, and
the fix for that last one forging writer identity in its first version.

**Gates that did not work** — the M4 acceptance test that had never once run on
this machine, the `-race -count=2` gate that could not pass inside its own
timeout, the mutation audit's orphan detector with a hole an M8 entry fell
straight through, three nightly fuzz jobs naming targets that did not exist and
exiting 0, a per-commit fuzz step broken by a test added elsewhere, and an
example printing a number one too high.

**Rules that nothing enforced** — five derived figures wrong out of six checked,
one exported symbol with no doc comment, a package missing from the repository
map since it was created, twelve of the nineteen public-API files missing from
the same map, and the discovery that the rules being broken were exactly the
rules with no test. `tests/repo/discipline_test.go` grew to cover all of them: every
mechanical rule in docs/design.md now has a test, and so do the build files, the
repository map, the API documentation's derived figures and the acceptance map —
the places verification is written down that no Go tool reads.

That sentence used to open with a count of the checks added, and that count is why
this paragraph is worth reading. It said "five", then "eight", then "nine", then
"ten", then "eleven" — each correction made by someone who had just been reminded
the number could be wrong, and each one stale again within the hour, because a
further check kept landing. Five wrong values for one figure, in the paragraph
introducing the section about figures going wrong.

The count is now gone rather than corrected a sixth time. "Eleven checks were
added this milestone" is a property of a diff against a commit that will move,
which is precisely the category this section concludes cannot be enforced and
should not be asserted; naming what is now covered is a property of the tree, and
a reader who wants the total can count the file. Taking that conclusion seriously
means deleting the number rather than maintaining it better.

**Claims that were true only sometimes** — a guarantee the README states
unconditionally that holds only for a cluster at rest, five places where a
retryable error was counted as data loss, and a test written to verify one of
this milestone's own fixes that turned out unable to fail.

The order below is chronological, because several findings only make sense as
consequences of the one before. The fourth category above was added after the
first three, when the section outgrew them. Adding it made the line introducing
them say three where four were listed — an eighth stale figure, created by the
fix to the seventh, one line above it. Both are recorded rather than smoothed
over: the point of this section is not that the author is careful, it is that
care does not scale and counting does.

### The M4 fix was right and covered fifteen fewer messages than it needed to

`internal/wire/bounded.go` exists because M4 found that a declared length bought
an allocation. It hardened the collection decoders and `FuzzDecode` covered five
message types, chosen as a representative sample when the protocol had few more
than five.

The protocol grew to twenty. The sample stayed at five.
`FuzzDecodeEveryMessage`, which decodes each input into *all* of them, found
`SubmitResponse` — the one message with a slice of structs and no custom decoder.
Six bytes, `91 dd ff ff ff ff`, ask for 4,294,967,295 `GroupFailure` values:
137 GB. Under `-race` the process does not panic, it dies — ThreadSanitizer
fails to map the shadow memory and aborts the binary.

**And the same test found a second, larger hole that no bounded decoder could
have closed.** msgpack v5's allocation limit lives in `Decoder.readN`; its
byte-string paths (`bytes`, `bytesPtr`) call the package-level `readN` instead,
whose first statement is `b = append(b, make([]byte, n-len(b))...)` with `n`
straight off the wire. Every `[]byte` field in every message — ten of the
twenty — turns `c6 ff ff ff ff` into a 4 GiB allocation. Values, call arguments,
call results, replicated payloads, catch-up entries.

The fix moved layer: `wire.Decode` now validates the frame's structure before the
decoder sees it, requiring every declared length to be no larger than the bytes
behind it. One function, every message, every future message. Decision 0019 has
the reasoning and the 25 ns it costs.

**The lesson is about where the M4 fix was aimed.** It was applied to the
decoders this package owns, and it could never have reached a hole in the
library's byte-string path however carefully it was written. What reached it was
pointing a fuzzer at the boundary rather than at the code someone had already
thought about.

### Question 38's own closing assertion was the thing that found the bug

Open question 38 recorded a simulation seed that lost synchronously acknowledged
keys after a single destruction at RF=3. The investigation proposed replica
eviction as the cause, wrote `TestEvictionDoesNotDropTheLastCopy` to reproduce
it, and could not — so the note attributed the losses to the documented AP window
*by inference*, said so plainly, and recorded what would settle it: assert that a
write acknowledged on the **majority** side of a split survives the heal.

Writing that assertion took an afternoon and failed immediately. Seed
`5593891418832731417`: 5 of 12 majority-side keys gone.

The cause was the eviction hazard the question had proposed and discarded. The
discarding was right about the evidence and wrong about the conclusion: ordinary
kill-and-rejoin churn does not reach it, because copies placed under the full
membership stay ranked when a member leaves and returns. **A partition does.**
Writes taken while one node is cut off place their copies at ranking positions
computed without it; its return moves the ranking underneath every one of those
keys at once, and the nodes holding them stop ranking and drop them. The copies
do not move — they evaporate, and catch-up cannot repair what nobody holds.

Reduced to a pair of tests that separate the steps rather than assuming one:
heal-only loses nothing, heal-then-two-destructions lost 20 of 94. The counters
turned it from plausible into measured — twenty-one replicas evicted
cluster-wide, twenty keys lost. Fixed by handing a moved replica to its new
holders before dropping the local copy (decision 0020). Both reduced tests pass,
the seed replays clean, and two 300-seed sweeps pass with 300 of 300 schedules
asserting at least one key on the second.

### A test helper that reported "wait a moment" as "gone forever"

More than half of that first failing run's lost keys were not lost. They returned
`ErrActivationPending` — a retryable "ask again shortly" — and `readAnywhere`
retried three times without advancing the fake clock, so it asked again at the
same instant and got the same refusal every time.

Had the real defect not also been present, this alone would have produced a
convincing, entirely false bug report. It is the mirror image of the M1 lesson: a
test that fails for the wrong reason costs exactly as much as one that passes for
the wrong reason, and both are found the same way — by reading what the failure
actually says rather than what it appears to confirm.

### And the audit immediately found the branch the fix's own tests missed

The five M8 mutation entries were written the same day as the fix. Four were
caught. The fifth — deleting `h.restore(w)` from the hand-off's "could not place
it anywhere" branch — left
`TestMajoritySideWritesSurviveAHealAndTwoDestructions` **passing**, because in
that schedule every hand-off succeeds.

That branch is the one deciding between "this costs memory" and "this loses
data", and nothing was looking at it. `TestAHandOffThatCannotBePlacedKeepsTheCopy`
now does: it strands one holder's outbound traffic with a one-way total drop
before the heal, so the node still learns membership grew but can hand nothing to
anybody, and asserts the copies come back. The mutation is caught.

Reaching it took four attempts, and each failure was informative rather than
fiddly. Kill-and-rejoin churn orphans nothing (that is
`TestEvictionDoesNotDropTheLastCopy`'s whole point). A kill *followed by* a
revive orphans nothing either, because it returns membership to the identical set
and the ranking with it. Only a partition heal — membership growing to a set it
has not been — moves keys. And once it did, the assertion had to be weakened from
an accounting identity to "some came back", because a restored copy is orphaned
again by the next reconciliation and retried, so both counters climb together
while the peer stays unreachable and any snapshot is taken mid-cycle.

### A test that measured the observer

Adding the hand-off broke `TestKeysAdoptedIsCounted`, which touches no drain
code and asserts a counter about one. It failed with "60 keys were handed off
and none were adopted; ... both are silent data loss", in a full-package run, on
a seed that passed when the test ran alone.

No data had been lost. A drain announces its departure and then transfers, with
nothing synchronising the two, so a receiver takes each key one of two ways:
**adoption**, when its view already names it the owner as the transfer lands, or
**promotion**, when the transfer arrives first, is filed as a replica, and the
next reconciliation moves it. Both end with the key in the owned store; the
library is built to tolerate either. One goroutine per cache was enough to shift
the scheduling so that every key took the second path.

The confirmation was the part worth recording. Adding a `t.Logf` above the
assertion to print the other counters made the test **pass** — the diagnostic
perturbed the timing it was diagnosing. A test that changes its answer when you
look at it is not measuring the system.

It now asserts the outcome rather than the route: every handed-off key is
accounted for by adoption *or* promotion, and every key written is still
readable. The fear it was written for — "sent and not adopted is silent data
loss" — is fully covered by that and was never covered by the stricter form,
which could only ever have been checking which of two correct paths won a race.

**The general point** is that this is the same failure as a vacuous test, seen
from the other side. A test that passes for the wrong reason and a test that
fails for the wrong reason cost the same, and the second is more dangerous
because it arrives looking like evidence.

### The fix's own first version forged provenance

Reviewing the hand-off before calling it done found that it stamped
`ReplicateRequest.From` with the forwarding node's ID. `From` names the node that
*wrote* a value, and a replica keeps it precisely so that two writers for one key
under one epoch can be reported as the split brain they are.

A hand-off is a relocation. Claiming authorship of it makes the receiver see the
original owner's replication and the forward as two writers — 19 fabricated
split-brain detections in the reduced test, on every membership change forever.
`SplitBrainDetections` is the counter README tells operators to investigate
whenever it is non-zero; a fix that made it lie on every rebalance would have been
worse than the metric not existing.

Nothing would have caught it. The data was correct, the tests passed, and the
only symptom was a number rising in a place nobody was looking yet. It was found
by asking what each field of a message means before sending it, and it is
recorded here because that is not a technique that scales — the reason it was
*possible* to spot is that `From` has a doc comment saying what it is for.

### What a coverage run found that the tests did not

Measuring coverage of the new code rather than assuming it turned up three
things, in descending order of how uncomfortable they were.

**`scan`, the frame validator, was at 67.6%.** Twelve uncovered branches, every
one a msgpack type byte this protocol's encoder never emits and a peer can send
freely — the extension family, the fixed-width integer widths, `map16`. Each
family has its own arithmetic for how many bytes follow, and a family whose
arithmetic is wrong lets a declared length through silently. Testing only the
third of the format we emit left the rest asserted by nothing.
`TestValidateHandlesEveryTypeByte` now walks all 256, accepting the shortest
well-formed value for each and requiring that same value one byte short to be
refused. 97.1%, and it cannot fall behind the format without failing.

**The two lines still uncovered are unreachable, and now say so.** The type switch
turned out to be total — msgpack assigns all 256 leading bytes — so the "unknown
type byte" default cannot be reached, which `TestEveryTypeByteIsClassified`
asserts rather than assumes. The `readLen` overflow check is dead on 64-bit and
live on 32-bit; `GOARCH=386 go build` confirms the platform it protects still
compiles. Both have comments saying which they are, because an uncovered line
that looks like a missing test costs the next reader an investigation.

**And the branch M8 itself added as a bug fix was uncovered.** The `rf < 2` guard
in reconciliation — the one preventing an unbounded restore-and-re-orphan spin —
had no test. Worse, trying to write one established that the condition cannot be
constructed through the public API in this harness: a drain at RF=1 pushes to two
targets and both receivers adopt their share, so the surplus replica the branch
exists for never appears. The test is kept, skipping, with the reason in its own
message; the branch carries a comment saying it is reasoned rather than measured.

That last one is the honest half of this section. A fix written in response to a
bug is exactly where an untested branch is most dangerous, and the recourse when
it cannot be tested is to say so in the two places somebody will look — not to
delete the test and leave the comment claiming more than it earned.

### And three goroutines leaked whenever New failed late

Adding a worker to `New` meant reading what happens when the constructor fails
after starting one. There is exactly one such path — the transport refusing to
serve — and it closed the ownership guard and nothing else, so a failed `New`
left the invalidator, the replicator and now the hand-off running with no handle
to stop them.

Pre-existing, invisible, and invisible for a specific reason: open question 25
records that goroutine leaks here are caught by targeted per-package
`TestCloseLeavesNoGoroutines` tests, and no test constructs a cache whose
transport refuses to serve. A leak on a path nothing exercises is a leak nothing
finds. All four are now closed on that path.

### The M4 acceptance test had never run on this machine

Checking that the frame validator had not cost anything on the remote path meant
running the latency gate, which had never been run here. It failed instantly:
`bind: An attempt was made to access a socket in a way forbidden by its access
permissions`, on a UDP port inside the 600-wide band this host reserves.

That is open question 27, for the third time. M6 fixed
`internal/membership`'s tests; M7 found `tests/cluster_test.go` had built its own
helper and fixed that too, and wrote the closing line "a correct fix applied to
some of the places that need it reads, in the commit log, exactly like a complete
one". `tests/acceptance_remote_test.go` had built a *third* helper, and it was never fixed.

What made it invisible is worth more than the bug. The test is behind an opt-in
latency gate that only CI's Linux runner sets, where the reserved band does not
exist — so it passed in the one place it ran and could not run in the one place
it failed. A gate nobody can trip locally is a gate nobody maintains.

The first fix attempt was also wrong, and instructively: wrapping the existing
`BindPort = 0` in `RetryBind` still failed twelve times out of twelve. `RetryBind`
is the backstop, not the defence — the defence is `LocalGossipConfig`, which
*probes* a bindable port before handing it to memberlist. Retrying a re-roll that
keeps landing in a 600-port band is not a strategy. Using the same helper as the
other two callers, which was the point all along, fixed it.

**And then it reported numbers that disagree with the record.** Four consecutive
runs measured 193k–195k rps at p99 244–249 µs, against the 45k–48k this project
documents as a shortfall for the same criterion.

The first instinct was that the load generator had been changed — and the comment
above the worker constant does reason to twelve workers while the constant says
thirty-six, which is a real defect worth fixing on its own. It is not the
explanation: the recorded table used thirty-six too.

So the sweep open question 29 named as the thing that would settle it was run.
Throughput does not flatten: 110k rps at twelve workers rising monotonically to
235k at sixty-four, p99 180 µs to 334 µs. At the same thirty-six workers the
record cites, the rate is four times higher and p99 six times lower. The
criterion is met at every point of the range.

**Question 29 is marked met, and the cause of the divergence is not.** Both sets
of numbers are kept side by side in `docs/benchmarks.md` rather than one
replacing the other. The one thing checked and eliminated: the 97 µs p50 that
looked impossibly fast against the recorded 175 µs serial round trip is not a
contradiction — `BenchmarkInvoke` re-measures at 70–177 µs, so the 175 µs was the
top of a noisy range quoted as a point.

### A documentation claim that cited a test which did not cover it

`docs/api.md` lists the metrics worth alerting on and closes with "All eight are
counters ... `TestCorrectnessMetricsAreCounters` fails if one is reclassified as
a gauge". M8 added two entries to that table and the sentence still said eight —
so the first correction was to the number.

The number was the smaller problem. The test enumerates the metrics it checks by
name, and the two new ones were not in the list. Changing "eight" to "ten" would
have produced a sentence that named a test as its evidence and was wrong about
what that test did — the exact failure this file exists to catch, in prose rather
than in code.

Both are now in the test, and reclassifying either to a gauge was confirmed to
fail it. Counting the table's rows before believing its summary took one command;
the alternative was a documented guarantee resting on nothing.

### The project's own gate could not pass

docs/design.md's Definition of Done requires `go test ./... -race` to pass, and the
Makefile and CI both run it as `-race -count=2 -timeout 300s`. Running that exact
command produced:

```
panic: test timed out after 5m0s
	running tests:
		TestRollingUpgradeLosesNoRequests (12s)
FAIL	github.com/peercache/peercache	300.197s
```

Not a deadlock — a test was running normally when the alarm fired, part-way
through the second pass. `-timeout` applies per test binary, so this step has to
fit *two* race passes of the root package inside 300s, and one pass measures
~215s.

**It was already failing before M8 touched anything.** The single-pass figure
measured 202.7s at the start of this milestone and 215.6s at the end; both double
past 300s. The suite grew across M5–M8 and crossed the line without anyone
deciding it should.

**What made it invisible is the same shape as the M4 port bug three sections up.**
`make test-race` is the gate a developer runs by hand, and the number it needed
had drifted out of reach gradually rather than in one commit — so every run
failed the same way, looked like an environment problem, and told nobody
anything. The steps that *do* run in CI on every push use `-timeout 300s` too,
but for a single non-race pass that takes 24s, where the bound is nowhere near
binding. One number, two very different jobs.

Split into `TIMEOUT` and `RACE_TIMEOUT` (900s), in the Makefile and in
`ci.yml`, each with a comment saying which job it belongs to. The corrected gate
was then run: **439.065s, exit 0**, every package green — 46% over the bound it
had been given, and comfortably inside the new one.

That run is the first time in this milestone that the `-count=2` check has been
observed to complete here. M3 credits it with catching a goroutine leak visible
only on a second pass, so it did work when the suite was smaller; **when it
stopped is not established.** What is measured is that a single pass was already
202.7s at the start of M8, so it was failing then, and the suite has grown across
four milestones since M3.

**Re-run at the end of the milestone: 527.308s, exit 0, every package green.**
Every test M8 added after the 439s measurement lands in the root package, and the
gate now costs 18% more than the figure the 900s timeout was chosen against. The
margin has gone from 2.0x to 1.7x in the space of one milestone.

Nothing is wrong: a timeout is a deadlock detector rather than a performance
budget, and 1.7x is a long way from binding. It is recorded because the direction
is one-way and the last time this number was left alone it went from comfortable
to unpassable without anybody deciding it should. `docs/benchmarks.md` carries the
figure; the thing to notice is that the *first* correct measurement of this gate
and the *second* are a milestone apart and 88 seconds apart, which is the rate a
future budget has to be set against.

The gap is worth naming rather than estimating: nobody was watching the number,
so the protection docs/design.md asks for degraded from real to nominal at some point
between M3 and M8 without a commit that did it.

Raising the bound is not weakening the gate: a timeout here exists to turn a hang into a failure, and
900s does that as well as 300s did. What 300s had additionally become was an
undeclared performance budget, and an undeclared budget that fails closed is a
gate people learn to ignore.

**2026-11-15: raised to 1500s.** A single `-count=1` race pass of `tests` took
816s on a branch and timed out once at 900s with `TestSimulation` four minutes
in under load; the margin had gone to 1.1x. The rate is the one this section
warned of: 527s at the end of M8, 560–770s across the post-v0.1 features, 816s
now.

### The audit's own guard had a hole, and an M8 entry fell through it

M7 built a static check into `scripts/mutation-audit.sh` after an entry lost its
`mutate` line and sat in the file looking audited for two milestones. The check
joins backslash continuations into logical lines and requires every line carrying
a `'TestSomething'` argument to begin with `mutate`.

It matches that argument with `'Test[A-Za-z0-9_|]*'` — every character a bare
name or an alternation needs, and no anchors. M8 added an entry passing
`'TestMajoritySideWritesSurviveAHeal$'`, anchored because another test's name
extends it. That does not match, so the check could not see the entry, and
deleting its `mutate` line was **silently undetected**.

Found by testing the guard rather than trusting it: the `mutate` line was removed
on purpose to watch the check fire, and it did not. Widening the character class
to the metacharacters a `-run` argument may legitimately contain fixes it, and
the same deletion is now refused by line number. Both directions confirmed —
intact script passes, orphaned entry caught.

Scanning all 107 entries for arguments the old pattern could not see returns
exactly one real entry — the M8 one that exposed the hole. Every pre-existing
entry uses a plain test name, so nothing historical was ever unguarded: the hole
opened the moment an entry needed a regex anchor, and the first one that needed
it made it.

**Why this one is worth the space.** The guard exists to stop an entry going
silently unrun. It was defeated not by the line structure it inspects but by the
*contents of the argument* it inspects, introduced by someone who had read the
guard and was relying on it. Every M8 mutation entry was individually
hand-verified to catch its mutation, so nothing was actually unprotected — but
that was luck of process, not the guard working, and without checking the guard
the difference would not have been visible.

### The full audit, run at last — and what two runs of it disagreed about

The audit refuses to run on a dirty tree, so it had not been run against any of
this milestone's work. It was run against a throwaway snapshot instead: the tree
copied to a scratch directory, committed there, audited there. The working tree
is never touched, and the objection the refusal exists for — `git checkout --`
discarding uncommitted work — does not apply to a commit made for the purpose.

**First run: 104 caught, 1 vacuous, 0 skipped.** All six M8 entries caught. The
vacuous one was caused by this milestone's own fix, described above: the frame
validator now refuses the inputs `TestALengthPrefixCannotBuyAnAllocation` uses,
so the mutation it was guarding — preallocating from a declared length — became
unobservable through `Decode`. Retargeted at
`TestABoundedDecoderStillAllocatesOnlyForWhatArrives`, which reaches the bounded
decoder with a frame the validator *must* accept, and catches the mutation at
6,266 KiB against a 1 MiB budget.

**Second run: 104 caught, 1 vacuous — a different entry.** "Handed-off keys are
not adopted by their new owner" was caught in the first run and vacuous in the
second, on the same code.

**Third and fourth runs: 105 caught, 0 vacuous, 0 skipped, exit 0.** The retarget
holds and the flaky entry was caught both times.

That disagreement is the finding, and M3 says why: an audit that is not
deterministic is not evidence. Across four full runs the entry was caught,
missed, caught, caught; run alone it is caught 5 times out of 5. Eight of nine
observed runs, so the escape needs the load of a full audit to appear. The likely mechanism —
likely, because it has not been demonstrated directly — is the same
adoption-versus-promotion race that made `TestKeysAdoptedIsCounted` fail earlier
in this milestone: a drain announces its departure and then transfers, and when
the receiver has not yet processed the departure the transfer is filed as a
replica and promoted afterwards, which preserves the data by the other path.

The consequence for the entry is worth stating plainly. **The guarantee holds on
both paths** — a graceful drain at RF=1 loses nothing either way — so this
mutation targets which store the key lands in rather than whether it is kept. It
is left in place, because a detector that fires in eight runs of nine is still a
detector, and annotated in the script with those numbers so the next
disagreement is recognised rather than investigated from scratch.

**The fourth run was worth doing for a specific reason.**
`TestDrainSavesDataWithoutReplication` had just been changed to read through
`readAnywhere`, which retries across every live node — plausibly making this
mutation *harder* to catch, since a retrying reader might find the key by the
promotion path. It did not. The mutation makes the receiver store nothing at all
when it owns the key, so the key is genuinely absent and retrying cannot conjure
it: retry rescues deferrals, never absence, which is exactly the distinction the
fix was about. The change removed a false negative without introducing a false
positive, and the audit is how that was established rather than assumed.

**Fifth run, at the end of the milestone: 110 caught, 0 vacuous, 0 skipped, exit
0, tree clean.** The five entries added for the fuzz targets and the
documentation checks bring the total from 105, and the flaky drain entry was
caught for the third consecutive time — nine of ten observed runs now.

This is the run the rest of this file has been writing cheques against. Every
claim of the form "this test would fail if the behaviour regressed" was made
about one entry at a time, verified by hand at the moment it was written, and
then left to age. Executing all 110 against the tree as it now stands is a
different statement from 110 statements about the tree as it was, and it is the
only one that covers the interactions — a test weakened in service of a later fix
would show here and nowhere else.

Nothing showed. That is the least interesting possible outcome and the reason for
running it.

### An example that printed a number one too high

The Makefile says examples are run rather than merely compiled because "an
example that is only compiled is documentation that can quietly start lying."
`examples/observability` was lying, quietly, about the size of the metrics surface.

It counted series with `strings.Count(body, "\n# TYPE ")` and added one. The
addition corrects for a first TYPE line with no newline before it — true of a
body that begins at `# TYPE `. The body begins at `# HELP`: every series emits
both lines, so *every* TYPE line is preceded by a newline, the correction became
an off-by-one, and the example reported 40 series where the endpoint serves 39.

Found by not taking the output on trust. The example prints a count rather than
asserting one, so nothing could have failed; the number was checked against a
direct count of `# TYPE ` prefixes and disagreed by exactly one. It now counts by
prefix, which needs no correction and cannot drift with the exposition format.

**The general point is about which numbers are load-bearing.** This one is
printed for a human and asserted by nothing, which is precisely why it could be
wrong for as long as it liked. The metric *count* in `docs/api.md` has the same
shape and was checked the same way earlier in this milestone — both were wrong,
in different directions, for the same reason: a derived figure nobody re-derives.

### The exhaustive fuzz target was not exhaustive

`FuzzDecodeEveryMessage` exists because a five-type sample let the M4 defect
survive in the fifteen types nobody pointed a fuzzer at. Counting the types it
actually decodes — prompted by the two wrong derived numbers above — gives
**twenty**, while its sibling `everyMessage()` covers **twenty-one**.

The missing one is `GroupBatch`. It had been added to the *seed* list earlier in
this milestone and never to the decode list, so inputs were encoded from it and
never decoded into it. It has its own custom encoder/decoder pair, which is its
own way to get length handling wrong.

A one-type gap in the target whose entire premise is that a sample is not enough.
Now decoded, with `GroupBatch.Messages` added to the length invariant, and the
count corrected in the four places prose asserted it.

**Five findings came from the same question** — "is that number still true?" —
asked systematically of every figure in this milestone's writing that could be
checked against the code:

| Claim | Where | Stated | Actual |
|---|---|---:|---:|
| exported metric series | `docs/api.md` | 8 alerting, 35 total | 10, 38 |
| series at the endpoint | `examples/observability` | 40 | 39 |
| message types fuzzed | four places | 20 | 21 |
| collection-bearing messages | decision 0019 | six | seven |
| deepest real nesting | decision 0019 | four levels | five |
| encoder/decoder pairs a per-message fix would need | decision 0019 | eight | eight ✓ |

Five wrong out of six checked, and every one of them written during this
milestone by someone who had the code open. Two were merely prose; three had
teeth — the metric count cited a test that did not cover what it claimed, the
example printed a false number to an operator, and the fuzz count concealed a
type the target never decoded.

A derived figure written once and never re-derived is not documentation, it is a
claim with a decaying half-life. The defence is not care at the time of writing —
all five were written carefully — it is recomputing rather than reading. The last
row is the useful control: checking found one claim that was right, which is what
makes the other five findings evidence rather than a fishing expedition.

**Ten instances accumulated before this milestone ended**, five of them written
*into this section* while it was being drafted, two of those created by the fix
to the one before. The rate did not fall as the author became more alert to it,
which is the argument against relying on alertness.

Three defences emerged, in ascending order of reliability:

1. **Check it mechanically.** Three of the ten are now enforced by tests. Only
   works for figures that are properties of the current tree — "how many rows in
   this table" qualifies; "how many checks were added this milestone" does not,
   being a property of a diff against a commit that will move.
2. **State the scope.** One figure was *measured* and still wrong, because
   "named in fourteen places" did not say fourteen places in what — `.go` and
   `.sh` gives 14, adding `.md` gives 18. A count without its scope is not a
   fact, it is a fact-shaped string that nobody can re-derive.
3. **Do not count at all.** The four categories introducing this section have
   never been wrong, and the reason is that they enumerate rather than total.
   "Defects in the library — a remote out-of-memory, a second one, a partition
   heal, a forged provenance" stays true when a fifth is added in a way that
   "four defects" does not. That was accidental rather than designed, and it is
   the only one of the three that costs nothing and cannot fail.

### One exported symbol in the repository had no doc comment

docs/design.md: "Every exported symbol has a doc comment. Every package has a
`doc.go`." Both are mechanically checkable and neither had been checked.

Scanning every top-level exported `func`, `type`, `const` and `var` outside test
files returns exactly one without a preceding comment: `WithTransport`, a public
constructor option, sitting between two neighbours that are both documented. It
is described in `docs/api.md`'s options table, so the information existed — it
just never reached `go doc`, which is where a caller looks.

The first version of the scan reported 45 hits and was wrong. Methods on
unexported types — `(*cache).Stats`, `(*group).Get` — are documented on the
interfaces they implement, which is where Go's own convention puts them.
Narrowing to genuine top-level declarations turned 45 into 1, and the 1 was real.
A checker that cries wolf 44 times out of 45 would have been abandoned before it
found anything, which is an argument for tuning the check rather than reading its
output selectively.

The `doc.go` half passes: every library package has one. The only directories
without are the eight `examples/mN` programs, which are `package main` and carry
package comments instead.

**Why that rule and not another** is the part worth keeping. `tests/repo/discipline_test.go`
already enforces docs/design.md's other mechanical rules as tests — no `time.Sleep`,
no `time.Now` outside `internal/clock`, no `log` package, no TODO markers, no
internal package importing the root, every package has a `doc.go`. All of them
hold. The exported-symbol rule was the only one with no test, and it was the only
one being broken. That is not a coincidence worth admiring; it is a measurement
of what an unenforced rule is worth.

`TestEveryExportedSymbolIsDocumented` closes it, and was confirmed by deleting
the comment again and watching it fail. It parses rather than greps, for the
reason above: the regex version reported 45 violations of which 44 were methods
on unexported types, and a check that is wrong 44 times in 45 is a check somebody
switches off.

**Then the same question was asked of every remaining rule.** Comparing
docs/design.md's mechanical rules against the tests in `tests/repo/discipline_test.go` left two
with no check at all:

- **"No `panic` in library code paths."** A safety non-negotiable. It held —
  zero panics in the shipping module — but nothing said so. The library recovers
  panics from *application* callbacks and reports them as `ErrLoaderPanic` and
  `ErrGroupFuncPanic`; a panic raised by the library itself has no such boundary
  and unwinds whichever caller's goroutine was running. In a cache embedded in
  every pod that is the application dying over a bug in its cache, which is what
  question 34 describes actually happening.
- **The dependency budget.** "Adding anything else requires a note in
  `docs/decisions/` first" rested entirely on remembering it at the moment
  `go get` makes it easiest not to.

Both now have tests — `TestNoPanicInLibraryPaths` and
`TestDependencyBudgetIsUnchanged` — and both were confirmed by breaking them:
inserting a `panic` into `namespace.go` and removing a module from the budget
list each fail with the message a reader needs. The budget test cannot check that a decision note exists
— it makes the diff demand one, by failing until the new module is recorded with
the slot it fills, which is the same sentence the note has to make.

Every mechanical rule in docs/design.md now has a test. Two of the nine did not an
hour ago, and one of those two was already broken.

**One documentation claim got the same treatment before it broke.**
`docs/api.md` opens with a table of every runnable example and what it covers,
and that table is how a reader decides whether the public API has been
demonstrated or merely described. Checked against `example_test.go` it was
exactly right — twelve claimed, twelve defined, no drift in either direction.

That is the moment to nail it down rather than the moment to move on. The failure
mode is not writing the table wrong; it is adding a thirteenth example and
forgetting the row. `TestApiDocExampleTableMatchesReality` now fails in both
directions — a row with no example is a promise the package does not keep, an
example with no row is a demonstration nobody will find — and was confirmed by
deleting a row and watching it fail. It is `TestProtoDeclaresEveryMessage`
applied to prose instead of a schema, which is the pattern this repository
already uses everywhere the two can disagree.

**And the citation sweep became a test too.** The records in `docs/` lean hard on
test names — a claim is credible here because it says which test would fail if it
stopped being true — so a stale name is worse than no name: it reads as evidence
and resolves to nothing. That sweep was run by hand three times during this
milestone. `TestDocsCiteOnlyRealTests` now runs it every time, over 132 cited
names, with an allowlist of four that deliberately do not resolve, each carrying
its reason. The allowlist is checked in reverse too: an entry that starts
resolving must be removed, so the exceptions cannot quietly become wrong.

It needed one refinement, and it is the same one the doc-comment check needed.
The first version flagged `TestReport` in decision 0005 — which is not a citation
but a `-run` *prefix pattern* in a shell command, matching three real tests.
Names inside a fenced code block are patterns; names in prose are claims. Skipping
fences distinguishes them, and shrank the allowlist by one at the same time.

Twice now a checker's first version has been wrong in the same direction: too
literal, flagging things that were never claims. Both times the fix was to narrow
what counts as a claim rather than to widen the list of exceptions, because an
allowlist that absorbs false positives is how a check stops meaning anything.

### A package missing from the map since the day it was created

README's layout block lists every directory in the repository. It listed fifteen
of the sixteen packages under `internal/`.

The missing one is `internal/wire`: the protocol codec, the only code here that
interprets bytes it did not write, and the package where two of this milestone's
four library defects lived. It had been absent since the package was created and
nothing noticed, because nothing was looking.

An omission from a map is worse than a map with no detail. A reader deciding
where message encoding lives would have found `codec/` and `transport/` and
concluded there was nothing between them — the absence reads as an answer.

Added, and `TestReadmeListsEveryInternalPackage` now fails if a package is
created without one. Confirmed by deleting the line again.

The check is one-directional on purpose. Every real package must appear; a line
that no longer matches a directory is caught by the same diff that removed the
directory, and demanding exact equality would fight the block's sub-entries and
prose for no gain.

**Then the same block was checked one directory over, where it was worse.** The
repository root is the public API — Go does not let a package span directories,
so every file a caller's import reaches is there. There are nineteen. The layout
block listed **seven**, and had listed those seven since M4 while twelve more
were added underneath it: the whole remote path, replication, promotion,
catch-up, invalidation, drain, readiness and the stats surface. `handoff.go`,
written this milestone, would have been the thirteenth.

The `internal/` omission was one package out of sixteen and is the kind of thing
a careful reader might notice. This one is twelve files out of nineteen, in the
one directory the map claims to describe completely, and it is the directory a
reader goes to first. It survived because the check that existed looked at
directories, and the root is a single directory that happens to contain most of
the library.

`TestReadmeListsEveryRootFile` closes it on the same terms as its sibling —
one-directional, confirmed by deleting `handoff.go`'s line and watching it fail.

**And two 17 MB binaries were sitting in that directory.** `m6.exe` and `m7.exe`,
left by a hand-run `go build ./examples/rolling-upgrade` during M6 debugging. `make examples`
uses `go run` and leaves nothing, so no target here produced them and no target
here removed them: `clean` deleted two of the seven artefact patterns
`.gitignore` names. Being gitignored kept them out of the history and did nothing
whatever about them being on disk, which is the distinction — an ignore rule
answers "should this be committed", not "should this be here". `clean` now
removes everything `.gitignore` lists.

### A false alarm found by running the suite one more time

`TestDrainSavesDataWithoutReplication` failed in a full-package run, on
unmutated code, reporting that a graceful shutdown had lost a key. It passes five
times out of five when run alone.

Nothing had been lost. The test read each key from one survivor with a single
`Get` and counted *any* error as loss. Two of the errors it can legitimately
receive mean "ask again": `ErrActivationPending`, because a membership change
starts the activation guard's delay on every group it moved, and `ErrNotOwner`,
because ownership may have moved off whichever survivor is first in the list.
Under full-package load one key landed inside that window.

This is the third time in this milestone that a retryable error has been read as
data loss — after `readAnywhere` reporting deferrals as lost keys, and the
adoption-versus-promotion race in `TestKeysAdoptedIsCounted`. The fix here is to
use `readAnywhere`, which already knows the difference and was written for
exactly this after the first occurrence. This test predates it and never got the
benefit.

**The general shape is worth naming, because three instances is a pattern.** A
distributed cache has a large class of errors meaning "not yet" and a small class
meaning "never". A test that treats the two alike does not fail *less* often than
one that gets it right — it fails at a rate set by machine load, and every one of
those failures is a confident, specific, false report of data loss. It is the
most expensive kind of flake, because the failure message is credible.

Worth noting how it was found: by running the full suite once more after a change
that could not have affected it. Three earlier full runs had passed.

**Then the pattern was hunted rather than waited for.** Three instances is enough
to search on, so every loop in the suite that counts keys as lost was checked for
the same shape — a bare `Get` on one node whose error is treated as loss. Two more
turned up, both in `tests/chaos_test.go`:

- `TestChaosLosesNoSynchronouslyWrittenKey`, the M5 acceptance criterion, had it
  exactly. After sixty seconds of kills the survivors are still settling, so a
  deferral there would have reported a key lost by chaos — a false alarm on an
  acceptance criterion, which is the most credible place in the suite to get one.
- `TestChaosAtReplicationFactorTwoCanLoseData` was more careful, separating
  `ErrNotFound` from other errors, but a deferral still produced a fatal
  "unexpected error". That test *expects* losses, which makes distinguishing
  "not found" from "not yet" more important there, not less: a deferral counted
  as either outcome corrupts the number it exists to report.

Both now use `readAnywhere`. The RF=2 test still reports 83 kept and 17 lost of
100, so the fix did not paper over the real losses it measures. A final sweep
confirms no loop in the suite still counts a bare `Get` error as loss.

Five instances, one root cause, found because the third one prompted a search
instead of a fix.

**And a sixth is now blocked rather than merely unlikely.**
`TestLossCountingTestsUseARetryAwareRead` requires any test file that counts keys
as lost to use one of the retry-aware helpers. It is deliberately coarse — a
file-level property, not a judgement about where in the file the bare read is —
because three text-based checks written this milestone were wrong on their first
version by being too literal, and a checker that over-fires is one that gets
switched off. This one cannot point at the line; it can say the file has no way
to be doing the right thing, which is enough and is impossible to argue with.

Confirmed by adding a file that counts losses and calls no helper, and watching
it fail by name. An earlier attempt to reintroduce the bug directly into
`tests/chaos_test.go` would not compile, because `readAnywhere` returns a string where
a bare `Get` returns bytes — an accidental guard, and not one to rely on.

### Readable is not replicated, and the test that proved the fix did not know it

The final `-race -count=2` gate failed in a test written earlier the same day to
verify the hand-off fix: `TestMajoritySideWritesSurviveAHealAndTwoDestructions`
lost one key of ninety-four, about one run in seven. It had passed every
non-race run and every earlier gate.

Three hypotheses, two wrong, each eliminated by measurement rather than argument:

1. **A hand-off still in flight.** The queue is drained by a worker, so killing
   nodes might race it. Waiting for `rehomed + retained` to account for every
   `evicted` did not fix it.
2. **The cluster had not re-converged.** Waiting until every key was readable
   before killing did not fix it either — and the failure moved to *after* that
   wait, which is what made the third hypothesis findable.
3. **Readable is not replicated.** A key is readable the moment one node holds
   it. Surviving two destructions needs three nodes to hold it. After a heal the
   copies are restored by catch-up, which M5 made an explicit call precisely
   because it is not automatic — and this test only called it *after* the kills.

Calling `CatchUp` before destroying anything fixed it: **thirty consecutive runs
under `-race -count=2`**, against a rate that had been roughly one in seven.

**The library was never wrong here.** The test was asserting "RF=3 survives two
destructions" against a cluster that had not finished restoring RF=3, which is
the documented behaviour of a cluster that has not caught up and the exact
situation the README's operational rule addresses — call `CatchUp` before
serving. The test was skipping the procedure and then blaming the guarantee.

Two things are worth carrying. **A test written to verify a fix is not
automatically a correct test**, and this one was wrong in a way that made the fix
look incomplete — the most expensive direction to be wrong in. And the two failed
hypotheses were not wasted: each was cheap, each was eliminated by a measurement
rather than by reasoning, and the second one relocated the failure to where the
third became visible.

**The same gap was then looked for elsewhere rather than assumed absent.** Every
test that destroys nodes was checked for whether it restores replication first:
`tests/chaos_test.go` catches up the revived victim each round and never has two nodes
down at once, so one copy always survives at RF=3; `tests/rolling_test.go` drains
gracefully; `tests/eviction_test.go` catches up after its single kill-and-revive.
`tests/partitionheal_test.go` was the only test removing two copies without restoring
the factor first.

There is a connection worth naming. `tests/chaos_test.go` already carries a long
comment about M5 discovering this for *restarts* — "558 of 600 keys lost, none of
them actually gone" — and concluding that a returning node must catch up before
serving. The failure here is the same class for a *heal*: membership changed,
placement moved, and the copies had not caught up yet. The project had learned
the lesson in one shape and did not recognise it in the other, which is the
ordinary way a lesson fails to transfer.

### A test written for the new guarantee, and deleted for being vacuous

Documenting the settling window raised the obvious next question: is there a test
that catch-up *restores* the replication factor? There was not — every test in
this area asserts readability, which is the exact distinction that had just cost
an afternoon.

So one was written. It counted copies in aggregate: each node reports what it
owns and what it holds as a replica, and summed across the cluster that should
come to three times the keyspace at RF=3. It passed, reporting 337 copies of 107
keys against 321 expected.

**Then it was mutated, and it passed with catch-up deleted entirely.** Vacuous.
The excess is why: some nodes retain copies they no longer rank for, so an
aggregate total can sit above the expected number while individual keys sit below
it. The measure could not see the thing it was named for.

Deleted rather than kept. This project's position on that is not ambiguous —
question 20 records deleting a test for the same reason, and the mutation audit
reports VACUOUS as a failure. A test that cannot fail is worse than an absent
one, because the absence is at least honest about what is not checked.

The guarantee is not unguarded. `TestMajoritySideWritesSurviveAHealAndTwoDestructions`
covers it behaviourally and far better: catch up, then destroy RF−1 nodes, then
require every key. That asserts the consequence anybody actually cares about
rather than a proxy for it, and it is the test whose failure produced the finding
in the first place.

The general point is the one this file keeps arriving at from different
directions. A test is not evidence because it passes; it is evidence because it
would fail. Checking which of those you have takes one mutation and about a
minute, and the answer here was not the one that was expected.

### Mutating the new tests, including the ones that looked safest

Having deleted one vacuous test, the rest of this milestone's tests were checked
the same way rather than assumed sound. Two results were worth keeping.

**`TestValidateHandlesEveryTypeByte` is not vacuous.** Changing the 64-bit
scalars to skip four bytes instead of eight — a plausible off-by-one in a family
nobody looks at twice — fails it immediately, and fails it on the *truncation*
half: "validate accepted a truncated value for type byte 0xcb". That half was
written on the theory that accepting a well-formed value proves nothing on its
own, and this is the theory paying out.

**The collection element-count bound is a fast path, not a correctness guard.**
Deleting it left the entire wire package passing. That looked like a coverage gap
and is not: `scanN` calls `scan` for each declared element, the first one runs
out of bytes, and the frame is rejected either way. The bound turns a loop over a
declared four billion into one comparison, and names the problem in the error
rather than reporting a truncation several levels down — neither of which is
correctness.

It is now commented as such, which is the same treatment `replicate.go` gives its
`rf < 2` branch after a mutation audit reached the identical conclusion there. A
line that cannot be mutated into a failure is either dead or a fast path, and the
difference belongs in a comment rather than in the reader's inference.

### Every test this milestone added, and the mutation that confirms it

Scattered through the sections above are individual confirmations. Collected, so
the claim "these tests would fail if the behaviour regressed" is auditable in one
place rather than reconstructed from a narrative:

| Test | Mutation | Result |
|---|---|---|
| `TestNoMessageAllocatesFromADeclaredLength` | run against the pre-validator code | **caught** — 4 GiB on `CallResponse`, 1 MiB on four others |
| `TestValidateRejectsUnboundedNesting` | `MaxNestingDepth` raised to 1<<30 | **caught** |
| `TestValidateHandlesEveryTypeByte` | 64-bit scalars skip 4 bytes instead of 8 | **caught** on the truncation half |
| `TestEveryTypeByteIsClassified` | `map16` case disabled | **caught** — names the byte reaching the default |
| `TestABoundedDecoderStillAllocatesOnlyForWhatArrives` | preallocate from the declared length | **caught** — 6,266 KiB against a 1 MiB budget |
| `TestMajoritySideWritesSurviveAHealAndTwoDestructions` | drop moved replicas instead of handing off | **caught** — 20 of 94 lost |
| `TestAHandOffThatCannotBePlacedKeepsTheCopy` | drop the copy when placement fails | **caught** |
| `TestMajoritySideWritesSurviveAHeal` | hand-off stamps itself as the writer | **caught** — 19 fabricated detections |
| `TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy` | degraded counter never increments | **caught** |
| `TestDegradedWritesStayZeroWhenTheFactorIsMet` | counter fires when the factor *is* met | **caught** — 4 spurious on a healthy cluster |
| The eleven discipline tests | the rule each enforces, broken | **caught**, each verified individually |
| `FuzzCodecRoundTrip` | msgpack encodes without the `json` struct tag | **caught** — every string field returned empty |
| `FuzzPartitionByPrefix` | split on the *last* separator instead of the first | **caught** — idempotence, on a seed |
| `TestFuzzTargetsNamedInCIExist` | a phantom target, then an unanchored pattern | **caught** both, naming file and pattern |
| `TestApiDocMetricCountsAreCorrect` | 38 → 37, and "ten" → "nine" | **caught** both halves |
| `TestReadmeListsEveryRootFile` | `handoff.go` removed from the layout block | **caught** |
| the audit's own orphan guard | a `mutate` line dropped from a fuzz entry | **VACUOUS twice, then caught** — see above |
| all five entries above, via `ONLY` | run by the script rather than by hand | **caught: 5, vacuous: 0, skipped: 0** |
| `TestMutationAuditCitesOnlyRealTests` | an entry's test name renamed in the script | **caught** — names the script and line, not the test |
| `examples/failure-guarantees` | the hand-off deleted from promote.go | **VACUOUS at first, then caught** — readability survives, the rehomed count does not |
| `examples/failure-guarantees`, second half | the degraded counter never incremented | **caught** |
| every entry in the audit, all 110 | run end to end against the final tree | **caught: 110, vacuous: 0, skipped: 0** |
| the aggregate copy-count test (name not kept; it was deleted) | catch-up removed | **VACUOUS — test deleted** |

Two entries are the interesting ones. The last is a test that could not fail and
was removed rather than kept. And the collection element-count bound in
`validate.go` survives deletion with the whole package still passing — recorded
above as a fast path rather than a gap, because `scanN` rejects the frame either
way.

Everything else in this milestone was confirmed by breaking it first.

Assembling this table tripped `TestDocsCiteOnlyRealTests` immediately: three rows
carried names truncated with an ellipsis to keep the columns narrow, and a
truncated name resolves to nothing. Table width is not a reason to write the name
of a test that does not exist, and the check said so before a reader had to. The
deleted test now has no code-formatted name at all, which is the honest way to
refer to something that is gone.

Writing *that* sentence tripped it again, because the first draft quoted one of
the truncated names to illustrate the problem — and a quoted prefix is exactly as
unresolvable as the original. Three times now a check added this milestone has
caught its own documentation while the documentation was being written. Each time
the fix was to stop naming the thing and describe it instead, which is also the
better prose.

### The nightly fuzz job named three targets that did not exist

`.github/workflows/nightly.yml` was written at the end of this milestone to carry
the long-running verification the per-commit gate cannot afford: thousands of
simulation seeds, an hour of soak, the mutation audit, and a fuzz matrix of seven
targets. Three of the seven — `FuzzCodecRoundTrip`, `FuzzMethodName` and
`FuzzPartitionByPrefix` — named tests nobody had written. They were invented
while writing the workflow, which is where a list of what *ought* to be fuzzed
turns into a list of what is.

**The part that makes it worth a section is what `go test` does about it.**
Nothing:

```
$ go test ./internal/transport -run '^$' -fuzz '^FuzzMethodName$' -fuzztime 5s
PASS
ok      github.com/peercache/peercache/internal/transport   1.763s
```

`-fuzz` takes a regexp. When it matches no target, the command does not error —
it runs the ordinary tests selected by `-run`, which is `'^$'`, which is none,
and exits 0. Three of the seven jobs would have gone green every night for as
long as anybody left them alone, and the only visible symptom is a job that
finished in two seconds instead of ten minutes, in a workflow nobody watches
because it passes.

This is the third gate this milestone that could not fail, after the M4
acceptance test that had never bound a socket and the audit guard with a hole in
it. All three share a shape: the gate was *configured* correctly and resolved to
nothing at run time, and the run-time result looked exactly like success.

**And the opposite failure was sitting next to it.** `FuzzDecodeEveryMessage` was
added to `internal/wire` earlier in this milestone. `ci.yml` ran
`-fuzz FuzzDecode`, unanchored, which had been unambiguous when it was written:

```
testing: will not fuzz, -fuzz matches more than one fuzz test: [FuzzDecodeEveryMessage FuzzDecode]
FAIL
```

The Makefile had been anchored by hand when the new target went in. `ci.yml` had
not, and nothing connected the two. A green step in the per-commit gate was
broken by a test being *added* in a package the step's file does not mention —
which is the same class as a stale documentation citation, except that the
compiler cannot see build files at all.

**Both are now checked by `TestFuzzTargetsNamedInCIExist`**, which reads the
Makefile and every workflow, resolves each `-fuzz` pattern against the fuzz
targets actually defined in the package the command names, and fails unless the
match count is exactly one. Zero is a gate that cannot fail; two is a gate that
cannot pass. Both original failures were reproduced against it and both are
caught, each naming the file, the pattern and what it found in that package.

**Two of the three invented targets were worth writing, and one was not.**
`FuzzMethodName` would have fuzzed `shortName`, a nine-line loop over a list of
compile-time constants that no untrusted input reaches. Writing it would have
produced a passing test that guards nothing, which is the failure this file spends
most of its length on; it was deleted from the matrix instead, and that is the
honest resolution when a gate names work that should not be done.

The other two are real properties, and both were confirmed by mutation before
being believed. `FuzzPartitionByPrefix` covers the group key, which decides
ownership — it asserts that the group is a prefix of the key, that grouping is
idempotent, and that two partitioners built from the same separator agree, the
last two being what stands between the library and a key with two owners.
Switching the implementation to split on the last separator rather than the first
fails it on a seed input.

**`FuzzCodecRoundTrip` found a real one on its first execution.** The
[`codec.Codec`](../internal/codec/codec.go) interface has said since M1 that "for
any v that the codec accepts, Unmarshal(Marshal(v)) reproduces v". The JSON
codec, which is the default, does not: `encoding/json` substitutes U+FFFD for
every byte of an invalid UTF-8 sequence in a string and reports no error. A value
written through it comes back changed, silently, which is precisely what the
sentence beneath that one calls "worse than one that errors".

`TestCodecRoundTrip` existed and did not catch it. It compares three hand-written
values, and for the two composite fields it compares their *lengths* — a codec
that replaced every map value with the empty string would pass it. It is the
same shape as the helper and the acceptance test recorded earlier in this
milestone: an assertion that something came back, not that the right thing did.

The finding is documented rather than fixed, for a reason recorded in open
question 41: the substitution happens inside `encoding/json`, no option disables
it, and it cannot be detected from the encoded bytes, which are valid UTF-8 by
then. `TestJSONCodecReplacesInvalidUTF8` pins the behaviour and fails if it ever
changes; the fuzz target excludes that one input class for the JSON codec by name
so that it still enforces the property everywhere else, rather than the codec
being exempted whole. MsgPack carries the same bytes through unharmed, and a
`[]byte` field survives JSON too because it is base64 — only Go strings are
affected, and `WithCodec` is still inert (question 17), so nothing in the library
reaches this today. It is a hole in a contract, found before the contract had a
caller.

**The codec target's own first version passed for the wrong reason.** It iterated
`codec.Names()`, the package-level registry, on the assumption that the property
should hold for every codec registered. It does not hold for the stubs that
neighbouring tests register — one returns `nil` from `Marshal` and ignores
`Unmarshal` — so the target passed under `-fuzz`, which runs no other test, and
failed under a plain `go test`, which runs them first. The bug was in the test and
the invariant was fine, but the shape is worth naming: a check whose result
depends on which other tests ran is a check that reports the schedule, not the
code. It now names the two built-in codecs, which are the ones the property is
about.

Both targets now have entries in `scripts/mutation-audit.sh`, and getting them
there found a third hole in that script's own guard — recorded below.

**Every target was then run for two minutes rather than the thirty seconds the
regression gate uses**, which is the first time the full set has been given a real
campaign on this machine:

| Target | Execs | New interesting | Result |
|---|---:|---:|---|
| `FuzzUnframe` | 2,879,453 | 0 | pass |
| `FuzzFrameRoundTrip` | 2,730,144 | 5 | pass |
| `FuzzCodecRoundTrip` | 3,862,004 | 45 | pass |
| `FuzzDecode` | 6,365,523 | 48 | pass |
| `FuzzDecodeEveryMessage` | 4,123,029 | 26 | pass |
| `FuzzPartitionByPrefix` | 1,663,706 | 0 | pass |

21.6 million executions, twelve minutes, no crashers, and `testdata/fuzz` still
holds exactly the one input an earlier milestone committed.

Two of those rows say something. The zeros are the targets whose input space is
small enough that the seed corpus already covers it — `FuzzPartitionByPrefix`
takes two strings and reached nothing new in 1.6 million tries, which is the
expected shape for a function with four branches. The 45 for `FuzzCodecRoundTrip`
is the opposite and also expected: it is two minutes into a target that had
previously seen twenty seconds. Neither number is evidence of a bug; both are
evidence about how much of each target's space is reachable, which is the thing
worth knowing before deciding what the nightly campaign should spend its time on.

**The general point is about where verification is written down.** Every check in
this repository lives in a `_test.go` file except the ones that live in a
Makefile or a workflow, and those had nothing checking them. A build file naming
a test by a string is an unresolved reference that no tool follows: `go vet` does
not read YAML, and the test it names does not know it is being named.

### Two more derived figures, pinned before they broke

`docs/api.md` tells an operator that "38 series come out of it", repeats the
figure in the next section — "most of the 38 are ordinary levels and rates" — and
introduces the alerting table with "these ten are not". Three numbers, all
derived from code, all maintained by remembering.

Checked: all three are right. 38 is `len(metrics.Names())` exactly, and the table
under "these ten" has ten rows.

That is the moment to nail them down rather than the moment to move on, which is
the argument `TestApiDocExampleTableMatchesReality` was written from earlier in
this milestone and the argument five wrong figures made the hard way. The "38"
was "35" until three counters were added here, and it was corrected by someone
who happened to remember.

`TestApiDocMetricCountsAreCorrect` takes the series count from `metrics.Names` —
the same list the handler writes from, so it cannot drift from the output the way
a hand-maintained list would — and the alerting count from the table's own rows,
so a sentence introducing a table has to agree with the table beneath it. It
reads *every* occurrence rather than the first, because the figure appears twice
in sections written a milestone apart and a check that fixed one and left the
other would be the failure it exists to prevent. Both halves were confirmed by
mutation: 38 → 37 and "ten" → "nine" each fail with the number found and the
number expected.

It also merged two `numberWords` maps into one. A second had been added for this
check before it was noticed that the acceptance-map check already had one — the
same duplication, in the file whose entire purpose is catching duplication that
drifts.

### A check that tested the directory instead of the reason

`TestEveryPackageHasDocGo` exempted `examples/` by path prefix. The reason in its
own comment is different: commands document themselves above `package main`, and
a separate `doc.go` in one would be worse than none. `examples/` was simply the
only directory that had ever held a command.

The first command written outside it — `scripts/clean`, below — would therefore
have been asked for a `doc.go` that the convention says not to write, and the
obvious fix under deadline is to add a second path to the exemption list. That is
how an exemption list becomes a place where checks go to stop meaning anything.

It now matches on the declaration, which is the property the comment was
reasoning about all along. This is the third time in this milestone that a check
was found testing a proxy for its subject rather than its subject: the M4 port
bug, the audit's `-run` pattern guard, and this.

### The clean target removed two of the seven things it names

`.gitignore` lists seven build and test artefact patterns. `make clean` removed
two of them — `coverage.out` and `bench.txt`. Two lists that have to agree, with
nothing checking them, which is the defect this whole section keeps finding one
level out from the code.

The consequence was sitting in the repository root: `m6.exe` and `m7.exe`, 34 MB
of example binaries left by a hand-run `go build`, matched by `.gitignore`'s
`*.exe` and by nothing in the target whose job is removing them. Being gitignored
kept them out of the history and did nothing whatever about them being on disk —
an ignore rule answers "should this be committed", not "should this be here".

`clean` now runs `scripts/clean`, which reads the artefact section of
`.gitignore` and removes what it names, so there is one list. It refuses to run
if that section is empty rather than silently removing nothing, does not recurse,
and skips directories — the second section of `.gitignore` names `.idea/` and
`.vscode/`, which belong to the developer and are not a clean target's business.

**And the first version of this entry was wrong in the way this file exists to
prevent.** It claimed the old recipe "could not run at all on this machine",
having seen `mingw32-make clean` fail from PowerShell with
`CreateProcess(NULL, rm -f ...) failed`. Tested properly, the recipe runs fine
from Git Bash, where `rm` is on PATH; GNU make reaches for a shell only when it
recognises something in the command line as needing one, and the glob in that
recipe did not trigger it. Three neighbouring recipes using pipes and `&&` were
also asserted to be broken, and measuring showed all three work, because those
characters do trigger it.

So the Go rewrite is portability, not repair, and the claim in the Makefile now
says so. The finding that stands on its own is the duplicated list. The one that
did not survive contact with a second measurement is recorded here at length
because it was made while writing a section about unverified claims, which is now
the fourth instance of that in this milestone and no longer a coincidence worth
remarking on — it is the base rate for asserting anything without running it.

### The audit's guard had a third hole, found by orphaning an entry rather than by reading it

Earlier in this milestone the mutation audit's orphan detector — the check that
refuses to run when an entry has lost its `mutate` line and would be silently
skipped — was found to have a hole, and was fixed by widening the character class
of the `-run` argument it matches on. Adding the first fuzz entries reopened it
twice more.

**First:** the pattern matched only arguments beginning with `Test`. A fuzz entry
begins with `Fuzz`, so an orphaned one would have been exactly as invisible as the
orphaned M6 entry the guard was built for. Adding the alternation was the obvious
fix.

**Second, and this is the one worth the space:** the obvious fix did not work. A
`-run` argument is a regexp and the new entries are written anchored, as
`'^FuzzCodecRoundTrip$'` — and a string beginning with a caret begins with
neither `Test` nor `Fuzz`. The widened guard was tested by orphaning one of the
new entries and it did not fire. The leading anchor is now optional too, and the
same test then produced:

```
  line 1167:   internal/codec/msgpack.go    'enc.SetCustomStructTag("json")' ...
refusing to run: an entry above is not attached to a 'mutate' command,
so it would be silently skipped. Restore its 'mutate' line.
```

Three widenings, each written against the entries that existed at the time, for a
guard whose entire job is catching an entry nobody has written yet. The pattern is
not that the regexp keeps being wrong; it is that reading it is not a test of it.
Every version of this guard looked right to whoever wrote it, and the two that
were not were each caught in seconds by orphaning a real entry — which is now how
it is verified whenever it changes.

**A filter was added at the same time, for a reason that is the same failure in
another shape.** The audit is a hundred-odd entries, each compiling and running a
test, and it refuses to start on a dirty tree. Verifying one newly written entry
therefore meant committing first and then paying for every other entry to find out
whether the new one worked. The predictable result is that entries get added and
reasoned about rather than run — the exact failure this script exists to catch,
committed by the script itself. `ONLY='pattern'` runs a subset, and the summary
says how many entries were passed over so that a filtered run cannot be read as a
full one.

**Five entries were added**, covering the two fuzz targets and three of the checks
on documentation and build files. The last three are the first entries here that
mutate something other than Go source, which is the point of them: a check that
reads a workflow or a table in `docs/api.md` is exactly as capable of being
vacuous as one that reads code, and rather more likely to be, because nothing
compiles it.

Each pair — mutation and the test that must notice — was applied and reverted by
hand before being written into the script, and each is in the table below. The
script was then run over them, which is a different claim and the one that counts:

```
$ ONLY='codecs stop agreeing|splits on the last separator|fuzz pattern in CI|...' \
    bash scripts/mutation-audit.sh
caught   the codecs stop agreeing on field names -- by ^FuzzCodecRoundTrip$
caught   grouping splits on the last separator, not the first -- by ^FuzzPartitionByPrefix$
caught   a fuzz pattern in CI goes back to being ambiguous -- by TestFuzzTargetsNamedInCIExist
caught   the documented metric count drifts from the handler -- by TestApiDocMetricCountsAreCorrect
caught   a root file disappears from the repository map -- by TestReadmeListsEveryRootFile

caught: 5   vacuous: 0   skipped: 0
FILTERED RUN: 105 entries were not executed.
this run makes no claim about them.
tree clean
```

Five caught, none vacuous, and the three entries that mutate a `.md` or a `.yml`
behaved exactly as the Go ones do. The line about 105 unexecuted entries is the
part of that output worth keeping: it is there so a filtered run cannot be quoted
as a full one, which is the failure mode a filter introduces and the reason the
filter had to print something rather than nothing.

### The milestone that built no features still needed an example

`examples/` holds one runnable program per milestone and stopped at m7. README
says "one runnable example per milestone", which had been true for eight
milestones and was not any more.

The tempting answer was to narrow the claim, on the grounds that M8 built
verification rather than features. That is wrong on the facts: M8 changed what
the library does under failure twice — a heal now moves replicas instead of
evicting them, and a synchronous write below its replication factor is now
counted. Both are user-visible, both are things an operator has to know, and
neither had a demonstration a reader could run.

`examples/failure-guarantees` runs both against a simulated network and a fake clock, and asserts
what it prints rather than only printing it:

```
  healed, ownership recomputed, catch-up run:
    peercache_replicas_rehomed_total    33
    peercache_replicas_retained_total   0
    keys still readable                 60 of 60

  pod-0 now sees 1 member(s) and was asked for 3 copies
  Set returned <nil>
    peercache_degraded_writes_total     1
```

It returns an error rather than printing a smaller number if a heal loses a key,
or if a write is acknowledged below its factor with nothing counting it. `make
examples` runs it, so both are gates rather than illustrations — which is the
rule the examples directory has had since M1 broke the M0 example and nothing
noticed.

**And the first version of it was vacuous**, which is the part worth keeping.

It asserted the obvious thing: after the heal, all 60 keys are still readable.
That assertion passes with the entire hand-off deleted. Reverting `promote.go` to
the eviction M8 replaced and running the example gives `rehomed 0` and
`readable 60 of 60`, exit 0 — because `CatchUp` then refetches the evicted copies
from their new owners. Readability is restored either way; what differs is
whether the copies *moved* or were dropped and fetched again, and the loss only
becomes visible when nodes are destroyed inside the settling window, which is
what `TestMajoritySideWritesSurviveAHealAndTwoDestructions` does and what an
example should not.

So the load-bearing assertion is the rehomed count, and the example says so in
its own output rather than leaving a reader to assume the readable line is the
evidence. Confirmed the same way the finding was: with the hand-off removed it
now exits 1.

This is the second time in this milestone that a check written to demonstrate one
of M8's own fixes turned out unable to fail — the first was a test, deleted for
the same reason. Writing the demonstration is not the hard part. Establishing
that the demonstration would notice the thing going away is, and it takes a
minute per artefact and was skipped here by someone who had spent the preceding
day writing about not skipping it.

Writing it surfaced one thing worth recording. The first version advanced the
clock past the activation delay and wrote immediately, and failed with
`activation deferred after a membership change` on the fifteenth key. A
membership change does not finish when the member counts agree: each node then
reconciles what it holds against what it should hold, and the guard defers
activating a group until its ownership has been stable. The example now waits for
reconciliation to go quiet, the same `settle` the tests use. An example that
retried past the guard would have been demonstrating a race and calling it a
feature.

### One more file citing tests, and nothing checking it

`TestDocsCiteOnlyRealTests` covers `docs/` and README. It does not cover the file
that names more tests than all of them together: `scripts/mutation-audit.sh`
cites one test per entry, over a hundred of them.

A stale name there is not silent — `go test -run` matching nothing passes, so the
entry reports VACUOUS and the run exits non-zero. It is worse than silent. VACUOUS
means "this test did not notice the behaviour breaking", which sends whoever reads
it to the test; the actual fault is a string in a shell script, and the two are
indistinguishable in the output. An hour-long audit that misattributes its own
failure is a tool people learn to argue with.

`TestMutationAuditCitesOnlyRealTests` names the real cause in two seconds. The
script makes 110 citations resolving to 102 distinct tests, and all of them
exist. It skips comment text, because the guard at the top of that file documents
itself with `'TestSomething'` — the same prose-versus-pattern distinction
`proseOnly` draws for Markdown, arrived at independently for the third time in
this milestone.

The two numbers in that sentence are not decoration. The first draft said "all
102 currently resolve", having counted distinct names with one script and
citations with another and then quoted whichever number was to hand — a
derived figure, wrong, in the paragraph introducing a check for derived figures
being wrong. It was caught by recomputing rather than by rereading, which by now
is the only method in this file with a record of working.

### What was counted rather than fixed

Open question 37 — a synchronous write acknowledged below its replication factor
by a node whose view has shrunk to itself — got its counter and not its cure.
`Stats().DegradedWrites` makes the window visible; refusing the write would
convert an available system into an unavailable one during a partition, which is
an AP/CP decision that should be taken deliberately and on evidence rather than
as a side effect of tidying up a metric.

### After M8: question 37's cure, opt-in

Decision 0021 added `WithRequireReplicationFactor`, which refuses the same write
with `ErrUnderReplicated`. The default is unchanged and the existing
`TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy` still pins it.

Four mutation entries were applied by hand on the working tree and reverted
from a copy, before the script could run them on a clean one:

| Mutation | Caught by |
|---|---|
| the strict namespace acknowledges below the factor | `TestRequireReplicationFactorRefusesAnIsolatedWrite` |
| the refusal after a partial placement is removed | `TestRequireReplicationFactorRefusesThroughAForwardedWrite` |
| `ErrUnderReplicated` stops surviving the gRPC hop | `TestSentinelErrorsSurviveTheHop` |
| the option is accepted without sync replication | `TestNamespaceOptionValidationSurfaces` |

All four caught. The second one needs its own test: an isolated node has no
replica targets at all and takes the other branch, so only a cluster with some
replicas but not enough — two nodes at RF=3 — reaches it.

**The example found a bug the tests did not.** `ExampleWithRequireReplicationFactor`
runs a single node with no membership, the plainest case of a write below its
factor, and expected `DegradedWrites` to read 1. It read 0. `Stats()` filled the
counter only inside its membership block, so a cache without membership counted
the write and reported nothing. Every existing test used a fabric with membership
and could not see it. Both counters now sit outside that block, beside
`MailboxRejections`, for the reason its comment already gave.

### The code review, and two drain races it led to

An independent review of the whole library at high effort reported fourteen
findings from reading alone, nothing executed. Each is being confirmed with a
failing test before it is fixed.

**Confirmed and fixed: ownership checked by the raw key.** The peer handler
asked whether this node owned the *key*; callers route by the key's *group*,
which a partitioner derives. In a namespace with `PartitionByPrefix`, the
group's owner refused every remote request whose raw key hashed elsewhere.
`TestAPartitionedNamespaceServesRemoteCallers` failed with ErrNotOwner from the
true owner, and passes now. No earlier test sent a remote request into a
partitioned namespace.

**Found on the way, not by the review: two races in a graceful drain at RF=1.**
Running the suite after that fix, `TestDrainSavesDataWithoutReplication` — long
recorded here as flaky — failed more often. Logging adoption and eviction in
failing runs separated two mechanisms:

1. *A copy stranded in the replica store.* A receiver checked whether it owned
   the key against its old view, the view changed and its reconciliation ran,
   and only then was the copy filed as a replica — after the one pass that would
   have promoted it. Unreadable until the next tick. The handler now checks
   ownership again after filing, and promotes the key at once if it is its own.
2. *A copy dropped.* At RF=1 reconciliation dropped every replica, including a
   hand-off whose receiver had not yet seen the sender leave — the only copy of
   the key. It now keeps a copy whose sender it still ranks as the owner, and
   never a copy of a key it owns itself; the second rule was found by logging
   every eviction across 1,500 runs and catching two, each of a key owned by the
   node that dropped it.

The rate went from 27 failures in 250 runs to 0 in 1,500. These are timing
races, so the mutation audit — one run per entry — cannot hold them, and they
are not in it; the evidence is the repetition count, recorded here.

**The rest of the fourteen.** Twelve were confirmed — ten with a test that
failed first, two from source — and fixed; one was a documentation gap; one is
reasoned:

| Finding | How it was confirmed | Test or evidence |
|---|---|---|
| BatchSet wrote locally, unrouted | failing test | `TestBatchSetRoutesEachKeyToItsOwner` |
| a misconfigured namespace's nil store crashed the pod | panic in `store.Set(nil)` | `TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash` |
| compressed values decoded twice remotely, framed twice on hand-off | failing tests | `TestCompressedValuesSurviveEveryRemotePath`, `TestACompressedHandOffIsNotFramedTwice` |
| a load overwrote a write that raced it | failing test | `TestALoadDoesNotOverwriteAWriteThatRacedIt` |
| a stale tombstone deleted a newer replica | failing test | `TestAStaleTombstoneDoesNotDeleteANewerReplica` |
| a timer due on a full mailbox was lost and pinned its group | failing tests | `TestATimerDueWhileTheMailboxIsFullStillFires`, `TestATimerNamingAnUnregisteredFunctionDoesNotPinTheGroup` |
| Close leaked each L1 tier's goroutine | goleak | `TestCloseReleasesTheL1Tier` |
| a failed New leaked probation, the ticker and the runtime | armed timers, goleak | `TestAFailedNewLeavesNothingRunning` |
| a snapshot page copied the whole store | 10.4 MB for one entry | `TestASnapshotPageAllocatesOnlyItsPage` |
| refused activations left empty records forever | 1000 left by 1000 calls | `TestARefusedActivationLeavesNoRecord` |
| gossip could deadlock republishing its epoch | memberlist source: a blocking send under the node lock | not reproducible below ~64 real nodes |
| Group's direct accessors fail on non-owners | by design; the interface did not say so | documentation fixed |
| a membership change briefly skipped the activation delay | reasoned: two adjacent atomic stores | not stageable without a hook |

Two of the timer tests hung in their first form on their own harness — a
probe that blocked on the busy group, and a wait that never moved the fake
clock — and were rewritten so that nothing in them can block. Every fix has an
audit entry except the activation-delay order, which no test can distinguish.

**Also fixed: a harness bug.** `tests/ownership_move_test.go`'s transport wrapper hid
`Ready`, so `New` returned before the node was serving and the node was
sometimes never admitted: 40 failures in 60 runs without `-race`, none with it.
The two older wrappers already forwarded it.

### A latency test that passed for the wrong reason

`TestLatencyHistogramsRecordRemoteOperations`, written in the coverage sweep,
asserted that a remote read under a 2 ms delay per hop took at least 4 ms — a
request and a response. It passed in every run in the session it was written,
then failed once in a full gate, reading 2 ms. The fake network delays a call's request and returns its response at
once, so the true lower bound is 2 ms; the test had passed only because its
waiting loop steps the fake clock and inflated every measurement it took. The
bound is 2 ms now, ten clean repetitions, and its mutation entry is still
caught. A test whose assertion is satisfied by its own scheduling noise is not
testing the code.

### The full audit after M10: 154 of 154

Every entry in `scripts/mutation-audit.sh`, run end to end on a clean tree after
M10 — the first full run since M8's 110. **caught: 154, vacuous: 0, skipped: 0,
tree clean**, in 11 m 11 s on Machine A.

It is not the same statement as the filtered runs that verified each entry as it
was written. Those said each entry worked against the tree of its day; this says
all of them work against the tree as it is, which is the only run that would
notice a later change weakening an earlier test. Before it could be run, a dry
check of every entry's text against its file found five that M9 and the coverage
sweep had silently turned into skips; they were repointed first. The drain entry
recorded above as flaky was caught.

### The S3 backend is outside the audit script, so its mutations were run by hand

`scripts/mutation-audit.sh` runs `go test "$pkg"` from the repository root, and
`persist/s3` is a nested module the root cannot reach. Its two properties that
matter most were broken by hand instead, and both were caught:

| mutation | test | result |
|---|---|---|
| every API error counts as "not found" | `TestOtherErrorsAreNotNotFound` | caught |
| a listing stops after its first page | `TestTheStoreMeetsTheContract` (the fake pages two keys at a time) | caught |

The first is the dangerous one: an access-denied read as a miss would make a
restore treat stored keys as absent.

**Since then the script reaches nested modules.** An entry whose package is
written `dir::pkg` runs `go test -C dir pkg`, inside that module's own
`go.mod`; both mutations above are now ordinary entries and run with the rest.

### The full audit after M14b: 236, eleven repointed, one re-aimed

Every entry run end to end after M14b, on Machine A, in 20 m 45 s: **caught:
224, vacuous: 1, skipped: 11**. Not a clean run, and recorded as it was.

The eleven skips were entries whose code the persistence plumbing had
reshaped — order arguments threaded through `fanOut` and `Restore`, the
`err :=` form open question 45 introduced, the `scan` refactor. Each was
regenerated against the code as it is, its text asserted present exactly once.

The vacuous one was "a promoted replica's order is not carried". Its test no
longer showed it, because observing an order on receipt is what makes the
replica's next write win. What the carried order protects is a later restore:
installed at order zero, a promoted value is outranked by any stored record.
`TestARestoreDoesNotUndoAPromotedWrite` was written for it — and was itself
vacuous twice before it worked. First a restore's overlay of pending records
masked the case; then it turned out the test helper `flushAll` returned when the
pending map emptied, which a flush does *before* its writes reach the store, so
the test had been running against an empty store. Both are fixed; the helper
now waits for the flush counter.

The twelve were then run by name: **caught: 12, vacuous: 0, skipped: 0, tree
clean**. No library code changed between that and the full run, so the 236 are
accounted for — but by two runs, not one, and this says so.

### The full audit after M13: 200 of 200

Every entry run end to end after M13, on Machine A, in 18 m 5 s: **caught: 197,
vacuous: 0, skipped: 3**. The three skips were entries whose code M13 had
moved — the L1 tier's stale check became a variable so that a floor could admit
its own stamp, and the metric count in docs/api.md went from 46 to 50. Each was
repointed at the new text and run on its own: **caught: 3, vacuous: 0,
skipped: 0, tree clean**. 200 entries, 200 caught.

The same milestone's race gate failed before it passed, and not on M13's code:
`internal/actor` let an internal "look again" sentinel escape `Do` when two
concurrent refusals removed a record twice (open question 47). It fails about
one run in a hundred in isolation, and this gate is the first place it was
seen.

### The documented way to build a cluster did not compile outside this module

The coverage sweep's remaining zeros were mostly trivial — no-op logger methods,
`String` methods, memberlist delegate stubs. One was not: `WithSeeds`, set by its
option and read by nothing. Following it found that the gossip membership and the
gRPC transport lived under `internal/`, and that `docs/api.md` told applications
to import them from there.

That was checked rather than assumed: a scratch module with a `replace` onto this
repository, containing the documented wiring, failed to build with "use of
internal package ... not allowed". An application could run one node and nothing
more. No test could see it, because every test and example is inside the module.

Both packages are public now (decision 0024), `WithSeeds` joins, and
`TestAnExternalModuleCanBuildACluster` compiles the documented wiring as a
separate module, offline, on every run. It was written first and failed for the
right reason — the packages were not at the public path — before the move.

### The rest of the coverage sweep: four more paths, three more findings

After the TTL fix, the remaining zero-count blocks were worked through in order
of what a failure there would cost. Eleven tests, eleven mutation entries, all
caught by hand.

- **Ownership moving under a write.** `Set` and `Group.Call` re-resolve once on
  `ErrNotOwner` and send to the new owner. No test had reached the branch that
  follows a *changed* answer, because ordinary gossip never makes a caller stale
  on the first attempt and fresh on the second. `tests/ownership_move_test.go` freezes
  one node's view, revives a node so the rest re-rank, and thaws the view from
  inside the transport at the first `ErrNotOwner`. The first version passed for
  `Call` and failed for `Set`: the chosen key was the stale caller's own under
  the old view, so it wrote locally and asked nobody. The test now tries keys
  until one reaches a stale owner.
- **Remote `GetOrLoad` with no loader said `ErrNotFound`.** Locally it said
  `ErrNoLoader`. The owner's handler answers "absent" when it has no loader, and
  the caller asked it even though its own namespace — configured identically on
  every pod — had none. A caller was told the key did not exist when the loader
  did not. Checked before routing now.
- **A peer's own failure crossed gRPC as `ErrNodeUnreachable`.** An unrecognised
  handler error was sent as `codes.Internal`, which gRPC also raises for a broken
  stream and which the client therefore maps to unreachable. A loader whose
  backend was down looked like a network fault. Sent as `codes.Unknown` now and
  surfaced with its message and no sentinel.
- **`EventEvict` is never emitted.** Documented as emitted on TTL or capacity
  eviction; group state has neither. Documented as reserved, open question 43.

Also covered for the first time: remote `BatchGet` and `Exists`, latency
histograms (benchmarked in M7, never tested), `WithEventHandler`, a second
`Drain`, `Drain` under join probation — whose wrapper forwards the departure
announcement — and `Drain` when that announcement fails.

### After M9: a coverage sweep, and a key that came back from the dead

Asked to test what was untested, the first step was to measure rather than
guess: `go test -coverpkg=./...` across the module, then every block with a zero
count, merged across packages by maximum. Two branches stood out because they
were the *only* code handling a TTL on a path between nodes: the drain's
remaining-TTL computation and catch-up's. Neither had ever run.

Writing tests for them found that the replica store held a TTL as a **duration**
and promotion restored it from the moment of promotion. A key that had expired
on its owner came back, with a fresh lifetime, on the node promoted in its
place; a key drained or promoted mid-life lived longer than it was written for.
The struct's own doc comment said the opposite — "so that a promoted replica
expires the key on the owner's schedule" — and nothing tested it. An expiry is
never replicated; it holds only if every holder agrees on the instant. Replicas
now hold a deadline. Nothing swept expired replicas either, so under a TTL
workload every replica ever sent stayed resident; `SweepExpired` runs on the
cache's tick.

`TestAnExpiredKeyStaysExpiredAfterFailover` failed with "the promoted replica
serves "v". An expired key came back", and `TestADrainedKeyKeepsItsRemainingTTL`
with the key still served past its TTL. `TestACaughtUpKeyKeepsItsRemainingTTL`
passed against the bug in its first form — it checked long after both the right
and the wrong deadline — and was rewritten to check between them.

**Two checks this harness cannot exercise.** Promotion and hand-off each skip a
replica whose deadline has passed, because between the last sweep and the next
there is up to a second in which an expired copy is still held, and restoring it
with a negative remainder would make the owned store keep it forever. In the
harness the sweep always gets there first: the fake ticker hands its tick to a
goroutine that reads the clock after the advance has finished. Removing either
check changes nothing a test sees, so neither is in the audit; both are reasoned.
The receiver also refuses a negative TTL on the wire, for the same reason.

### M9 — resilience under resource pressure, and what checking it found

Eleven mutation entries, every one applied by hand and caught before it was
written into the audit, and each later re-run through the script on a clean
tree. Four things the process found are worth keeping:

**A test expectation that was wrong, not the code.** The first version of
`TestSheddingTakesL1AndReplicasBeforeOwnedData` expected an L1-only shed after
filling L1, and failed: replicas were shed too. The node had already been shed
to its low-water mark and sat just above it, so even an empty L1 left it over,
and the remainder correctly came from replicas. The test now brings the nodes
well below the mark first, which is the only condition under which "L1 first"
is distinguishable from "everything at once".

**A vacuous entry.** Removing the budget check on replica arrivals changed
nothing any test saw: the same node's own writes and the periodic tick caught
the growth anyway. `TestAReplicaOnlyNodeStaysWithinItsBudget` gives a node only
replicas and no clock tick, which is the inflow a failover produces.

**A resurrection hazard the first design left open.** Shedding an owned key
first only sent its replicas a removal. Under queue pressure that message can be
dropped, and then the rewrite of the key — restarting its version at 1 (question
18) — loses to the replica's copy and a failover promotes the shed value. The
store now keeps a per-shard version floor at the highest version shed, so
correctness does not depend on the message (design principle 6 (docs/design.md)). The same
hazard exists for an ordinary asynchronous Delete; that is open question 42.

**A flake.** `MembersOnProbation` was cached by the relay goroutine and read
once as 1 after the probationed node had died, in four of ten runs. It is
computed from the authority now, as `Members` is; ten repetitions clean.

**And a measured cost.** The cascade test with a budget keeps every survivor
under it and loses 348 of 1500 keys across three deaths at RF=2; without one it
loses none on survivors holding 1.9× the budget. Recorded in `docs/api.md`.

### After M8: question 40, and a CatchUp error that had never been returned

`ReadyState.ReplicasCurrent` reports whether a node has completed a catch-up,
with no failure, under the membership epoch it holds now. Defining it as "the
last catch-up succeeded" meant reading what `CatchUp` returns, and it returned
`nil` unconditionally: since M5 the per-peer error was logged, counted in
`CatchUpFailures`, and never assigned to the variable that was returned. The
metric was right and the return value was not, and every test that checked the
return value did so on a converged view, where nil was also the correct answer.

`TestCatchUpReportsAPeerItCouldNotReach` partitions the asker while its view
still lists both peers. It was written before the fix and could not compile
until `ReplicasCurrent` existed, so its failure against the old code is shown by
the first audit entry below, which restores that code, rather than by a run
before the fix. The root package ran green under `-race` with the error now
returned, so no existing caller had been relying on the nil.

| Mutation | Caught by |
|---|---|
| `CatchUp` discards the per-peer error again | `TestCatchUpReportsAPeerItCouldNotReach` |
| `ReplicasCurrent` ignores the epoch | `TestReplicasCurrentFollowsCatchUpAndMembership` |
| a failed catch-up still marks replicas current | `TestCatchUpReportsAPeerItCouldNotReach` |

---

## The acceptance map

docs/design.md's Definition of Done requires that "the milestone's acceptance criteria
in docs/design.md are each covered by a named test that would fail if the behavior
regressed". Every section above records a check against *one* invariant. None of
them answered the question the rule actually asks: is every criterion covered?

That mapping existed only in the heads of whoever wrote each milestone. It is
written down here because a Definition of Done nobody can audit is a Definition
of Done in name only — the same failure this whole file exists to prevent, one
level up.

docs/design.md §4 carries thirteen `**Acceptance:**` blocks, M0 through M6, M9, M10, M12, M13, M14 and M15;
M7 states its requirements as bullets instead. Broken into individually
testable claims they come to seventy. Each one, and the named test that would fail if it regressed:

| Milestone | Criterion | Named test |
|---|---|---|
| M0 | 5-node cluster, 3/2 partition, +1h, heal, assert membership, under 50ms | `TestM0Acceptance` |
| M0 | zero `time.Sleep` | `TestNoSleepAnywhere` |
| M1 | local Get p99 under 1 µs at 1M keys | `TestLocalGetP99AtOneMillionKeys` |
| M1 | correct TTL expiry under a fake clock | `TestTTLExpiryUnderFakeClock` |
| M1 | 1000 concurrent misses invoke the loader exactly once | `TestGetOrLoadCollapsesConcurrentMisses` |
| M2 | 10k concurrent `Call`s across 1k groups, serialized under `-race` | `TestTenThousandCallsAcrossOneThousandGroups` |
| M2 | memory at 1M addressable / 20k active inside budget | `TestMemoryAtOneMillionAddressableGroups` |
| M2 | deactivate and reactivate preserves state and pending timers | `TestDeactivationPreservesPendingTimers` |
| M3 | 100k keys over 7 nodes within 5% of uniform | `TestOwnershipIsUniform` |
| M3 | removing one node moves approximately 1/7 and no more | `TestRemovingANodeMovesOnlyItsKeys` |
| M3 | a partitioned minority stops activating within `activationDelay` | `TestPartitionedMinorityStopsActivating` |
| M4 | remote Get p99 under 2 ms, p999 under 10 ms, at 50k rps | `TestRemoteGetThroughputAndLatency` |
| M4 | 10k messages across 3 owners issues at most 3 RPCs per flush | `TestRouterIssuesOneRequestPerOwner` |
| M4 | killing the transport mid-request surfaces an error, never a hang | `TestKillingTheTransportMidRequestSurfacesAnError` |
| M5 | killing an owner ungracefully promotes and resumes service | `TestKillingAnOwnerPromotesAReplica` |
| M5 | chaos: a kill every 2s for 60s loses no synchronously written key | `TestChaosLosesNoSynchronouslyWrittenKey` |
| M5 | split-brain writes detected and counted in metrics | `TestTwoWritersFromOneBaseIsAFork`, `TestAWriteOverAValueItNeverReceivedIsAFork`, `TestAGracefulRollingUpgradeCountsNoFork`, `TestEveryStatsFieldIsExported` |
| M6 | 9-node rolling upgrade: zero failed requests, movement within ±10% | `TestRollingUpgradeLosesNoRequests` |
| M7 | soak at target throughput, memory flat | `TestSoakMemoryStaysFlat`, `TestSoakGoroutinesStayFlat` |
| M7 | correctness metrics exported as counters | `TestCorrectnessMetricsAreCounters`, `TestEveryStatsFieldIsExported` |
| M7 | examples runnable, `docs/api.md` complete | `TestApiDocExampleTableMatchesReality`, `make examples` |
| M9 | five nodes killed down to two: every survivor within one shedding step of its budget, every shed counted by tier | `TestAFailoverCascadeStaysWithinBudget`, `TestSheddingTakesL1AndReplicasBeforeOwnedData` |
| M9 | a key shed and rewritten is never served at an older value, before or after a failover | `TestAShedKeyIsNeverServedAtAnOlderValue`, `TestARewriteAfterShedIsVersionedAboveWhatWasShed` |
| M9 | a node restarted every 2 s for 60 s causes no key held by the healthy nodes to be reported missing | `TestACrashloopingNodeIsNeverRanked` |
| M9 | every message to one node delayed 200 ms: no split brain, no latency change for keys owned elsewhere | `TestAStarvedNodeSlowsOnlyItsOwnKeys` |
| M10 | a typed value set on one node reads back equal on every node through Get and BatchGet | `TestTypedValuesRoundTripAcrossNodes` |
| M10 | the same through GetOrLoad with a typed loader | `TestTypedGetOrLoadAcrossNodes` |
| M10 | invalid UTF-8 in a string round-trips intact under the default codec | `TestInvalidUTF8SurvivesTheDefaultCodec` |
| M10 | one name opened with two types is ErrInvalidOption; an undecodable value is ErrCodec | `TestOneNameTwoTypesIsInvalid`, `TestAnUndecodableValueIsErrCodec` |
| M10 | the typed path costs the byte path plus one encode or decode, measured | `BenchmarkTypedGet` and its comparisons, recorded in docs/benchmarks.md |
| M12 | a thousand concurrent increments from every node return distinct results and total a thousand | `TestConcurrentIncrementsFromEveryNodeLoseNothing` |
| M12 | of a hundred concurrent SetIfAbsent calls exactly one stores, and every node reads it | `TestExactlyOneConcurrentSetIfAbsentWins` |
| M12 | a stale CompareAndSwap changes nothing; on a compressed namespace a matching one succeeds | `TestCompareAndSwapChangesOnlyWhatItWasShown`, `TestCompareAndSwapComparesWhatGetReturns` |
| M12 | Increment of a non-integer or an overflow is ErrNotAnInteger and leaves the value | `TestIncrementRefusesWhatIsNotAnInteger` |
| M12 | a touched key outlives its original deadline, on its owner and after failover | `TestTouchOutlivesTheOriginalDeadlineAcrossFailover` |
| M12 | an increment applied before its reply was lost is not applied again | `TestAnIncrementWhoseOwnerFailedAfterApplyingIsNotRetried` |
| M12 | a local Increment within twice a local Set, measured | `BenchmarkNamespaceIncrement`, recorded in docs/benchmarks.md |
| M13 | a thousand concurrent reads of one key owned elsewhere cost its owner one RPC | `TestConcurrentRemoteMissesShareOneFetch` |
| M13 | a caller that quits a shared fetch, or whose deadline ends it, does not fail the others | `TestACancelledWaiterLeavesTheSharedFetch`, `TestALeadersDeadlineIsNotEveryones` |
| M13 | at RF 3 replica reads spread across every holder and fewer than half reach an owner | `TestReplicaReadsSpreadAcrossHolders` |
| M13 | a replica copy one TTL old is declined and the owner answers | `TestAReplicaCopyOlderThanTheTTLGoesToTheOwner` |
| M13 | a node that no longer ranks as a replica refers the read | `TestANodeThatNoLongerRanksRefersTheRead` |
| M13 | a replica without the key does not produce ErrNotFound | `TestAReplicaWithoutTheKeyIsNotAMiss` |
| M13 | a peer without ReplicaGet sends the read to the owner | `TestAPeerWithoutReplicaGetFallsBackToTheOwner` |
| M13 | a replica answer below an invalidation floor is discarded | `TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded` |
| M13 | a self-replica answers without an RPC | `TestASelfReplicaServesWithoutAnRPC` |
| M13 | with replication and invalidations lost, every node reads the new value one TTL later, and a replica copy is cached only for what is left of its TTL | `TestADroppedReplicationIsReadAfterOneTTL`, `TestAReplicaCopyIsCachedForWhatIsLeftOfItsTTL` |
| M13 | replica reads at RF 1 or without an L1 tier are ErrInvalidOption | `TestReplicaReadsNeedReplicasAndAnL1` |
| M13 | the coalesced and replica-read paths, measured | `BenchmarkRemoteGetOneHotKey`, `BenchmarkColdRemoteGet`, recorded in docs/benchmarks.md |
| M14 | killing every holder of a key restores it on its new owner | `TestKillingEveryHolderRestoresFromStorage` |
| M14 | a full restart reads every flushed key, and no deleted or expired one | `TestAWholeClusterRestartRestoresEverything`, `TestAColdStartRestoresWhatWasFlushed` |
| M14 | a restore merges every snapshot; a failed snapshot deletes nothing | `TestARestoreMergesEverySnapshot`, `TestAFailedSnapshotDeletesNothing` |
| M14 | a promoted replica's next write outranks its dead owner's records | `TestAPromotedReplicasWriteOutranksItsOldOwner` |
| M14 | a shed owned key is restored on its next access | `TestAShedKeyIsRestoredNotLost` |
| M14 | an unavailable store fails no write and loses nothing; a restore that cannot read it degrades to memory | `TestAnUnavailableStoreLosesNothing`, `TestAnUnavailableStoreDegradesToMemory` |
| M14 | a namespace without persistence within noise of before, measured | `BenchmarkNamespaceSet`, `BenchmarkNamespaceGet`, `BenchmarkPersistentSet`, recorded in docs/benchmarks.md |
| M14 | `persist.Dir` meets the blob-store contract and writes data, renames, then syncs the directory | `TestDirMeetsTheContract`, `TestPutIsWriteSyncRenameSyncDir` |
| M14 | a Dir Put interrupted at any step leaves a whole object; a crash's leftovers are invisible and cleaned up | `TestAnInterruptedPutLeavesTheOldObjectOrTheNew`, `TestACrashsLeftoversAreInvisibleAndCleanedUp` |
| M14 | the loss window on `persist.Dir`, measured | `BenchmarkPersistLossWindow`, `BenchmarkPersistFlushSpread`, `BenchmarkDirPut`, recorded in docs/benchmarks.md |
| M14 | one flush is one object; a split moves every log into partition segments and deletes the logs | `TestAFlushWritesOneObject`, `TestASplitMovesLogsIntoPartitions` |
| M14 | a split that fails part-way loses nothing and the next one completes it | `TestAFailedSplitLosesNothing` |
| M14 | the flush interval is a namespace option; a non-positive one is refused | `TestWithFlushIntervalSetsTheCadence`, `TestANonPositiveFlushIntervalIsRefused` |
| M14 | the S3 backend meets the contract over paged listings and tells absent from refused | `TestTheStoreMeetsTheContract`, `TestNotFoundIsRecognisedEitherWay`, `TestOtherErrorsAreNotNotFound` |
| M14 | a namespace survives a restart over S3, one PUT per flush | `TestACacheRestoresThroughS3` |
| M15 | Describe reports node, view, readiness and each namespace's options and counts | `TestDescribeReportsTheNodeAndItsNamespaces` |
| M15 | DescribeKey agrees on owner and holders everywhere and tells each node's role and copies truly | `TestEveryNodeDescribesAKeyTheSameWay`, `TestDescribeKeyOfAnUnopenedNamespaceIsNotFound` |
| M15 | no description or placement contains a value | `TestADescriptionNeverContainsAValue` |
| M15 | the debug handler serves JSON, refuses other methods, and answers bad requests with a 4xx | `TestTheDebugHandlerServesJSONAndRefusesTheRest` |
| M15 | the OpenTelemetry bridge makes each series one instrument of its kind, with help, node attribute and a counter's name less _total | `TestEverySeriesIsBridged`, `TestCounterNamesDropTheirTotal` |
| M15 | the bridge follows the cache between collections and stops when unregistered | `TestValuesFollowTheCache`, `TestUnregisterStopsObserving` |

All eighty-seven names in the table were verified to exist before it was
committed to, which is the discipline the rest of this file argues for and which
five wrong figures in the M8 section demonstrate the need for.

Twice that was not enough, in this paragraph alone. The first draft said
"eighteen distinct criteria" above a table of twenty-one rows. The second said
"twenty-two names" where the table cites twenty-three — because the verification
loop was written from memory of which tests to check rather than from the table,
and missed one added earlier the same day.

Both are the same mistake as the five above and were caught the same way, by
counting the artifact instead of recalling it. The instructive part is that they
were made *while writing the argument against making them*, by someone who had
spent the preceding hour fixing five instances. A habit of recomputing is not
acquired by being convinced; the count has to actually be run, every time, and
the loop has to read the thing it is checking rather than a list of what the
author believes is in it.

So it is no longer a habit. `TestAcceptanceMapCountsAreCorrect` counts the rows
and the distinct test names in this section and fails if the prose disagrees with
either, and it also fails if docs/design.md states an acceptance block with no row here —
a milestone dropping silently out of the map being the failure the map exists to
make impossible. Both of the mistakes described above were reproduced against it
and both are caught, each naming the number it found and the number it expected.

That is the seventh check added this milestone and the only one written because
the author kept making the same error rather than because someone else had. It is
the better reason of the two.

**What the map is not.** It says a named test exists for each criterion. It does
not say that test would fail if the behaviour regressed — that is the mutation
audit's job, and `scripts/mutation-audit.sh` covers many of these and not all of
them. Two things are worth knowing about the gap: M6's criterion was audited by
an entry that could not have caught its own mutation until M7 rewrote it
(recorded above), and M4's ran for the first time in M8 because the test could
not bind a socket on this machine. A row in this table is a claim of coverage,
not of proof.

---

## Method

- Mutations are applied by hand, one at a time, and reverted immediately.
- `git diff --stat` confirms the tree is clean afterwards. Any mutation helper
  (a package-level sink variable, a `if false &&` guard) is removed too.
- Mutations are never committed, including behind a flag. A mutation that can be
  switched on is a mutation that will eventually be switched on by accident.
