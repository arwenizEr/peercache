# Verification record

CLAUDE.md's Definition of Done requires that each milestone's acceptance
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
membership. Adding `cluster_test.go` to the root package put four gossip nodes
in the same binary; under `-count=2`, the goroutines leaked by the first
iteration pushed the second iteration's p99 from 0.5µs to 1.475µs and broke the
1µs budget.

Two things are worth taking from that.

**The `-count=2` gate earned its place.** CLAUDE.md requires it to catch state
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
precisely the inversion CLAUDE.md non-negotiable 6 exists to prevent, on a path
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

## Automated audit

`scripts/mutation-audit.sh` applies each mutation below, runs only the test that
claims to guard it, and restores the file. It refuses to start on a dirty tree —
it reverts with `git checkout --`, which would otherwise discard uncommitted
work — and fails if any mutation is left behind or any test turns out to be
vacuous.

Latest run, all fifty-nine invariants:

```
=== store ===
caught   Set no longer copies the caller's buffer   -- TestSetCopiesTheCallersBuffer
caught   Get copies instead of aliasing             -- TestGetReturnsTheStoredSlice
caught   read-path expiry disabled                  -- TestExpiryIsObservedOnRead
caught   shard selection collapses to one shard     -- TestKeysSpreadAcrossShards
caught   TTL deadline overflow reintroduced         -- TestUnrepresentableTTLDoesNotExpireImmediately
caught   store version stops incrementing           -- TestVersionsIncrementPerKey

=== L1 ===
caught   L1 accepts an older version                -- TestOlderVersionIsRejected
caught   L1 stops clamping TTL to MaxTTL            -- TestEveryEntryIsTTLBounded
caught   L1 write stripe removed                    -- TestConcurrentPutsNeverLeaveAnOlderVersion
caught   invalidation ignores the resident version  -- TestInvalidateOlderThanKeepsNewerCopies

=== codec ===
caught   frame parser accepts an unknown code       -- TestUnframeRejectsCorruptInput

=== singleflight ===
caught   loader panic is no longer contained        -- TestPanickingLoaderBecomesAnError
caught   completed flights are never released       -- TestSequentialCallsDoNotCollapse

=== clock ===
caught   drops the creation-order tie-break         -- TestFakeEqualDeadlinesFireInCreationOrder

=== namespace ===
caught   namespaces share one store                 -- TestNamespacesAreIsolated
caught   singleflight bypassed                      -- TestGetOrLoadCollapsesConcurrentMisses
caught   namespace skips the context check          -- TestCancelledContextIsReportedFirst
caught   loader runs without the namespace deadline -- TestLoaderTimeoutIsTheNamespacesNotTheCallers
caught   Items snapshot aliases group state         -- TestItemsSnapshotDoesNotAliasGroupState

=== actor runtime ===
caught   escaped group state is honoured            -- TestEscapedStateIsInertAndCounted
caught   a full mailbox blocks instead of rejecting -- TestFullMailboxRejectsRatherThanBlocking
caught   a group function panic is not contained    -- TestPanickingGroupFuncDoesNotWedgeTheGroup
caught   deactivation discards group state          -- TestDeactivationPreservesState
caught   deactivation discards the timer schedule   -- TestDeactivationPreservesPendingTimers
caught   pending timers no longer block idle exit   -- TestDeactivationPreservesPendingTimers
caught   the active-group cap is ignored            -- TestActiveCapEvictsLeastRecentlyActivated
caught   rescheduling a name accumulates timers     -- TestReschedulingATimerReplacesIt

=== ownership ===
caught   ownership ignores the key                  -- TestOwnershipIsUniform
caught   node identity no longer decorrelates       -- TestOwnershipIsUniform
caught   an empty membership owns everything        -- TestEmptyMembershipOwnsNothing
caught   the epoch stops advancing                  -- TestUpdateAdvancesTheEpoch
caught   zero nodes enter the view                  -- TestUpdateIgnoresZeroNodes
caught   the replica ranking is unsorted            -- TestReplicasAreARankedPrefix
caught   an unknown membership claims everything    -- TestGuardWithNoMembershipOwnsNothing
caught   the guard activates groups it does not own -- TestGuardRefusesGroupsItDoesNotOwn
caught   newly acquired groups activate immediately -- TestGuardDefersOnlyNewlyAcquiredGroups
caught   compares the previous view, not the settled -- TestPartitionedMinorityStopsActivating
caught   a stale epoch is accepted                  -- TestStaleEpochIsRefused
caught   a caller ahead of this node is not deferred -- TestFutureEpochDefersRatherThanRefusing
caught   the epoch is checked before ownership      -- TestNotOwnerBeatsStaleEpoch
caught   local activation ignores the current epoch -- TestCanActivateUsesTheCurrentEpoch

=== membership and wiring ===
caught   gossip metadata updates advance the epoch  -- TestEpochDoesNotRunAway
caught   the gossip event goroutine is never released -- TestCloseLeavesNoGoroutines
caught   the guard is built but never consulted     -- TestTwoNodesAgreeOnOwnership
caught   the guard never learns the membership      -- TestJoiningNodeDefersNewlyAcquiredGroups

=== wire and transport ===
caught   a length prefix buys an allocation          -- TestALengthPrefixCannotBuyAnAllocation
caught   the L1 tier compares versions, not stamps   -- TestANewOwnerSupersedesAHigherVersion
caught   invalidation compares versions, not stamps  -- TestInvalidationRespectsTheEpoch
caught   the epoch does not dominate the version     -- TestStampOrdering
caught   the transport codec drops the body          -- TestRoundTripOverLoopback
caught   a caller does not identify itself           -- TestRoundTripOverLoopback
caught   sentinel errors do not survive the hop      -- TestSentinelErrorsSurviveTheHop
caught   Unavailable is read as a deferred activation -- TestUnavailableDefaultsToUnreachable
caught   connections are not pooled                  -- TestDialPoolsConnections
caught   the server does not verify client certs     -- TestMutualTLSRejectsAnImpostorCA
caught   the client does not verify the server       -- TestMutualTLSRejectsAnUntrustedServer
caught   the TLS floor drops to 1.0                  -- TestTLSFloorIsEnforced
caught   gossip advertises the SWIM port             -- TestGossipAdvertisesTheRPCAddress

=== routing ===
caught   the router batches per group, not per owner -- TestRouterIssuesOneRequestPerOwner
caught   a batch is applied one message at a time    -- TestRouterSubmit
caught   every node runs its own loader              -- TestTheLoaderRunsOnlyOnTheOwner
caught   enqueueing an invalidation blocks when full -- TestInvalidationBacklogIsDroppedNotQueued

caught: 59   vacuous: 0   skipped: 0
tree clean
```

They are chosen to be the invariants whose quiet failure would be worst:
the aliasing contract, every rule that bounds staleness, both halves of the
version discipline, the panic containment that keeps application bugs local, and
the deadline rules. Not everything is audited — the p99 criterion is mutated by
hand because a single run costs a minute, and the reference-model and fuzz tests
audit themselves by construction.

The last two are a different kind of mutation, added in M3, and worth calling
out. Every other entry breaks logic inside a package and is caught by that
package's own tests. Those two break *wiring* — they leave `internal/ownership`
untouched and correct, and only stop `peercache.New` from connecting it to the
membership authority or to activation. Nothing in `internal/ownership` notices,
because from its point of view nothing is wrong.

They are the reason `cluster_test.go` exists. Before it, thirteen ownership
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

## Method

- Mutations are applied by hand, one at a time, and reverted immediately.
- `git diff --stat` confirms the tree is clean afterwards. Any mutation helper
  (a package-level sink variable, a `if false &&` guard) is removed too.
- Mutations are never committed, including behind a flag. A mutation that can be
  switched on is a mutation that will eventually be switched on by accident.
