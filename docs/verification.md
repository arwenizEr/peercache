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

## Automated audit

`scripts/mutation-audit.sh` applies each mutation below, runs only the test that
claims to guard it, and restores the file. It refuses to start on a dirty tree —
it reverts with `git checkout --`, which would otherwise discard uncommitted
work — and fails if any mutation is left behind or any test turns out to be
vacuous.

Latest run, all twenty-seven invariants:

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

caught: 27   vacuous: 0   skipped: 0
tree clean
```

The eighteen are chosen to be the invariants whose quiet failure would be worst:
the aliasing contract, every rule that bounds staleness, both halves of the
version discipline, the panic containment that keeps application bugs local, and
the deadline rules. Not everything is audited — the p99 criterion is mutated by
hand because a single run costs a minute, and the reference-model and fuzz tests
audit themselves by construction.

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
