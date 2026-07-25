# 0003 — The harness membership is a reachability oracle, not a failure detector

**Status:** accepted, M0
**Affects:** `internal/testkit`, and what M0's acceptance criterion proves

## The problem

M0's acceptance criterion ends with "assert on membership". Membership is M3.
So the harness needs *some* membership implementation now, and the instruction
is explicit that future milestones must not be stubbed.

## The arithmetic that decides it

The criterion also says the whole scenario must finish in under 50 ms of wall
clock, including advancing a fake clock by an hour.

A heartbeat-based failure detector does `(elapsed / interval) x N^2` message
deliveries. Five nodes at a one-second interval across a simulated hour is:

```
3600 rounds x 5 senders x 4 receivers = 72,000 deliveries
```

Each delivery is a goroutine spawn and a channel round trip — call it a
microsecond. That is roughly 72 ms spent simulating *silence*, before the test
asserts anything. Widening the interval to ten seconds only divides it by ten,
and makes detection latency unrealistic in the process.

The property the criterion is really asking for is that simulated time is free.
A polling detector does not have that property at any interval.

## Decision

`testkit.Membership` is a **reachability oracle**. It does not gossip, does not
send heartbeats, and is not a preview of M3.

- The `Network` notifies every membership when reachability may have changed:
  a registration, a partition, a heal, an isolation.
- Each membership asks the network what it can currently reach.
- Detection *latency* is modelled with one timer per observed peer, armed at
  the moment reachability changes: `FailureTimeout` for a departure,
  `JoinDelay` for an arrival.
- A graceful leave is announced directly to every peer that can currently hear
  it, skipping the failure timeout — which is the entire point of leaving
  gracefully. A partitioned minority cannot hear it and still times the node
  out the slow way.

Cost of an idle hour: zero timers fire, because nothing changed. Measured at
23–38 ns for the advance itself.

## What this does and does not prove

**Proves:**

- Detection is timed, not instantaneous. A test must advance past
  `FailureTimeout` to see a failure, and past `JoinDelay` to see a join.
- Both sides of a partition converge on their own half, and each bumps its
  epoch.
- Graceful and ungraceful departure differ in exactly the way that matters.
- A node returning from the dead starts with an empty view.
- A transition armed by a condition that stopped being true is not applied —
  the flapping case, which is the bug this design most invites.

**Does not prove:**

- Gossip convergence, anti-entropy, or the piggybacking behaviour of
  memberlist.
- That two nodes agree. Each node's view is computed independently from the
  same oracle, so they agree by construction rather than by protocol. Under a
  real gossip layer they would converge, briefly disagreeing.
- Anything about message volume or gossip fan-out.

That second point is the honest limitation. Until M3, a test using this harness
cannot distinguish "the algorithm converges" from "the oracle told everyone the
same thing".

## What happens at M3

The memberlist-backed `internal/membership` satisfies the same
`core.MembershipAuthority` and consumes the same `core.Transport`, so it runs on
this harness unchanged. At that point `testkit.Membership` becomes the fast path
for tests that are not about membership, and the real implementation is used for
tests that are.

## Reversal

Cheap and additive: adding a real gossip implementation does not require
removing this one, and tests select which they want.
