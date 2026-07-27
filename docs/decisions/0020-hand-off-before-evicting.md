# 0020 — Hand a moved replica off before dropping it

**Status:** accepted
**Milestone:** M8
**Adds:** nothing — `handoff.go`, no new dependency, no new RPC

## Context

Reconciliation runs on every membership change. It promotes replicas this node
has become the owner of, and it used to drop the ones it had stopped ranking for:

```go
dropped := c.replicas.Evict(name, func(key string) bool {
        return c.stillReplicaFor(ns.groupKeyFor(key), rf, self)
})
```

The predicate is answered entirely from the local ranking. Nothing asks whether a
node that *now* ranks for the key holds a copy.

Open question 38 proposed exactly that hazard, wrote
`TestEvictionDoesNotDropTheLastCopy` to reproduce it, and could not: killing a
node and letting it rejoin produces zero evictions, because copies placed under
the full membership stay ranked when a member leaves and returns. The question
was left recorded as a real hazard that this schedule does not reach, and the
simulation's losses were attributed — by inference — to the documented AP window.

Writing question 38's own closing assertion is what settled it. When the
simulation began attributing acknowledged writes to a *side* of a partition and
asserting that majority-side writes survive the heal, seed
`5593891418832731417` lost 5 of 12 of them.

### What a partition changes

Writes accepted while one node is cut off place their copies at ranking positions
computed *without* the absent node. Its return moves the ranking underneath every
one of those keys at once: the rejoining node ranks for many of them and holds
nothing, and the nodes that hold them stop ranking and drop them.

The copies do not move. They evaporate. Catch-up cannot repair it — it pulls from
whoever has the key, and after the eviction nobody does.

Reduced to two tests that separate the steps rather than assuming one:
`TestMajoritySideWritesSurviveAHeal` passes, so the heal alone loses nothing;
`TestMajoritySideWritesSurviveAHealAndTwoDestructions` lost 20 of 94. The
counters made it a measurement rather than a hypothesis: twenty-one replicas
evicted cluster-wide, twenty keys lost.

## Decision

A replica this node has stopped ranking for is sent to the nodes that now rank
for it, and dropped only once one of them has accepted it.

Reconciliation drains the orphans out of the replica store under the same lock
`Evict` held, and queues them for a worker goroutine that places them. Every
failure path — a full queue, no resolvable targets, an unreachable peer, a
namespace that has gone away, a shutdown mid-flight — puts the entry back through
the replica store's ordinary `Put`, so the stamp check still applies and a copy
that was overtaken while in transit is correctly refused.

`Stats().ReplicasRehomed` and `ReplicasRetained` account for every replica
`ReplicasEvicted` counts: each moved copy was either placed elsewhere or kept.

### Why not a smarter eviction test

The condition that would make dropping safe — "a node that ranks for this key
holds it" — is not answerable locally. Asking costs a round trip per key on a
membership change, which is the same round trip a hand-off costs, except that the
hand-off also fixes the problem instead of only detecting it.

### Why no new RPC

The receiving side already does the right thing. `MethodReplicate`'s handler
files an incoming copy in the owned store when the receiver owns the key and in
the replica store when it does not — that branch exists for `Drain`, which makes
the same move for a departing node. This applies it to keys that departed rather
than nodes.

### The forwarded copy keeps the original writer's identity

`ReplicateRequest.From` names the node that *wrote* the value, and a replica
records it so that two writers for one key under one membership epoch can be
reported as the split brain they are. A hand-off is a relocation, not a write.

Stamping it with the forwarding node's ID — the obvious thing, and what the first
version did — makes the receiver see the original owner's replication and this
forward as two writers, count a split brain that did not happen, and do it on
every membership change. That would poison the one counter README tells operators
to investigate whenever it is non-zero, which is worse than the metric not
existing. `TestMajoritySideWritesSurviveAHeal` asserts zero detections after a
heal in which only one side ever wrote, and catches the mutation.

### Why it never drops on failure

Keeping a replica too long costs memory that the next reconciliation reclaims.
Dropping one too early costs the key. The asymmetry decides every failure path,
and it is the same reasoning `stillReplicaFor` already used for an unknown
membership view.

## Consequences

**A membership change now moves data.** It always should have; what it did before
was discard it. The volume is bounded by the keys the ranking reassigned, which
`internal/rebalance.ExpectedMoveFraction` already quantifies, and it happens off
the request path on one goroutine with a bounded queue.

**A slow or unreachable peer costs memory rather than data.** `ReplicasRetained`
rising under stable membership is the signal that hand-offs are failing; it is in
`docs/api.md` among the series worth alerting on.

**Reconciliation stays non-blocking.** It runs inside the guard's update callback,
so it enqueues and returns; the network work happens on the worker. A full queue
restores the copy rather than blocking membership processing, which is the same
trade the invalidation and replication queues make and for the same reason.

**The fix was confirmed against the unfixed code.** The reduced test lost 20 of 94
keys before the change and none after, and the simulation seed that found it
replays clean. That is the rule open question 20 records the cost of learning.
