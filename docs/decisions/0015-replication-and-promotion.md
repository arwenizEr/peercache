# 0015 — Replication, promotion, and why a restarted node must catch up

**Status:** accepted
**Milestone:** M5
**Supersedes:** nothing

## Context

M4 left a cluster where every key had exactly one home. Losing a pod lost its
data, which CLAUDE.md's durability section permits — state is lossy by design —
but "permitted" and "acceptable to run" are different bars. SPEC.md §4 asks for
replication factors 1 to 3, promotion on owner failure, catch-up for a returning
node, and a measured data-loss window for the asynchronous default.

## Decisions

### Replicas are the runners-up in the rendezvous ranking

The replica set for a key is the prefix of the same ranking that chooses its
owner. Nothing is stored about placement and no messages are exchanged to decide
it.

This is what makes promotion free. When a node dies, the node that ranked second
for each of its keys ranks first on every surviving node at the same instant,
computed from the same membership. There is no election, no lease, no handover
protocol, and nothing to get wrong under partition — because nothing is being
agreed.

What the M5 work actually had to add is that the new owner *holds the data*. The
promotion mechanism was already there in M3; the missing part was the bytes.

### The replica store is separate from the owned store

A flag on an entry would have been less code. It was rejected for three reasons
that only became clear while writing promotion.

Ownership moves back and forth as members come and go, so a per-entry flag means
rewriting every key on every membership change to record something derivable
from the ownership table in nanoseconds.

The two have different lifetimes: owned entries expire on their own TTL, replica
entries live until the owner says otherwise or this node stops ranking for them.

And `Stats().ReplicaKeys` has to mean something. "How much of this pod's memory
is data it does not own" gets an answer from a separate map and a guess from a
shared one.

### Asynchronous by default, synchronous per namespace, one code path

Both modes send the same request to the same nodes. The only difference is
whether the owner waits for the answer.

That is deliberate. A system where the two modes take different code paths is
one where the rarely-used mode is the broken one, discovered during the incident
it was turned on to prevent.

Synchronous replication holds the group's activation while the fan-out
round-trips, so the group's write rate becomes its slowest replica's latency.
That is stated in `WithSyncReplication`'s documentation rather than discovered:
it buys durability with throughput on the group, not with throughput on the
cluster. Acking outside the mailbox would avoid the cost and break the ordering
that makes the actor model worth having.

### Conflicts are detected and counted, never merged

One group has one owner, so two *different* nodes writing the same key under the
*same* epoch cannot happen unless two believe they own it. The replica is the
only party positioned to notice — neither owner can see the other.

It is counted in `Stats().SplitBrainDetections` and the write still applies.
Silently dropping one would be a merge policy chosen by accident, and CLAUDE.md
is explicit that conflicting writes are not automatically merged.

### Promotion runs on the ownership table's update, not on a membership watcher

This was a bug before it was a decision. The promoter originally watched
membership itself, alongside the guard, and two independent watchers of the same
authority have no ordering between them. The promoter could reconcile *before*
the table held the new view: it would see the dead node still owning its keys,
promote nothing, and then wait forever for an event that had already been
delivered.

Promotion now hangs off `Guard.OnUpdate`, so it runs after the table is current
by construction rather than by hoping the scheduler is kind.

### A restarted node must call `CatchUp` before serving

This is the most operationally important thing in this milestone and it was
found by the chaos test rather than by design.

Replication is a stream of writes, not a state transfer. A node that restarts
comes back with empty memory and receives only the keys written *after* it
returned — every key that was quiet when it died stays missing indefinitely.

Worse, the ranking hands it back ownership immediately. So it answers "not
found" with total confidence for data sitting safely on the node that covered
for it. In the chaos test that presented as **558 of 600 keys lost, none of them
actually gone**. A caller cannot tell that apart from a genuine miss, which
makes it worse than the node being unavailable.

`Cache.CatchUp` is therefore a required part of the restart procedure, not an
optimisation. It is explicit rather than automatic on join because a node that
has just started is usually also serving traffic, and pulling an entire replica
set at that moment competes with the requests it exists to answer — the
application knows when it can afford it and the library does not.

Catch-up pages, sorted, with the owner computing entitlement from the ranking
rather than trusting the request. A node cannot obtain keys it has no business
holding by asking for them.

## Consequences

**The asynchronous window is a measured number.** Mean ≈101 µs, p99 1.46 ms at
RF=3 over loopback gRPC. Recorded in `docs/benchmarks.md` and SPEC.md §6, which
had been carrying a placeholder since the spec was written.

**RF=2 loses data when both holders die, and that is documented as a test**
rather than a caveat in prose, so it stays true as the code changes. What is
asserted at RF=2 is that survivors are *correct*: losing data there is what RF=2
means, corrupting it would be a bug.

**Detection latency must beat the failure rate.** A cluster that loses a node
every two seconds while taking three to notice never converges. This is a
configuration constraint on the deployment, not something the library can fix.

**A dropped replication is a real loss of redundancy**, unlike a dropped
invalidation. Both queues drop under pressure, but
`Stats().ReplicationsDropped` being non-zero means a key that should exist on
three nodes exists on fewer until it is next written, so it is counted
separately from the invalidation counters and worth alerting on.

## Alternatives rejected

**Checking the replica store on every miss in the owned store.** Needs no
membership watcher at all and would have made promotion nearly free to write.
Rejected because it puts a second map lookup on every miss for the benefit of
the rare moments after a failure, and it leaves the node unable to answer "how
many keys do I own" — the answer would depend on which store a reader consulted
first.

**Automatic catch-up on join.** Tempting, and it would have hidden the trap
described above. Rejected because it makes a starting node's first act a bulk
transfer that competes with the traffic it was started to serve, with no way for
the application to say "not yet".

**Quorum writes.** Would close the window without the throughput cost of
synchronous replication on the group. Rejected outright: it needs consensus,
which SPEC.md §5 defers permanently, and CLAUDE.md forbids the words "strong"
and "quorum" in this system's vocabulary for exactly this reason.
