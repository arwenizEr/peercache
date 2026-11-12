# 0021 — Refusing a write below its replication factor is opt-in

**Status:** accepted
**Milestone:** post-M8
**Adds:** `WithRequireReplicationFactor`, `ErrUnderReplicated`,
`Stats().UnderReplicatedRefusals`, `peercache_under_replicated_refusals_total`.
No new dependency, no new RPC.

## Context

Open question 37: a node cut off from the cluster eventually sees itself alone,
ranks itself the owner of everything, and acknowledges each synchronous write
holding the one copy it has. The caller asked for RF copies and cannot tell. M8
counted this (`DegradedWrites`) and left the remaining two options open:

2. report it with a distinguishable error;
3. refuse it.

## What reading the write path settled

The two options are the same option. A synchronous write is stored on the owner
*before* it fans out, and there is no rollback: when a replica does not answer,
`localSet` already returns an error while the value stays held locally. So
"refuse" cannot mean "not written" — it can only mean "reported as not
confirmed", which is what option 2 says. The real question was never 2 against 3.
It was whether the behaviour is the default.

## Decision

Opt-in, per namespace: `WithRequireReplicationFactor()`.

A synchronous write that cannot place `ReplicationFactor` copies from the
writer's view returns an error wrapping `ErrUnderReplicated`. The copies that can
be placed are placed first, so the value is held as widely as the view allows.
An unreachable replica still fails the write with its own error, which takes
precedence because it is the more specific diagnosis. The option without
`WithSyncReplication` is `ErrInvalidOption`: an asynchronous write returns before
it knows how many copies landed.

Over gRPC the sentinel travels as `codes.Internal` plus its text, recovered by
`fromStatus`, the same pattern `ErrActivationPending` uses for `Unavailable`. No
gRPC code means "written, but not to enough nodes", and borrowing one would
mislead anything that reads codes alone.

## Why not the default

Turned on everywhere, the same check refuses every write on a cluster smaller
than the factor. That is one node running alone in development and the examples,
and a pod that has started but not yet joined anyone, which is every pod for a
moment during a rolling restart. From inside the node those are
indistinguishable from isolation; that is the whole of question 37. Making a
partition an outage is a choice a namespace's owner should make on purpose,
which is what CLAUDE.md non-negotiable 7 already says about replication:
configurable per namespace.

## Why this is reversible

The default is unchanged, so nothing that worked stops working. If deployment
evidence shows `degraded_writes` is zero outside real partitions, the default can
be flipped later: the option, the sentinel and the counter are all already in
place, and flipping is one line in `DefaultNamespaceOptions`.

## What it does not do

It does not make writes agree on anything across nodes. A writer that can see
enough peers accepts the write whether or not those peers agree about membership,
and the two-activation window of a partition (non-negotiable 5) is unchanged.

## Also fixed on the way

`Stats()` filled `DegradedWrites` only inside its membership block, so a cache
without membership counted degraded writes and reported zero. The single-node
example is what showed it. Both counters now sit beside `MailboxRejections`,
outside the block, for the reason that comment already gave.
