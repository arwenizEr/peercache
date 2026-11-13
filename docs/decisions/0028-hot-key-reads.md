# 0028 — Hot-key reads: coalesced misses and opt-in replica reads

**Status:** accepted, built.
**Milestone:** M13.
**Bound by:** design principle 6 (docs/design.md) — a non-owned copy is
TTL-bounded and version-stamped, and correctness never depends on an
invalidation arriving — and principle 7: replication is asynchronous and its
delay is a measured number, not a bound.

## Context

A read of a key owned elsewhere is served from the L1 tier if a fresh copy is
there and from the owner otherwise (`namespace.remoteGet`). Two things make a
single hot key expensive for its owner.

**Misses are not coalesced.** `GetOrLoad` single-flights its loader; `Get` does
not. When a hot key's L1 copy expires, or an invalidation drops it, every
goroutine on every pod that reads it in that instant sends its own RPC. The
owner's load per expiry is the number of concurrent *callers* in the cluster,
not the number of pods.

**Every refill goes to one node.** Even with one RPC per pod, a key that is
written often — so invalidated often — is re-fetched from its owner by every
pod after every write. With a replication factor above one, the same value is
already held on `RF-1` other nodes that answer nothing.

Group sharding for hot keys stays deferred (design.md section 5). This decision
is about reads of namespace keys only; a hot *group* is still bounded by its
owner.

## Decision

### 1. Coalesced remote misses — always on

`remoteGet` runs the owner fetch under a per-namespace single-flight keyed by
the key, the same `internal/singleflight` that `GetOrLoad` uses. Concurrent
L1 misses for one key on one pod share one RPC and its result. A caller whose
context ends leaves the shared fetch running for the others and returns its own
`ctx.Err()`.

This changes no guarantee. The shared result is the owner's answer to a request
made after every waiting caller missed L1, so it is as fresh as any of them
would have got alone. It is on for every namespace, with no option, because
there is no workload it makes worse.

### 2. `WithReplicaReads()` — opt-in per namespace

```go
// WithReplicaReads lets a read of a key owned elsewhere be answered by one of
// the key's replicas instead of only its owner, spreading a hot key's refills
// across every node that holds it.
//
// A replica answers only with a copy it received less than the namespace's L1
// TTL ago, and the reader caches it for what is left of that TTL. So a value
// read this way is stale by at most the L1 TTL plus the replication delay —
// and the replication delay is measured (docs/benchmarks.md), not bounded.
// It requires a replication factor of at least 2 and an enabled L1 tier;
// otherwise construction fails with ErrInvalidOption.
func WithReplicaReads() NamespaceOption
```

It applies to `Get` on `Namespace` and therefore `TypedNamespace[T].Get`. It
does not apply to `GetOrLoad` — the loader belongs to the owner — nor to
`BatchGet`, `Exists` or any write.

#### Choosing a holder

On an L1 miss the reader ranks the key's holders (owner plus replicas, as
`Table.Replicas(groupKey, RF)` gives them) and picks one by rendezvous hash of
its own node ID and the key. Each pod therefore always asks the same holder for
a given key — which keeps the holder's own coalescing effective — and pods as a
whole spread across all `RF` holders. If the reader is itself one of the
holders, it reads its own replica store under the rule below, with no RPC.

#### What a replica will answer

A new RPC, `replicaGet`, carrying the reader's epoch. The replica answers with
the value and stamp only if all of these hold:

- it ranks as a replica for the key under its own current table. Not "at an
  epoch not older than the reader's", as this said when proposed: epochs are
  per-view and comparing them across nodes was already found to refuse
  requests between nodes that agree (handler.go, `noteEpoch`). The local
  ranking is the question that can be answered, as `assertOwned` answers it
  for owners;
- it holds a live, unexpired entry for the key;
- the entry was received less than the namespace's L1 TTL ago, by the
  replica's own clock.

Otherwise it answers `refer`, and the reader asks the owner within the same
deadline. A replica never answers "not found": a missing replica copy may be a
dropped write or a catch-up not yet done, and only the owner knows the key is
absent. A peer on an older build answers `ErrNotImplemented`, which is treated
as `refer`.

The replica returns the entry's age with it. The reader caches the value for
`L1TTL − age`, so the copy's whole life — on the replica and in the reader's
L1 — stays inside one L1 TTL of the moment the replica received it.

`replication.Entry` gains `ReceivedAt`, the replica's clock at the moment it
accepted the copy (from replication or from catch-up). It is set by the
receiver; nothing crosses the network about it.

#### Invalidation floors

Invalidation and replication leave the owner on separate queues, so a reader
can get the invalidation for stamp S before the replica it asks has the write.
Without a guard it would refill from the replica with the old value and keep it
for most of a TTL — which would make invalidation useless for exactly the hot,
frequently written keys this exists for.

So in a namespace with replica reads, an invalidation for stamp S leaves a
*floor* in the reader's L1: a marker that holds no value, carries S, and expires
at the L1 TTL. A replica answer with a stamp older than the floor is discarded
and the read goes to the owner. An owner answer replaces the floor as any `Put`
does.

This is still only a latency optimisation. If the invalidation is lost there is
no floor, and the copy is bounded by the L1 TTL as before.

### Observability

`Stats` gains `CoalescedReads` (callers that shared another's fetch),
`ReplicaReadsServed` and `ReplicaReadsReferred` (answered by this node as a
replica, and referred to the owner), and `ReplicaReadsFloored` (replica answers
discarded under an invalidation floor). A rising referred count with a steady
served count is what "keys quieter than the TTL" looks like.

### The staleness statement

Docs and README say, for a namespace with replica reads:

> A read of a key owned elsewhere may be stale by up to the L1 TTL plus the
> replication delay. The delay is measured, not bounded; under a stalled
> replication queue it is longer.

That replaces nothing for namespaces without the option. They keep "stale by up
to the L1 TTL".

### As built

- Holders are found with `Table.Holders`, an allocation-free copy of
  `Table.Replicas`' ranking added for this path and tested to agree with it at
  every replication factor. The first version called `Replicas` twice per
  replica read and allocated 58 times per cold read against the owner path's
  41; with `Holders` it is 40.
- A synchronous write that cannot reach a replica returns before it enqueues
  its invalidation. That predated this decision and was recorded as open
  question 45, since resolved; the floor test was written with asynchronous
  replication because of it.

## Alternatives rejected

- **Coalescing only.** Fixes the per-caller stampede but leaves every refill of
  a frequently written key on one node. It is the first half of this decision,
  not an alternative to it.
- **Owner-issued leases for a hard staleness bound.** The owner would promise
  "no write before T" to its replicas. T is a time on the owner's clock, and
  pods do not share a clock; turning it into a bound needs either clock
  agreement this design does not assume or a round trip per lease, which spends
  most of what replica reads save.
- **A separate `maxStaleness` knob.** A second staleness number per namespace
  is one more thing for a caller to reason about, and the L1 TTL already is
  that number. If one turns out to be needed, it can be added without changing
  this one's meaning.
- **Replicas revalidating old copies with the owner.** A key quiet for longer
  than the L1 TTL then always falls back to its owner, which is the stated
  limitation. Revalidation would recover it at the cost of a new owner-to-replica
  path; deferred until a measurement shows the need.

## Acceptance

- 1,000 concurrent `Get`s of one non-owned key on one pod with a cold L1 cost
  the owner exactly one read RPC, and every caller gets the value.
- A caller whose context is cancelled while waiting on a shared fetch returns
  its context error; the others still get the value.
- In a five-node cluster at RF 3 with replica reads, reads of one key from
  every non-holder spread across all three holders, and the owner serves fewer
  than all of them.
- A replica copy older than the L1 TTL is not served: the read goes to the
  owner, which is checked by counting the owner's RPCs.
- A node that no longer ranks as a replica for a key refers the read to the
  owner instead of answering from its old copy.
- A replica that lacks a key the owner holds does not produce `ErrNotFound`:
  the read returns the owner's value.
- After an invalidation for stamp S, a replica answer older than S is discarded
  and the read returns the new value.
- A node that is itself a replica for a key serves the read from its own copy
  without an RPC.
- A write whose replication is dropped is read with its new value on every pod
  once one L1 TTL has passed on the fake clock.
- `WithReplicaReads` at RF 1 or with the L1 tier disabled is `ErrInvalidOption`.
- A peer answering `ErrNotImplemented` to `replicaGet` is treated as `refer`.
- Benchmarked: parallel remote misses of one key, with and without coalescing;
  a replica-served read against an owner-served one.
