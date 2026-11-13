# 0026 — External persistence: a design, not an implementation

**Status:** superseded by `0029-persistence-as-a-segment-log.md`, which keeps
this note's goals and replaces its per-key conditional write with a segment log
over a blob store; each of the ten review findings below is answered there. Kept
for the reasoning, not as a design.
**Milestone:** none yet; a candidate for the first one after v1.
**Bound by:** design principle 4 (docs/design.md) — "Durability: none… Persistence hooks may be
designed for, but nothing is written to disk in v1" — and docs/design.md §5, which
defers WAL, snapshots and object-storage backup.

This note is what decision 4 allows: the shape of a hook, its guarantees, and the
problems that have to be solved before it is worth building. It exists so that
the decision to build it is made against a design, not against the word
"durable".

## What is already there, and what is missing

**Reading from an external store is solved.** `WithLoader` and `WithTypedLoader`
run on a key's owner, collapse concurrent misses into one call, and repopulate
the cache. A namespace backed by a database loses nothing it can reload.

**Writing to one is not.** A value written to PeerCache and nowhere else lives on
its owner and RF−1 replicas. Lose them all ungracefully and it is gone. Today the
application has to write to its database itself, alongside the cache, and keep
the two straight — which is the dual-write problem with none of the library's
knowledge of ownership, ordering or failure.

## The hook

```go
// Persister is an external store the cache writes owned data through to.
type Persister interface {
    // Apply makes each op durable. It must be idempotent and must apply an
    // op only if it is newer than what the store holds for that key, by the
    // op's Order (see "Ordering", the hard part). Ops for one key arrive in
    // the order the owner made them, but may be retried.
    Apply(ctx context.Context, ops []PersistOp) error
    // Load reads a key back, with the Order it was stored under.
    Load(ctx context.Context, namespace, key string) (PersistedValue, bool, error)
}

type PersistOp struct {
    Namespace, Key string
    Value          []byte // nil with Deleted
    Deleted        bool
    Order          uint64 // see below; not the in-memory Stamp
    ExpiresAt      int64  // Unix ns, 0 for none — the owner's deadline
}
```

Configured per namespace — `WithPersister(p, WithWriteBehind())` — because
durability is per namespace, as replication is (decision 7). A namespace with a
persister and no loader gets `Load` as its loader.

**The library ships the interface and a fake, not drivers.** docs/design.md's
dependency budget does not admit database clients, and should not. Adapters —
Postgres, Redis, S3, DynamoDB — live in separate modules that import this one.

## Write-behind, not write-through, as the default

**Write-through** — `Set` returns when the persister has the value — makes every
write as slow as the database, which removes most of the reason to have this
cache. Worse, a persister failure after the value is stored in memory leaves the
two disagreeing, and resolving that is a transaction by another name, which
docs/design.md rules out. It may be offered later, with the same contract as
synchronous replication today: a failure is reported, not rolled back.

**Write-behind** is the default. The owner enqueues the op after the write lands
in memory and returns. That leaves a loss window — ops queued when the owner dies
— and the window is the thing to engineer and to measure, the way M5 measured the
asynchronous replication window rather than leaving it unknown (decision 7).

The queue **coalesces by key**: only the newest op per key is kept, so a hot key
written a thousand times a second costs one persister write per flush, and the
queue's size is bounded by distinct keys, not by write rate. It counts toward the
memory budget (M9). When it is full the oldest op is not dropped silently; it is
counted, and the key is marked so the next flush rereads its current value from
the owned store.

## Ordering: the hard part

The persister must apply an op only if it is newer, or a delayed retry
overwrites a newer value and a stale owner's write wins after a failover. That
needs an order every owner of a key agrees on, over time.

**The in-memory stamp is not that order.** `core.Stamp` compares epochs first,
and an epoch is per node — `peercache_epoch` is documented as "per-view, not
globally agreed". A key's new owner after a failover can hold a lower epoch than
its old one, and its writes would be refused as older. The stamp works inside the
cluster because replicas and L1 copies only ever compare stamps from the same
ownership lineage under bounded staleness; a persister compares across all of
time.

**Proposed: a per-key order that survives ownership changes.** Every write the
owner makes carries `Order = max(stored order seen for this key) + 1`, where the
owner learns the stored order:

- from its own previous writes (in memory);
- from a replica it promotes — replicas already carry the previous owner's
  version sequence, and promotion continues it (`store.Restore`);
- from `Persister.Load` when it loads a key it does not hold.

The persister applies an op only if `op.Order > stored.Order`. The version floor
built in M9 and extended for question 42 already keeps a single owner's versions
from going backwards across deletions and sheds; this extends the same idea
across owners, with the persister as the place the floor is remembered.

**What this does not fix:** a partition with two activations (decision 5). Both
owners may issue the same `Order` for different values. The persister cannot tell
which is right, and neither can the cluster — conflicting writes are detected,
not merged. The proposal is to make it detectable: the persister rejects an
equal `Order` with a different value and the library counts it as a split brain,
which `peercache_split_brain_detections_total` already exists for.

## Failure behaviour

| Event | With write-behind persistence |
|---|---|
| Owner killed, replicas alive | Promotion; the new owner re-enqueues the keys it promoted, conditionally — so an op lost in the dead owner's queue but already replicated becomes durable anyway. Loss now needs both the replication *and* the persistence of a write to be lost. |
| Owner and all replicas killed | Keys whose ops were persisted reload through the loader. Keys whose ops were still queued are lost — the measured window. |
| Persister unavailable | Writes keep succeeding in memory; the queue fills and coalesces; failures and queue depth are exported. Readiness is unaffected — the cache can serve — and `peercache_persist_lag_seconds` says how far behind durability is. |
| Graceful drain | `Drain` flushes the queue before handing off; a drain that cannot flush reports it. |
| Memory shedding (M9) | Shedding an owned key must **not** delete it from the persister — shedding is memory relief, not a deletion. With a persister, a shed key is reloadable, which turns M9's data loss into a cache miss. This is the best argument for building it. |
| TTL expiry | `ExpiresAt` is persisted; the adapter expires or filters on load. Not a delete op. |

## Metrics it would add

`peercache_persist_queue_depth`, `peercache_persist_lag_seconds` (age of the
oldest unflushed op), `peercache_persist_failures_total`,
`peercache_persist_coalesced_total`, `peercache_persist_conflicts_total`, and a
measured loss window in `docs/benchmarks.md` like M5's.

## What it would not do

- Cross-key or cross-group atomicity. Ever (docs/design.md).
- Persist group state. `GroupState` lives in activations, not namespaces; a group
  persistence hook is a separate design.
- Replace the loader as the source of truth for data owned by a database. If the
  database is the truth, use a loader and write to the database; this is for data
  whose truth is the cache.

## Open questions to answer before building

1. Is the per-key `Order` scheme sound across sheds, sweeps and question 18's
   remaining restart (an expired entry the sweeper reclaims)? An expiry is not
   persisted as a delete, so a rewrite after one must still outrank the stored
   expired value — probably by seeding from `Load`.
2. Should promotion re-enqueue everything it promotes, or only keys whose last
   persisted `Order` is behind? The first is simple and costs a burst of writes
   per failover; the second needs the persisted order replicated.
3. Rate limits on that burst, so a failover does not become a database outage.
4. Does write-through earn a place, given synchronous replication already exists?

## How it would be tested

A fake persister in `internal/testkit` with injected latency, failure,
reordering and duplication, and the existing chaos and simulation harnesses
extended with "kill every holder of a key" — the case that today loses data by
design — asserting the key reloads at its last persisted value and never at an
older one.

## Review findings — what must be resolved before building

An independent code review of this note (high effort) checked it against the
code and found the following. Each is a reason the design, as written above,
would lose or misattribute writes.

1. **The Order seed needs a read the write path never makes.** `Set` is a blind
   write. A new owner writing a key it does not hold starts at Order 1, and a
   persister holding Order 7 refuses every write until the counter passes it —
   while `Set` reports success. The seed has to come from somewhere every write
   reaches, not only from loads.
2. **A promoted replica can be behind what is persisted.** Under asynchronous
   replication the old owner may have flushed Order 5 and replicated only 4. The
   new owner continues from 4, its next write collides with the stored 5, and
   the collision is counted as a split brain that never happened. Promotion must
   seed from the persister, or the persisted order must be replicated.
3. **Replicas carry the in-memory version, not the Order.** The note says the
   new owner learns the order from replicas, and also that the Order is not the
   Stamp. The version restarts when the sweeper reclaims an entry, and the
   version floor is memory-only. Nothing currently carries the Order.
4. **`Apply` needs a result per op.** Applied, refused as older, and rejected as
   a conflict are three different outcomes, and one error for a batch cannot say
   which key had which.
5. **A delete loses its Order.** If the adapter drops the row, a delayed older
   write is applied over nothing and the value comes back — question 42 at the
   persistence layer. If it keeps a tombstone that `Load` reports as absent, the
   next owner has nothing to start above. `Load` must return tombstones with
   their Order.
6. **Overflow cannot reread what it dropped.** A key marked for reread may have
   been deleted or merely shed (M9); the owned store cannot tell which, and one
   choice loses a delete while the other deletes a shed key. The marked set is
   also unbounded.
7. **Real split brain is mostly undetected.** Two activations advance their
   Orders independently and coalesce, so their writes usually differ in Order;
   the lower is refused as older with no signal. "Make it detectable" is not
   achieved by equal-Order detection alone.
8. **Persister conflicts must not feed `peercache_split_brain_detections_total`.**
   That metric means two nodes wrote under one epoch; persister conflicts include
   false ones from finding 2. They belong in their own counter only.
9. **`Load` cannot be adapted into the existing `Loader` type** without losing
   the Order and the absolute deadline, which the scheme depends on.
10. **Word choice** — corrected above.

Also noted by the review: the claim that stamps are compared only within one
ownership lineage contradicts `Stamp.NewerThan`'s own comment, whose epoch rule
exists to order writes across an ownership change; and a persisted absolute
deadline is filtered by the adapter's clock, not the owner's, so skew moves
expiry on load.

The common root of 1–5 and 9: the design needs a per-key order that is durable,
replicated and carried by every path that writes or loads a key, and v1 has no
such thing. Building one is the first milestone of this work, not a detail of it.

