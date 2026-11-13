# 0029 — Opt-in persistence as a segment log over a blob store

**Status:** accepted; M14b and M14c built, M14d not yet. Supersedes 0026.
Its layout is amended by 0030: a flush writes one log object, split into the
partitions in the background.
**Milestone:** M14, in four parts (below).
**Changes:** design principle 4 (docs/design.md, CLAUDE.md). It said
"Durability: none… nothing is written to disk in v1". On 2026-11-13 the project
owner decided that persistence becomes **opt-in, per namespace, with pluggable
backends**: memory, local disk, and cloud object stores. A namespace without it
behaves exactly as before. This note is the design that decision is built
against.
**Bound by:** principle 5 (per-group serializability, AP membership, no
consensus), principle 6 (correctness never depends on a message arriving),
principle 7 (loss windows are measured numbers), the dependency budget, and the
rule that there are no cross-group transactions.

## Context

Decision 0026 proposed a per-key write-behind `Persister` — the owner writes
each key to an external store, which applies it only if newer. An independent
review found ten defects; their common root is that the scheme needs a per-key
order that is durable, replicated and carried by every path that writes or loads
a key, and none exists.

0026's model also fits the wrong stores. A per-key conditional write suits
Postgres or Redis. It does not suit the backends the owner asked for: an object
store charges per request, has no "write only if newer", and is slow per object;
a local disk wants appends, not millions of small files. Both are good at
exactly one thing — **writing immutable files and reading them back whole**.

So the model changes, and with it most of the review's findings stop applying
rather than needing patches.

## Decision

### The backend is a blob store, and nothing more

```go
// BlobStore is where a persistent namespace keeps its segments and snapshots.
// Every method is safe for concurrent use. Put must be atomic: a reader sees
// the whole object or nothing, never a prefix.
type BlobStore interface {
	Put(ctx context.Context, name string, data []byte) error
	Get(ctx context.Context, name string) ([]byte, error) // ErrNotFound if absent
	List(ctx context.Context, prefix string) ([]string, error)
	Delete(ctx context.Context, name string) error
}
```

No ordering, no conditional writes, no per-key operations. All of the logic is
in the core, written once, and tested once against a fake. A backend is a few
dozen lines:

| Backend | Module | Atomic Put by |
|---|---|---|
| `persist.Memory()` | core | a map under a lock; for tests, survives nothing |
| `persist.Dir(path)` | core, stdlib only | write to a temp file, fsync, rename, fsync the directory |
| `s3`, `gcs`, `azblob` | separate modules, own `go.mod` | the store's own PUT semantics |

The cloud SDKs never enter the core module; the dependency budget does not
change. A namespace opts in with `WithPersistence(store, opts...)`.

### What is written: records, in segments, in partitions

A **record** is one key's state as one write left it:

```
namespace, key, value | tombstone, expiresAt (absolute, Unix ns), order, writer
```

A namespace is split into a fixed number of **partitions** (64 — first 256, cut
when M14c measured a flush of 256 durable objects outrunning its own interval;
chosen
at first use and recorded in a manifest object, never changed after; a node
configured with a different count reports `ErrInvalidOption` rather than
writing into the wrong layout). A key's
partition is a hash of its *group key*, so a group's keys are always together.

Each owner keeps a **dirty map** per namespace — key → newest record, coalesced
— and flushes it every `FlushInterval` (default 100 ms) or at `FlushBytes`
(default 1 MiB), writing one **segment** object per partition touched:

```
<namespace>/p<NNN>/seg-<order>-<writer>.bin
```

Segments are immutable and never rewritten. A hot key written a thousand times
between flushes costs one record.

### Order: a hybrid logical clock, carried everywhere

`order` is a 64-bit hybrid logical clock: the high 48 bits are milliseconds of
the owner's wall clock, the low 16 a counter. Each node keeps one HLC.

- A write on the owner takes `order = hlc.Next()`: the larger of now and the
  last order this node issued or observed, plus one.
- Every order a node **receives** advances its HLC — replicated writes,
  catch-up pages, records read back from the store.
- The order travels with the value: in the owned store entry, in the
  replication and catch-up messages, and in promotion.

Ties are broken by the writer's node ID, so two records never compare equal
unless they are the same record.

The merge rule for one key is **last writer wins by (order, writer)**. A map of
last-writer-wins registers is a CRDT: merging is commutative, associative and
idempotent. That is the property everything below leans on — segments may be
read in any order, twice, by several nodes at once, and the answer is the same.

### Recovery

When a node acquires a group whose keys it does not hold — at cold start, or
when every holder of a key has died — it **restores** that group's partition:
read the newest snapshot and every segment it does not name, keep the records for
groups this node owns, drop expired ones, and merge each into memory **only if
its order is newer than what memory already holds**. Merge-if-newer makes a
restore safe to run at any time, concurrently with live writes, and more than
once.

A read of a key in a partition whose restore is still running waits for it,
bounded by the caller's deadline; it does not report a miss that a restore
would have answered.

Restores are triggered by membership changes, as promotion is, and rate-limited
per node so that a failover does not become a read storm on the store.

### Compaction

Each partition has a compactor: the rendezvous owner of the synthetic group key
`persist/<namespace>/<partition>`. It periodically merges the newest snapshot
and the segments after it into a new snapshot, then deletes the segments it
covered. A snapshot **names** the segments it merged. Coverage by order range
would be wrong: a slow writer's segment can carry an older order than one the
snapshot already merged and still arrive after it. Tombstones are kept in snapshots for `TombstoneGrace` (default 1 h)
after their order, then dropped.

Two compactors at once — a partition, decision 5 — each write a snapshot. Both
are correct merges of what they read; a reader takes the newest snapshot and
every segment it does not cover, and the CRDT merge makes the overlap harmless.
A segment is deleted only after a snapshot covering it is durable, so no record
is lost to a race between two compactors.

### Shedding, expiry and draining

- **Shedding (M9)** drops a key from memory, never from the store. With
  persistence a shed owned key becomes a restore on its next read: M9's data
  loss turns into a cache miss.
- Before shedding owned data the node flushes; a dirty key that cannot be
  flushed is not shed, and is counted.
- **Expiry** writes nothing. `expiresAt` is in the record; a restore drops
  expired records. It is the owner's clock that wrote it and the restoring
  node's clock that reads it, so expiry on restore is off by their skew.
- **Drain** flushes before it hands off and reports a flush it could not finish.

### Failure behaviour

| Event | Result |
|---|---|
| Owner killed, replicas alive | Promotion, as today. The promoted replica holds the orders it was sent, so its next write outranks the old owner's. |
| Owner and every replica killed | The new owner restores the partition: everything flushed survives. Writes in the last flush interval, not yet flushed, are lost — **the loss window**, measured and published as M5's was. |
| Whole cluster lost | Cold start restores from the store; the same window applies. |
| Store unavailable | Writes succeed in memory; the dirty map coalesces and keeps growing by distinct keys only, counted against the memory budget; flush failures and lag are exported. Readiness is unaffected. |
| Partition, two activations | Both write. On restore, last writer by order wins, deterministically. The in-cluster split-brain detection (replicas seeing two writers under one epoch) is unchanged, and persistence adds no false detections to it. |

## The review's ten findings, resolved

1. **The order seed needs a read the write path never makes.** The HLC is
   seeded by the wall clock, so a blind `Set` on a key the node never held still
   outranks every write made more than one clock skew earlier. What remains is
   stated: *two writes to one key less than the cluster's clock skew apart, by
   two different owners with no replication between them, may be ordered either
   way on restore.* Kubernetes nodes run NTP; the skew is milliseconds and is
   exported.
2. **A promoted replica behind what was persisted.** Its next write takes the
   HLC — now, or later — not "previous version + 1", so it outranks the flushed
   write and nothing is reported as a conflict.
3. **Replicas carry the version, not the order.** The order is added to the
   owned store entry, the replica entry, the replication and catch-up messages,
   and promotion. The in-memory `Stamp` is untouched; the two answer different
   questions.
4. **`Apply` needs a result per op.** There is no `Apply`. The store is written
   blind and never asked to decide; ordering is resolved by merge on read.
5. **A delete loses its order.** Deletes are tombstone records with an order,
   kept through compaction for the tombstone grace.
6. **Overflow cannot reread what it dropped.** Nothing is dropped. The dirty
   map coalesces by key and holds the record itself, so shedding the key from
   the owned store does not lose what the map must write. Its size is bounded
   by distinct keys written since the last successful flush and charged to the
   memory budget.
7. **Real split brain is mostly undetected.** Persistence does not claim to
   detect it. It resolves it deterministically, last writer wins, and the
   cluster's own detection stays the signal. This is said in the README.
8. **Persister conflicts must not feed the split-brain metric.** There are no
   persister conflicts; the metric is untouched.
9. **`Load` cannot be adapted into `Loader`.** Restore is not a loader. A
   namespace may have both: a restored key is resident; a key absent from memory
   and from the store still goes to the loader.
10. **Word choice.** No "durable" without its window; no "strong", no "quorum".

## The parts

- **M14a** — this note, design principle 4 reworded in CLAUDE.md and
  docs/design.md, the M14 section with acceptance.
- **M14b** — `BlobStore`, `persist.Memory`, the HLC and the order carried
  through store, replication, catch-up and promotion; the dirty map and flush;
  restore; compaction. Chaos tests that kill every holder of a key.
- **M14c** — `persist.Dir`, with the fsync discipline tested by a fault-injecting
  file system, and the loss window measured on Machine A.
- **M14d** — `persist/s3` as its own module, tested against a fake S3 and,
  optionally, MinIO.

## Alternatives rejected

- **0026's per-key conditional write.** See Context; its defects were
  structural.
- **A write-ahead log fsynced per write.** Turns every `Set` into a disk sync,
  which is a database. The segment flush is a write-behind log; its interval is
  the knob between latency and the loss window.
- **Raft or a consensus log for ordering.** Exactly what principle 5 declined,
  and the HLC plus LWW gives a deterministic answer without it.
- **Persisting group state.** `GroupState` lives in activations; a hook for it
  is a separate design.

## Acceptance (M14b–d, stated now so that they cannot drift)

- Kill every holder of a key ungracefully: after the flush interval it restores
  at its last flushed value, and never at an older one.
- Kill the whole cluster and start it again: every flushed key comes back; a
  deleted key does not; an expired key does not.
- Records read in any order, duplicated, or by two compactors at once restore
  to the same state.
- A promoted replica's next write outranks every record its old owner flushed.
- A shed owned key in a persistent namespace is restored on its next read, not
  reported missing.
- Store unavailable: writes succeed, the dirty map is bounded by distinct keys,
  lag and failures are counted, and everything is written once the store returns.
- The loss window (write acknowledged → record durable) is measured and
  published, for `persist.Dir` on Machine A.
- A namespace without `WithPersistence` performs within noise of today,
  benchmarked.
