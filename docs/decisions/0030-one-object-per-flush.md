# 0030 — One object per flush, split into partitions in the background

**Status:** accepted, being built in M14d.
**Amends:** 0029's layout. Its ordering, merge, restore and compaction rules
stand; what a flush writes, and where, changes.
**Decided by:** the project owner on 2026-11-13, choosing this over making the
flush interval and partition count configurable (open question 48).

## Context

0029 had a flush write one segment per partition it touched. M14c measured what
that costs:

| distinct keys a node writes per 100 ms flush | objects per flush |
|---:|---:|
| 1 | 1.0 |
| 10 | 9.2 |
| 100 | 51.4 |
| ≥ 300 | 64.0 |

A loaded node writes 64 objects per flush, ten flushes a second. On a disk that
is merely slow; on an object store that charges per request it is about 640
PUTs a second per node — at S3 Standard's list price roughly $8,000 a month per
node, for a cache.

## Decision

### A flush writes one object

Every flush writes **one log object** holding all its records, whatever
partitions they belong to:

```
<namespace>/log/seg-<largest order>-<writer>.bin
```

One PUT per flush per node, whatever the load. Durable at the same moment a
0029 flush was, so the loss window is unchanged: one flush interval plus one
write.

### A splitter moves logs into partitions

Each persistent namespace has a **splitter**: the rendezvous owner of the
synthetic group key `persist/<namespace>/log`, so every node agrees without
asking. Every `SplitInterval` (10 s) it lists the logs, reads them, and for each
partition with records writes one ordinary partition segment:

```
<namespace>/p<NNN>/seg-<largest order>-<splitter>-<hash of the logs>.bin
```

Only once every one of those segments is durable does it delete the logs. A
split interrupted in between leaves the logs and some segments both holding
the same records; the merge is idempotent, so a reader sees no difference, and
the next split writes them again and finishes.

That is at most 64 PUTs per split **for the whole cluster**, not per node.
Per-partition compaction (0029) then folds those segments into snapshots as
before, so no step rewrites more than one partition at a time.

### A restore reads logs too

Restoring partition p reads p's snapshots, p's segments no snapshot names, and
every log that has not been split yet, keeping only p's records. Logs are
bounded by the split interval — a few seconds of flushes per node — so this is
a handful of extra reads. Compaction does not fold or delete logs; only the
splitter does.

### The flush interval is a persistence option

```go
WithPersistence(store, persist.WithFlushInterval(time.Second))
```

The default stays 100 ms, which suits a disk. An object store should flush every
second or more: one PUT a second per node is the request budget, and the loss
window grows to match. The trade is the namespace's to make, and docs/api.md
prices it.

## Cost, stated

At S3 Standard list prices, per loaded node and per cluster, with a 1 s flush:

| | requests | ≈ per month |
|---|---:|---:|
| flushes, per node | 1 PUT/s | $13 |
| splits, per cluster | ≤ 6.4 PUT/s | ≤ $83 |
| restores and compaction | GETs, per event | cents |

Against 640 PUT/s per node before. Prices are the published list prices at the
time of writing; check them.

## Alternatives rejected

- **Configurable partitions and interval only** (option B). Cheap only with a
  multi-second loss window and few partitions, which makes every restore read
  more. The owner chose against it.
- **An index inside the log, read with ranged GETs.** Fewer bytes per restore,
  more format; restores are rare and logs short-lived. Revisit if a measurement
  says restores are slow.
- **The splitter rewriting partition snapshots directly.** Every split would
  rewrite every partition's whole snapshot — work proportional to all the data,
  every ten seconds. Writing small segments and leaving folding to the existing
  compaction keeps each step proportional to what changed.

## Acceptance

- One flush writes one object, whatever the number of partitions it touches.
- A split moves every log's records into partition segments and deletes the
  logs only after those segments are durable; a split that fails part-way loses
  nothing and the next one completes it.
- A restore before, during and after a split reads the same values.
- Everything 0029 accepted still holds: kill every holder, full restart,
  promotion, shedding, compaction.
- `WithFlushInterval` changes the flush cadence; invalid values are refused.
- Objects per flush and per split are counted in a test, not estimated.
