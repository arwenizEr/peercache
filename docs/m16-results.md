# M16 — the shop on Kubernetes, results

M16 runs `examples/shop` — a shop service that uses replica-read catalog
reads, cart groups through `Call`, `Increment` for stock and rate limits, and
orders and stock persisted to MinIO through `persist/s3` — as five pods on a
kind cluster, and disrupts it under load. `examples/shop/deploy/up.sh` builds
the cluster; `examples/shop/deploy/scenarios.sh` runs the scenarios and checks
the invariants against the load generator's ledger.

**Machine:** Machine A (docs/benchmarks.md), Docker Desktop 29.7.2, kind v0.33.0,
Kubernetes v1.37.0: one control plane and three workers, five shop pods
(`stock` and `orders` RF 3, synchronous, persistent; `catalog` RF 3 with replica
reads), MinIO, 32 load-generator workers.

## Invariants

- **Lost:** an order the shop acknowledged that does not read back as placed.
  Must be zero.
- **Stock:** each of 50 items' stock equals its starting count minus its
  acknowledged orders — *exact* — or differs by no more than the checkouts the
  shop reported uncertain (an `Increment` whose outcome the caller could not
  learn) — *within uncertainty*. Anything else is *wrong*.
- **Forks:** `peercache_split_brain_detections_total`, summed over the pods at
  the check. A pod the scenario restarted reports only what it counted since.

## v1.1.0 candidate (2026-11-17, main at 1cce4a4)

### The six scenarios, on the data earlier runs left

`up.sh` from main, then `scenarios.sh`. MinIO was not emptied: it held what
two days of runs had written.

| Scenario | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|
| steady | 112,287 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | 156,214 | 0 | 43 | 48 / 2 / 0 | 0 |
| rolling | 249,989 | 0 | 382 | 36 / 14 / 0 | 0 |
| partition | 130,508 | 0 | 231 | 38 / 12 / 0 | 1 |
| coldstart | 35,540 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio, and the cold start after it | 219,011 | 0 | 0 | 50 / 0 / 0 | 0 |

The partition's fork is the documented window — two activations of a group
while gossip disagrees, detected by epoch, not prevented — and this run's
writes raced inside it once; stock stayed within uncertainty. The v1.0.0
candidates on accumulated data managed 6,000 to 17,000 orders in the
scenarios that restart pods; restores now run in parallel (#81), read each
log once (#89) and a quiet tick no longer reconciles (#86).

### Accumulated data, then every pod restarted

Five steady runs to accumulate (109,059 to 120,086 orders each, none lost),
leaving each pod about 517,000 owned keys and 910,000 replicas — about 2.6
million keys in MinIO. Then `rollout restart`, three minutes with no load,
and steady twice:

| Run | Orders | Lost | replicationsDropped |
|---|---|---|---|
| first after the restart | 111,755 | 0 | 224,622 |
| second | 108,225 | 0 | 5,292 |

**Open: a restore's fan-out starves live asynchronous replication.** 219,224
of the first run's drops were on shop-4, which restored 248,997 keys in that
minute — the first pod the rollout restarted, it sees four more membership
views, and with no load during the warm-up its partitions restore when traffic
first touches them. Every restored key is sent to its replicas through the
same 4,096-entry asynchronous queue as live writes (`replicateRestored`), so
the burst overflows it and drops restored copies and live asynchronous
copies alike. Faster restores (#89) made the burst shorter and sharper; on
the same cluster earlier the same day, before #89, the first run dropped
about 5,300. v1.0.0 dropped about 250,000 a run in steady state (fixed by #85),
so this is not a regression against the last release. No acknowledged write
was lost: the persistent namespaces replicate synchronously and are in
MinIO. Fixed after v1.1.0: restored keys have a queue and senders of their
own, and their drops are counted apart (`restoreReplicationsDropped`);
`TestARestoreDoesNotCrowdOutLiveReplication` holds it.

### The fix on kind, and a cliff past 2.6 million keys

The same procedure with the fix (c4bdb82) accumulated five more runs on top,
to about 597,000 owned keys and 1.2 million replicas a pod — some 3 million in
MinIO. It did not show the fix either way: by the measured runs the restores
had finished during the warm-up, and `restoreReplicationsDropped` stayed 0.
It showed something else:

| Build, same cluster | Orders, two steady runs | epochSkew a run | Rehomed + evicted a run | Promotion rounds a run |
|---|---|---|---|---|
| fix (c4bdb82), after the restart | 6,819 · 8,437 | 17,000–70,000 per pod | ~60,000 | ~470 |
| v1.1.0 (1153e3d), redeployed on it | 25,731 · 28,615 | 248,783 · 275,237 (sum) | 131,994 · 197,922 | 392 · 471 |

No order was lost in either. Both builds collapse alike, so the data, not the
fix, is the cause: at 2.6 million keys the same cluster ran 108,000 to 120,000
orders a minute with no churn.

### The cliff: a promotion scan on every tick — fixed

The first reading was a gossip loop: views diverging (`epochSkew`), each new
view opening a reconciliation. Measured on the cluster that reproduced it, that
was wrong:

- Membership had not moved since the rollout. Each pod's epoch stood still
  (5, 7, 9, 11, 13). `epochSkew` counts requests whose caller's epoch differs
  from the receiver's, and the epochs are local counters that drift apart
  with every view each pod saw; it rises with traffic, not with churn.
- Not CPU starvation and not GC: a 30-second profile of a pod under load
  showed 0.74 of a core in use on a 20-core machine, mostly gRPC syscalls,
  and the GC's share of CPU was 0.01–0.04%.
- Idle, with no load at all, two pods were still handing off about 40,000
  replicas a minute and reconciling every second. One held 1,345,226 replicas
  against about 1,227,000 on the others: some 117,000 copies the rolling
  restart had left out of place. The hand-off queue takes 4,096 a tick, so
  every tick was dirty for minutes.
- Every dirty tick ran a full reconciliation, whose promotion scan ranks every
  replica the node holds for ownership under the namespace's replica lock. A
  20-second profile of that pod idle: 6.5 seconds in it, most of that hashing
  for rendezvous ranking. Every copy arriving for `orders` meanwhile waited on
  the lock.

A dirty tick under a settled view has nothing for that scan to find, so it no
longer runs one; it runs on a view change, while a view settles, at the end of
a catch-up, and once under each epoch for a namespace opened late
(`TestAStrayTickDoesNotScanForPromotions`,
`TestANamespaceOpenedLateOnAQuietClusterPromotesItsReplicas`). Same cluster,
redeployed with the fix, at about 642,000 owned keys and 1.28 million replicas
a pod:

| Build | Orders, two steady runs | Rehomed + evicted a run | Promotion rounds a run |
|---|---|---|---|
| v1.1.0 (1153e3d) | 25,731 · 28,615 | 131,994 · 197,922 | 392 · 471 |
| the fix | 69,749 · 73,160 | 173,358 · 127,068 | 140 · 119 |

No order was lost. It is not yet the 108,000 to 120,000 of 2.6 million keys:
the backlog was still draining during both runs, at about 400 hand-offs a
second from one pod, and the hand-off pass itself still scans for strays
under the same lock. Those two are next.

Found on the way, not the cause: the memory budget's accounting reads about
two thirds of the live heap (311 MB accounted against 478 MB live after a
GC, 1.83 million entries). The shop's 512 MiB budget therefore never shed,
with the heap at 600–780 MB against a 920 MiB `GOMEMLIMIT`. Open.

## v1.0.0 candidates (2026-11-16), accumulated data

### First candidate, ec5836b: two scenarios lost orders

| Scenario | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|
| steady | 143,327 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | 145,941 | 0 | 47 | 48 / 2 / 0 | 0 |
| rolling | 106,976 | 0 | 380 | 27 / 23 / 0 | 0 |
| partition | 45,711 | **8,448** | 71 | 34 / 3 / **13** | 0 |
| coldstart | 36,808 | **7,400** | 0 | 37 / 0 / **13** | 0 |
| minio, and the cold start after it | 237,663 | 0 | 0 | 50 / 0 / 0 | 0 |

Every lost order was owned by shop-4. It restarted during the partition while
MinIO was unreachable from its node; the shop joined the cluster before opening
its namespaces, waiting for the bucket in between, and its peers' writes created
`orders` and `stock` there with default options — replication factor 1, no
persistence, and options are fixed at creation. shop-4 then owned keys it
neither replicated nor stored: "not found" after the heal, gone after the cold
start. The new warning for ignored namespace options named it in the pod's log.
Fixed in PR #79: a peer's request for a namespace this node has not opened is
refused with `ErrActivationPending` (open question 32, reversed), and the shop
opens its namespaces before it joins.

### Second candidate, 8a71f7e: every invariant held

| Scenario | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|
| steady | 11,607 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | 13,347 | 0 | 80 | 48 / 2 / 0 | 0 |
| rolling | 12,599 | 0 | 561 | 22 / 28 / 0 | 0 |
| partition | 16,866 | 0 | 294 | 44 / 6 / 0 | 0 |
| coldstart | 6,260 | 0 | 8 | 49 / 1 / 0 | 0 |
| minio, and the cold start after it | 236,091 | 0 | 0 | 50 / 0 / 0 | 0 |

shop-4 restarted during the partition again, and this time refused its peers'
requests for `orders`, `ratelimit` and `catalog` until it had opened them, as
logged. Nothing was lost.

**Throughput after a restart.** The low counts are the minute after a
restart, not the release. MinIO held about 2.4 million keys by this run, and a
restarted pod restores its share — about 400,000 — while writes to partitions it
has not restored yet wait with `ErrActivationPending` (open question 57).
Bisected on the same cluster, the minute after a `rollout restart`:

| Build | Orders in 60 s | Checkout p50 |
|---|---|---|
| ec5836b (before #79) | 26,845 · 9,081 | 8.5 ms · 113 ms |
| 8a71f7e (with #79) | 8,664 · 30,365 | 72.7 ms · 7.3 ms |
| 8a71f7e, warm | 130,481 | 7.3 ms |

The two builds spread the same way, so #79 costs nothing measurable here;
restore volume does. A restart's slow minute grows with what the namespace
holds; the fresh-data runs above did not show it.

## Run of 2026-11-15 (v0.2.1 candidate, fb059b7), fresh data

The first run in which every scenario, the partition included, holds every
invariant: no order lost, no stock count wrong, no fork.

| Scenario | Disruption | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|---|
| steady | none, 60 s | 122,606 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | SIGKILL one pod mid-load | 185,339 | 0 | 50 | 44 / 6 / 0 | 0 |
| rolling | `rollout restart` of all five pods under load | 202,425 | 0 | 185 | 42 / 8 / 0 | 0 |
| partition | one worker (two pods) cut off for 60 s, then healed | 107,644 | 0 | 59 | 43 / 7 / 0 | 0 |
| coldstart | SIGKILL every pod at once | 42,631 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio | object store unreachable for 60 s | 278,546 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio, then cold start | every pod restarted after the outage, restored from MinIO | 278,546 | 0 | — | 50 / 0 / 0 | — |

Steady: a catalog update visible on all five pods at p50 3 ms, max 6 ms; a rate
limit of 6,000 admitted exactly 6,000. The host was shared with other work for
part of the day (CPU near 90%), so throughput is not comparable between runs;
the invariants are.

### How it got here from v0.2.0

v0.2.0 passed this suite on fresh data. Run again on the data two days of runs
had left, and then repeatedly through rolling restarts, it lost acknowledged
writes by four routes, each found on kind and each fixed with a test that
failed before:

| | Found as | Cause | Open question |
|---|---|---|---|
| F13 | OOM kill during a rolling restart over ~600k orders a pod; ~54,000 orders lost | a restore held the flush lock, compaction ran on the flusher's goroutine — pending records piled up past the one-second window; and the memory budget does not bound the heap (now: GOMEMLIMIT guidance and a warning) | 55 |
| F14 | every counter one restarted pod owned off by thousands | local callers skipped the activation delay; a drained node kept serving | 56 |
| F15 | a counter reset to its initial stock as its owner returned | restores failing on logs the splitter deleted; a failed restore answered absent keys from memory | 57 |
| F16 | the same, about one rolling restart in six | a write that waited for a restore was applied after its key had moved, and replicated to the new owner with a newer order | 58 |

F15 and F16 were found by sampling every counter through a rolling restart and,
for F16, a temporary build logging every store write of the affected keys with
its caller. After F16, six traced rolling restarts in a row held every count.

## Run of 2026-11-15 (v0.2.0, 4d8e4d2), fresh data

MinIO emptied and every pod restarted before the run, as on 2026-11-14.

| Scenario | Disruption | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|---|
| steady | none, 60 s | 136,750 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | SIGKILL one pod mid-load | 165,545 | 0 | 45 | 46 / 4 / 0 | 0 |
| rolling | `rollout restart` of all five pods under load | 310,204 | 0 | 126 | 24 / 26 / 0 | 0 |
| partition | one worker (two pods) cut off for 60 s, then healed | 199,831 | 0 | 123 | 38 / 12 / 0 | 0 |
| coldstart | SIGKILL every pod at once | 65,906 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio | object store unreachable for 60 s | 252,401 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio, then cold start | every pod restarted after the outage, restored from MinIO | 252,401 | 0 | — | 50 / 0 / 0 | — |

Steady: a catalog update visible on all five pods at p50 15 ms, max 15 ms over
30 updates; a rate limit of 6,000 admitted exactly 6,000. The partition counted
no forks and no wrong stock this time (33 and 1 on 2026-11-14): the window is
the same, this run's writes simply did not race inside it.

## The same release on accumulated data — F13, open

Run first, on the cluster as two days of runs had left it: `orders` has no TTL
and is persisted, so every run's orders were still there — 603,073 owned on
shop-0 alone. Each pod has a 1 GiB limit and `WithMemoryBudget(512 MiB)`.

| Scenario | Acknowledged | Lost | Stock wrong |
|---|---|---|---|
| steady, kill9, minio | 16,459 / 17,940 / 192,734 | 0 | 0 |
| rolling | 60,045 | 35 | 7 |
| partition | 98,195 | 6,134 | 9 |
| coldstart (after the partition) | 39,902 | 7,896 | 15 |
| rolling, run again alone | 376,163 | **54,000** | **50** |
| coldstart, run again alone | 95,062 | 0 | 0 |

In the second rolling run shop-4 was **OOMKilled** five times and the rollout
took 10 minutes instead of one. Lost orders read back 404. Two defects, not yet
separated (open question 55):

- the memory budget does not bound the process: a pod budgeted at 512 MiB
  exceeded 1 GiB;
- acknowledged orders were lost while pods crashed one at a time, with RF 3,
  synchronous replication and persistence — which should survive that.

Fresh data passes everything, so these are not regressions since 595bda3 in the
failures that run covers; they are what that run's volume never reached.

## Run of 2026-11-14 (main at 595bda3)

| Scenario | Disruption | Acknowledged | Lost | Uncertain | Stock exact / within / wrong | Forks |
|---|---|---|---|---|---|---|
| steady | none, 60 s | 116,663 | 0 | 0 | 50 / 0 / 0 | 0 |
| kill9 | SIGKILL one pod mid-load | 60,020 | 0 | 302 | 46 / 4 / 0 | 0 |
| rolling | `rollout restart` of all five pods under load | 117,685 | 0 | 651 | 44 / 6 / 0 | 0 |
| partition | one worker (two pods) cut off for 60 s, then healed | 95,642 | 0 | 34 | 40 / 9 / **1** | **33** |
| coldstart | SIGKILL every pod at once | 26,874 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio | object store unreachable for 60 s | 301,653 | 0 | 0 | 50 / 0 / 0 | 0 |
| minio, then cold start | every pod restarted after the outage, restored from MinIO | 301,653 | 0 | — | 50 / 0 / 0 | — |

Steady also measured: a catalog update visible on all five pods at p50 4 ms,
max 6 ms over 30 updates; a rate limit of 6,000 admitted exactly 6,000.

## What the numbers say

- **No acknowledged order was lost in any scenario**, including every pod
  killed at once and a cold start after the object store was down.
- **The partition is the only scenario with wrong stock, and the only one with
  forks.** Gossip membership is AP (design decision 5): cut off, each side
  declares the other dead and both own the same keys for the length of the
  partition. One item's stock came out one off, and the fork detector counted
  33 copies whose histories forked, on four of the five pods. That is the documented
  window, detected rather than eliminated — and it is now detected *only* there.
- **A rolling restart counts no forks.** The run of 2026-11-13, before the
  fork detector (open question 50), counted tens of thousands after one rolling
  restart and 290,000 after an idle redeploy: every handover looked like a
  split. `TestAGracefulRollingUpgradeCountsNoFork` holds that in CI.

## Earlier run (2026-11-13, before the fork detector)

Same scenarios, before PRs #24 and #25. Lost 0 everywhere; stock wrong only in
the partition (2 items). Forks were not reported per scenario; the counter's
total after one rolling restart was in the tens of thousands, which is what
open question 50 fixed.
