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
