#!/usr/bin/env bash
# M16's scenarios against the kind cluster up.sh builds. Each runs the load
# generator inside the cluster, disrupts the shop while it runs, lets the
# cluster settle, then checks the invariants against the run's ledger:
#
#   - no acknowledged order is lost (and each reads back as placed);
#   - stock is exact, give or take the checkouts the shop reported uncertain;
#   - (steady only) replica staleness and the rate limit's exactness.
#
# Usage: scenarios.sh [name...]   names: steady kill9 rolling partition
#                                        coldstart minio   (default: all)
# Output goes to $OUT (default ./m16-out), one directory per scenario.
#
# PRELOAD=N writes N placed orders before the first scenario, so runs started
# from an empty store hold the same data (orders persist; a store kept across
# runs grows with every pass). TRACE_EVERY=N gives every Nth checkout an order
# ID the shop traces: a lost one prints its history from every pod.
set -uo pipefail
K="kubectl --context kind-m16"
OUT=${OUT:-m16-out}
mkdir -p "$OUT"
WORKERS=${WORKERS:-32}
PRELOAD=${PRELOAD:-0}
TRACE_EVERY=${TRACE_EVERY:-0}
# A fresh stock prefix per invocation: stock persists across runs, so a
# repeated scenario name would otherwise start from the last run's counts.
RUN=$(date +%s)
PODS=http://shop-0.shop-peers:8080,http://shop-1.shop-peers:8080,http://shop-2.shop-peers:8080,http://shop-3.shop-peers:8080,http://shop-4.shop-peers:8080

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }

# node_of POD: the kind node (a docker container) a pod runs on.
node_of() { $K get pod "$1" -o jsonpath='{.spec.nodeName}'; }

# crash POD: SIGKILL the shop process from the node, as a crash would. The
# kubelet restarts the container, as it would after a real crash.
crash() {
  local node; node=$(node_of "$1")
  docker exec "$node" sh -c \
    "crictl stop --timeout 0 \$(crictl ps -q --name shop --pod \$(crictl pods -q --name '^$1\$'))" >/dev/null
}

wait_ready() {
  $K rollout status statefulset/shop --timeout=600s >/dev/null
  for _ in $(seq 1 120); do
    n=$($K get pods -l app=shop -o jsonpath='{range .items[*]}{.status.containerStatuses[0].ready}{"\n"}{end}' | grep -c true)
    [ "$n" -eq 5 ] && return 0
    sleep 2
  done
  log "WARNING: not all shop pods ready"; return 1
}

# oom_counts: kernel OOM kills so far on each worker, "node count" per line.
# The kernel's log is per node and survives container restarts, which is what
# tells an OOM kill from a probe kill or a crash after the fact.
oom_counts() {
  for n in $(docker ps --format '{{.Names}}' | grep -E '^m16-worker[0-9]*$' | sort); do
    echo "$n $(docker exec "$n" dmesg 2>/dev/null | grep -c 'Killed process')"
  done
}

# load NAME SECONDS: start the load generator in the background, and note when
# the scenario began, for the evidence check collects.
load() {
  SCENARIO_START=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  oom_counts > "$OUT/$1/oom-before.txt"
  $K exec loadgen -- rm -rf "/tmp/$1"
  $K exec loadgen -- shopload run -target http://shop -duration "${2}s" \
    -workers "$WORKERS" -prefix "$1-$RUN-" -ledger "/tmp/$1" -trace-every "$TRACE_EVERY" > "$OUT/$1/load.txt" 2>&1 &
  LOADPID=$!
}

# check NAME: the invariants, after the cluster has settled.
check() {
  wait_ready
  sleep 5 # past a flush interval and a catch-up
  # -pods: a wrong count prints its key's history from every pod (decision 0043).
  $K exec loadgen -- shopload check -target http://shop -ledger "/tmp/$1" -pods "$PODS" > "$OUT/$1/check.txt" 2>&1
  local rc=$?
  echo "check exit $rc" >> "$OUT/$1/check.txt"
  for p in $($K get pods -l app=shop -o name); do
    $K logs "$p" --since=10m > "$OUT/$1/$(basename "$p").log" 2>&1
    $K logs "$p" --previous > "$OUT/$1/$(basename "$p").previous.log" 2>/dev/null || true
  done
  evidence "$1"
  # Forks counted since each pod last started: a pod the scenario restarted
  # reports only what it saw after the restart.
  local forks=0 n p
  : > "$OUT/$1/forks.txt"
  for p in $($K get pods -l app=shop -o jsonpath='{.items[*].metadata.name}'); do
    n=$($K exec "$p" -- wget -qO- http://localhost:8080/metrics 2>/dev/null \
      | awk '$1 == "peercache_split_brain_detections_total" { print int($2) }')
    echo "$p ${n:-?}" >> "$OUT/$1/forks.txt"
    forks=$((forks + ${n:-0}))
  done
  log "$1: $(grep -E '^(acknowledged|uncertain|stock items)' "$OUT/$1/check.txt" | tr '\n' ' ') forks: $forks rc=$rc"
}

# evidence NAME: what explains a scenario after the fact, beside the logs.
#
#   events.txt     cluster events since the scenario began: probe failures,
#                  kills, back-offs, scheduling
#   restarts.txt   each pod's restart count and how its last container ended
#                  (reason, exit code, when): OOMKilled, Error, or Completed
#   oom.txt        kernel OOM kills during the scenario, per worker
#   <pod>.metrics  every pod's /metrics, the library's counters included
#   <pod>.namespaces.json  every pod's view of its namespaces
#
# The partition run of 2026-11-18 restarted two pods for a reason nothing kept:
# not OOM by the kernel's count, and nothing else was recorded.
evidence() {
  local d="$OUT/$1" p
  $K get events --sort-by=.metadata.creationTimestamp \
    -o custom-columns='TIME:.metadata.creationTimestamp,TYPE:.type,REASON:.reason,OBJECT:.involvedObject.name,COUNT:.count,MESSAGE:.message' \
    2>/dev/null | awk -v s="$SCENARIO_START" 'NR == 1 || $1 >= s' > "$d/events.txt"
  $K get pods -l app=shop -o jsonpath='{range .items[*]}{.metadata.name}{" restarts="}{.status.containerStatuses[0].restartCount}{" last="}{.status.containerStatuses[0].lastState.terminated.reason}{" exit="}{.status.containerStatuses[0].lastState.terminated.exitCode}{" started="}{.status.containerStatuses[0].lastState.terminated.startedAt}{" finished="}{.status.containerStatuses[0].lastState.terminated.finishedAt}{"\n"}{end}' \
    > "$d/restarts.txt" 2>/dev/null
  oom_counts > "$d/oom-after.txt"
  join "$d/oom-before.txt" "$d/oom-after.txt" 2>/dev/null | awk '{ print $1, $3 - $2 }' > "$d/oom.txt"
  for p in $($K get pods -l app=shop -o jsonpath='{.items[*].metadata.name}'); do
    $K exec "$p" -- wget -qO- http://localhost:8080/metrics > "$d/$p.metrics" 2>/dev/null || true
    $K exec "$p" -- wget -qO- http://localhost:8080/debug/peercache/namespaces > "$d/$p.namespaces.json" 2>/dev/null || true
  done
  local restarted ooms probes
  restarted=$(awk '$2 != "restarts=0"' "$d/restarts.txt" | tr '\n' ';')
  ooms=$(awk '{ n += $2 } END { print n + 0 }' "$d/oom.txt")
  probes=$(grep -c 'Unhealthy' "$d/events.txt")
  log "$1: evidence: kernel OOM kills $ooms, probe failures $probes, restarted: ${restarted:-none}"
}

steady() {
  load steady 60; wait $LOADPID; check steady
  $K exec loadgen -- shopload stale -rounds 30 -pods \
    http://shop-0.shop-peers:8080,http://shop-1.shop-peers:8080,http://shop-2.shop-peers:8080,http://shop-3.shop-peers:8080,http://shop-4.shop-peers:8080 \
    > "$OUT/steady/stale.txt" 2>&1
  $K exec loadgen -- shopload limit -target http://shop > "$OUT/steady/limit.txt" 2>&1
  log "steady: stale: $(tail -1 "$OUT/steady/stale.txt"); limit: $(tail -1 "$OUT/steady/limit.txt")"
}

kill9() {
  load kill9 90; sleep 30
  log "kill9: SIGKILL shop-2"; crash shop-2
  wait $LOADPID; check kill9
}

rolling() {
  load rolling 240; sleep 20
  log "rolling: rollout restart"; $K rollout restart statefulset/shop
  $K rollout status statefulset/shop --timeout=600s >/dev/null
  log "rolling: rollout done"
  wait $LOADPID; check rolling
}

partition() {
  local victim; victim=$(node_of shop-1)
  load partition 150; sleep 20
  log "partition: disconnect $victim ($($K get pods -l app=shop -o wide --no-headers | awk -v n="$victim" '$7==n{print $1}' | tr '\n' ' '))"
  docker network disconnect kind "$victim"
  sleep 60
  log "partition: reconnect $victim"
  docker network connect kind "$victim"
  wait $LOADPID; sleep 30; check partition
}

# Every shop process killed at once, after load has stopped and a flush has
# passed: what comes back must come back from MinIO alone.
coldstart() {
  load coldstart 40; wait $LOADPID; sleep 5
  log "coldstart: SIGKILL every shop pod"
  for p in shop-0 shop-1 shop-2 shop-3 shop-4; do crash "$p" & done; wait
  sleep 5; check coldstart
}

# MinIO unreachable for a minute under load, by pointing its service at
# nothing: writes go on, flushes queue, and drain when it is back.
minio() {
  load minio 120; sleep 20
  log "minio: service selector -> nothing"
  $K patch service minio -p '{"spec":{"selector":{"app":"nobody"}}}' >/dev/null
  sleep 60
  $K exec shop-0 -- wget -qO- http://localhost:8080/debug/peercache/namespaces > "$OUT/minio/during.json" 2>/dev/null || true
  log "minio: service restored"
  $K patch service minio -p '{"spec":{"selector":{"app":"minio"}}}' >/dev/null
  wait $LOADPID; sleep 10; check minio
  # And the queued flushes must have reached the store: a cold start now
  # must lose nothing either.
  log "minio: cold start after the outage"
  for p in shop-0 shop-1 shop-2 shop-3 shop-4; do crash "$p" & done; wait
  sleep 5; wait_ready; sleep 5
  $K exec loadgen -- shopload check -target http://shop -ledger /tmp/minio > "$OUT/minio/check-after-coldstart.txt" 2>&1
  echo "check exit $?" >> "$OUT/minio/check-after-coldstart.txt"
  log "minio after cold start: $(grep -E '^(acknowledged|stock items)' "$OUT/minio/check-after-coldstart.txt" | tr '\n' ' ')"
}

if [ "$PRELOAD" -gt 0 ]; then
  log "=== preload $PRELOAD orders"
  $K exec loadgen -- shopload preload -target http://shop -orders "$PRELOAD" > "$OUT/preload.txt" 2>&1
  log "preload: $(tail -1 "$OUT/preload.txt")"
fi

names=("$@")
[ ${#names[@]} -eq 0 ] && names=(steady kill9 rolling partition coldstart minio)
for n in "${names[@]}"; do
  mkdir -p "$OUT/$n"
  log "=== $n"
  "$n"
done
