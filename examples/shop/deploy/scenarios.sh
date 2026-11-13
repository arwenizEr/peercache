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
set -uo pipefail
K="kubectl --context kind-m16"
OUT=${OUT:-m16-out}
mkdir -p "$OUT"
WORKERS=${WORKERS:-32}

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

# load NAME SECONDS: start the load generator in the background.
load() {
  $K exec loadgen -- rm -rf "/tmp/$1"
  $K exec loadgen -- shopload run -target http://shop -duration "${2}s" \
    -workers "$WORKERS" -prefix "$1-" -ledger "/tmp/$1" > "$OUT/$1/load.txt" 2>&1 &
  LOADPID=$!
}

# check NAME: the invariants, after the cluster has settled.
check() {
  wait_ready
  sleep 5 # past a flush interval and a catch-up
  $K exec loadgen -- shopload check -target http://shop -ledger "/tmp/$1" > "$OUT/$1/check.txt" 2>&1
  local rc=$?
  echo "check exit $rc" >> "$OUT/$1/check.txt"
  for p in $($K get pods -l app=shop -o name); do
    $K logs "$p" --since=10m > "$OUT/$1/$(basename "$p").log" 2>&1
    $K logs "$p" --previous > "$OUT/$1/$(basename "$p").previous.log" 2>/dev/null || true
  done
  $K exec shop-0 -- wget -qO- http://localhost:8080/debug/peercache/namespaces > "$OUT/$1/namespaces.json" 2>/dev/null || true
  log "$1: $(grep -E '^(acknowledged|uncertain|stock items)' "$OUT/$1/check.txt" | tr '\n' ' ') rc=$rc"
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

names=("$@")
[ ${#names[@]} -eq 0 ] && names=(steady kill9 rolling partition coldstart minio)
for n in "${names[@]}"; do
  mkdir -p "$OUT/$n"
  log "=== $n"
  "$n"
done
