#!/usr/bin/env bash
# Brings up M16's cluster: builds the shop for linux on the host, bakes it into
# an image, creates the kind cluster, loads the image, and deploys MinIO, the
# bucket, the shop StatefulSet and the load generator pod. Idempotent enough to
# re-run after a code change: it rebuilds, reloads, and restarts the shop.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
kind=${KIND:-$(go env GOPATH)/bin/kind}
build=$(mktemp -d)
trap 'rm -rf "$build"' EXIT

(cd "$here/.." && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$build/shop" . \
  && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o "$build/shopload" ./cmd/shopload)
cp "$here/Dockerfile" "$build/"
docker build -q -t peercache-shop:m16 "$build"

if ! "$kind" get clusters | grep -qx m16; then
  "$kind" create cluster --config "$here/kind.yaml" --wait 120s
fi
"$kind" load docker-image peercache-shop:m16 --name m16

kubectl --context kind-m16 apply -f "$here/minio.yaml"
kubectl --context kind-m16 delete job minio-bucket --ignore-not-found
kubectl --context kind-m16 rollout status deployment/minio --timeout=300s
existed=$(kubectl --context kind-m16 get statefulset shop -o name 2>/dev/null || true)
kubectl --context kind-m16 apply -f "$here/shop.yaml"
if [ -n "$existed" ]; then
  kubectl --context kind-m16 rollout restart statefulset/shop
  kubectl --context kind-m16 delete pod loadgen --wait=true
  kubectl --context kind-m16 apply -f "$here/shop.yaml"
fi
kubectl --context kind-m16 rollout status statefulset/shop --timeout=600s
kubectl --context kind-m16 wait --for=condition=ready pod/loadgen --timeout=120s
# Observability: Prometheus scraping every shop pod, Grafana summing them.
kubectl --context kind-m16 create configmap grafana-dashboards   --from-file=peercache.json="$here/peercache.json" --dry-run=client -o yaml   | kubectl --context kind-m16 apply -f -
kubectl --context kind-m16 apply -f "$here/observability.yaml"
kubectl --context kind-m16 rollout status deployment/grafana --timeout=300s
kubectl --context kind-m16 get pods -o wide
