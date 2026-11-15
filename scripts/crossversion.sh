#!/usr/bin/env bash
# Runs a cluster of the previous release and this tree side by side, over real
# gossip and real gRPC on localhost, and fails if either release cannot read
# what the other sends.
#
#   scripts/crossversion.sh [previous-ref]
#
# previous-ref defaults to the latest tag before HEAD. The node program
# is tests/crossversion, built once against each tree; it writes keys, a batch
# and group state, reads every other node's, then one node drains and the rest
# read everything again. Two clusters run: one where the previous release is
# the minority and drains, one where this tree is the minority and drains, so
# each release both sends and receives the hand-off stream.
#
# internal/wire's compat tests pin one message at a time against a hand-copied
# earlier shape; this checks the whole conversation against the real earlier
# code. Decision 0041.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
# HEAD^, so that on a release commit the tag just made is not compared with
# itself.
prev=${1:-$(git -C "$root" describe --tags --abbrev=0 HEAD^)}
base=${PORT_BASE:-27946}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Go on Windows cannot read a Git Bash path such as /tmp/x.
native() {
  if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s\n' "$1"; fi
}

exe=""
case "$(go env GOOS)" in windows) exe=".exe" ;; esac

echo "previous release: $prev ($(git -C "$root" rev-parse --short "$prev"))"
echo "this tree:        $(git -C "$root" rev-parse --short HEAD)$(git -C "$root" diff --quiet || echo ' + local changes')"

mkdir -p "$work/prev-tree"
git -C "$root" archive "$prev" | tar -x -C "$work/prev-tree"

# build NAME TREE: the node program compiled against the library at TREE.
build() {
  local src="$work/src-$1"
  mkdir -p "$src"
  cp "$root/tests/crossversion/main.go" "$root/tests/crossversion/go.mod" "$root/tests/crossversion/go.sum" "$src/"
  (
    cd "$src"
    go mod edit -replace "github.com/arwenizEr/peercache=$(native "$2")"
    go mod tidy
    go build -o "$work/node-$1$exe" .
  )
}
build prev "$work/prev-tree"
build head "$root"

# cluster NAME DRAINER BUILD...: one node per BUILD, node n0 first; DRAINER
# is the id of the node that drains. STORE, when set, is the directory the
# durable namespace persists to; VERIFY, when set, makes every node a restore
# node reading back what the comma-separated ids in it persisted.
cluster() {
  local name=$1 drainer=$2
  shift 2
  local builds=("$@") dir="$work/$name" ids="" pids=() i failed=0
  mkdir -p "$dir"
  for i in "${!builds[@]}"; do ids="${ids:+$ids,}n$i"; done
  echo
  echo "== $name: ${builds[*]} (n0..n$((${#builds[@]} - 1))), $drainer drains"
  for i in "${!builds[@]}"; do
    local seed=""
    [ "$i" -gt 0 ] && seed="127.0.0.1:$base"
    "$work/node-${builds[$i]}$exe" -id "n$i" -label "${builds[$i]}" \
      -gossip $((base + 2 * i)) -data $((base + 2 * i + 1)) -seed "$seed" \
      -peers "$ids" -drainer "$drainer" -dir "$(native "$dir")" \
      ${STORE:+-store "$(native "$STORE")"} ${VERIFY:+-verify-writers "$VERIFY"} \
      >"$dir/n$i.log" 2>&1 &
    pids+=($!)
  done
  for i in "${!pids[@]}"; do
    wait "${pids[$i]}" || failed=1
  done
  for i in "${!builds[@]}"; do sed "s/^/  /" "$dir/n$i.log"; done
  base=$((base + 2 * ${#builds[@]}))
  return $failed
}

status=0
mkdir -p "$work/store-a" "$work/store-b"
STORE="$work/store-a" cluster "previous-release-drains" n0 prev head head || status=1
STORE="$work/store-b" cluster "this-tree-drains" n0 head prev prev || status=1

# Cold starts over what the mixed clusters persisted, by one release alone:
# each release reads storage both releases wrote.
VERIFY=n0,n1,n2 STORE="$work/store-a" cluster "previous-release-restores" none prev prev prev || status=1
VERIFY=n0,n1,n2 STORE="$work/store-b" cluster "this-tree-restores" none head head head || status=1

echo
if [ $status -ne 0 ]; then
  echo "FAIL: $prev and this tree do not interoperate"
  exit 1
fi
echo "ok: $prev and this tree interoperate"
