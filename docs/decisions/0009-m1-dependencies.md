# 0009 — Dependencies added in M1

**Status:** accepted, M1
**Affects:** `go.mod`

CLAUDE.md sets a dependency budget: "`memberlist`, `grpc`, a local cache
library, a codec, and test deps. Adding anything else requires a note in
`docs/decisions/` first." This is that note.

## Added

| Module | Purpose | Budget line | Go floor |
|---|---|---|---|
| `github.com/maypok86/otter` v1.2.4 | L1 eviction | "a local cache library" | 1.19 |
| `github.com/vmihailenco/msgpack/v5` | MsgPack codec | "a codec" | 1.19 |
| `github.com/cespare/xxhash/v2` | shard selection | **outside the budget** | 1.11 |
| `github.com/pierrec/lz4/v4` | compression | **outside the budget** | 1.17 |

Transitively: `dolthub/maphash`, `gammazero/deque` (otter);
`vmihailenco/tagparser/v2` (msgpack). Seven modules in total, none of which
pull in anything beyond the standard library.

## The two outside the budget

Both are named by docs/design.md section 4 (M1) directly: "xxhash key hashing" and
"Compression: LZ4". The build plan is more specific than the budget list, and
where they disagree the plan wins — but not silently, which is why this file
exists.

**xxhash** could in principle be avoided. `hash/maphash` is in the standard
library and is fast, but its seed is randomised per process and cannot be fixed.
That is harmless for shard selection, which is process-local, and fatal for
ownership hashing in M3, which must produce the same answer on every pod. Using
one hash for both is worth a small dependency; using two would be a trap waiting
for whoever forgets which is which.

**lz4** is not avoidable without writing a compressor.

## Rejected

**`golang.org/x/sync/singleflight`.** Its current releases require Go 1.25 and
CLAUDE.md fixes the floor at 1.23; it also re-panics in every waiter when the
shared call panics, which is unacceptable for user-supplied loader code. Hand
rolled instead — see `0006-hand-rolled-singleflight.md`.

**`hashicorp/golang-lru`, `maypok86/otter/v2`, `dgraph-io/ristretto/v2`.**
Evaluated as L1 candidates and not selected. They exist only in
`internal/l1/eval`, which is a nested module with its own `go.mod`, so none of
them appear in the shipping dependency graph. Losing candidates should not be
paid for forever — see `0005-l1-cache-library.md`.

## The Go floor

Every selected dependency builds on Go 1.23, which is the floor CLAUDE.md
states. Two candidates were rejected partly for requiring 1.24 and one for
requiring 1.25. Raising the floor is a change that every consumer of this
library pays for, and it should be a deliberate decision rather than a
side effect of picking a convenient package.

## Still to come

`memberlist` (M3) and `grpc` (M4) are budgeted and not yet added.
