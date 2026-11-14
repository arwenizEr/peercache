# 0035 — Loading many keys in one call

**Status:** accepted.
**Bound by:** decision 2 (a key's loads run on its owner), decision 3 (loaders
are registered by name, never shipped), the no-cross-key-atomicity rule, and
`GetOrLoad`'s `SetIfAbsent` rule (a load never stores over a newer write).

## Context

`GetOrLoad` takes one key and `BatchGet` does not load. A page that misses 50
keys therefore costs 50 `GetOrLoad` calls: 50 owner round trips and 50 loader
calls, each usually a database query. Caffeine (`getAll`/`loadAll`), Coherence
(`CacheStore.loadAll`) and Hazelcast (`MapLoader.loadAll`) let one miss of N
keys cost one query. For an embedded cache the database behind the loader is
usually the bottleneck, so this is the cost that matters.

## Decision

**`Namespace.BatchGetOrLoad(ctx, keys) (map[string][]byte, error)`** groups keys
by owner, as `BatchGet` does, and sends each owner one request. Each owner
answers what it holds and loads what it does not:

- With **`WithBatchLoader(func(ctx, keys []string) (map[string][]byte,
  time.Duration, error))`**, it calls the batch loader **once** with every
  missing key it owns. A key absent from the returned map does not exist. One
  TTL applies to the call's values.
- Without one, it runs the namespace's ordinary loader per missing key, through
  the same single-flight as `GetOrLoad`. That still saves the round trips; only
  the queries stay per key.
- With neither, `ErrNoLoader`, as `GetOrLoad`.

Each loaded value is stored with `SetIfAbsent`, so a write that landed while
the loader ran wins and is what the caller gets. Results are sparse like
`BatchGet`'s: a key that does not exist is absent from the map, not an error.
A failing owner's keys are missing from the result and its error is joined into
the returned error, beside every other owner's keys — `BatchGet`'s partial
rule. There is no atomicity across keys: a batch loader that returns half its
keys and an error stores nothing, and the keys it returned are not served.

**On the wire** it is `BatchGetRequest` with a new field, `load` (field 4). A
peer on an earlier release ignores it and answers what it holds, so a mixed
cluster during a rolling upgrade degrades to `BatchGet` rather than failing —
the caller sees misses, which it must already handle.

**Single-flight per batch, not per key.** A batch load does not join, and is not
joined by, a concurrent `GetOrLoad` of one of its keys: both may call their
loader, and `SetIfAbsent` keeps whichever stored first. Collapsing a batch into
per-key flights would mean splitting a batch around keys already in flight and
waiting on several flights at once; the cost of not doing it is an occasional
duplicate query for a key that is hot enough to be in flight, never a wrong
value. The batch loader is bounded by `WithLoaderTimeout`, like the per-key
loader, and runs with the caller's cancellation stripped.

A namespace with `WithNegativeCaching` (decision 0034) skips keys it remembers
as absent and remembers the keys the batch loader omitted.

## Rejected

- **A new RPC method:** a peer on an earlier release would answer
  `ErrNotImplemented` for the whole batch instead of what it holds.
- **Fanning out to `GetOrLoad` from the caller:** one round trip per key, which
  is the cost this exists to remove.
- **Per-key TTLs from the batch loader:** no system surveyed needed it, and a
  map of TTLs is a second map to keep aligned with the values.

## Acceptance

- Fifty keys missing on two owners cost two owner requests and two batch loader
  calls, each with exactly that owner's keys.
- Keys the batch loader omits are absent from the result and not stored.
- A write that lands while the batch loader runs is what the caller gets.
- Without a batch loader, the per-key loader runs once per missing key, on the
  owner; with no loader at all, `ErrNoLoader`.
- A failing owner's keys are missing and its error is returned with every
  other owner's keys.
- A peer that ignores `load` answers as `BatchGet` does.
