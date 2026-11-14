# 0034 — Remembering that a loader found nothing

**Status:** accepted.
**Bound by:** decision 2 (one owner runs a key's loads), decision 4 (state is
lossy; this is a cache of an absence, and losing it costs one load), and the
`SetIfAbsent` rule of `GetOrLoad` (a load never stores over a newer write).

## Context

`GetOrLoad` collapses *concurrent* misses into one load. A loader error is
returned and nothing is stored, so a stream of lookups for keys that do not
exist — deleted users, bots probing IDs, a typo in a link that went viral —
reaches the database once per request, cluster-wide, forever. Caches mostly
leave this to applications (Caffeine treats a null load as absent and does not
cache it), and each application builds it slightly wrong: a sentinel value
stored in the cache shows up in `Exists`, `Keys`, replication and persistence.

## Decision

`WithNegativeCaching(ttl)`, per namespace, off by default. When the namespace's
loader returns an error matching `ErrNotFound`, the owner remembers the
absence for `ttl`; until then `GetOrLoad` of that key answers `ErrNotFound`
without calling the loader. Other loader errors are never remembered: an
outage must not be cached as an absence.

The absence is **not a value**. It lives beside the owned store's entries, per
shard, under the shard's lock, and nothing that reads the store sees it: not
`Get`, `Exists`, `Keys`, `Items`, snapshots, replication, hand-off, catch-up or
persistence. Only `GetOrLoad` consults it, and only after the store missed.

**Every write forgets it.** Each store mutation of the key — `Set`, `Delete`,
`SetIfAbsent`, the atomic operations, `Update`, `Restore`, a persistence merge
— removes the key's absence under the same lock, so it cannot outlive a write:

- A `Set` after the absence is served as usual (the store answers first).
- A `Delete` — how an application tells the cache its source changed — makes the
  next `GetOrLoad` load again, not answer from the absence.
- A value that was set and then expired is loaded again on its next miss.

An absence is recorded only if the store still holds no live entry for the
key, the same condition the load's own store uses: a write that landed while
the loader ran wins.

**Bounded.** At most 256 absences per shard (65,536 for a namespace at the
default 256 shards). A full shard drops expired absences first, then an
arbitrary one: forgetting an absence costs one load, never a wrong answer. The
sweep that removes expired entries removes expired absences.

**Owner-local.** Absences are not replicated and do not move with ownership.
A new owner loads once and remembers for itself. Non-owners do not cache the
`ErrNotFound` in their L1: a probe still costs the caller a round trip to the
owner, but the database is asked once per `ttl` per key, which is the harm this
exists to stop.

## Rejected

- **Loader-returned TTL on `ErrNotFound`** as the switch, with no option: it
  gives an existing signature a new meaning found only by reading docs.
- **A sentinel value in the store:** every reader of the store would have to
  filter it, and any one that forgot would serve it.
- **Caching every loader error:** an outage would be served as "not found" for
  the TTL after the database came back.

## Acceptance

- A loader returning `ErrNotFound` is called once for repeated `GetOrLoad`
  within the TTL, and again after it.
- `Set`, `Delete`, and a value set then expired, each make the next `GetOrLoad`
  see the store or call the loader, never the remembered absence.
- A non-`ErrNotFound` loader error is never remembered.
- A remote caller's `GetOrLoad` gets `ErrNotFound` and the owner's loader is
  not called again within the TTL.
- `Exists` and `Keys` do not see an absence; the per-shard bound holds.
- Without the option nothing changes.
