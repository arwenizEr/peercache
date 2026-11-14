# 0037 — Spreading the expiry of keys written together

**Status:** accepted.
**Bound by:** decision 6 (a TTL bounds staleness, so it may be shortened but
never lengthened), and the rule that replicas and persistence carry the
owner's absolute deadline, never a duration (see `replication.Entry`).

## Context

Keys written in one burst with one TTL expire in one instant: a warm-up at
deploy, a `BatchGetOrLoad`, a persistence restore, or simply a loader that
always answers the same TTL. Their reloads then reach the database together,
the same herd single-flight prevents for one key, spread over many. Refresh-ahead
(decision 0036) removes this for keys that are being read; it does nothing for
`Set` with a TTL or for keys read only after they expire.

## Decision

`WithTTLJitter(fraction)`, per namespace, `0 <= fraction < 1`, off by default.
Every TTL the namespace's owner assigns — `Set`, `SetIfAbsent`, loads, the
atomic operations — is shortened by `ttl * fraction * f(key)`, where `f` maps
the key's hash to `[0, 1)`.

- **Shorter, never longer.** A TTL is often a bound on how stale a value may be;
  a jitter that could lengthen it would break the bound the caller chose.
- **Fixed per key.** The amount depends on the key alone, so a key rewritten
  keeps its lifetime, the same key has the same lifetime on every owner it ever
  has, and tests see the same deadlines every run. Keys differ, which is the
  spread.
- **Applied once, on the owner, in the store.** Replicas, hand-off, catch-up and
  persistence carry the deadline the owner computed; `Restore` and the
  persistence merge take a deadline already given and never jitter it again,
  or a key would lose lifetime on every move.
- The hash is mixed before use: shard selection takes the low bits of the same
  hash, and a fraction correlated with the shard would bunch each shard's
  expiries.

## Rejected

- **Random per write:** a rewrite of the same key would change its lifetime
  each time, and no test could predict a deadline.
- **Jitter in both directions:** lengthens some TTLs past what the caller asked.
- **Jitter on every node independently:** owners and replicas would disagree
  on when a key expires.

## Acceptance

- A thousand keys written in one instant with jitter 0.5 — by `Set` and by
  `Increment` — none expires before half the TTL, none outlives it, and about
  half are gone at three quarters of it.
- A key rewritten gets the same deadline; two hundred keys get distinct ones.
- A restored key keeps the deadline it carried.
- Outside `[0, 1)` is `ErrInvalidOption`; without the option TTLs are exact.
