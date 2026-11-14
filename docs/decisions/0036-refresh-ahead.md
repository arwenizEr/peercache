# 0036 — Refreshing a loaded key before it expires

**Status:** accepted.
**Bound by:** decision 2 (a key's loads run on its owner), decision 6 (non-owned
copies are TTL-bounded; a refresh does not change that bound), and the rule
every load already follows: a load never stores over a newer write.

## Context

A popular key loaded with a TTL is reloaded synchronously when it expires.
Single-flight collapses the herd into one load, but every caller of that key
waits for it: the latency spike lands on the hottest keys, once per TTL. And if
the database is down at that moment, the key is simply gone. Caffeine
(`refreshAfterWrite`) and Coherence (refresh-ahead factor) reload a key in the
background while still serving the current value; XFetch does it
probabilistically on every node.

## Decision

`WithRefreshAhead(window time.Duration)`, per namespace, off by default, and
only with a loader. When `GetOrLoad` on a key's **owner** finds the key resident
with less than `window` of its lifetime left, it returns the current value at
once and starts **one** background reload of the key.

- **One at a time, with the misses.** The reload runs in the key's single-flight,
  under the same key a miss uses. A second hit while it runs does not start
  another; a caller that misses because the key expired meanwhile joins the
  reload instead of starting its own.
- **Never over a newer write.** The reload remembers the version it saw and
  stores only if the key still holds that version and is still live. A `Set`, a
  `Delete` or an atomic operation that landed while the loader ran wins, and the
  reload's value is dropped. A key that expired during the reload is not
  stored either: the next miss loads it.
- **A failed reload keeps the value.** A loader error — `ErrNotFound` included —
  leaves the current value until its own expiry and is counted in
  `RefreshFailures`. The refresh is an optimisation; the TTL stays the bound.
- **Owner only, and only `GetOrLoad`.** `Get` never loads, so it never
  refreshes. Non-owners read through their L1 or ask the owner, whose
  `GetOrLoad` is what refreshes. A refreshed value reaches non-owners' L1 the
  way every value does: by their next read after their copy's L1 TTL.
- **No timers.** It is driven by reads, so a key nobody reads is never
  refreshed and simply expires, and nothing runs on the clock.
- **Tracked.** A reload is a goroutine the cache waits for in `Close`, bounded
  by `WithLoaderTimeout` and not by any caller's context.

A one-shot window rather than a fraction of the TTL: the store records when a
key expires, not the TTL it was given, and keeping the TTL per entry would cost
eight bytes on every key of every namespace for an option most do not use.

## Rejected

- **XFetch (probabilistic early expiry):** it exists so that independent nodes
  do not all refresh at once. A key has one owner here; coordination is free.
- **Timer-driven refresh of every key:** cost proportional to the keyspace,
  including keys nobody reads.
- **Storing the reload unconditionally:** it would put back a value read before
  a `Delete` the application issued because the source changed.

## Acceptance

- A `GetOrLoad` hit inside the window returns the current value and reloads the
  key once in the background; the next read sees the reloaded value with a new
  lifetime. Outside the window, nothing reloads.
- Concurrent hits inside the window start one reload.
- A `Set` or `Delete` during the reload wins; the reload's value is not stored.
- A failing reload keeps the current value and counts a failure.
- `Close` waits for a reload in flight.
- Without the option nothing changes; a window with no loader, or a negative
  window, is `ErrInvalidOption`.
