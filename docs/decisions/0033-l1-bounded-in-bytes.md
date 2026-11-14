# 0033 — The L1 tier is bounded in bytes, per namespace

**Status:** accepted and built.
**Bound by:** decision 6 (non-owned copies are TTL-bounded; this changes how
many are kept, never how long) and decision 0005 (otter is the L1 library).

## Context

Every namespace's non-owned tier was built with a fixed capacity of 100,000
entries and no option to change it. An entry count says nothing about memory:
a namespace of 1 MiB thumbnails could hold 100 GB of copies before evicting
one, and open question 16's advice — configure the capacity with headroom —
had nothing to configure. The memory budget could only shed the whole tier
once the process was already over.

Caffeine (`maximumWeight`), Ristretto (cost) and otter (`Cost`) all bound by
weight for this reason.

## Decision

`WithL1MaxBytes(n int64)`, per namespace, default 64 MiB. otter's capacity
becomes a total cost and each copy costs what `Cache.Bytes` already charges it:
key bytes, value bytes and `EntryOverhead` (128). The bound, the L1 bytes
gauge and the memory budget's accounting therefore use one charge.

- Zero or negative is `ErrInvalidOption`. Turning the tier off stays with
  `WithL1TTL(-1)`; two ways to say "off" would be one too many.
- A copy larger than the whole bound is not admitted (otter refuses it), and
  the read that fetched it still returns the value.
- No entry-count option survives. One bound, in the unit memory is spent in.

## Consequences

- The default changes what is kept: a namespace whose key and value come to under about 540
  bytes now holds more than 100,000 copies, one of larger values fewer. Pre-v1,
  so changed rather than gated; noted in the CHANGELOG.
- Eviction stays asynchronous, so a burst can still overshoot (open question
  16); the bound is now something an operator can set below what is available.
- The cost function runs on every Put; its price is in docs/benchmarks.md.

## Acceptance

- A namespace bounded at 32 KiB, read two hundred 1 KiB keys owned elsewhere,
  keeps no more than fit, while one on the default keeps them all:
  `TestTheL1TierIsBoundedInBytes`.
- Mixed value sizes ten times over the bound settle within it:
  `TestMaxBytesIsBounded`; a copy larger than the bound is not cached:
  `TestAnEntryLargerThanTheBoundIsNotCached`.
- A bound of zero or less is refused: `TestAnL1BoundMustBePositive`.
