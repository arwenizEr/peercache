# 0022 — A memory budget that sheds, cheapest tier first

**Status:** accepted
**Milestone:** M9
**Adds:** `WithMemoryBudget`, `PressureCheckInterval`, `ErrOverMemoryBudget`,
five `Stats` fields and five metric series. No new dependency, no new RPC.

## Context

Before M9 only the L1 tier was bounded, and by entry count. The owned stores and
the replica store grew without limit. That makes a failover cascade possible: a
node dies, rendezvous hashing hands its share to the survivors, a survivor sized
for its steady state goes over its container limit and is OOM-killed, and its
share — now larger — moves to the next. Nothing in the library could stop it.

## Decision

A per-node byte budget over all three tiers, off by default. Over budget, shed
down to nine tenths of it, in this order:

1. **L1, whole.** TTL-bounded copies of keys owned elsewhere. Shed whole because
   otter reports removals asynchronously; a whole-tier clear is accounted
   exactly, a partial one only once the library's goroutine catches up — late
   enough that the next tier would be shed for pressure already relieved.
2. **Replicas.** Lowers that key's replication factor; the owner still has it.
3. **Owned data.** Loses the key. design principle 4 (docs/design.md) makes state lossy by
   design, and every namespace is already required to tolerate loss or rebuild
   from a loader.

The user chose shedding over refusing writes when asked.

## Sampled, not LRU

Entries are taken in Go's randomised map iteration order from a rotating shard.
An LRU needs every read to record an access, and the owned store's read path is
the 515 ns figure M1 was built around. This is a pressure valve for a node in
trouble, not a steady-state eviction policy; a namespace that needs one has its
TTL.

## Checked every 64 writes, on large writes, and once a second

Summing every tier on each write would put several hundred atomic loads on the
write path. Every 64th write bounds the overshoot to 64 entries; a write of
64 KiB or more checks at once; and the sweep tick checks once a second, which
covers the L1 tier growing on reads. Replica arrivals and hand-off adoptions
check like local writes, because a failover's inflow is copies from peers, not
local writes.

## No stale resurrection, without depending on a message

A store version restarts at 1 when an entry is reclaimed (open question 18). If
the owner shed a key at version 5 and rewrote it at version 1, a replica still
holding version 5 would refuse the rewrite, and a failover would promote the
shed value over the newer one.

Two things prevent it. Shedding an owned key in a replicated namespace sends its
replicas the tombstone a `Delete` sends — asynchronously, since a node relieving
pressure must not wait on the network — which frees their memory too. And each
store shard remembers the highest version it ever shed, so a first write to an
absent key in that shard starts above it. The second is what correctness rests
on; the first is memory relief. design principle 6 (docs/design.md) says correctness must not
depend on a message arriving, and here it does not.

## Catch-up stops rather than thrashes

A node over budget would shed what catch-up pulls. So catch-up checks before
every page and stops with `ErrOverMemoryBudget`; `ReplicasCurrent` stays false.

## Why off by default

A budget is only right relative to a container limit the library cannot see.
`GOMEMLIMIT` would be a candidate to derive one from, but it is set for the whole
process, and the application's own memory is in it. Guessing a fraction would
shed data in deployments that never asked for it. Reversible: a default can be
derived later without changing the option.

## What it does not count

Activated groups (bounded by `WithMaxActiveGroups`), the Go runtime, and
buffers in flight. The overheads per entry are estimates, set high.
