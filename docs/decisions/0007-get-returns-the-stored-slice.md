# 0007 — Set copies, Get aliases

**Status:** accepted, M1
**Affects:** `internal/store`, `Namespace`

## The decision

`Set` copies the value it is given, so a caller may reuse its buffer.
`Get` returns the stored slice without copying, so a caller must not modify it.

The asymmetry is deliberate: the library controls what it stores, and callers
control what they read.

## Why not copy on read

Copying on read is the safe default and it was measured before being rejected.
On Machine A, at 1024 keys with 64-byte values:

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `Get` (aliasing) | 29.1 | 0 | 0 |
| `Get` + copy | 75.0 | 64 | 1 |

Two costs, and the second is the one that decides it.

**2.6x slower.** The M1 acceptance criterion is a p99 under one microsecond at a
million keys, where the memory hierarchy already costs around 320 ns per read.
Adding an allocation and a copy to every read spends a large fraction of the
budget on defending against a caller mistake.

**It reintroduces garbage.** The read path is allocation-free, which is asserted
directly by `TestGetIsAllocationFree`. That property is what makes the garbage
collector irrelevant to the latency tail: a read that allocates will eventually
trigger a collection and inherit the pause. Copy-on-read would put 64 bytes of
garbage on every read of a million-key cache — the exact shape of workload where
a collector starts deciding your tail latency for you.

## The risk being accepted

A caller that writes through the returned slice corrupts the store for every
subsequent reader, with no error and no obvious symptom. That is a real hazard
and it is why:

- both halves of the contract are documented on `Store`, on `Namespace`, and on
  the package;
- `TestGetReturnsTheStoredSlice` asserts the aliasing, so a future change to
  copy-on-read is a deliberate, visible decision rather than a silent
  performance regression;
- `TestSetCopiesTheCallersBuffer` asserts the other half, so the safe direction
  cannot quietly become unsafe either.

This is the same contract the standard library uses for `[]byte` returned from
`bytes.Buffer.Bytes`, and the same one most Go caches use.

## Interaction with compression

With `WithCompression`, an uncompressed frame is unwrapped by re-slicing, so it
still aliases the store. A compressed frame is decompressed into a fresh buffer
and does not. Callers cannot tell which, and must therefore assume aliasing —
which is the documented rule regardless.

## Reversal

Adding a copying variant later is additive and cheap: `GetCopy`, or an option.
Changing `Get` itself to copy would be a silent behaviour change for anyone
relying on the current cost, which is why the aliasing is tested rather than
merely documented.
