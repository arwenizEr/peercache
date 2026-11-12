# 0027 — Atomic operations on namespace keys

**Status:** accepted, not yet built.
**Milestone:** M12.
**Bound by:** design principle 3 (docs/design.md) — closures never cross the
network — and principle 6: correctness never depends on a message arriving.

## Context

A namespace offers `Get`, `Set` and `Delete`. Anything conditional — create a
key only if it is absent, change it only if it still holds what the caller
read, add to a counter — is a read followed by a write, and between them another
pod can write the same key. The answer today is a group: register a function,
call it on the owner, let the single activation serialize it. That is right for
logic over several keys, and heavy for "add one to this counter".

These four are what a caller coming from Redis looks for first, and the owner
is already the one place every write to a key passes through, which is what
makes them cheap to do correctly.

## Decision

Four methods on `Namespace`, each executed on the key's owner under the store
shard's lock, then replicated and invalidated exactly as `Set` is:

```go
// SetIfAbsent stores val under key for ttl unless a live value is there, and
// reports whether it stored.
SetIfAbsent(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error)

// CompareAndSwap replaces key's value with new, for ttl, only if it currently
// equals old, and reports whether it did. A missing key never matches.
CompareAndSwap(ctx context.Context, key string, old, new []byte, ttl time.Duration) (bool, error)

// Increment adds delta to the integer stored under key and returns the result.
// A missing key counts as zero and is created with ttl; an existing key keeps
// its deadline. The value is a base-10 integer, so Get reads it as text.
Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)

// Touch sets a new ttl on key without changing its value, and reports whether
// the key existed. A non-positive ttl removes the deadline.
Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)
```

`TypedNamespace[T]` gets `SetIfAbsent` and `Touch`. It does not get
`CompareAndSwap`: comparing two encodings of a `T` is not comparing two `T`s —
msgpack writes a map in iteration order, so equal values can encode
differently — and comparing decoded values needs `T` to be comparable, which a
struct with a slice is not. A typed caller who needs it uses `Raw()` and accepts
byte equality explicitly. `Increment` is a byte-namespace operation by nature.

New sentinel: `ErrNotAnInteger`, returned by `Increment` when the stored value
does not parse as a base-10 `int64`, and when the sum would overflow.

### On the owner

`internal/store` gains one primitive:

```go
// Update applies fn to key's current value under the shard lock and stores
// what it returns. fn must not block or call back into the store.
func (s *Store) Update(key string, fn func(cur []byte, ok bool, expiresAt int64) (next []byte, nextExpiresAt int64, write bool, err error)) (version uint64, wrote bool, err error)
```

All four operations are written in terms of it, so there is one locked
read-modify-write path to get right and to audit. The version and the floor
follow `Set`'s rules. Comparison and parsing work on the *decoded* value: a
namespace with compression stores framed bytes, and two framings of one value
need not be equal.

### Over the network

Three new RPC methods — conditional set (carrying `SetIfAbsent` and
`CompareAndSwap`), increment, touch — on the existing transport, each carrying
the caller's epoch like every other routed request. A peer on an older build
answers `ErrNotImplemented`, which is what a rolling upgrade should see.

### Retries

`Set` re-resolves the owner once on `ErrNotOwner`, `ErrStaleEpoch` or
`ErrActivationPending`. The new operations do the same and nothing more, because
those three are refusals *before* anything was applied. Any other error —
`ErrNodeUnreachable`, a deadline — is returned as it is: the owner may have
applied the operation before the reply was lost, and retrying an `Increment` or
a `CompareAndSwap` would apply it twice. The documentation says so in the
method comments, not only here.

### Replication and failure

The resulting value is replicated as a write with a new version, by the same
fan-out `Set` uses, under the namespace's replication mode. So the guarantees are
`Set`'s:

- Async replication: an acknowledged `Increment` can be lost if its owner dies
  inside the measured loss window, and the promoted replica continues from the
  previous value. A counter is therefore *at most* as durable as a `Set`.
- A partition with two activations can accept increments on both sides. The
  replica sees two writers under one epoch and counts a split-brain detection;
  it does not add the two sides together. The words "atomic" and "linearizable"
  in the docs are scoped to a single owner, as per-group serializability is.

`Touch` replicates the new deadline, not a duration, as every write does.

## Alternatives rejected

- **A generic `Update(ctx, key, func(old) new)`.** The function would have to
  run on the owner, which means serializing a closure. Principle 3.
- **Exposing versions for optimistic concurrency** (`Get` returns a version,
  `SetIfVersion` takes one). Versions are not monotonic across a key's
  lifetime — open question 18 — so a version a caller holds is not a safe
  precondition.
- **Implementing these as built-in group functions.** Namespace keys live in
  the owned store, not in group state; routing them through an activation would
  add a mailbox hop and a goroutine per key for no gain in correctness.

## Acceptance

- 1,000 concurrent `Increment(+1)` calls on one key from every node of a 3-node
  cluster return 1,000 distinct results and leave the key at 1,000.
- 100 concurrent `SetIfAbsent` calls on one key from every node: exactly one
  reports true, and every node then reads that caller's value.
- `CompareAndSwap` with a stale `old` changes nothing and reports false; on a
  compressed namespace, a matching `old` succeeds.
- `Increment` on a non-integer value and on overflow returns `ErrNotAnInteger`
  and leaves the value unchanged.
- `Touch` extends a key's life past its original deadline on the owner and, after
  a failover, on the promoted replica.
- A remote `Increment` whose reply is lost is not retried: the counter advances
  by exactly one.
- A local `Increment` costs within 2× a local `Set`, benchmarked.
