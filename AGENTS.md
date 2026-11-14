# PeerCache — Working Agreement

Read this before every task. It is short on purpose. Detailed plan: `docs/design.md`.

## What this is

An embedded Go library that gives an application cluster a shared, partitioned,
in-memory state layer with no external cache tier. Every pod runs the library.
Data has a single owner; computation moves to the owner rather than data moving
to the caller.

## What this is not

- Not a database. State is **lossy by default** (see Durability below), and
  even with persistence there is a measured loss window and no cross-key
  atomicity.
- Not a general distributed transaction system. There are no cross-group
  transactions. Ever. If a task seems to need one, stop and ask.
- Not a Redis replacement for pub/sub, streams, or Lua scripting.
- Not multi-region. Single Kubernetes cluster, single failure domain.

## Non-negotiable design decisions

These were argued out already. Do not relitigate them in code.

1. **Ownership is rendezvous (HRW) hashing**, not a consistent hash ring. One
   algorithm, one file, `internal/ownership`. Cache the sorted node list;
   recompute only on membership change.

2. **Groups are virtual actors.** A group is addressable at all times but
   *activated* lazily on its owner and *deactivated* after idle timeout. All
   operations on an activated group run on a single goroutine, serialized
   through a bounded mailbox. This is why there is no lock API.

3. **Remote code is registered, never serialized.** Closures do not cross the
   network. Functions and loaders are registered by name at startup; RPCs carry
   `(name, []byte args)`. All pods run the same binary — this is assumed.

4. **Durability: none by default; opt-in persistence per namespace.** The owned
   store is memory-only unless a namespace is opened with `WithPersistence`.
   Without it, graceful shutdown transfers ownership and streams state and
   ungraceful termination loses it, so such a namespace must be reconstructible
   from a loader or tolerate loss. With it, owners flush coalesced records as
   immutable segments to a pluggable blob store — memory, local disk, or a cloud
   object store through a separate module — and the loss window is the flush
   interval, measured. Changed by the project owner on 2026-11-13; see
   `docs/decisions/0029-persistence-as-a-segment-log.md`.

5. **Consistency ceiling is per-group serializability**, provided by the single
   activation. Membership is gossip-based and therefore AP: a network partition
   can produce two activations of one group. This window is *narrowed* by
   activation delay and *detected* via epochs, not eliminated. Do not use the
   word "strong" or "quorum" in docs or API names.

6. **Non-owned L1 entries are TTL-bounded and version-stamped.** Invalidation
   broadcast is a latency optimization only. Correctness must never depend on an
   invalidation message arriving.

7. **Replication is async by default**, configurable per namespace. The data
   loss window is a documented, measured number — not an unknown.

## Engineering rules

- Go 1.23+. Generics where they remove reflection, not for decoration.
- Dependency budget: `memberlist`, `grpc`, a local cache library, a codec, and
  test deps. Adding anything else requires a note in `docs/decisions/` first.
  Persistence backends that need an SDK (S3, GCS, Azure) live in their own
  modules with their own `go.mod`; the core module never imports them.
- Every exported symbol has a doc comment. Every package has a `doc.go`.
- **Every concurrency-touching test runs under `-race`.** CI fails otherwise.
- No `time.Now()` outside `internal/clock`. No `time.Sleep` in tests, ever —
  use the fake clock. A sleeping test is a broken test.
- No `panic` in library code paths. No `log` package; a `Logger` interface with
  a no-op default.
- Errors wrap with `%w` and are checked with `errors.Is/As`. Sentinel errors
  live in the root package.
- Context deadlines propagate across RPC boundaries and into group execution.
- Benchmarks live next to the code they measure and are run in CI for
  regression, not just written once.

## Definition of done for any milestone

A milestone is not complete until all of these hold:

- `go test ./... -race` passes.
- The milestone's acceptance criteria in `docs/design.md` are each covered by a named
  test that would fail if the behavior regressed.
- `go vet` and `staticcheck` are clean.
- Benchmarks for the milestone exist and their numbers are recorded in
  `docs/benchmarks.md` with the machine spec.
- Public API additions are in `docs/api.md` with a runnable example.

## How to work

Build depth-first, not breadth-first. A single feature that is correct,
tested, and benchmarked beats eight stubs. If you find yourself writing
`// TODO: implement` in more than one place, stop and finish something.

When a decision is genuinely ambiguous, write the question in
`docs/decisions/open.md` and pick the simplest option that can be reversed
later. Do not silently guess on anything touching correctness under failure.