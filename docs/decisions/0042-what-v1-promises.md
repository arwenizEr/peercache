# 0042 — What v1 promises, and how each promise is kept

**Status:** accepted (2026-11-16).
**Bound by:** decision 3 (all pods run the same binary, except during a rolling
upgrade), decision 0041 (every change runs beside the previous release).

## Context

Until `v1.0.0` a minor version may change the public API, and v0.2 and v0.3 both
did. Nothing written down said what would stop changing at v1, so there was
nothing to hold a change against and nothing for a user to plan around. Four
kinds of surface are involved, and they break in different ways:

- Go API that users **call** — functions, options, types, and the interfaces
  the library hands back (`Cache`, `Namespace`, `Group`, `TypedNamespace`,
  `Router`).
- Go interfaces that users **implement** and pass in — `Transport`,
  `PeerClient`, `Handler`, `MembershipAuthority`, `Logger`, `Tracer`, `Span`,
  `Partitioner`, `persist.BlobStore`, `Clock`.
- Formats another process reads — the wire protocol, persisted segments, the
  debug handler's JSON, metric names.
- Behaviour — the guarantees in the README.

A method added to an interface is harmless to a caller and fatal to an
implementer, so the first two cannot share one rule.

## Decision

From `v1.0.0`, under semantic versioning, for the packages `peercache`,
`peercache/transport`, `peercache/membership`, `peercache/persist`,
`peercache/persist/persisttest`, `peercache/peercachetest` and the separate
modules `peercache/otel` and `peercache/persist/s3`. `internal/` promises
nothing.

1. **Called API is additive in minor releases.** No exported function, type,
   field, option or sentinel error is removed or changes signature before v2.
   New ones may appear.

2. **Interfaces the library returns may gain methods in a minor release.**
   `Cache`, `Namespace`, `Group`, `TypedNamespace` and `Router` are implemented
   by this module only; their doc comments say so. A test that needs one should
   use `peercachetest`, which runs real caches in process, rather than a
   hand-written fake that a new method would break. They are not sealed with an
   unexported method: that would forbid mocking outright, and sealing can be
   added later only as a breaking change, while documenting first leaves both
   paths open.

3. **Interfaces users implement are frozen.** Their method sets do not change
   before v2. A capability added later is an optional interface found by type
   assertion — the `io.WriterTo` pattern — so an implementation written against
   v1.0 keeps compiling and keeps working, without the new capability.
   `TestImplementedInterfacesAreFrozen` records each method set and fails on any
   change, so a change is a decision someone makes, not one that slips in.

4. **Wire and persisted formats stay readable across one minor release, both
   ways.** A cluster may run release N and N+1 together for a rolling upgrade,
   and either may read the other's persisted segments. Upgrades that skip a
   minor release are not promised. Decision 0041's cross-version job enforces
   this on every change against the latest tag.

5. **Debug JSON and metric names are additive.** A field or series is never
   renamed or removed before v2; new ones may appear, so readers must ignore
   what they do not know. JSON names are camelCase
   (`TestDebugHandlerJSONNamesAreCamelCase`). A field's meaning may be refined
   in its documentation; its unit and kind (counter, gauge) do not change.

6. **The README's guarantees are the behaviour contract.** A change that
   weakens one is a breaking change even if no signature moves. A change that
   strengthens one, or fixes behaviour that contradicted one, is a fix.

7. **The Go floor** is the `go` line of `go.mod` (1.25 now). It moves only in a
   minor release, never past the oldest Go release the Go team still supports.

## Before tagging v1.0.0

- Doc comments on the returned interfaces state promise 2 (done with this
  decision).
- `TestImplementedInterfacesAreFrozen` exists (done with this decision).
- The M16 scenarios run clean on the release candidate, on kind.
- The README's Status section points here instead of "may still change".

## Consequences

- An implementer of `Transport` or `Logger` can upgrade any v1 minor release
  without a code change.
- Adding streaming to the transport (open question 1) after v1 means an
  optional `StreamingPeerClient`, never a new method on `PeerClient`.
- A user who wrote their own `Namespace` fake will see it break in a minor
  release. That is said plainly in the type's documentation, which is the most
  this library can do without forbidding the fake.
