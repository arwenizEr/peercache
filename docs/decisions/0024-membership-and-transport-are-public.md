# 0024 — Membership and transport are public packages

**Status:** accepted
**Milestone:** post-M9
**Moves:** `internal/membership` → `membership`, `internal/transport` →
`transport`. **Changes:** `WithSeeds` now joins; seeds without a membership
authority are `ErrInvalidOption`.

## Context

A coverage sweep showed `WithSeeds` at zero. It was set by its option and read
by nothing. Looking for who was meant to read it found the larger problem: the
gossip membership and the gRPC transport — the only implementations of
`MembershipAuthority` and `Transport` there are — lived under `internal/`, and
`docs/api.md` told applications to import them from there. Go forbids that from
any other module. Building a scratch module that followed the documentation
failed with:

    use of internal package github.com/peercache/peercache/internal/membership not allowed

So an application depending on this library could run a single node and nothing
else, unless it wrote both interfaces itself. Every test and example lives inside
this module, where the import is allowed, which is why nine milestones of them
never noticed.

## Options

1. **Make the two packages public** — chosen by the user.
2. Have `New` build gossip and gRPC from `WithBindAddr` and `WithSeeds` when no
   authority or transport is given. Simplest to call; needs new options for TLS,
   the gossip port and the memberlist profile, and grows `New`.
3. Thin constructors in the root package over the internal ones. Narrowest
   surface; duplicates the option types.

## Decision

Both packages move to the module root. Their exported APIs were already
documented to the standard every package here is held to. They name only types
an application can also name: `peercache.Node`, `peercache.Logger` and the
others are aliases of the same `internal/core` types, so a value built with the
root package's names fits the transport's and membership's fields.

`WithSeeds` becomes what it said it was: `New` calls the authority's `Join` with
the seeds once the transport is serving, and fails — closing what it started —
if the join does. A node told to join and unable to is not a working node, and
returning it would leave it serving as a cluster of one.

## Kept honest by

`TestAnExternalModuleCanBuildACluster` writes the documented wiring into a
temporary module that depends on this one through a `replace` directive, and
compiles it offline. It fails on any regression of this kind: a package moving
back under `internal/`, or a public signature naming a type an application
cannot.

## Cost

The two packages' APIs are now public contract. Their internal-only exports —
method name constants, the codec — are visible too. That is acceptable for a
pre-1.0 module and is the price of an application being able to build a cluster
at all.
