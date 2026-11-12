# 0001 — Core types live in internal/core, re-exported as aliases

**Status:** accepted, M0
**Affects:** every package

## The problem

docs/design.md section 3 defines the public API in the root `peercache` package. Some
of those definitions are concrete types — `Node`, `Event`, `Epoch`, `Stats` —
that appear in the signatures of interfaces the internal packages must
implement.

Go's structural interface satisfaction does not rescue us here. An internal
package can declare its own `Transport`-shaped interface, but `Dial` takes a
named struct type, and two identically shaped structs in different packages are
distinct types. So `internal/transport` needs *the* `Node`, not a copy of it.

If the root package owns those definitions, then `internal/transport`,
`internal/membership` and `internal/actor` all import the root package, while
the root package's own constructor imports them back. That is an import cycle,
and it arrives the moment M2 starts wiring `internal/actor` into `Cache`.

## Options considered

1. **Root owns the types; internal packages import root.** Cycle at M2.
2. **Each internal package defines its own equivalents; adapt at the boundary.**
   Works for interfaces, fails for structs. Would need conversion functions for
   `Node` at every call site, and `GroupFunc` — a func type over `GroupState` —
   would need a wrapper closure per registration.
3. **A shared internal package, re-exported by the root package.** One
   indirection, no cycle, no per-call-site cost.

## Decision

Option 3. `internal/core` holds the section 3 definitions. The root package
re-exports them as type aliases:

```go
type Node = core.Node
type Epoch = core.Epoch
type GroupState = core.GroupState
```

An alias is not a wrapper. `peercache.Node` and `core.Node` are the same type,
so the public API is byte-identical to what docs/design.md specifies and no caller can
tell the difference.

Sentinel errors are handled the same way, by assignment rather than aliasing:

```go
var ErrNotFound = core.ErrNotFound
```

Assignment copies the pointer, so `errors.Is` behaves identically through either
name. CLAUDE.md says sentinels live in the root package; they are *declared* one
package deeper and the public surface still reads as the root package owning
them. This is the one place M0 bends a rule, and it is bent because the
alternative is a cycle.

## The rule this buys

> No package under `internal/` may import the root `peercache` package.

Enforced by `TestInternalPackagesDoNotImportRoot` in `discipline_test.go`.

`internal/testkit` follows the same rule, which has a useful side effect: the
harness is written against the same interfaces the real implementations satisfy,
rather than against a concrete `Cache`, and the root package's own tests can use
it without a test-only cycle.

## Costs, honestly

- Public documentation lives on the aliases in the root package, so `core`'s own
  comments stay terse. Two places to look when editing a type.
- `go doc` renders `peercache.Node` as an alias to an internal type. Slightly
  worse than a direct declaration.
- Adding a public type means touching two files.

## Reversal

Cheap. Inlining the definitions back into the root package is mechanical, and
because the public names are aliases, doing so is not a breaking change for any
caller. That is exactly why aliases were chosen over wrapper types.
