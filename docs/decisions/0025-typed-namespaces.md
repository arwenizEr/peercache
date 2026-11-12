# 0025 — Typed namespaces: `NamespaceOf[T]` over the byte namespace

**Status:** accepted
**Milestone:** M10
**Adds:** `NamespaceOf`, `TypedNamespace`, `WithTypedLoader`,
`DefaultTypedCodec`, `ErrCodec`. `WithCodec` now acts. No new dependency.

## Context

Open question 12, open since M1: SPEC.md section 2 promised "typed
Get/Set/GetOrLoad" and section 3.1 delivered `[]byte`. `WithCodec` validated a
codec name and encoded nothing (question 17). The codec registry existed and was
tested, and nothing on a data path used it.

## Decisions, and who made them

- **Shape: `NamespaceOf[T](cache, name, opts...)`** — chosen by the user over a
  thin `Typed[T](ns)` wrapper. A package function because Go interfaces cannot
  have type parameters; it returns a `TypedNamespace[T]` interface with the
  operations of `Namespace` plus `Raw()`.
- **Default codec: MessagePack** — chosen by the user. JSON replaces invalid
  UTF-8 in strings with U+FFFD and reports success (question 41); a default
  must not corrupt data. JSON stays available by name.
- **A view, not a second store.** The typed namespace encodes to bytes and calls
  the byte namespace. Everything below the value is untouched, so none of M1–M9's
  guarantees needed re-establishing.
- **`WithTypedLoader[T]` is an ordinary `NamespaceOption`.** The codec is not
  known until every option is applied — `WithCodec` may come after it — so the
  option holds a `core.TypedLoaderSpec` and the namespace builds the byte loader
  from it at construction. It conflicts with `WithLoader`.
- **Mismatches are `ErrInvalidOption`.** One name holds one type's encoding. A
  second `NamespaceOf` with another `T` on the same name, or a typed loader whose
  `T` is not the namespace's, would decode garbage — or worse, zero values — as
  success. The first type to open a name binds it; later handles of another type
  report the error on every operation.
- **Undecodable or unencodable values are `ErrCodec`**, a new sentinel, and not
  retryable.

## What it costs

On Machine A a typed `Get` of a four-field struct is 487 ns against the byte
path's 37 ns, and the difference is one MessagePack decode. That meets SPEC.md's
criterion — the byte path plus one decode — and it means the codec, not the view,
is the cost. A hot path that cannot spend half a microsecond per read keeps the
byte API, and `Raw()` reaches it from a typed handle.

## Typed group functions, added after

Left out of M10 at first and added straight after it:
`RegisterTypedGroupFunc[A, R]` and `CallTyped[A, R]`. Only the argument and
result are typed; `GroupState` stays byte-valued, because a group's keys are the
function's own business and a typed state would need a type per key.

The type check uses the local registration. Every pod runs the same binary
(CLAUDE.md decision 3), so what this node registered under a name is what every
node did, and a call naming other types is refused with `ErrInvalidOption`
before anything is sent rather than decoded wrongly on the owner. `ErrCodec`
gained a gRPC mapping — `codes.Internal` plus its text, as `ErrUnderReplicated`
has — so an argument that fails to decode on a remote owner is still `ErrCodec`
at the caller.
