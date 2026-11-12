# 0006 — Singleflight is hand-rolled, not golang.org/x/sync

**Status:** accepted, M1
**Affects:** `internal/singleflight`, `Namespace.GetOrLoad`

## The requirement

docs/design.md section 3.1: concurrent misses on the owner collapse via singleflight.
The M1 acceptance criterion makes it measurable — a loader counting invocations
under a thousand concurrent misses must report exactly one.

`golang.org/x/sync/singleflight` is the obvious answer. It was rejected for
three reasons, in order of weight.

## 1. It re-panics in every waiter

When the shared call panics, that package re-panics in each waiting goroutine.
A loader is application code that talks to a database. A nil-pointer bug in it
would become a panic in a thousand unrelated goroutines that merely asked for a
value, and docs/design.md forbids panics in library paths.

Here a panicking loader is recovered and converted to `ErrLoaderPanic` for
everyone who was waiting — a result they can handle. `runtime.Goexit` (what
`t.Fatal` does) is handled too, as `ErrLoaderFailed`; without that, the deferred
publish would hand every waiter a nil value and a nil error, which reads as a
successful load of nothing.

## 2. The loader's context must not be the leader's

Concurrent misses collapse into one load. If that load inherits the context of
whichever caller happened to arrive first, then a caller with five milliseconds
left can abort a load that nine hundred callers with a second left were
depending on.

The loader therefore runs under `context.WithoutCancel` of the leader's
context — request-scoped values such as a trace identifier still propagate,
cancellation does not — with a deadline supplied by the namespace's
`WithLoaderTimeout`. The bound on a load is a property of the namespace, not of
the first caller through the door.

`x/sync` offers `DoChan`, which lets a *caller* stop waiting. It does not
address what context the shared call runs under.

## 3. Its current releases require Go 1.25

docs/design.md fixes the floor at Go 1.23. Pinning an old release to dodge that is a
maintenance trap that expires the first time anything else in the graph wants a
newer x/sync.

> **Superseded, M3.** It expired exactly as predicted: memberlist pulls `x/sync`
> into the graph, and the floor moved to 1.25 regardless — see
> `0013-go-floor-raised-to-1.25.md`. This reason no longer holds.
>
> Reasons 1 and 2 are untouched and are the ones that matter, so the
> implementation stays. Left in place rather than rewritten: the reasoning was
> sound on the information available, and a decision log that quietly edits
> itself is worth less than one that shows where it was overtaken.

## What this costs

About ninety lines, and the following measured overhead on Machine A:

| Benchmark | ns/op | allocs/op |
|---|---:|---:|
| Uncontended (one caller, no collapsing) | 1051 | 4 |
| Distinct keys, parallel | 898 | 3 |
| One hot key, parallel | 540 | 1 |

The uncontended path is not cheap: it allocates a call record and a channel and
starts a goroutine. The goroutine is deliberate — running the loader inline
would commit the leader to waiting for the whole load regardless of its own
deadline, and "no path blocks past its context deadline" has to hold for the
leader too.

Roughly a microsecond is the price of that guarantee, against a load that
reaches a database and costs milliseconds. If a future profile shows this on a
hot path, the fix is to skip the goroutine when the caller's context has no
deadline, not to remove the guarantee.

Note that the collapsing case is *cheaper* than the uncontended one, because
nine hundred and ninety-nine of a thousand callers do no allocation at all.

## Reversal

The package is small and its interface is one method. Swapping to `x/sync`
later means reinstating the panic behaviour and the context problem, so the
reversal is possible but is unlikely to be wanted.
