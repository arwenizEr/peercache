# 0038 — Trace propagation through the cache

**Status:** accepted.
**Bound by:** decision 0017 and the dependency budget (no tracing library in
the core module; OpenTelemetry lives in the `otel` module, decision 0032), and
decision 0031's rule that introspection never exposes values or keys.

## Context

In an embedded cache the hop to a key's owner happens inside the application's
own request. With no tracing, that hop is invisible in the application's
traces: a slow `Get` that crossed to a throttled owner looks exactly like a slow
handler, and a loader's database query appears with no parent. Dapr propagates
W3C trace context on every actor call for this reason.

## Decision

A **`Tracer` hook** in the core, no-op by default, set with `WithTracer`:

```go
type Tracer interface {
    Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
    Inject(ctx context.Context, carrier map[string]string)
    Extract(ctx context.Context, carrier map[string]string) context.Context
}
type Span interface{ End(err error) }
type Attr struct{ Key, Value string }
```

The core knows nothing of any tracing system; `otel.Tracer(provider,
propagator)` in the `otel` module adapts OpenTelemetry to it.

**Spans**, all children of whatever span the caller's context carries:

| Span | Where | Attributes |
|---|---|---|
| `peercache.rpc <method>` | the calling node, around each peer request | `peercache.namespace`, `peercache.peer`, `peercache.node` |
| `peercache.serve <method>` | the serving node, around handling it | `peercache.peer` (the caller), `peercache.node` |
| `peercache.load` | around each loader call, per-key, batch or refresh | `peercache.namespace`, `peercache.keys` (a count) |

Keys are never attributes: they are application data, often personal, and
ADR 0031 keeps them out of everything the library emits. Values never are.

**Propagation rides the transport's metadata, not the wire messages.** The
caller injects its span context into a carrier and attaches it to the request
context; the gRPC transport sends each carrier entry as a request header
(`peercache-trace-<name>`), and the server side reads them back into the
handler's context, where the cache extracts them before starting its serve
span. No message changes, so a peer on an earlier release simply ignores the
headers and its spans are missing, never wrong.

**Cost when off:** the no-op tracer's `Start` returns its arguments; the
default path allocates nothing for tracing.

## Rejected

- **A field in every request message:** twenty message types to change, and a
  codec concern for something the transport already has a channel for.
- **Depending on OpenTelemetry's API in the core:** breaks the dependency
  budget; the hook is four methods.
- **Spans for every local operation:** a local `Get` is a few hundred
  nanoseconds; a span would cost more than the operation. Only hops and loads,
  which are the slow parts, are traced.

## Acceptance

- A remote `Get` produces a client span on the caller and a serve span on the
  owner, the serve span a child of the client span, both children of the
  caller's span.
- A loader runs under a `peercache.load` span on the owner, child of the serve
  span when the load was forwarded.
- No span carries a key or a value.
- The headers cross the real gRPC transport.
- Without `WithTracer` nothing is traced and no allocation is added.
