# 0032 — An OpenTelemetry bridge, as its own module

**Status:** accepted and built (M15b).
**Bound by:** decision 0017 — the core carries no metrics client — and the
dependency budget, which keeps vendor libraries out of the core module.

## Context

`Cache.Metrics()` returns every series as plain values, and its documentation
sketches the dozen lines an application needs to hand them to OpenTelemetry.
Every application using OpenTelemetry would write the same dozen lines and get
one thing subtly wrong: OpenTelemetry's Prometheus exporter appends `_total` to
monotonic counters, so a counter registered under its Prometheus name comes out
as `peercache_…_total_total`.

## Decision

A nested module, `github.com/arwenizEr/peercache/otel`, depending on the
OpenTelemetry **API** only (`go.opentelemetry.io/otel/metric`); the SDK appears
in its tests. One function:

```go
func Register(meter metric.Meter, cache peercache.Cache) (metric.Registration, error)
```

It creates an observable counter or gauge per series, with the series' help as
its description, and one callback reading `cache.Metrics()` once per
collection. A counter's instrument name drops its trailing `_total`; every
observation carries `peercache.node`, the cache's node ID.

OpenTelemetry v1.47 requires Go 1.26, so this module does; the core stays at
1.25.

Latency histograms are not bridged: OpenTelemetry histograms record each
measurement, and the core keeps only bucket counts.

## Acceptance

- Every series becomes exactly one instrument of its kind, with its help text
  and the node attribute.
- No counter's instrument name ends in `_total`; gauges keep their names.
- A later collection observes the cache as it is then.
- After `Unregister`, nothing is observed.
