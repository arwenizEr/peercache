# 0017 — Metrics without a client library

**Status:** accepted
**Milestone:** M7
**Deviates from:** docs/design.md §4, which says "Prometheus and OTel: the metrics from
the original spec, plus correctness metrics"

## Context

M7 asks for Prometheus and OpenTelemetry instrumentation. The obvious reading is
two direct dependencies: `github.com/prometheus/client_golang` and the OTel
metric SDK, each registering its own collectors.

Two facts make that a worse trade here than it looks.

**PeerCache is embedded.** It runs inside somebody else's binary, and a library
that drags in a metrics client imposes that choice on every importer — including
the ones already using the other one, or a third, or none. The cost is paid by
people who get no benefit from it.

**The dependency budget is deliberately small.** CLAUDE.md permits memberlist,
gRPC, a cache library, a codec, and test dependencies, and requires a note in
`docs/decisions/` before anything else. `client_golang` and the OTel SDK are
both substantial transitive graphs — larger, between them, than everything the
budget currently allows.

Against that: an application wanting Prometheus has to write an adapter, which
is work this library could have done once.

## Decision

Export values, not instruments.

- `Cache.Metrics()` returns `[]Metric` — name, kind, help, value — collected
  from the `Stats()` surface the earlier milestones already built.
- `Cache.WriteMetrics(w)` renders the Prometheus text exposition format
  directly. The format is small, stable, and specified; writing it is about
  thirty lines and imports nothing.
- `Cache.MetricsHandler()` serves that over HTTP with the correct content type,
  because a wrong content type is a silent scrape failure and everybody gets it
  wrong once.

An application wanting the real client libraries writes a dozen lines, and both
adapters are in the `Metrics()` documentation ready to copy.

### Metric names are hand-written, not derived

`Collect` spells out every name rather than reflecting over the struct. Names
are a contract with dashboards and alerting rules: deriving them from Go field
names means a rename silently breaks somebody's paging rule, and every future
field becomes a public series whether or not it means anything to an operator.

The cost is that a field can be added to `Stats` and forgotten.
`TestEveryStatsFieldIsExported` closes that by reflecting over `core.Stats` and
requiring either a metric or an explicit exemption with a reason. It caught two
mis-mapped names the first time it ran.

### Latency histograms are opt-in, and the reason is a measurement

An observation costs 15.8 ns uncontended and 36.9 ns under concurrency,
allocation-free. Against the 515 ns local read path M1 measured, that is about
7%.

Worse, the platform's monotonic clock resolves to roughly 500 µs, so timing a
sub-microsecond operation records zero. Instrumenting the local path would pay
7% for no information at all.

So `WithLatencyHistograms` instruments the *remote* path only, where a 1.7 ms
round trip makes the observation immeasurable by comparison, and it is off by
default. The option's documentation carries these numbers rather than saying
"for performance reasons".

## Consequences

**No `peercache_*` series appear until the application asks for them.** There is
no global registry and nothing self-registers. That is the right default for a
library but it means a team can deploy without metrics and not notice; the
deploy README and the guarantees section both point at the endpoint.

**Two exposition formats can drift.** `WriteMetrics` and `MetricsHandler` are
tested to produce identical output for exactly this reason.

**A cardinality decision is baked in.** No per-namespace or per-key labels: the
aggregate is what operators alert on, and a cardinality explosion is a
production incident of its own. A namespace breakdown would need an explicit
option and is not built.

**If the spec intended literal direct dependencies, this does not satisfy it.**
That is why this file exists. The functionality is present and testable; what is
absent is the import.

## Alternatives rejected

**Direct `client_golang` and OTel SDK dependencies.** The conventional answer,
and correct for an application. Rejected for an embedded library on dependency
grounds, and because the adapter is genuinely small.

**Adapters in a nested module**, as `internal/l1/eval` does for its benchmark
candidates. This was close to being chosen and remains a reasonable future step:
it would let `peercache/prometheus` exist with the dependency contained, at the
cost of a second module to version and release. Deferred because the adapter is
a dozen lines and a module is forever.

**A `MetricsCollector` interface the application implements.** More flexible,
and it inverts the dependency properly. Rejected as more machinery than the
problem needs: a slice of values is already the simplest thing that admits any
backend, and an interface would have to grow a method per metric type.
