// Package otel exports a PeerCache node's measurements through OpenTelemetry.
//
// It is its own module, so that the OpenTelemetry API is a dependency of
// programs that choose it and of nothing else; the core library carries no
// metrics client (docs/decisions/0017-metrics-without-a-client-library.md).
// It needs Go 1.26, because OpenTelemetry v1.47 does; the core needs 1.25.
//
//	import pcotel "github.com/arwenizEr/peercache/otel"
//
//	reg, err := pcotel.Register(otel.Meter("peercache"), cache)
//	...
//	defer reg.Unregister()
//
// # What it registers
//
// One asynchronous instrument per series cache.Metrics() returns — an
// observable counter for a counter, an observable gauge for a gauge — with the
// series' help text as the description, and one callback that reads
// cache.Metrics() once per collection and observes them all. Every observation
// carries the attribute peercache.node, the cache's node ID, so two caches in
// one process stay apart.
//
// # Names
//
// Series keep their Prometheus names, except that a counter's trailing _total
// is dropped: OpenTelemetry's Prometheus exporter appends it to monotonic
// counters itself, and a name kept whole would come out as _total_total. On
// any exporter the meaning is the same; on a Prometheus one the name is the
// one the core's own MetricsHandler serves.
//
// Latency histograms (WithLatencyHistograms) are not bridged: OpenTelemetry
// histograms are synchronous and record each measurement, and the core keeps
// only bucket counts.
package otel
