package peercache

import (
	"io"
	"net/http"
	"strings"

	"github.com/arwenizEr/peercache/internal/metrics"
)

// MetricKind classifies an exported series.
type MetricKind = metrics.Kind

// Metric kinds.
const (
	// CounterMetric only ever increases.
	CounterMetric = metrics.Counter
	// GaugeMetric moves in both directions.
	GaugeMetric = metrics.Gauge
)

// Metric is one exported measurement.
type Metric = metrics.Metric

// Metrics returns this node's current measurements.
//
// # Why this rather than a Prometheus or OTel dependency
//
// PeerCache is embedded in somebody else's binary. Forcing a metrics client on
// every importer is a cost paid by applications that already have a different
// one, and both client libraries carry substantial transitive graphs — against
// a dependency budget this library deliberately keeps small.
//
// So this returns plain values and the adapter is a dozen lines you write once.
// [Cache.WriteMetrics] covers the Prometheus text format directly, which is
// enough for a scrape endpoint without importing anything at all. See
// docs/decisions/0017-metrics-without-a-client-library.md.
//
// # For a Prometheus client
//
//	for _, m := range cache.Metrics() {
//	    desc := prometheus.NewDesc(m.Name, m.Help, nil, nil)
//	    kind := prometheus.GaugeValue
//	    if m.Kind == peercache.CounterMetric {
//	        kind = prometheus.CounterValue
//	    }
//	    ch <- prometheus.MustNewConstMetric(desc, kind, m.Value)
//	}
//
// # For OpenTelemetry
//
//	meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
//	    for _, m := range cache.Metrics() {
//	        o.ObserveFloat64(gauges[m.Name], m.Value)
//	    }
//	    return nil
//	}, observables...)
func (c *cache) Metrics() []Metric {
	return metrics.Collect(c.Stats())
}

// WriteMetrics renders this node's measurements in the Prometheus text
// exposition format.
//
// It exists so that a scrape endpoint costs one handler and no dependency:
//
//	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
//	    w.Header().Set("Content-Type", "text/plain; version=0.0.4")
//	    _ = cache.WriteMetrics(w)
//	})
func (c *cache) WriteMetrics(w io.Writer) error {
	var sb strings.Builder
	metrics.WriteText(&sb, c.Metrics())
	if c.remoteLatency != nil {
		metrics.WriteHistogram(&sb, "peercache_remote_duration_seconds",
			"Latency of operations served by a peer, in seconds.", c.remoteLatency)
	}
	_, err := io.WriteString(w, sb.String())
	return err
}

// RemoteLatency returns quantiles of remote operation latency in nanoseconds,
// or zeros when [WithLatencyHistograms] is not set.
//
// Each value is a bucket's upper bound, so it overestimates by at most the
// bucket width — a p99 of 2ms means "at most 2ms". That is stated rather than
// implied, because a percentile presented as exact when it is bucketed is the
// kind of number people build budgets on.
func (c *cache) RemoteLatency() (p50, p99, p999 uint64) {
	if c.remoteLatency == nil {
		return 0, 0, 0
	}
	return c.remoteLatency.Quantile(0.50),
		c.remoteLatency.Quantile(0.99),
		c.remoteLatency.Quantile(0.999)
}

// MetricsHandler returns an http.Handler serving the Prometheus text format.
//
// Provided because the three-line handler above is written identically by
// everyone, and getting the content type wrong is a silent scrape failure.
func (c *cache) MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_ = c.WriteMetrics(w)
	})
}
