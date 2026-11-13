package otel

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/arwenizEr/peercache"
)

// NodeAttribute is the attribute every observation carries: the node ID of the
// cache that produced it.
const NodeAttribute = "peercache.node"

// Register creates an instrument on meter for each of cache's metric series and
// a callback observing them, and returns the registration; Unregister it before
// closing the cache.
//
// The instruments are created from the series the cache reports when Register
// is called. They are fixed per release, so that is every series there is.
func Register(meter metric.Meter, cache peercache.Cache) (metric.Registration, error) {
	series := cache.Metrics()
	counters := make(map[string]metric.Float64ObservableCounter, len(series))
	gauges := make(map[string]metric.Float64ObservableGauge, len(series))
	observables := make([]metric.Observable, 0, len(series))

	for _, m := range series {
		switch m.Kind {
		case peercache.CounterMetric:
			c, err := meter.Float64ObservableCounter(InstrumentName(m), metric.WithDescription(m.Help))
			if err != nil {
				return nil, fmt.Errorf("peercache/otel: %s: %w", m.Name, err)
			}
			counters[m.Name] = c
			observables = append(observables, c)
		default:
			g, err := meter.Float64ObservableGauge(InstrumentName(m), metric.WithDescription(m.Help))
			if err != nil {
				return nil, fmt.Errorf("peercache/otel: %s: %w", m.Name, err)
			}
			gauges[m.Name] = g
			observables = append(observables, g)
		}
	}

	node := metric.WithAttributes(attribute.String(NodeAttribute, cache.Stats().Self.ID))
	return meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		for _, m := range cache.Metrics() {
			if c, ok := counters[m.Name]; ok {
				o.ObserveFloat64(c, m.Value, node)
			} else if g, ok := gauges[m.Name]; ok {
				o.ObserveFloat64(g, m.Value, node)
			}
		}
		return nil
	}, observables...)
}

// InstrumentName is the OpenTelemetry name of a series: its Prometheus name,
// less a counter's trailing _total, which OpenTelemetry's Prometheus exporter
// adds back.
func InstrumentName(m peercache.Metric) string {
	if m.Kind == peercache.CounterMetric {
		return strings.TrimSuffix(m.Name, "_total")
	}
	return m.Name
}
