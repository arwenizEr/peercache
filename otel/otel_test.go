package otel_test

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/arwenizEr/peercache"
	pcotel "github.com/arwenizEr/peercache/otel"
)

func newCache(t *testing.T, id string) peercache.Cache {
	t.Helper()
	c, err := peercache.New(peercache.WithNodeID(id))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// collect registers cache on a fresh SDK meter and returns one collection,
// indexed by instrument name.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := map[string]metricdata.Metrics{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func setup(t *testing.T, cache peercache.Cache) (*sdkmetric.ManualReader, func()) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	reg, err := pcotel.Register(provider.Meter("peercache"), cache)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	return reader, func() { _ = reg.Unregister() }
}

// point returns a metric's single data point value and its attributes.
func point(t *testing.T, m metricdata.Metrics) (float64, attribute.Set, bool) {
	t.Helper()
	switch d := m.Data.(type) {
	case metricdata.Sum[float64]:
		if !d.IsMonotonic {
			t.Fatalf("%s is a non-monotonic sum; a counter must be monotonic", m.Name)
		}
		if len(d.DataPoints) != 1 {
			t.Fatalf("%s has %d points, want 1", m.Name, len(d.DataPoints))
		}
		return d.DataPoints[0].Value, d.DataPoints[0].Attributes, true
	case metricdata.Gauge[float64]:
		if len(d.DataPoints) != 1 {
			t.Fatalf("%s has %d points, want 1", m.Name, len(d.DataPoints))
		}
		return d.DataPoints[0].Value, d.DataPoints[0].Attributes, false
	default:
		t.Fatalf("%s has data of type %T", m.Name, m.Data)
	}
	return 0, attribute.Set{}, false
}

// TestEverySeriesIsBridged: one instrument per series, of the series' kind,
// carrying its value and the node attribute.
func TestEverySeriesIsBridged(t *testing.T) {
	cache := newCache(t, "pod-7")
	reader, _ := setup(t, cache)
	got := collect(t, reader)
	series := cache.Metrics()
	if len(got) != len(series) {
		t.Fatalf("%d instruments collected for %d series", len(got), len(series))
	}
	for _, s := range series {
		m, ok := got[pcotel.InstrumentName(s)]
		if !ok {
			t.Fatalf("series %s has no instrument %s", s.Name, pcotel.InstrumentName(s))
		}
		_, attrs, monotonic := point(t, m)
		if monotonic != (s.Kind == peercache.CounterMetric) {
			t.Fatalf("%s: counter-ness %v, series kind %v", s.Name, monotonic, s.Kind)
		}
		if m.Description != s.Help {
			t.Fatalf("%s: description %q, want the series help", s.Name, m.Description)
		}
		if node, ok := attrs.Value(pcotel.NodeAttribute); !ok || node.AsString() != "pod-7" {
			t.Fatalf("%s: node attribute %v, %v", s.Name, node, ok)
		}
	}
}

// TestCounterNamesDropTheirTotal: OpenTelemetry's Prometheus exporter adds
// _total to counters itself.
func TestCounterNamesDropTheirTotal(t *testing.T) {
	for _, s := range newCache(t, "n").Metrics() {
		name := pcotel.InstrumentName(s)
		if s.Kind == peercache.CounterMetric && strings.HasSuffix(name, "_total") {
			t.Fatalf("counter %s is bridged as %s, which an exporter would make _total_total", s.Name, name)
		}
		if s.Kind != peercache.CounterMetric && name != s.Name {
			t.Fatalf("gauge %s renamed to %s", s.Name, name)
		}
	}
}

// TestValuesFollowTheCache: a later collection observes the cache as it is then.
func TestValuesFollowTheCache(t *testing.T) {
	cache := newCache(t, "n")
	reader, _ := setup(t, cache)
	const owned = "peercache_owned_keys"
	before, _, _ := point(t, collect(t, reader)[owned])

	ns := cache.Namespace("orders")
	for i := 0; i < 5; i++ {
		_ = ns.Set(context.Background(), string(rune('a'+i)), []byte("v"), 0)
	}
	after, _, _ := point(t, collect(t, reader)[owned])
	if after-before != 5 {
		t.Fatalf("%s went from %v to %v after five writes", owned, before, after)
	}
}

func TestUnregisterStopsObserving(t *testing.T) {
	cache := newCache(t, "n")
	reader, unregister := setup(t, cache)
	if len(collect(t, reader)) == 0 {
		t.Fatal("nothing collected while registered")
	}
	unregister()
	for name, m := range collect(t, reader) {
		switch d := m.Data.(type) {
		case metricdata.Sum[float64]:
			if len(d.DataPoints) != 0 {
				t.Fatalf("%s still observed after Unregister", name)
			}
		case metricdata.Gauge[float64]:
			if len(d.DataPoints) != 0 {
				t.Fatalf("%s still observed after Unregister", name)
			}
		}
	}
}
