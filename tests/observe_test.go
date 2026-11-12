package tests

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMetricsReflectRealActivity checks the whole path: work happens, Stats
// records it, Metrics exports it. A metric that is correct in isolation and
// disconnected from the cache is the failure this catches.
func TestMetricsReflectRealActivity(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("m")
	ctx := t.Context()

	for i := 0; i < 10; i++ {
		if err := ns.Set(ctx, "k"+string(rune('a'+i)), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if _, err := ns.Get(ctx, "ka"); err != nil {
		t.Fatalf("Get: %v", err)
	}

	got := make(map[string]float64)
	for _, m := range c.Metrics() {
		got[m.Name] = m.Value
	}
	if got["peercache_owned_keys"] != 10 {
		t.Errorf("peercache_owned_keys = %v after 10 writes, want 10", got["peercache_owned_keys"])
	}
	if got["peercache_hits_total"] < 1 {
		t.Errorf("peercache_hits_total = %v after a successful read, want at least 1",
			got["peercache_hits_total"])
	}
}

// TestMetricsHandlerServesTheTextFormat covers the endpoint an operator
// actually scrapes, including the content type — a wrong one is a silent
// scrape failure, which is the worst kind.
func TestMetricsHandlerServesTheTextFormat(t *testing.T) {
	c, _ := newCache(t)
	if err := c.Namespace("m").Set(t.Context(), "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	rec := httptest.NewRecorder()
	c.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q; a scraper needs text/plain and fails silently "+
			"without it", ct)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"# HELP peercache_owned_keys ",
		"# TYPE peercache_owned_keys gauge",
		"peercache_owned_keys 1",
		"# TYPE peercache_split_brain_detections_total counter",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output missing %q:\n%s", want, body)
		}
	}
}

// TestWriteMetricsMatchesTheHandler pins that the two paths cannot drift.
func TestWriteMetricsMatchesTheHandler(t *testing.T) {
	c, _ := newCache(t)

	var direct strings.Builder
	if err := c.WriteMetrics(&direct); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}

	rec := httptest.NewRecorder()
	c.MetricsHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if direct.String() != rec.Body.String() {
		t.Error("WriteMetrics and MetricsHandler produced different output")
	}
}

// TestWriteMetricsReportsWriteErrors matters because a scrape endpoint that
// swallows a broken connection looks healthy while delivering nothing.
func TestWriteMetricsReportsWriteErrors(t *testing.T) {
	c, _ := newCache(t)
	if err := c.WriteMetrics(failingWriter{}); err == nil {
		t.Error("WriteMetrics swallowed a write error")
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
