package tests

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// TestTheDashboardIsServed: /ui is the embedded page, HTML, uncached, and
// allowed to load nothing from anywhere else.
func TestTheDashboardIsServed(t *testing.T) {
	c, _ := newCache(t)
	srv := httptest.NewServer(c.DebugHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/ui")
	if err != nil {
		t.Fatalf("GET /ui: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("GET /ui = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(resp.Header.Get("Content-Security-Policy"), "default-src 'self'") {
		t.Fatalf("the dashboard may load from elsewhere: CSP %q", resp.Header.Get("Content-Security-Policy"))
	}
}

// TestEveryRouteTheDashboardReadsExists: the page fetches routes by relative
// path; each must answer JSON, or the page shows an error instead of data.
func TestEveryRouteTheDashboardReadsExists(t *testing.T) {
	c, _ := newCache(t)
	c.Namespace("n")
	srv := httptest.NewServer(c.DebugHandler())
	defer srv.Close()
	page, err := http.Get(srv.URL + "/ui")
	if err != nil {
		t.Fatalf("GET /ui: %v", err)
	}
	var body strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := page.Body.Read(buf)
		body.Write(buf[:n])
		if err != nil {
			break
		}
	}
	page.Body.Close()
	routes := map[string]bool{}
	for _, m := range regexp.MustCompile(`get\("\./([a-z]*)`).FindAllStringSubmatch(body.String(), -1) {
		routes[m[1]] = true
	}
	if len(routes) < 3 {
		t.Fatalf("found only %v in the page; the pattern no longer matches how it fetches", routes)
	}
	for route := range routes {
		url := srv.URL + "/" + route
		if route == "key" {
			url += "?namespace=n&key=k"
		}
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET /%s: %v", route, err)
		}
		var v any
		err = json.NewDecoder(resp.Body).Decode(&v)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || err != nil {
			t.Fatalf("GET /%s = %d, decode %v; the dashboard reads it", route, resp.StatusCode, err)
		}
	}
}
