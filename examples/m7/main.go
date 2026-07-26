// Command m7 demonstrates observability, completed by milestone M7.
//
// The earlier milestones built the counters — split-brain detections,
// stale-epoch refusals, implicit namespaces, replication failures. This one
// exports them, and shows the operational surface an application should wire
// up: a scrape endpoint, readiness and liveness probes, a drain hook, and
// pprof.
//
// It also demonstrates the counters doing their job, by causing the conditions
// they exist to report and showing them move.
//
// Run it with:
//
//	go run ./examples/m7
//
// It prints the metrics endpoint's output and the handler wiring, then exits.
// Nothing here listens on a port; the handlers are shown so they can be copied.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
)

var detection = testkit.MembershipConfig{
	FailureTimeout: 300 * time.Millisecond,
	JoinDelay:      200 * time.Millisecond,
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type cluster struct {
	clk     *clock.Fake
	net     *testkit.Network
	reg     *testkit.Registry
	ids     []string
	nodes   map[string]core.Node
	caches  map[string]peercache.Cache
	members map[string]*testkit.Membership
	dead    map[string]bool
}

func run() error {
	c, err := newCluster(3)
	if err != nil {
		return err
	}
	defer c.close()

	for _, step := range []func(*cluster) error{
		scrapeEndpoint,
		correctnessCountersMove,
		latencyHistograms,
	} {
		if err := step(c); err != nil {
			return err
		}
	}

	printWiring()
	fmt.Println("\nThat is the whole system: seven milestones, and the honest")
	fmt.Println("guarantees are in README.md.")
	return nil
}

func newCluster(n int) (*cluster, error) {
	clk := clock.NewFake(clock.Epoch)
	c := &cluster{
		clk:     clk,
		net:     testkit.NewNetwork(clk, 1),
		reg:     testkit.NewRegistry(),
		nodes:   make(map[string]core.Node, n),
		caches:  make(map[string]peercache.Cache, n),
		members: make(map[string]*testkit.Membership, n),
		dead:    make(map[string]bool, n),
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("pod-%d", i)
		c.ids = append(c.ids, id)
		c.nodes[id] = core.Node{ID: id, Addr: id + ":7947"}
		if err := c.start(id); err != nil {
			return nil, err
		}
	}
	c.clk.Advance(2 * detection.JoinDelay)
	if err := c.converge(n); err != nil {
		return nil, err
	}
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	fmt.Printf("cluster of %d formed\n", n)
	return c, nil
}

func (c *cluster) start(id string) error {
	m := testkit.NewMembership(c.nodes[id], c.net, c.clk, c.reg, detection)
	if err := m.Join(nil); err != nil {
		return err
	}
	cache, err := peercache.New(
		peercache.WithNodeID(id),
		peercache.WithClock(c.clk),
		peercache.WithMembership(m),
		peercache.WithTransport(c.net.Transport(c.nodes[id])),
		// Off by default; see the option's documentation for the measured
		// cost and why the local path is not instrumented.
		peercache.WithLatencyHistograms(),
	)
	if err != nil {
		return err
	}
	c.caches[id] = cache
	c.members[id] = m
	c.dead[id] = false
	return nil
}

func (c *cluster) live() []string {
	out := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		if !c.dead[id] {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) converge(want int) error {
	const maxYields = 10_000_000
	for i := 0; i < maxYields; i++ {
		agreed := true
		for _, id := range c.live() {
			if c.caches[id].Stats().Members != want {
				agreed = false
				break
			}
		}
		if agreed {
			return nil
		}
		runtime.Gosched()
	}
	return errors.New("cluster did not converge")
}

func (c *cluster) close() {
	for _, id := range c.live() {
		_ = c.caches[id].Close(context.Background())
	}
	_ = c.net.Close()
}

// ns opens the namespace on every node, which M6 established is a correctness
// requirement: a namespace created implicitly by a routed write gets default
// options and silently replicates nothing.
func (c *cluster) openAll(name string, opts ...peercache.NamespaceOption) {
	for _, id := range c.live() {
		c.caches[id].Namespace(name, opts...)
	}
}

func scrapeEndpoint(c *cluster) error {
	fmt.Println("\n--- the scrape endpoint ---")

	c.openAll("orders",
		peercache.WithReplicationFactor(2),
		peercache.WithSyncReplication())

	ctx := context.Background()
	for i := 0; i < 500; i++ {
		if err := c.caches[c.live()[0]].Namespace("orders").
			Set(ctx, fmt.Sprintf("orders/%d", i), []byte("v"), 0); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}
	for i := 0; i < 200; i++ {
		if _, err := c.caches[c.live()[1]].Namespace("orders").
			Get(ctx, fmt.Sprintf("orders/%d", i)); err != nil {
			return fmt.Errorf("get: %w", err)
		}
	}

	// Exactly what a Prometheus scrape would receive.
	rec := httptest.NewRecorder()
	c.caches[c.live()[0]].MetricsHandler().
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	fmt.Printf("  GET /metrics -> %d %s\n", rec.Code, rec.Header().Get("Content-Type"))
	shown := 0
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		if shown < 8 {
			fmt.Println("   ", line)
			shown++
		}
	}
	total := strings.Count(rec.Body.String(), "\n# TYPE ")
	fmt.Printf("  ... %d series in total, no dependency required\n", total+1)
	return nil
}

func correctnessCountersMove(c *cluster) error {
	fmt.Println("\n--- the counters that matter ---")
	fmt.Println("  These exist because this system is honest about having windows in it,")
	fmt.Println("  and a window nobody counts is indistinguishable from one that is not there.")

	// Cause a real misconfiguration: a namespace only one node has opened.
	// Every other owner creates it implicitly, at replication factor 1.
	ctx := context.Background()
	writer := c.live()[0]

	// Measure the *delta*, not the total. ReplicationsSent is cumulative
	// across every namespace, and quoting the running total next to "RF=2
	// over 200 keys" would overstate the shortfall by counting the healthy
	// namespace's traffic as if it belonged to the broken one.
	var sentBefore int64
	for _, id := range c.live() {
		sentBefore += c.caches[id].Stats().ReplicationsSent
	}

	ns := c.caches[writer].Namespace("misconfigured",
		peercache.WithReplicationFactor(2),
		peercache.WithSyncReplication())
	for i := 0; i < 200; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("m/%d", i), []byte("v"), 0); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	var implicit, sentAfter int64
	for _, id := range c.live() {
		st := c.caches[id].Stats()
		implicit += st.ImplicitNamespaces
		sentAfter += st.ReplicationsSent
	}
	sent := sentAfter - sentBefore
	fmt.Printf("\n  a namespace opened on 1 of %d nodes:\n", len(c.live()))
	fmt.Printf("    peercache_implicit_namespaces_total  %d\n", implicit)
	fmt.Printf("    replications for those 200 keys      %d  (RF=2 wants 200)\n", sent)
	fmt.Println("    ^ this is the misconfiguration that looked like a data-loss bug for")
	fmt.Println("      most of M6. Alert on the first counter being non-zero.")
	return nil
}

func latencyHistograms(c *cluster) error {
	fmt.Println("\n--- remote latency ---")

	ctx := context.Background()
	reader := c.live()[1]
	for i := 0; i < 2000; i++ {
		_, _ = c.caches[reader].Namespace("orders").Get(ctx, fmt.Sprintf("orders/%d", i%500))
	}

	p50, p99, p999 := c.caches[reader].RemoteLatency()
	fmt.Printf("  p50 %s  p99 %s  p99.9 %s\n", ns(p50), ns(p99), ns(p999))
	fmt.Println()
	fmt.Println("  Those numbers are not a measurement of anything, and it is worth saying")
	fmt.Println("  so rather than letting them look like one. This example runs on a")
	fmt.Println("  simulated clock that only moves when the code moves it, so every")
	fmt.Println("  operation appears to take no time and lands in the first bucket. Real")
	fmt.Println("  figures, measured over gRPC on a real clock, are in docs/benchmarks.md.")
	fmt.Println()
	fmt.Println("  What is real here is the shape: values are bucket upper bounds, so each")
	fmt.Println("  is an over-estimate by at most one bucket width.")
	fmt.Println("  Off by default: an observation is ~37ns under contention, which is 7% of")
	fmt.Println("  the local read path — and a clock that resolves to 500us would record")
	fmt.Println("  zeros there anyway. Only remote operations are instrumented.")
	return nil
}

func ns(v uint64) string {
	switch {
	case v == 0:
		return "n/a"
	case v < 1_000:
		return fmt.Sprintf("%dns", v)
	case v < 1_000_000:
		return fmt.Sprintf("%.1fus", float64(v)/1e3)
	default:
		return fmt.Sprintf("%.1fms", float64(v)/1e6)
	}
}

func printWiring() {
	fmt.Println("\n--- the operational surface ---")
	fmt.Println(`
  mux.Handle("/metrics", cache.MetricsHandler())

  mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
      if st := cache.Ready(); !st.Ready {
          http.Error(w, st.String(), http.StatusServiceUnavailable)
          return
      }
      fmt.Fprintln(w, "ready")
  })

  mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
      // Liveness is deliberately NOT readiness. A node that has joined but
      // not caught up is unready, not dead; restarting it would discard the
      // catch-up progress that is its only route to readiness.
      fmt.Fprintln(w, "ok")
  })

  mux.HandleFunc("/drain", func(w http.ResponseWriter, r *http.Request) {
      ctx, cancel := context.WithTimeout(r.Context(), 50*time.Second)
      defer cancel()
      if err := cache.Drain(ctx); err != nil {
          http.Error(w, err.Error(), http.StatusInternalServerError)
          return
      }
      fmt.Fprintln(w, "drained")
  })

  // pprof, on a port that is not public.
  import _ "net/http/pprof"
  go func() { _ = http.ListenAndServe("localhost:6060", nil) }()`)

	fmt.Println("\n  Manifests wiring these to Kubernetes probes are in deploy/.")
}
