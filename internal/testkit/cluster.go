package testkit

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// ClusterConfig configures [NewCluster].
type ClusterConfig struct {
	// Nodes is the number of nodes to create. Default 3.
	Nodes int
	// Membership tunes detection latency.
	Membership MembershipConfig
	// NewHandler builds the server-side handler for a node. Default
	// [EchoHandler].
	NewHandler func(self core.Node) core.Handler
	// SkipConverge leaves the cluster unformed, so that a test can observe
	// the join process itself rather than its result.
	SkipConverge bool
}

// ClusterOption configures a [Cluster].
type ClusterOption func(*ClusterConfig)

// WithNodes sets the number of nodes.
func WithNodes(n int) ClusterOption {
	return func(c *ClusterConfig) { c.Nodes = n }
}

// WithMembershipConfig sets the detection timings.
func WithMembershipConfig(mc MembershipConfig) ClusterOption {
	return func(c *ClusterConfig) { c.Membership = mc }
}

// WithHandler sets the factory that builds each node's request handler.
func WithHandler(f func(self core.Node) core.Handler) ClusterOption {
	return func(c *ClusterConfig) { c.NewHandler = f }
}

// WithoutConverge returns a cluster whose nodes have not yet discovered each
// other.
func WithoutConverge() ClusterOption {
	return func(c *ClusterConfig) { c.SkipConverge = true }
}

// Node is one in-process member of a [Cluster].
type Node struct {
	// Info is the node's identity.
	Info core.Node
	// Transport is this node's view of the fabric.
	Transport core.Transport
	// Membership is this node's view of the cluster.
	Membership *Membership
	// Handler is this node's server side.
	Handler core.Handler

	alive bool
}

// Alive reports whether the node's process is notionally running. A node killed
// with [Cluster.HardKill] is not alive; one merely cut off by
// [Network.Isolate] still is.
func (n *Node) Alive() bool { return n.alive }

// Cluster is N in-process nodes over a shared fake [Network] and a shared fake
// clock.
//
// Everything about it is deterministic. Time only moves when the test moves it,
// message loss is drawn from the printed seed, and no goroutine is waited on
// implicitly. A test that passes here passes for a reason.
type Cluster struct {
	// TB is the owning test.
	TB testing.TB
	// Clock is the shared fake clock. Advance it to move every node's time
	// at once.
	Clock *clock.Fake
	// Net is the shared fabric. Inject faults through it.
	Net *Network
	// Registry is how memberships find one another.
	Registry *Registry
	// Seed is the seed for this cluster's randomness, printed on failure.
	Seed int64
	// Rand is a generator for the test's own use. It is not safe for
	// concurrent use, matching math/rand.
	Rand *rand.Rand

	cfg      ClusterConfig
	leakOpts []goleak.Option

	mu     sync.Mutex
	nodes  map[string]*Node
	order  []string
	closed bool
}

// NewCluster builds a cluster and registers its teardown with tb.
//
// Unless [WithoutConverge] is given, the clock is advanced past the join delay
// so that every node already sees every other. That advance is the only time
// NewCluster moves the clock.
//
// Teardown asserts three things that a harness is uniquely able to check: no
// clock misuse, no timer left pending, and no leaked goroutine. Each of those
// is a bug class that otherwise surfaces as a flake in some unrelated test
// weeks later.
func NewCluster(tb testing.TB, opts ...ClusterOption) *Cluster {
	tb.Helper()

	cfg := ClusterConfig{Nodes: 3}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Nodes <= 0 {
		tb.Fatalf("testkit: cluster needs at least one node, got %d", cfg.Nodes)
	}
	cfg.Membership.withDefaults()
	if cfg.NewHandler == nil {
		cfg.NewHandler = func(self core.Node) core.Handler { return EchoHandler(self) }
	}

	seed := Seed(tb)
	clk := clock.NewFake(clock.Epoch)
	clk.SetMisuseHandler(func(msg string) { tb.Errorf("clock misuse: %s", msg) })

	c := &Cluster{
		TB:       tb,
		Clock:    clk,
		Net:      NewNetwork(clk, seed),
		Registry: NewRegistry(),
		Seed:     seed,
		Rand:     rand.New(rand.NewSource(seed)),
		cfg:      cfg,
		leakOpts: []goleak.Option{goleak.IgnoreCurrent()},
		nodes:    make(map[string]*Node),
	}

	for i := 0; i < cfg.Nodes; i++ {
		c.addNode(fmt.Sprintf("n%d", i))
	}
	// One notification after every node exists, rather than one per node:
	// the intermediate views are not interesting and arming timers for them
	// only makes the convergence advance harder to reason about.
	c.Net.notify()

	if !cfg.SkipConverge {
		c.Clock.Advance(cfg.Membership.JoinDelay)
	}

	tb.Cleanup(c.Close)
	return c
}

// addNode creates and registers one node. The caller triggers the change
// notification.
func (c *Cluster) addNode(id string) *Node {
	info := core.Node{ID: id, Addr: id + ":7946"}
	n := &Node{
		Info:      info,
		Transport: c.Net.Transport(info),
		Handler:   c.cfg.NewHandler(info),
		alive:     true,
	}
	n.Membership = NewMembership(info, c.Net, c.Clock, c.Registry, c.cfg.Membership)

	c.mu.Lock()
	c.nodes[id] = n
	c.order = append(c.order, id)
	c.mu.Unlock()

	c.Net.Register(info, n.Handler)
	return n
}

// Node returns the node with the given ID, failing the test if there is none.
func (c *Cluster) Node(id string) *Node {
	c.TB.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		c.TB.Fatalf("testkit: no such node %q", id)
	}
	return n
}

// IDs returns every node ID in creation order, including dead ones.
func (c *Cluster) IDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.order...)
}

// Advance moves the shared clock, which is the only way anything timed happens.
func (c *Cluster) Advance(d time.Duration) { c.Clock.Advance(d) }

// HardKill removes a node the way a SIGKILL does: no announcement, no
// ownership transfer, no chance to say goodbye. Peers notice only after the
// failure timeout elapses.
func (c *Cluster) HardKill(id string) {
	c.TB.Helper()
	n := c.Node(id)

	c.mu.Lock()
	n.alive = false
	c.mu.Unlock()

	// Stop observing before disappearing. A dead process does not keep a
	// view, and leaving its timers armed would let a corpse participate in
	// the test's convergence.
	n.Membership.Close()
	_ = n.Transport.Close()
	c.Net.Unregister(id)
}

// GracefulKill removes a node the way a PreStop hook should: it announces its
// departure first, so peers that can hear it update immediately instead of
// waiting out the failure timeout.
//
// Peers that cannot hear the announcement — a partitioned minority — still
// learn the slow way, which is the case worth writing tests about.
func (c *Cluster) GracefulKill(ctx context.Context, id string) error {
	c.TB.Helper()
	n := c.Node(id)
	if err := n.Membership.Leave(ctx); err != nil {
		return fmt.Errorf("testkit: graceful kill of %s: %w", id, err)
	}

	c.mu.Lock()
	n.alive = false
	c.mu.Unlock()

	n.Membership.Close()
	_ = n.Transport.Close()
	c.Net.Unregister(id)
	return nil
}

// Revive brings a killed node back as a fresh process: same ID and address,
// empty membership view, no memory of what it knew before.
//
// The empty view is the point. A node returning from the dead that remembers
// its old view is the bug this models, not the behaviour.
func (c *Cluster) Revive(id string) *Node {
	c.TB.Helper()
	old := c.Node(id)
	if old.Alive() {
		c.TB.Fatalf("testkit: node %q is already alive", id)
	}

	n := &Node{
		Info:      old.Info,
		Transport: c.Net.Transport(old.Info),
		Handler:   c.cfg.NewHandler(old.Info),
		alive:     true,
	}
	n.Membership = NewMembership(old.Info, c.Net, c.Clock, c.Registry, c.cfg.Membership)

	c.mu.Lock()
	c.nodes[id] = n
	c.mu.Unlock()

	c.Net.Rejoin(id)
	c.Net.Register(n.Info, n.Handler)
	if err := n.Membership.Join(nil); err != nil {
		c.TB.Fatalf("testkit: revive %s: %v", id, err)
	}
	return n
}

// Live returns the IDs of nodes that are still running.
func (c *Cluster) Live() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.order))
	for _, id := range c.order {
		if c.nodes[id].alive {
			out = append(out, id)
		}
	}
	return out
}

// Close tears the cluster down and asserts the invariants a harness is
// uniquely placed to check. It is registered with t.Cleanup and is idempotent.
func (c *Cluster) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.mu.Unlock()

	for _, n := range nodes {
		n.Membership.Close()
		_ = n.Transport.Close()
	}
	_ = c.Net.Close()

	for _, msg := range c.Clock.Misuses() {
		c.TB.Errorf("clock misuse: %s", msg)
	}
	if pending := c.Clock.Pending(); pending != 0 {
		c.TB.Errorf("timer leak: %d timer(s) still armed after teardown", pending)
	}
	goleak.VerifyNone(c.TB, c.leakOpts...)
}

// EchoHandler returns a handler that echoes the request back, prefixed with the
// responding node's ID so a caller can tell who answered.
func EchoHandler(self core.Node) core.Handler {
	return core.HandlerFunc(func(_ context.Context, _ core.Node, method string, req []byte) ([]byte, error) {
		out := make([]byte, 0, len(self.ID)+len(method)+len(req)+2)
		out = append(out, self.ID...)
		out = append(out, '|')
		out = append(out, method...)
		out = append(out, '|')
		return append(out, req...), nil
	})
}

// RecordingHandler wraps h and appends every request it sees to a slice that
// the test can inspect. It is safe for concurrent use.
type RecordingHandler struct {
	inner core.Handler

	mu       sync.Mutex
	requests []RecordedRequest
}

// RecordedRequest is one observed call.
type RecordedRequest struct {
	// From is the calling node.
	From core.Node
	// Method is the invoked method name.
	Method string
	// Body is a copy of the request bytes, safe to retain.
	Body []byte
}

// NewRecordingHandler wraps h.
func NewRecordingHandler(h core.Handler) *RecordingHandler {
	return &RecordingHandler{inner: h}
}

// Handle implements [core.Handler].
func (r *RecordingHandler) Handle(ctx context.Context, from core.Node, method string, req []byte) ([]byte, error) {
	r.mu.Lock()
	r.requests = append(r.requests, RecordedRequest{
		From:   from.Clone(),
		Method: method,
		Body:   append([]byte(nil), req...),
	})
	r.mu.Unlock()
	return r.inner.Handle(ctx, from, method, req)
}

// Requests returns a copy of everything seen so far, in arrival order.
func (r *RecordingHandler) Requests() []RecordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]RecordedRequest(nil), r.requests...)
}

// Methods returns just the method names, in arrival order. Use it to assert on
// delivery order under reordering faults.
func (r *RecordingHandler) Methods() []string {
	reqs := r.Requests()
	out := make([]string, len(reqs))
	for i, req := range reqs {
		out[i] = req.Method
	}
	return out
}

var _ core.Handler = (*RecordingHandler)(nil)
