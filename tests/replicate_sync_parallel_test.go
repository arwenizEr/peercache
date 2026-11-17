package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// copyGate holds one node's replica copies to one peer until opened.
type copyGate struct {
	core.Transport
	mu   sync.Mutex
	to   string
	held chan struct{}
}

func (g *copyGate) hold(to string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.to, g.held = to, make(chan struct{})
}

func (g *copyGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held != nil {
		close(g.held)
		g.to, g.held = "", nil
	}
}

func (g *copyGate) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := g.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &copyGateClient{PeerClient: inner, to: node.ID, g: g}, nil
}

// Ready forwards the wrapped transport's readiness; see countingTransport.
func (g *copyGate) Ready() <-chan struct{} {
	if r, ok := g.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

type copyGateClient struct {
	core.PeerClient
	to string
	g  *copyGate
}

func (c *copyGateClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method == transport.MethodReplicate {
		c.g.mu.Lock()
		held := c.g.held
		if c.g.to != c.to {
			held = nil
		}
		c.g.mu.Unlock()
		if held != nil {
			select {
			case <-held:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

// TestASyncWriteSendsItsCopiesAtOnce: a synchronous write at replication
// factor 3 sends both copies at once, so its second replica holds the value
// while the first has not answered. One after another, the second waited for
// the first: two round trips per write, and a replica that did not answer held
// the other copy for its whole timeout.
func TestASyncWriteSendsItsCopiesAtOnce(t *testing.T) {
	var gate *copyGate
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, ownership.DefaultActivationDelay,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			if id == "n0" {
				gate = &copyGate{Transport: tr}
				return gate
			}
			return tr
		}, nil))
	ns := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("sync3",
			peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		ns(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	var key string
	for i := 0; key == "" && i < 10_000; i++ {
		if k := fmt.Sprintf("sync/%d", i); f.ownerOf(t, k) == "n0" {
			key = k
		}
	}
	p, err := f.caches["n0"].DescribeKey("sync3", key)
	if err != nil || len(p.Holders) != 3 || p.Holders[0] != "n0" {
		t.Fatalf("DescribeKey = %+v, %v; want n0 then two replicas", p, err)
	}
	// Each replica held in turn: whichever a fan-out tries first, the other
	// must not wait for it. The fake clock does not move, so a held copy
	// never times out.
	// Registered before the fabric's own cleanup runs, so a failure here
	// lets the held copy through rather than leaving Close waiting on it.
	t.Cleanup(gate.open)
	for i, held := range []string{p.Holders[1], p.Holders[2]} {
		other := p.Holders[2-i]
		gate.hold(held)
		done := make(chan error, 1)
		go func() { done <- ns("n0").Set(context.Background(), key, []byte(held), 0) }()

		// The value written is the held node's name, so each round's copy
		// is told apart from the last one's.
		waitFor(t, fmt.Sprintf("%s to hold the copy while %s is held", other, held), func() bool {
			for _, e := range catchUpPage(t, f, "n0", other, "sync3", 3, true) {
				if e.Key == key {
					return string(e.Value) == held
				}
			}
			return false
		})
		gate.open()
		if err := <-done; err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
}
