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
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// How invalidation hints travel: batched under pressure, to every peer at
// once, and not at all for a namespace no peer can cache.

// hintGate wraps one node's transport: it records every invalidation the node
// sends, by destination, and can hold those to chosen peers until opened.
type hintGate struct {
	mu   sync.Mutex
	held map[string]chan struct{} // destination -> closed when opened
	sent []sentHint
}

type sentHint struct {
	to  string
	req wire.InvalidateRequest
}

func (g *hintGate) hold(ids ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.held == nil {
		g.held = map[string]chan struct{}{}
	}
	for _, id := range ids {
		g.held[id] = make(chan struct{})
	}
}

func (g *hintGate) open() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for id, ch := range g.held {
		close(ch)
		delete(g.held, id)
	}
}

// messages returns what was sent so far.
func (g *hintGate) messages() []sentHint {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]sentHint(nil), g.sent...)
}

func (g *hintGate) wrap(tr core.Transport) core.Transport {
	return &gatedTransport{Transport: tr, g: g}
}

type gatedTransport struct {
	core.Transport
	g *hintGate
}

func (t *gatedTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &gatedClient{PeerClient: inner, to: node.ID, g: t.g}, nil
}

// Ready forwards the wrapped transport's readiness; see countingTransport.
func (t *gatedTransport) Ready() <-chan struct{} {
	if r, ok := t.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

type gatedClient struct {
	core.PeerClient
	to string
	g  *hintGate
}

func (c *gatedClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method != transport.MethodInvalidate {
		return c.PeerClient.Invoke(ctx, method, req)
	}
	var in wire.InvalidateRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	c.g.mu.Lock()
	c.g.sent = append(c.g.sent, sentHint{to: c.to, req: in})
	held := c.g.held[c.to]
	c.g.mu.Unlock()
	if held != nil {
		select {
		case <-held:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

// hintFabric is three nodes whose first, the writer, sends through a gate.
func hintFabric(t *testing.T) (*fabric, *hintGate) {
	g := &hintGate{}
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, ownership.DefaultActivationDelay,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			if id == "n0" {
				return g.wrap(tr)
			}
			return tr
		}, nil))
	return f, g
}

// ownedKeys returns n keys named prefix/i that id owns.
func ownedKeys(t *testing.T, f *fabric, id, prefix string, n int) []string {
	t.Helper()
	var out []string
	for i := 0; len(out) < n && i < 100_000; i++ {
		if k := fmt.Sprintf("%s/%d", prefix, i); f.ownerOf(t, k) == id {
			out = append(out, k)
		}
	}
	if len(out) < n {
		t.Fatalf("found %d keys owned by %s, want %d", len(out), id, n)
	}
	return out
}

// primeCopies writes v0 to every key on the writer and reads each from the
// readers, so each holds an L1 copy that only an invalidation can replace
// before its TTL: the fake clock does not move.
func primeCopies(t *testing.T, f *fabric, ns func(string) peercache.Namespace, keys []string, readers ...string) {
	t.Helper()
	ctx := context.Background()
	for _, k := range keys {
		if err := ns("n0").Set(ctx, k, []byte("v0"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	for _, id := range readers {
		for _, k := range keys {
			if got, err := ns(id).Get(ctx, k); err != nil || string(got) != "v0" {
				t.Fatalf("%s read %s = %q, %v; want v0", id, k, got, err)
			}
		}
	}
}

// readsEventually waits until reader sees want for every key.
func readsEventually(t *testing.T, ns func(string) peercache.Namespace, reader string, keys []string, want string) {
	t.Helper()
	waitFor(t, reader+" to read "+want+" for every key", func() bool {
		for _, k := range keys {
			got, err := ns(reader).Get(context.Background(), k)
			if err != nil || string(got) != want {
				return false
			}
		}
		return true
	})
}

// hintsAbove counts the messages to peer that carry a hint of a version
// above v: the second writes of the keys, not the priming ones.
func hintsAbove(msgs []sentHint, peer string, v uint64) int {
	n := 0
	for _, m := range msgs {
		if m.to != peer {
			continue
		}
		carries := m.req.Stamp.Version > v && len(m.req.Keys) > 0
		for _, e := range m.req.Entries {
			carries = carries || e.Stamp.Version > v
		}
		if carries {
			n++
		}
	}
	return n
}

// TestHintsQueuedTogetherShareMessages: hints that queue while a round is on
// the wire leave in the next round together, one message per namespace and
// peer, rather than one round trip per hint per peer.
func TestHintsQueuedTogetherShareMessages(t *testing.T) {
	f, g := hintFabric(t)
	ns := func(id string) peercache.Namespace { return f.caches[id].Namespace("hints") }
	for _, id := range f.ids {
		ns(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	const keys = 50
	ks := ownedKeys(t, f, "n0", "batch", keys)
	primeCopies(t, f, ns, ks, "n1", "n2")

	// The first round's messages wait at the gate; every write after it
	// queues behind them.
	g.hold("n1", "n2")
	ctx := context.Background()
	for _, k := range ks {
		if err := ns("n0").Set(ctx, k, []byte("v1"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	g.open()
	readsEventually(t, ns, "n1", ks, "v1")
	readsEventually(t, ns, "n2", ks, "v1")

	msgs := g.messages()
	for _, peer := range []string{"n1", "n2"} {
		if n := hintsAbove(msgs, peer, 1); n == 0 || n > keys/5 {
			t.Fatalf("%d hints reached %s in %d messages; queued together they share a few", keys, peer, n)
		}
	}
}

// TestAHeldPeerDoesNotHoldTheOthersHints: a round goes to every peer at once,
// so a peer that does not answer delays nobody else's hint. Sent one peer
// after another, the peers after a silent one waited out its timeout first.
func TestAHeldPeerDoesNotHoldTheOthersHints(t *testing.T) {
	f, g := hintFabric(t)
	ns := func(id string) peercache.Namespace { return f.caches[id].Namespace("hints") }
	for _, id := range f.ids {
		ns(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	ks := ownedKeys(t, f, "n0", "held", 1)
	primeCopies(t, f, ns, ks, "n2")

	// n1 is first among the peers; the fake clock does not move, so its
	// held message never times out.
	g.hold("n1")
	defer g.open()
	if err := ns("n0").Set(context.Background(), ks[0], []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	readsEventually(t, ns, "n2", ks, "v1")
}

// TestANamespaceWithoutL1SendsNoHints: with a negative L1 TTL no node holds
// a non-owned copy, so a write there tells no peer anything.
func TestANamespaceWithoutL1SendsNoHints(t *testing.T) {
	f, g := hintFabric(t)
	bare := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("bare", peercache.WithL1TTL(-1))
	}
	cached := func(id string) peercache.Namespace { return f.caches[id].Namespace("cached") }
	for _, id := range f.ids {
		bare(id)
		cached(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	ctx := context.Background()
	for _, k := range ownedKeys(t, f, "n0", "bare", 10) {
		if err := bare("n0").Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	// Hints leave in order, so once one written after these has arrived,
	// any of theirs would have too.
	ks := ownedKeys(t, f, "n0", "cached", 1)
	primeCopies(t, f, cached, ks, "n1")
	if err := cached("n0").Set(ctx, ks[0], []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	readsEventually(t, cached, "n1", ks, "v1")

	for _, m := range g.messages() {
		if m.req.Namespace == "bare" {
			t.Fatalf("a write to a namespace without L1 sent %s a hint: %+v", m.to, m.req)
		}
	}
}
