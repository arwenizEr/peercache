package transport_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/transport"
)

// These tests bind real loopback sockets and speak real gRPC.
//
// That is the point of them. internal/testkit already provides an in-process
// transport with fault injection, and every milestone up to M3 was tested
// against it deterministically. What the fake cannot tell us is whether gRPC
// propagates a deadline the way we assume, whether a sentinel error survives a
// status round trip, or whether killing a server mid-request produces an error
// rather than a hang. Those are properties of the real library, and only the
// real library can be asked.
//
// They use real time for the same reason internal/membership does: gRPC's
// timers are its own and no injected clock reaches them. Every wait is bounded
// and nothing sleeps.

// budget bounds every wait. Loopback gRPC round-trips in microseconds; anything
// near this is a failure, not slowness.
const budget = 30 * time.Second

// peer is one node's transport plus the listener it is serving on.
type peer struct {
	node    core.Node
	tr      *transport.Transport
	served  chan error
	handler *recordingHandler
}

// recordingHandler answers requests and records what it saw, so that tests can
// assert on the server's view rather than inferring it from the client's.
type recordingHandler struct {
	mu       sync.Mutex
	calls    int
	lastFrom core.Node
	lastMeth string
	deadline time.Time
	hasDL    bool

	// respond, if set, replaces the default echo. It runs on the server
	// goroutine with the request's context.
	respond func(ctx context.Context, method string, req []byte) ([]byte, error)
}

func (h *recordingHandler) Handle(ctx context.Context, from core.Node, method string, req []byte) ([]byte, error) {
	h.mu.Lock()
	h.calls++
	h.lastFrom = from
	h.lastMeth = method
	h.deadline, h.hasDL = ctx.Deadline()
	respond := h.respond
	h.mu.Unlock()

	if respond != nil {
		return respond(ctx, method, req)
	}
	// Default: echo, so a round trip proves the payload survived intact.
	out := make([]byte, len(req))
	copy(out, req)
	return out, nil
}

func (h *recordingHandler) snapshot() (calls int, from core.Node, method string, deadline time.Time, hasDL bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls, h.lastFrom, h.lastMeth, h.deadline, h.hasDL
}

// newPeer starts a transport serving on an ephemeral loopback port.
func newPeer(t *testing.T, id string, tlsCfg *transport.TLSConfig) *peer {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	node := core.Node{ID: id, Addr: l.Addr().String()}

	tr, err := transport.New(transport.Options{Self: node, TLS: tlsCfg})
	if err != nil {
		t.Fatalf("transport.New(%s): %v", id, err)
	}

	h := &recordingHandler{}
	p := &peer{node: node, tr: tr, served: make(chan error, 1), handler: h}
	go func() { p.served <- tr.Serve(l, h) }()

	// Wait until the peer is actually accepting before handing it to the
	// test. Otherwise Close can win the race to the mutex and Serve returns
	// ErrClosed for a transport the test genuinely asked to serve.
	select {
	case <-tr.Ready():
	case err := <-p.served:
		t.Fatalf("Serve(%s) returned before becoming ready: %v", id, err)
	case <-time.After(budget):
		t.Fatalf("Serve(%s) was not ready within %v", id, budget)
	}

	t.Cleanup(func() {
		if err := tr.Close(); err != nil {
			t.Errorf("Close(%s): %v", id, err)
		}
		select {
		case err := <-p.served:
			if err != nil {
				t.Errorf("Serve(%s) returned %v; a closed transport must return nil", id, err)
			}
		case <-time.After(budget):
			t.Errorf("Serve(%s) did not return within %v of Close", id, budget)
		}
	})
	return p
}

// dial connects from one peer to another.
func dial(t *testing.T, from *peer, to *peer) core.PeerClient {
	t.Helper()
	c, err := from.tr.Dial(to.node)
	if err != nil {
		t.Fatalf("Dial %s -> %s: %v", from.node.ID, to.node.ID, err)
	}
	return c
}

func TestRoundTripOverLoopback(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	payload := []byte("the quick brown fox")
	got, err := dial(t, a, b).Invoke(ctx, transport.MethodGet, payload)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Invoke returned %q, want %q; the payload must cross unchanged", got, payload)
	}

	calls, from, method, _, _ := b.handler.snapshot()
	if calls != 1 {
		t.Fatalf("handler saw %d calls, want 1", calls)
	}
	if from.ID != "node-a" {
		t.Fatalf("handler saw caller %q, want %q; ownership is derived from the node ID, so "+
			"the receiver must be told the name rather than infer it from the connection", from.ID, "node-a")
	}
	if method != transport.MethodGet {
		t.Fatalf("handler saw method %q, want %q", method, transport.MethodGet)
	}
}

// TestEmptyPayloadRoundTrips pins the boundary the codec has to get right: a
// zero-length body is a legitimate message, not an absent one.
func TestEmptyPayloadRoundTrips(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	got, err := dial(t, a, b).Invoke(ctx, transport.MethodDelete, nil)
	if err != nil {
		t.Fatalf("Invoke with an empty payload: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Invoke returned %d bytes for an empty request, want 0", len(got))
	}
	if calls, _, _, _, _ := b.handler.snapshot(); calls != 1 {
		t.Fatalf("handler saw %d calls, want 1; an empty body must still reach the handler", calls)
	}
}

// TestLargePayloadRoundTrips checks the codec against a body big enough to
// arrive as several gRPC buffers, which is where an aliasing bug would show.
func TestLargePayloadRoundTrips(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	got, err := dial(t, a, b).Invoke(ctx, transport.MethodBatchGet, payload)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("got %d bytes back, want %d", len(got), len(payload))
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("byte %d is %d, want %d; a multi-buffer body was reassembled wrongly",
				i, got[i], payload[i])
		}
	}
}

// TestDeadlinePropagatesToTheHandler is the M4 requirement that the caller's
// budget reaches the remote side. CLAUDE.md requires context deadlines to
// propagate across RPC boundaries; this is the evidence.
func TestDeadlinePropagatesToTheHandler(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	const callBudget = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), callBudget)
	defer cancel()
	before := time.Now()

	if _, err := dial(t, a, b).Invoke(ctx, transport.MethodCall, []byte("x")); err != nil {
		t.Fatalf("Invoke: %v", err)
	}

	_, _, _, deadline, hasDL := b.handler.snapshot()
	if !hasDL {
		t.Fatal("the handler's context had no deadline; the caller's budget did not cross the " +
			"hop, so a slow owner would consume the caller's time without knowing it had any")
	}
	// gRPC transmits the remaining duration, not the absolute instant, so
	// the reconstructed deadline is close to ours rather than identical.
	// The assertion is that it is in the right neighbourhood: comfortably
	// in the future, and not beyond what we granted.
	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatalf("the handler's deadline had already passed (%v ago)", -remaining)
	}
	if elapsed := time.Since(before); remaining > callBudget-elapsed+time.Second {
		t.Fatalf("the handler's deadline is %v away but only %v was granted; a hop must never "+
			"extend the caller's budget", remaining, callBudget)
	}
}

// TestInvokeReturnsWhenTheContextExpires is the "never a hang" half of the M4
// acceptance criterion, in its simplest form: the server is alive and simply
// never answers.
func TestInvokeReturnsWhenTheContextExpires(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	// Block until the request's own context is done, which is what a
	// wedged owner looks like from outside.
	b.handler.respond = func(ctx context.Context, _ string, _ []byte) ([]byte, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := dial(t, a, b).Invoke(ctx, transport.MethodCall, []byte("x"))
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Invoke returned %v, want a deadline error; a call that outlives its "+
				"context consumes the caller's budget silently", err)
		}
	case <-time.After(budget):
		t.Fatalf("Invoke did not return within %v of a 200ms deadline; it hung", budget)
	}
}

// TestKillingTheTransportMidRequestSurfacesAnError is the M4 acceptance
// criterion stated in SPEC.md §4: "killing the transport mid-request surfaces a
// deadline error, never a hang."
//
// The request is confirmed to be in flight before the server is killed, so this
// tests the mid-request case rather than racing the kill against the dial.
func TestKillingTheTransportMidRequestSurfacesAnError(t *testing.T) {
	a := newPeer(t, "node-a", nil)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	victim := core.Node{ID: "node-victim", Addr: l.Addr().String()}
	vt, err := transport.New(transport.Options{Self: victim})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}

	inFlight := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h := &recordingHandler{respond: func(ctx context.Context, _ string, _ []byte) ([]byte, error) {
		once.Do(func() { close(inFlight) })
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, ctx.Err()
	}}
	served := make(chan error, 1)
	go func() { served <- vt.Serve(l, h) }()
	defer close(release)
	select {
	case <-vt.Ready():
	case <-time.After(budget):
		t.Fatalf("the victim transport was not ready within %v", budget)
	}

	// A generous deadline on purpose. If this test passes because the
	// context expired, it has proved nothing about killing the transport.
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := dial(t, a, &peer{node: victim, tr: vt}).Invoke(ctx, transport.MethodCall, []byte("x"))
		done <- err
	}()

	select {
	case <-inFlight:
	case <-time.After(budget):
		t.Fatalf("the request never reached the handler within %v", budget)
	}

	if err := vt.Close(); err != nil {
		t.Fatalf("closing the victim transport: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Invoke succeeded after its server was killed mid-request")
		}
		// The contract is an error rather than a hang. Which error
		// depends on whether the RST beat the deadline, and both are
		// acceptable; what is not acceptable is blocking forever.
		if !errors.Is(err, core.ErrNodeUnreachable) && !errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, context.Canceled) {
			t.Fatalf("Invoke returned %v; want ErrNodeUnreachable or a context error", err)
		}
	case <-time.After(budget):
		t.Fatalf("Invoke did not return within %v of its server being killed; it hung, which "+
			"is the failure this criterion exists to prevent", budget)
	}

	select {
	case <-served:
	case <-time.After(budget):
		t.Error("Serve did not return after Close")
	}
}

// TestSentinelErrorsSurviveTheHop is what makes a remote call feel local.
//
// A caller writes errors.Is(err, ErrNotOwner) without caring whether the answer
// came from this process or another one. That only works if the sentinel is
// reconstructed on the near side, because an error crossing gRPC arrives as a
// status code and a string.
func TestSentinelErrorsSurviveTheHop(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	cases := []struct {
		name string
		err  error
	}{
		{"not found", core.ErrNotFound},
		{"not owner", core.ErrNotOwner},
		{"stale epoch", core.ErrStaleEpoch},
		{"activation pending", core.ErrActivationPending},
		{"group overloaded", core.ErrGroupOverloaded},
		{"func not registered", core.ErrFuncNotRegistered},
		{"invalid option", core.ErrInvalidOption},
		{"under replicated", core.ErrUnderReplicated},
		{"codec", core.ErrCodec},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b.handler.respond = func(context.Context, string, []byte) ([]byte, error) {
				return nil, fmt.Errorf("remote said no: %w", tc.err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()

			_, err := dial(t, a, b).Invoke(ctx, transport.MethodGet, []byte("k"))
			if !errors.Is(err, tc.err) {
				t.Fatalf("Invoke returned %v; want it to wrap %v so that errors.Is works "+
					"identically for a local and a remote answer", err, tc.err)
			}
		})
	}
}

// TestUnavailableDefaultsToUnreachable pins the one ambiguous status mapping.
//
// gRPC raises Unavailable for a broken connection, and the handler raises it for
// a deferred activation. They call for opposite retries: elsewhere versus here.
// Defaulting to "pending" would pin a caller to a node that is never coming
// back, so the default is unreachable and only an explicit pending message
// overrides it.
func TestUnavailableDefaultsToUnreachable(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	b.handler.respond = func(context.Context, string, []byte) ([]byte, error) {
		return nil, fmt.Errorf("shutting down: %w", core.ErrClosed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	_, err := dial(t, a, b).Invoke(ctx, transport.MethodGet, []byte("k"))
	if !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("a remote ErrClosed came back as %v; want ErrNodeUnreachable, so the caller "+
			"goes elsewhere rather than retrying a node that is shutting down", err)
	}
	if errors.Is(err, core.ErrActivationPending) {
		t.Fatal("a shutting-down peer was reported as a pending activation; the caller would " +
			"retry a node that is never coming back")
	}
}

// TestDialPoolsConnections asserts the pool, which is what stops a per-request
// handshake. It is checked by identity rather than by counting sockets: two
// Dials for one node must hand back the same client.
func TestDialPoolsConnections(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	first := dial(t, a, b)
	second := dial(t, a, b)
	if first != second {
		t.Fatal("two Dials for the same node returned different clients; every request would " +
			"pay for a new connection")
	}

	// Closing a pooled client must evict it, or a later Dial would hand
	// out a closed connection.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	third := dial(t, a, b)
	if third == first {
		t.Fatal("Dial returned a client that had already been closed; the pool did not evict it")
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if _, err := third.Invoke(ctx, transport.MethodGet, []byte("k")); err != nil {
		t.Fatalf("the replacement client does not work: %v", err)
	}
}

// TestDialReplacesANodeThatMoved covers a pod restarting at a new address under
// the same ID, which is ordinary Kubernetes behaviour. Reusing the pooled
// connection would leave the caller talking to an address nobody is on.
func TestDialReplacesANodeThatMoved(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	old := dial(t, a, b)

	moved := core.Node{ID: b.node.ID, Addr: "127.0.0.1:1"}
	replacement, err := a.tr.Dial(moved)
	if err != nil {
		t.Fatalf("Dial after a move: %v", err)
	}
	if replacement == old {
		t.Fatal("Dial reused the connection to the old address for a node that moved")
	}
}

func TestDialValidates(t *testing.T) {
	a := newPeer(t, "node-a", nil)

	if _, err := a.tr.Dial(core.Node{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Dial(zero node) = %v, want ErrInvalidOption", err)
	}
	if _, err := a.tr.Dial(core.Node{ID: "b"}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Dial(node with no address) = %v, want ErrInvalidOption", err)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := transport.New(transport.Options{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("New with no Self = %v, want ErrInvalidOption", err)
	}
	if _, err := transport.New(transport.Options{
		Self:            core.Node{ID: "a"},
		MaxMessageBytes: -1,
	}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("New with a negative message cap = %v, want ErrInvalidOption", err)
	}
}

func TestServeValidates(t *testing.T) {
	tr, err := transport.New(transport.Options{Self: core.Node{ID: "a"}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	if err := tr.Serve(l, nil); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Serve with a nil handler = %v, want ErrInvalidOption", err)
	}
	if err := tr.Serve(nil, &recordingHandler{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Serve with a nil listener = %v, want ErrInvalidOption", err)
	}
}

// TestCloseIsIdempotentAndReleasesGoroutines guards the rule that cost M3 a
// milestone: a Close that does not close is a leak that no assertion in the
// package can see.
func TestCloseIsIdempotentAndReleasesGoroutines(t *testing.T) {
	settle()
	before := runtime.NumGoroutine()

	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		node := core.Node{ID: fmt.Sprintf("leak-%d", i), Addr: l.Addr().String()}
		tr, err := transport.New(transport.Options{Self: node})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		served := make(chan error, 1)
		go func() { served <- tr.Serve(l, &recordingHandler{}) }()
		select {
		case <-tr.Ready():
		case <-time.After(budget):
			t.Fatal("Serve was not ready in time")
		}

		// Dial ourselves so the pool has something in it to release.
		if _, err := tr.Dial(node); err != nil {
			t.Fatalf("Dial: %v", err)
		}

		if err := tr.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := tr.Close(); err != nil {
			t.Fatalf("second Close: %v; Close must be idempotent", err)
		}
		select {
		case err := <-served:
			if err != nil {
				t.Fatalf("Serve returned %v after Close, want nil", err)
			}
		case <-time.After(budget):
			t.Fatal("Serve did not return after Close")
		}
	}

	deadline := time.Now().Add(budget)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not return to baseline: %d now, %d before",
				runtime.NumGoroutine(), before)
		}
		runtime.Gosched()
	}
}

// TestDialAfterCloseFails stops a caller getting a client that can never work.
func TestDialAfterCloseFails(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	node := core.Node{ID: "a", Addr: l.Addr().String()}
	tr, err := transport.New(transport.Options{Self: node})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	go func() { _ = tr.Serve(l, &recordingHandler{}) }()
	select {
	case <-tr.Ready():
	case <-time.After(budget):
		t.Fatal("Serve was not ready in time")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := tr.Dial(node); !errors.Is(err, core.ErrClosed) {
		t.Fatalf("Dial after Close = %v, want ErrClosed", err)
	}
}

// TestConcurrentDialsShareOneConnection exercises the pool's double-check.
// Without it, N goroutines racing on the first Dial would each build a
// connection and N-1 would be orphaned.
func TestConcurrentDialsShareOneConnection(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)

	const callers = 32
	clients := make([]core.PeerClient, callers)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := a.tr.Dial(b.node)
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			clients[i] = c
		}()
	}
	wg.Wait()

	for i, c := range clients {
		if c != clients[0] {
			t.Fatalf("caller %d got a different client; concurrent Dials must collapse onto "+
				"one pooled connection", i)
		}
	}
}

// TestProtoMatchesMethodConstants keeps proto/peercache.proto honest.
//
// The .proto is normative documentation rather than the source of generated
// code, which means nothing mechanical stops it drifting from the constants the
// server actually registers. This is that mechanism. An rpc added to one and
// not the other fails here rather than becoming a peer that answers "unknown
// method" in production.
func TestProtoMatchesMethodConstants(t *testing.T) {
	body, err := os.ReadFile("../proto/peercache.proto")
	if err != nil {
		t.Fatalf("reading the proto: %v", err)
	}

	declared := make(map[string]bool)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "rpc ") {
			continue
		}
		rest := strings.TrimPrefix(line, "rpc ")
		name, _, ok := strings.Cut(rest, "(")
		if !ok {
			t.Fatalf("cannot parse rpc line: %q", line)
		}
		declared[strings.TrimSpace(name)] = true
	}
	if len(declared) == 0 {
		t.Fatal("no rpc declarations found in the proto; the parser or the file is wrong")
	}

	registered := make(map[string]bool, len(transport.Methods))
	for _, full := range transport.Methods {
		idx := strings.LastIndex(full, "/")
		if idx < 0 {
			t.Fatalf("method %q is not a full gRPC path", full)
		}
		registered[full[idx+1:]] = true
		if want := "/" + transport.ServiceName + "/"; !strings.HasPrefix(full, want) {
			t.Errorf("method %q does not start with %q", full, want)
		}
	}

	for name := range declared {
		if !registered[name] {
			t.Errorf("proto declares rpc %s but internal/transport does not serve it", name)
		}
	}
	for name := range registered {
		if !declared[name] {
			t.Errorf("internal/transport serves %s but the proto does not declare it", name)
		}
	}
}

// settle yields until the goroutine count stops moving, so a baseline is taken
// at rest rather than mid-teardown.
func settle() {
	stable, last := 0, -1
	for stable < 1000 {
		if n := runtime.NumGoroutine(); n == last {
			stable++
		} else {
			stable, last = 0, n
		}
		runtime.Gosched()
	}
}

// TestAnApplicationErrorIsNotReportedAsUnreachable: a handler that fails for a
// reason of its own — a loader whose database is down — must not reach the
// caller as ErrNodeUnreachable. The peer answered. A caller told it did not
// would retry elsewhere, or alert on a network that is fine.
func TestAnApplicationErrorIsNotReportedAsUnreachable(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)
	b.handler.respond = func(context.Context, string, []byte) ([]byte, error) {
		return nil, errors.New("backend unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	_, err := dial(t, a, b).Invoke(ctx, transport.MethodGet, []byte("k"))
	if err == nil {
		t.Fatal("the handler failed and Invoke returned nil")
	}
	if errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("a handler's own failure came back as %v, which wraps ErrNodeUnreachable: "+
			"the peer answered", err)
	}
	if !strings.Contains(err.Error(), "backend unavailable") {
		t.Fatalf("the handler's message was lost: %v", err)
	}
}
