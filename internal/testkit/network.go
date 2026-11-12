package testkit

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
)

// Wildcard matches any node in a fault rule, so that a rule can be set for one
// direction, one sender, one receiver, or globally.
const Wildcard = ""

// Network is an in-process message fabric with fault injection.
//
// It implements the request/response shape of [core.Transport] without sockets,
// serialization or a scheduler of its own. Every delay is measured on the
// injected fake clock, so a test controls exactly when a delayed message is
// delivered.
//
// A Network is safe for concurrent use.
type Network struct {
	clk *clock.Fake

	mu        sync.Mutex
	rng       *rand.Rand
	endpoints map[string]*endpoint
	// partition maps node ID to a partition index. A nil map means the
	// network is whole. A node missing from a non-nil map is isolated,
	// which is what makes Partition safe to call with an incomplete list.
	partition map[string]int
	isolated  map[string]bool
	dropRate  map[edge]float64
	delays    map[edge]time.Duration
	reorder   time.Duration
	pending   map[*delivery]struct{}
	watchers  []func()
	closed    bool

	closedCh chan struct{}
	// handlers counts in-flight handler goroutines so that Close can wait
	// for them instead of leaving them for goleak to find.
	handlers sync.WaitGroup
}

// edge names a directed pair of nodes in a fault rule. Either end may be
// [Wildcard].
type edge struct{ from, to string }

// endpoint is one registered node's server side.
type endpoint struct {
	node    core.Node
	handler core.Handler
}

// delivery is a message waiting on the clock before its handler runs.
type delivery struct {
	timer  clock.Timer
	cancel func()
}

// NewNetwork returns a fabric whose delays are measured on clk and whose
// probabilistic faults are drawn from seed.
func NewNetwork(clk *clock.Fake, seed int64) *Network {
	return &Network{
		clk:       clk,
		rng:       rand.New(rand.NewSource(seed)),
		endpoints: make(map[string]*endpoint),
		isolated:  make(map[string]bool),
		dropRate:  make(map[edge]float64),
		delays:    make(map[edge]time.Duration),
		pending:   make(map[*delivery]struct{}),
		closedCh:  make(chan struct{}),
	}
}

// Register attaches h as node's server side, replacing any previous handler.
func (n *Network) Register(node core.Node, h core.Handler) {
	n.mu.Lock()
	n.endpoints[node.ID] = &endpoint{node: node.Clone(), handler: h}
	n.mu.Unlock()
	n.notify()
}

// Unregister detaches a node's server side. Traffic to it then fails as
// unreachable, which is what a process disappearing looks like from outside.
func (n *Network) Unregister(id string) {
	n.mu.Lock()
	delete(n.endpoints, id)
	n.mu.Unlock()
	n.notify()
}

// Nodes returns the registered node identities, ordered by ID.
func (n *Network) Nodes() []core.Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]core.Node, 0, len(n.endpoints))
	for _, ep := range n.endpoints {
		out = append(out, ep.node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OnChange registers fn to be called whenever reachability may have changed:
// a registration, a partition, a heal, an isolation.
//
// fn is invoked with no network lock held, so it may call back into the
// Network. Registrations cannot be removed; a component that has shut down must
// make its own callback a no-op.
func (n *Network) OnChange(fn func()) {
	n.mu.Lock()
	n.watchers = append(n.watchers, fn)
	n.mu.Unlock()
}

// notify runs the change watchers. It must be called without holding mu: the
// watchers call back into Reachable, and holding the lock across them would
// invert the documented Membership-then-Network ordering.
func (n *Network) notify() {
	n.mu.Lock()
	watchers := make([]func(), len(n.watchers))
	copy(watchers, n.watchers)
	n.mu.Unlock()
	for _, fn := range watchers {
		fn()
	}
}

// Partition splits the network into groups that cannot talk to each other.
// Nodes inside a group can still reach one another.
//
// A node omitted from every group is isolated rather than silently left
// reachable, so a typo in a partition spec fails loudly instead of producing a
// test that proves nothing.
func (n *Network) Partition(groups ...[]string) {
	p := make(map[string]int)
	for i, g := range groups {
		for _, id := range g {
			p[id] = i
		}
	}
	n.mu.Lock()
	n.partition = p
	n.mu.Unlock()
	n.notify()
}

// Heal removes any partition. Isolated nodes stay isolated; [Network.Rejoin]
// undoes that separately, because healing a partition and reviving a dead node
// are different events and conflating them hides bugs in the recovery path.
func (n *Network) Heal() {
	n.mu.Lock()
	n.partition = nil
	n.mu.Unlock()
	n.notify()
}

// Isolate cuts a node off in both directions without removing its handler. This
// is a network failure, as distinct from a process failure.
func (n *Network) Isolate(id string) {
	n.mu.Lock()
	n.isolated[id] = true
	n.mu.Unlock()
	n.notify()
}

// Rejoin reverses [Network.Isolate].
func (n *Network) Rejoin(id string) {
	n.mu.Lock()
	delete(n.isolated, id)
	n.mu.Unlock()
	n.notify()
}

// SetDropRate makes the given fraction of messages from -> to vanish. Either
// end may be [Wildcard]. A rate of 1 drops everything.
//
// A rate of 0 is stored, not forgotten, so that it overrides a broader
// wildcard rule: SetDropRate(Wildcard, Wildcard, 1) followed by
// SetDropRate("n0", "n1", 0) means "lose everything except that one edge".
// Pass a negative rate to remove a rule entirely and fall back to the
// wildcards.
//
// A dropped message does not produce an error. The caller waits until its
// context expires, because that is what a lost packet looks like from above and
// it is the only way to prove that a caller's correctness does not depend on a
// reply arriving. See docs/decisions/0004-dropped-messages-look-like-silence.md.
func (n *Network) SetDropRate(from, to string, rate float64) {
	n.mu.Lock()
	if rate < 0 {
		delete(n.dropRate, edge{from, to})
	} else {
		n.dropRate[edge{from, to}] = rate
	}
	n.mu.Unlock()
}

// SetDelay adds latency to messages from -> to, measured on the fake clock.
// Either end may be [Wildcard]. A zero delay is stored so that it overrides a
// broader wildcard rule; pass a negative duration to remove a rule.
//
// A delayed message is not delivered until the test advances the clock past the
// delay. That is deliberate: it makes "this request is still in flight" a state
// the test can hold and inspect.
func (n *Network) SetDelay(from, to string, d time.Duration) {
	n.mu.Lock()
	if d < 0 {
		delete(n.delays, edge{from, to})
	} else {
		n.delays[edge{from, to}] = d
	}
	n.mu.Unlock()
}

// SetReorderWindow adds a random delay in [0, d) to every message, so that
// messages sent in one order can arrive in another.
//
// Reordering is expressed as jitter because this transport is request/response:
// two concurrent calls have no defined arrival order to begin with, and the
// only meaningful reordering is between messages a test sent deliberately in
// sequence. Since the jitter is a delay, a reordering test must advance the
// clock for anything to be delivered.
func (n *Network) SetReorderWindow(d time.Duration) {
	n.mu.Lock()
	n.reorder = d
	n.mu.Unlock()
}

// Reachable reports whether a message from can currently arrive at to.
func (n *Network) Reachable(from, to string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reachableLocked(from, to)
}

func (n *Network) reachableLocked(from, to string) bool {
	if n.closed {
		return false
	}
	if n.isolated[from] || n.isolated[to] {
		return false
	}
	if _, ok := n.endpoints[to]; !ok {
		return false
	}
	if _, ok := n.endpoints[from]; !ok {
		return false
	}
	if n.partition != nil {
		a, aok := n.partition[from]
		b, bok := n.partition[to]
		if !aok || !bok || a != b {
			return false
		}
	}
	return true
}

// dropRateLocked resolves the drop rule for an edge, most specific first.
func (n *Network) dropRateLocked(from, to string) float64 {
	for _, e := range []edge{{from, to}, {from, Wildcard}, {Wildcard, to}, {Wildcard, Wildcard}} {
		if r, ok := n.dropRate[e]; ok {
			return r
		}
	}
	return 0
}

func (n *Network) delayLocked(from, to string) time.Duration {
	var d time.Duration
	for _, e := range []edge{{from, to}, {from, Wildcard}, {Wildcard, to}, {Wildcard, Wildcard}} {
		if v, ok := n.delays[e]; ok {
			d = v
			break
		}
	}
	if n.reorder > 0 {
		d += time.Duration(n.rng.Int63n(int64(n.reorder)))
	}
	return d
}

func (n *Network) rollDropLocked(from, to string) bool {
	rate := n.dropRateLocked(from, to)
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	return n.rng.Float64() < rate
}

// schedule arranges for run to execute on its own goroutine after d of
// simulated time. It reports false if the network is already closed.
//
// run is never invoked on the goroutine that advances the clock: a handler that
// blocks would otherwise deadlock the advance, and every test in the repository
// would inherit that hazard.
func (n *Network) schedule(d time.Duration, run, cancel func()) bool {
	dl := &delivery{cancel: cancel}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return false
	}
	n.pending[dl] = struct{}{}
	n.mu.Unlock()

	t := n.clk.AfterFunc(d, func() {
		n.mu.Lock()
		_, live := n.pending[dl]
		delete(n.pending, dl)
		n.mu.Unlock()
		if !live {
			// Close got here first and has already run cancel.
			return
		}
		go run()
	})

	// Hand the timer over under the lock. Between the push above and here,
	// either the timer fired (and removed itself from pending) or Close ran
	// (and drained pending without a timer to stop). In the second case the
	// timer is still armed and nobody else holds a reference to it, so it
	// must be stopped here — otherwise it outlives the network, and the
	// harness's own "no timer left armed" check fails intermittently in
	// whichever test happens to close while a delivery is in flight.
	n.mu.Lock()
	_, live := n.pending[dl]
	if live {
		dl.timer = t
	}
	n.mu.Unlock()
	if !live {
		t.Stop()
	}
	return true
}

// Close shuts the fabric down: in-flight calls fail, scheduled deliveries are
// cancelled, and handler goroutines are waited for.
//
// Waiting matters. A harness that leaves handler goroutines running turns every
// subsequent goleak check into a coin flip, and a flaky leak check is quickly
// ignored, which defeats having one.
func (n *Network) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	drop := make([]*delivery, 0, len(n.pending))
	for dl := range n.pending {
		drop = append(drop, dl)
	}
	n.pending = make(map[*delivery]struct{})
	n.mu.Unlock()

	close(n.closedCh)
	for _, dl := range drop {
		if dl.timer != nil {
			dl.timer.Stop()
		}
		if dl.cancel != nil {
			dl.cancel()
		}
	}
	n.handlers.Wait()
	return nil
}

// Transport returns the [core.Transport] view of this network for one node.
func (n *Network) Transport(self core.Node) core.Transport {
	return &netTransport{
		net:   n,
		self:  self.Clone(),
		done:  make(chan struct{}),
		ready: make(chan struct{}),
	}
}

// netTransport is one node's handle on the fabric.
type netTransport struct {
	net  *Network
	self core.Node

	once sync.Once
	done chan struct{}

	// ready is closed once Serve has registered this node's handler.
	//
	// Registration is what makes the node visible to its peers, and Serve
	// runs on its own goroutine, so without this a caller can return from
	// construction and be treated as a cluster member before it can answer
	// anything. The gRPC transport has the same signal for the same reason.
	readyOnce sync.Once
	ready     chan struct{}
}

// Ready is closed when this transport has registered and is answering.
func (t *netTransport) Ready() <-chan struct{} { return t.ready }

// Dial implements [core.Transport]. It never fails: an unreachable peer
// surfaces at Invoke, where the caller has a context to bound the wait.
func (t *netTransport) Dial(node core.Node) (core.PeerClient, error) {
	return &peerClient{net: t.net, from: t.self, to: node.Clone()}, nil
}

// Serve implements [core.Transport]. The listener is ignored: an in-process
// transport has nothing to listen on, and SPEC.md section 3.6 fixes the
// signature. It blocks until the transport or the network is closed.
func (t *netTransport) Serve(_ net.Listener, h core.Handler) error {
	if h == nil {
		return fmt.Errorf("testkit: nil handler: %w", core.ErrInvalidOption)
	}
	t.net.Register(t.self, h)
	t.readyOnce.Do(func() { close(t.ready) })
	select {
	case <-t.done:
	case <-t.net.closedCh:
	}
	return nil
}

// Close implements [core.Transport].
func (t *netTransport) Close() error {
	t.once.Do(func() {
		close(t.done)
		t.net.Unregister(t.self.ID)
	})
	return nil
}

// peerClient is a handle from one node to another.
type peerClient struct {
	net  *Network
	from core.Node
	to   core.Node
}

// result carries a handler's return values back to the caller.
type result struct {
	out []byte
	err error
}

// Invoke implements [core.PeerClient].
//
// It returns when ctx is done no matter what the fabric is doing, because a
// call that outlives its deadline is a worse failure than one that errors: it
// consumes the caller's budget silently and leaves nothing in metrics.
func (p *peerClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	n := p.net
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil, fmt.Errorf("testkit: network closed: %w", core.ErrNodeUnreachable)
	}
	if !n.reachableLocked(p.from.ID, p.to.ID) {
		n.mu.Unlock()
		return nil, fmt.Errorf("testkit: %s cannot reach %s: %w", p.from.ID, p.to.ID, core.ErrNodeUnreachable)
	}
	ep := n.endpoints[p.to.ID]
	dropReq := n.rollDropLocked(p.from.ID, p.to.ID)
	dropResp := n.rollDropLocked(p.to.ID, p.from.ID)
	delay := n.delayLocked(p.from.ID, p.to.ID)
	if !dropReq {
		// The Add happens under the same lock that Close uses to set
		// closed, so no Add can race with the Wait in Close. Doing it
		// after the unlock would be a WaitGroup misuse that only shows
		// up as an occasional leaked handler goroutine.
		n.handlers.Add(1)
	}
	n.mu.Unlock()

	if dropReq {
		return nil, p.awaitDeadline(ctx)
	}

	// Copy the request: core.Handler forbids retaining it, and a shared
	// slice would let a handler mutate the caller's buffer, which no real
	// transport would do.
	reqCopy := append([]byte(nil), req...)
	resCh := make(chan result, 1)

	run := func() {
		defer n.handlers.Done()
		out, err := ep.handler.Handle(ctx, p.from, method, reqCopy)
		if dropResp {
			// The handler ran and its side effects happened; only the
			// reply was lost. This asymmetry is the interesting one:
			// it is how a caller learns not to assume that no answer
			// means no effect.
			return
		}
		resCh <- result{out: append([]byte(nil), out...), err: err}
	}

	if delay > 0 {
		if !n.schedule(delay, run, func() { n.handlers.Done() }) {
			n.handlers.Done()
			return nil, fmt.Errorf("testkit: network closed: %w", core.ErrNodeUnreachable)
		}
	} else {
		go run()
	}

	select {
	case r := <-resCh:
		return r.out, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.closedCh:
		return nil, fmt.Errorf("testkit: network closed: %w", core.ErrNodeUnreachable)
	}
}

// awaitDeadline blocks until the caller's context ends or the network closes.
// It is what a dropped message feels like from the sending side.
func (p *peerClient) awaitDeadline(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.net.closedCh:
		return fmt.Errorf("testkit: network closed: %w", core.ErrNodeUnreachable)
	}
}

// Close implements [core.PeerClient]. Clients are stateless here, so this is a
// no-op that exists to satisfy the interface honestly rather than by omission.
func (p *peerClient) Close() error { return nil }

var (
	_ core.Transport  = (*netTransport)(nil)
	_ core.PeerClient = (*peerClient)(nil)
)
