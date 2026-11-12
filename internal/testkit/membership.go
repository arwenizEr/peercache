package testkit

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// Membership defaults. They are short because simulated time is free, and
// distinct from one another so that a test asserting on one cannot pass by
// accidentally satisfying the other.
const (
	// DefaultFailureTimeout is how long a peer must be unreachable before
	// it is declared failed.
	DefaultFailureTimeout = 3 * time.Second
	// DefaultJoinDelay is how long a peer must be reachable before it is
	// admitted to the view. It is non-zero so that a flapping link does not
	// produce a storm of epoch bumps.
	DefaultJoinDelay = 500 * time.Millisecond
	// watchBuffer is the depth of each Watch channel. A watcher slower than
	// this loses changes rather than stalling membership, which is the same
	// trade the real implementation must make.
	watchBuffer = 64
)

// MembershipConfig tunes detection latency. A zero field takes its default.
type MembershipConfig struct {
	// FailureTimeout is how long a peer must stay unreachable before it is
	// declared failed. Defaults to [DefaultFailureTimeout].
	FailureTimeout time.Duration
	// JoinDelay is how long a peer must stay reachable before it is
	// admitted. Defaults to [DefaultJoinDelay].
	JoinDelay time.Duration
}

func (c *MembershipConfig) withDefaults() {
	if c.FailureTimeout <= 0 {
		c.FailureTimeout = DefaultFailureTimeout
	}
	if c.JoinDelay <= 0 {
		c.JoinDelay = DefaultJoinDelay
	}
}

// Membership is the harness's [core.MembershipAuthority]: a reachability
// oracle with a configurable detection delay.
//
// It is not a failure detector and not a preview of M3. See the package
// documentation for why polling heartbeats are the wrong shape for a harness
// that must be able to skip an idle hour for free.
type Membership struct {
	self core.Node
	net  *Network
	clk  clock.Clock
	reg  *Registry
	cfg  MembershipConfig

	mu sync.Mutex
	// view is what this node currently believes is alive, including self.
	view map[string]core.Node
	// departed records peers that announced a graceful leave, so that a
	// still-registered endpoint is not immediately re-admitted.
	departed map[string]bool
	epoch    core.Epoch
	// transitions holds the timer arming a pending change per peer, with
	// true meaning "about to join" and false meaning "about to fail".
	transitions map[string]*transition
	watchers    []chan core.MembershipChange
	closed      bool
}

// transition is a scheduled view change for one peer.
type transition struct {
	timer clock.Timer
	join  bool
}

// Registry lets the memberships in one cluster find each other. A graceful
// leave is delivered through it rather than over the network, because the
// harness is modelling the announcement, not the wire format.
type Registry struct {
	mu   sync.Mutex
	byID map[string]*Membership
}

// NewRegistry returns an empty registry. One cluster shares one registry.
func NewRegistry() *Registry { return &Registry{byID: make(map[string]*Membership)} }

func (r *Registry) put(m *Membership) {
	r.mu.Lock()
	r.byID[m.self.ID] = m
	r.mu.Unlock()
}

func (r *Registry) drop(id string) {
	r.mu.Lock()
	delete(r.byID, id)
	r.mu.Unlock()
}

func (r *Registry) all() []*Membership {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Membership, 0, len(r.byID))
	for _, m := range r.byID {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].self.ID < out[j].self.ID })
	return out
}

// NewMembership creates the membership view for one node.
//
// The node starts believing only in itself; peers are admitted as reachability
// says so, after JoinDelay.
func NewMembership(self core.Node, n *Network, clk clock.Clock, reg *Registry, cfg MembershipConfig) *Membership {
	cfg.withDefaults()
	// The process start, as a real authority publishes it: a restarted node
	// is a new Membership and therefore young again.
	self = self.Clone()
	if self.Meta == nil {
		self.Meta = map[string]string{}
	}
	if _, set := self.Meta[core.MetaStartedKey]; !set {
		self.Meta[core.MetaStartedKey] = strconv.FormatInt(clk.Now().UnixNano(), 10)
	}
	m := &Membership{
		self:        self.Clone(),
		net:         n,
		clk:         clk,
		reg:         reg,
		cfg:         cfg,
		view:        map[string]core.Node{self.ID: self.Clone()},
		departed:    make(map[string]bool),
		transitions: make(map[string]*transition),
	}
	reg.put(m)
	n.OnChange(m.Refresh)
	return m
}

// Self implements [core.MembershipAuthority].
func (m *Membership) Self() core.Node { return m.self.Clone() }

// Members implements [core.MembershipAuthority]. The result is sorted by ID so
// that a test can compare views without sorting at every call site.
func (m *Membership) Members() ([]core.Node, core.Epoch) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]core.Node, 0, len(m.view))
	for _, n := range m.view {
		out = append(out, n.Clone())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, m.epoch
}

// MemberIDs is a convenience for assertions.
func (m *Membership) MemberIDs() []string {
	members, _ := m.Members()
	out := make([]string, len(members))
	for i, n := range members {
		out[i] = n.ID
	}
	return out
}

// Epoch returns the current view epoch.
func (m *Membership) Epoch() core.Epoch {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.epoch
}

// Watch implements [core.MembershipAuthority]. Each call returns its own
// channel, closed when the membership is closed. Sends are non-blocking: a
// watcher that stops reading loses changes rather than stalling the view.
func (m *Membership) Watch() <-chan core.MembershipChange {
	ch := make(chan core.MembershipChange, watchBuffer)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		close(ch)
		return ch
	}
	m.watchers = append(m.watchers, ch)
	m.mu.Unlock()
	return ch
}

// Join implements [core.MembershipAuthority]. The harness has no seed
// discovery: every node is already known to the Registry, so joining is the act
// of clearing any recorded departure and re-evaluating reachability.
func (m *Membership) Join([]string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return core.ErrClosed
	}
	m.departed = make(map[string]bool)
	m.mu.Unlock()

	for _, peer := range m.reg.all() {
		if peer.self.ID != m.self.ID {
			peer.clearDeparted(m.self.ID)
		}
	}
	m.net.notify()
	return nil
}

// Leave implements [core.MembershipAuthority]: a graceful departure, announced
// to every peer that can currently hear it.
//
// The announcement is immediate and skips the failure timeout, which is the
// whole point of leaving gracefully. Peers that cannot hear it — a partitioned
// minority — learn the slow way, which is exactly the case a test wants to be
// able to construct.
func (m *Membership) Leave(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return core.ErrClosed
	}
	m.mu.Unlock()

	// Collect targets before touching any peer's lock, so that the
	// Membership-then-Network ordering is never inverted and two nodes
	// leaving concurrently cannot deadlock on each other.
	for _, peer := range m.reg.all() {
		if peer.self.ID == m.self.ID {
			continue
		}
		if !m.net.Reachable(m.self.ID, peer.self.ID) {
			continue
		}
		peer.applyLeave(m.self)
	}
	return nil
}

// applyLeave records a peer's announced departure.
func (m *Membership) applyLeave(peer core.Node) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	m.departed[peer.ID] = true
	m.cancelTransitionLocked(peer.ID)
	if _, present := m.view[peer.ID]; !present {
		return
	}
	delete(m.view, peer.ID)
	m.epoch++
	m.emitLocked(core.MembershipChange{Kind: core.ChangeLeave, Node: peer.Clone(), Epoch: m.epoch})
}

// clearDeparted forgets a recorded departure so the node can be re-admitted.
func (m *Membership) clearDeparted(id string) {
	m.mu.Lock()
	delete(m.departed, id)
	m.mu.Unlock()
}

// Refresh re-evaluates every peer against current reachability, arming or
// cancelling transition timers. It is registered as a network change watcher
// and is safe to call at any time.
func (m *Membership) Refresh() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	// Candidates are the peers the registry knows about *and* everyone
	// already in the view. The second half is not redundant: a node killed
	// outright leaves the registry, and considering only registered peers
	// would mean nothing ever arms its failure timer and a corpse stays in
	// the view for good.
	candidates := make(map[string]core.Node)
	for _, peer := range m.reg.all() {
		if peer.self.ID != m.self.ID {
			candidates[peer.self.ID] = peer.self.Clone()
		}
	}
	m.mu.Lock()
	for id, node := range m.view {
		if id == m.self.ID {
			continue
		}
		if _, known := candidates[id]; !known {
			candidates[id] = node.Clone()
		}
	}
	m.mu.Unlock()

	// Reachability is read outside m.mu on purpose: the lock order is
	// Membership then Network, and reading first keeps the critical section
	// free of any call back into the fabric.
	// Sorted, because map iteration order would leak into the order timers
	// are created, and creation order is the tie-break the fake clock uses
	// when two timers share a deadline. A harness whose firing order varies
	// run to run is not a harness.
	ids := make([]string, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	want := make(map[string]bool, len(candidates))
	for _, id := range ids {
		want[id] = m.net.Reachable(m.self.ID, id)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	for _, id := range ids {
		node := candidates[id]
		reachable := want[id]
		if reachable && m.departed[id] {
			// A node that announced its departure stays out until it
			// rejoins explicitly. Otherwise a graceful shutdown that
			// has not yet closed its listener would be re-admitted
			// on the next refresh.
			reachable = false
		}
		_, present := m.view[id]
		switch {
		case reachable == present:
			m.cancelTransitionLocked(id)
		case reachable:
			m.armTransitionLocked(id, node, true)
		default:
			m.armTransitionLocked(id, m.view[id], false)
		}
	}
}

// armTransitionLocked schedules a view change for id, unless one of the same
// direction is already pending. Callers must hold mu.
func (m *Membership) armTransitionLocked(id string, node core.Node, join bool) {
	if t, ok := m.transitions[id]; ok {
		if t.join == join {
			return
		}
		t.timer.Stop()
	}
	d := m.cfg.FailureTimeout
	if join {
		d = m.cfg.JoinDelay
	}
	t := &transition{join: join}
	t.timer = m.clk.AfterFunc(d, func() { m.applyTransition(id, node, join) })
	m.transitions[id] = t
}

// cancelTransitionLocked drops any pending change for id. Callers must hold mu.
func (m *Membership) cancelTransitionLocked(id string) {
	if t, ok := m.transitions[id]; ok {
		t.timer.Stop()
		delete(m.transitions, id)
	}
}

// applyTransition commits a scheduled change, re-checking the condition that
// armed it. Reachability can flap between arming and firing, and applying a
// conclusion the world has already contradicted is how a flapping link turns
// into a permanently wrong view.
func (m *Membership) applyTransition(id string, node core.Node, join bool) {
	reachable := m.net.Reachable(m.self.ID, id)

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}
	t, armed := m.transitions[id]
	if !armed || t.join != join {
		return
	}
	delete(m.transitions, id)

	if m.departed[id] {
		reachable = false
	}
	if reachable != join {
		return
	}
	_, present := m.view[id]
	if present == join {
		return
	}

	if join {
		m.view[id] = node.Clone()
		m.epoch++
		m.emitLocked(core.MembershipChange{Kind: core.ChangeJoin, Node: node.Clone(), Epoch: m.epoch})
		return
	}
	delete(m.view, id)
	m.epoch++
	m.emitLocked(core.MembershipChange{Kind: core.ChangeFail, Node: node.Clone(), Epoch: m.epoch})
}

// emitLocked publishes a change to every watcher without blocking. Callers must
// hold mu.
func (m *Membership) emitLocked(c core.MembershipChange) {
	for _, ch := range m.watchers {
		select {
		case ch <- c:
		default:
		}
	}
}

// Close stops every pending transition and closes the watch channels. It is
// idempotent.
//
// Stopping the timers matters beyond tidiness: a pending transition on a fake
// clock is a live entry on the timer heap, and a harness that leaves them
// behind cannot assert that a test finished with no timers outstanding.
func (m *Membership) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for id, t := range m.transitions {
		t.timer.Stop()
		delete(m.transitions, id)
	}
	watchers := m.watchers
	m.watchers = nil
	m.mu.Unlock()

	m.reg.drop(m.self.ID)
	for _, ch := range watchers {
		close(ch)
	}
}

var _ core.MembershipAuthority = (*Membership)(nil)
