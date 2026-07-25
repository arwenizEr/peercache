package membership

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/peercache/peercache/internal/core"
)

// watchBuffer is the depth of each Watch channel. A watcher slower than this
// loses changes rather than stalling gossip, which is the same trade the
// harness implementation makes and for the same reason: a slow consumer must
// not be able to wedge failure detection.
const watchBuffer = 64

// Options configures a [Gossip].
type Options struct {
	// Self is this node's identity. ID must be stable and cluster-unique:
	// ownership is derived from it, so a pod that restarts under a new ID
	// takes a different share of the keyspace.
	Self core.Node
	// BindAddr is the host:port to gossip on. A zero port asks the OS for
	// an ephemeral one, which is what the tests use.
	BindAddr string
	// Logger receives diagnostics.
	Logger core.Logger
	// Config, if set, is used as the memberlist configuration base. It lets
	// a caller pick LAN, WAN or local timing profiles without this package
	// enumerating them.
	Config *memberlist.Config
}

// Gossip is a [core.MembershipAuthority] backed by memberlist.
type Gossip struct {
	self   core.Node
	list   *memberlist.Memberlist
	logger core.Logger

	// epoch increments on every membership transition this node observes.
	// It is per-view: two nodes will disagree during a partition, which is
	// the point of comparing epochs rather than trusting them.
	epoch atomic.Uint64

	// localAddr is captured once at construction. memberlist's LocalNode
	// returns a pointer into state that gossip rewrites, so reading it
	// later would race.
	localAddr string

	// stopConsume is closed by Close to release the event goroutine, and
	// consumeDone is closed by that goroutine on its way out.
	//
	// Both are needed. memberlist never closes the channel it delivers
	// events on, not even on Shutdown, so a goroutine ranging over it waits
	// forever — one leaked goroutine per node, which is what happened here
	// until an end-to-end test dumped the stacks. Waiting on consumeDone
	// rather than merely signalling makes the exit path proven rather than
	// assumed: when Close returns, the goroutine is gone.
	stopConsume chan struct{}
	consumeDone chan struct{}

	mu       sync.Mutex
	watchers []chan core.MembershipChange
	closed   bool
	// members is this node's own view, maintained from the event stream.
	//
	// It exists because memberlist's Members() returns pointers into its
	// internal node state, and reading Node.Address() off one races with
	// the gossip goroutine rewriting it — the race detector catches this
	// immediately on a two-node cluster. ChannelEventDelegate, by contrast,
	// copies each node into the event and sends it blocking, so the event
	// stream is both race-free and lossless. Owning the map is therefore
	// strictly better than borrowing theirs.
	members map[string]core.Node

	// meta is republished when the epoch changes, so peers can see how
	// far along this node's view is.
	meta atomic.Pointer[nodeMeta]
}

// nodeMeta is what this node advertises about itself through gossip.
type nodeMeta struct {
	Epoch core.Epoch
}

// New starts gossiping and returns the authority.
//
// It does not join any cluster; call [Gossip.Join] with seed addresses.
func New(opts Options) (*Gossip, error) {
	if opts.Self.IsZero() {
		return nil, fmt.Errorf("membership: Self.ID is required: %w", core.ErrInvalidOption)
	}
	if opts.Logger == nil {
		opts.Logger = core.NopLogger{}
	}

	g := &Gossip{
		self:        opts.Self.Clone(),
		logger:      opts.Logger,
		members:     make(map[string]core.Node),
		stopConsume: make(chan struct{}),
		consumeDone: make(chan struct{}),
	}
	g.meta.Store(&nodeMeta{})

	cfg := opts.Config
	if cfg == nil {
		cfg = memberlist.DefaultLANConfig()
	}
	events := make(chan memberlist.NodeEvent, watchBuffer)
	cfg.Name = opts.Self.ID
	cfg.Delegate = (*delegate)(g)
	cfg.Events = &memberlist.ChannelEventDelegate{Ch: events}
	// memberlist logs to the standard logger by default, which CLAUDE.md
	// forbids; silence it and route what matters through our own Logger.
	cfg.LogOutput = discard{}

	if opts.BindAddr != "" {
		host, port, err := net.SplitHostPort(opts.BindAddr)
		if err != nil {
			return nil, fmt.Errorf("membership: bind address %q: %w", opts.BindAddr, core.ErrInvalidOption)
		}
		cfg.BindAddr = host
		p, err := strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("membership: bind port %q: %w", port, core.ErrInvalidOption)
		}
		cfg.BindPort = p
		cfg.AdvertisePort = p
	}

	list, err := memberlist.Create(cfg)
	if err != nil {
		return nil, fmt.Errorf("membership: starting memberlist: %w", err)
	}
	g.list = list
	local := list.LocalNode()
	g.localAddr = net.JoinHostPort(local.Addr.String(), strconv.Itoa(int(local.Port)))

	// Seed the view with this node. The event stream will report the local
	// join too, but not necessarily before the caller asks — and a node is
	// unconditionally a member of its own cluster, so answering "no members"
	// in that window would be wrong rather than merely early.
	g.self.Addr = g.localAddr
	g.members[g.self.ID] = g.self.Clone()

	go g.consume(events)
	return g, nil
}

// consume translates memberlist events into membership changes.
//
// Only joins and departures advance the epoch and republish this node's
// metadata. A NodeUpdate is reported but changes nothing, and that asymmetry is
// load-bearing rather than a nicety.
//
// Republishing metadata *is* a NodeUpdate on every peer. If an update advanced
// the epoch, each republish would provoke a republish on every peer, which
// would provoke another here: an unbounded gossip amplification loop with an
// epoch climbing as fast as the network allows. The first version of this
// function did exactly that and reached epoch 222 in a two-node test that ran
// for a fraction of a second.
func (g *Gossip) consume(events <-chan memberlist.NodeEvent) {
	defer close(g.consumeDone)
	for {
		var event memberlist.NodeEvent
		select {
		case <-g.stopConsume:
			return
		case ev, open := <-events:
			if !open {
				return
			}
			event = ev
		}

		var (
			kind       core.ChangeKind
			transition bool
		)
		switch event.Event {
		case memberlist.NodeJoin:
			kind, transition = core.ChangeJoin, true
		case memberlist.NodeLeave:
			// memberlist reports a graceful leave and a detected
			// failure as the same event. They are different facts —
			// one had a chance to hand over its data and the other
			// did not — but the wire does not distinguish them, so
			// this reports the pessimistic one.
			kind, transition = core.ChangeFail, true
		case memberlist.NodeUpdate:
			// Metadata only: somebody's epoch moved. That is worth
			// telling watchers about and must not move ours.
			kind, transition = core.ChangeUpdate, false
		default:
			continue
		}

		epoch := core.Epoch(g.epoch.Load())
		if transition {
			epoch = core.Epoch(g.epoch.Add(1))
			g.meta.Store(&nodeMeta{Epoch: epoch})
			if g.list != nil {
				// Best effort: republishing is how peers learn
				// our epoch, and failing is not fatal.
				if err := g.list.UpdateNode(100 * time.Millisecond); err != nil {
					g.logger.Debug("membership: republishing node metadata", "error", err)
				}
			}
		}

		// event.Node is a copy made by ChannelEventDelegate, so reading
		// it here is safe in a way that reading memberlist's live node
		// state is not.
		node := core.Node{ID: event.Node.Name, Addr: event.Node.Address()}
		if len(event.Node.Meta) > 0 {
			// The peer's advertised epoch, which is how its staleness
			// becomes visible without a round trip.
			node.Meta = map[string]string{metaEpochKey: string(event.Node.Meta)}
		}
		g.apply(kind, node)
		g.emit(core.MembershipChange{Kind: kind, Node: node, Epoch: epoch})
	}
}

// apply folds a change into this node's own view.
func (g *Gossip) apply(kind core.ChangeKind, node core.Node) {
	g.mu.Lock()
	defer g.mu.Unlock()
	switch kind {
	case core.ChangeJoin, core.ChangeUpdate:
		g.members[node.ID] = node
	case core.ChangeFail, core.ChangeLeave:
		delete(g.members, node.ID)
	}
}

func (g *Gossip) emit(change core.MembershipChange) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return
	}
	for _, ch := range g.watchers {
		select {
		case ch <- change:
		default:
		}
	}
}

// Self implements [core.MembershipAuthority].
func (g *Gossip) Self() core.Node { return g.self.Clone() }

// Members implements [core.MembershipAuthority], sorted by ID so that callers
// need not sort before comparing views.
func (g *Gossip) Members() ([]core.Node, core.Epoch) {
	g.mu.Lock()
	out := make([]core.Node, 0, len(g.members))
	for _, n := range g.members {
		out = append(out, n.Clone())
	}
	g.mu.Unlock()

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, core.Epoch(g.epoch.Load())
}

// Watch implements [core.MembershipAuthority]. Each call returns its own
// channel, closed when the authority is closed. Sends are non-blocking.
func (g *Gossip) Watch() <-chan core.MembershipChange {
	ch := make(chan core.MembershipChange, watchBuffer)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		close(ch)
		return ch
	}
	g.watchers = append(g.watchers, ch)
	return ch
}

// Join implements [core.MembershipAuthority]. An empty seed list forms a
// cluster of one and waits to be joined.
func (g *Gossip) Join(seeds []string) error {
	if len(seeds) == 0 {
		return nil
	}
	if _, err := g.list.Join(seeds); err != nil {
		return fmt.Errorf("membership: joining %v: %w", seeds, err)
	}
	return nil
}

// Leave implements [core.MembershipAuthority]: a graceful departure, broadcast
// so that peers remove this node without waiting out the failure detector.
//
// The context bounds the broadcast. If it expires the departure becomes
// ungraceful, which is exactly the difference between a PreStop hook that had
// time to run and a SIGKILL.
func (g *Gossip) Leave(ctx context.Context) error {
	timeout := 5 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		// The clock here is memberlist's, not ours: it is broadcasting
		// on the wire and there is no injected clock on that path.
		if remaining := time.Until(deadline); remaining > 0 {
			timeout = remaining
		} else {
			timeout = 0
		}
	}
	if err := g.list.Leave(timeout); err != nil {
		return fmt.Errorf("membership: leaving: %w", err)
	}
	return nil
}

// Close shuts gossip down. It is idempotent.
func (g *Gossip) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	watchers := g.watchers
	g.watchers = nil
	g.mu.Unlock()

	err := g.list.Shutdown()

	// Stop the event goroutine and wait for it, before closing the watcher
	// channels it publishes to. Shutdown does not release it: memberlist
	// leaves its event channel open.
	close(g.stopConsume)
	<-g.consumeDone

	for _, ch := range watchers {
		close(ch)
	}
	if err != nil {
		return fmt.Errorf("membership: shutting down: %w", err)
	}
	return nil
}

// LocalAddr returns the address this node is gossiping on, which is how a test
// discovers an ephemeral port.
func (g *Gossip) LocalAddr() string { return g.localAddr }

// delegate carries this node's metadata through gossip. It is the Gossip
// itself under another name, so that the delegate methods do not widen the
// public surface.
type delegate Gossip

// NodeMeta implements [memberlist.Delegate]: the epoch, encoded compactly.
func (d *delegate) NodeMeta(limit int) []byte {
	meta := (*Gossip)(d).meta.Load()
	encoded := strconv.FormatUint(uint64(meta.Epoch), 10)
	if len(encoded) > limit {
		return nil
	}
	return []byte(encoded)
}

// NotifyMsg implements [memberlist.Delegate]. PeerCache carries no user data
// over gossip — that is what the transport in M4 is for — so this is empty.
func (d *delegate) NotifyMsg([]byte) {}

// GetBroadcasts implements [memberlist.Delegate].
func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }

// LocalState implements [memberlist.Delegate].
func (d *delegate) LocalState(join bool) []byte { return nil }

// MergeRemoteState implements [memberlist.Delegate].
func (d *delegate) MergeRemoteState(buf []byte, join bool) {}

// PeerEpoch returns the epoch a peer last advertised, and whether it is known.
//
// This is what makes another node's staleness visible without a round trip: if
// a peer is several epochs behind, anything it says about ownership was decided
// under a membership that has since changed.
func (g *Gossip) PeerEpoch(id string) (core.Epoch, bool) {
	g.mu.Lock()
	node, known := g.members[id]
	g.mu.Unlock()
	if !known {
		return 0, false
	}
	raw, ok := node.Meta[metaEpochKey]
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return core.Epoch(parsed), true
}

// metaEpochKey is where a peer's advertised epoch is kept in [core.Node.Meta].
const metaEpochKey = "epoch"

// discard swallows memberlist's own logging. CLAUDE.md forbids the log package,
// and memberlist writes to it by default.
type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

var _ core.MembershipAuthority = (*Gossip)(nil)
var _ memberlist.Delegate = (*delegate)(nil)
