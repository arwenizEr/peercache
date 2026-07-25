package ownership

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// DefaultActivationDelay is how long a node waits, after a membership change,
// before activating a group it has just acquired.
//
// It is a guess about how fast the previous owner notices, and it trades
// availability for a narrower double-activation window. Too short and both
// nodes serve the group; too long and every membership change is a brownout
// for the groups that moved.
const DefaultActivationDelay = 3 * time.Second

// Guard decides whether this node may activate a group.
//
// It answers two questions, and they fail differently on purpose:
//
//   - Does this node own the key? If not, [core.ErrNotOwner] — the caller
//     should re-resolve and go elsewhere.
//   - Did this node acquire the key so recently that the previous owner may not
//     have noticed? If so, [core.ErrActivationPending] — the caller should
//     retry here shortly.
//
// A Guard is safe for concurrent use.
type Guard struct {
	self  core.Node
	table *Table
	clk   clock.Clock

	activationDelay time.Duration

	// changedAt is when membership last changed, in Unix nanoseconds.
	// Atomic because it is read on every activation and written only by the
	// membership watcher.
	changedAt atomic.Int64

	// settled is the last view that survived a whole activation delay
	// without being replaced.
	//
	// It is emphatically *not* the immediately preceding view. A partition
	// does not arrive as one membership change: each peer is detected
	// separately, so a five-node cluster splitting produces a burst of
	// updates. Comparing against the previous update would mean comparing a
	// post-partition view with another post-partition view, in which nothing
	// looks newly acquired and the deferral never fires — which is precisely
	// the moment it exists for. Holding the last settled view instead treats
	// a burst as the single transition it really is.
	settled atomic.Pointer[snapshot]

	// deferrals and refusals are the correctness metrics M7 exports. A
	// non-zero refusal count under steady membership means clients are
	// resolving ownership differently from this node.
	deferrals atomic.Int64
	refusals  atomic.Int64

	mu       sync.Mutex
	watching bool
	stop     chan struct{}
	stopped  chan struct{}
}

// GuardOptions configures a [Guard].
type GuardOptions struct {
	// Self is this node. Required.
	Self core.Node
	// Table supplies ownership. Required.
	Table *Table
	// Clock measures the activation delay. Required.
	Clock clock.Clock
	// ActivationDelay is how long to defer newly acquired groups. Zero
	// takes [DefaultActivationDelay]; negative disables deferral, which is
	// only sane on a single node.
	ActivationDelay time.Duration
}

// NewGuard returns a [Guard].
func NewGuard(opts GuardOptions) (*Guard, error) {
	if opts.Self.IsZero() {
		return nil, fmt.Errorf("ownership: Self is required: %w", core.ErrInvalidOption)
	}
	if opts.Table == nil {
		return nil, fmt.Errorf("ownership: Table is required: %w", core.ErrInvalidOption)
	}
	if opts.Clock == nil {
		return nil, fmt.Errorf("ownership: Clock is required: %w", core.ErrInvalidOption)
	}
	if opts.ActivationDelay == 0 {
		opts.ActivationDelay = DefaultActivationDelay
	}

	g := &Guard{
		self:            opts.Self.Clone(),
		table:           opts.Table,
		clk:             opts.Clock,
		activationDelay: opts.ActivationDelay,
		stop:            make(chan struct{}),
		stopped:         make(chan struct{}),
	}
	// No membership has been learned yet, so there is nothing to defer
	// against; the first Update starts the first real window.
	g.changedAt.Store(opts.Clock.Now().Add(-opts.ActivationDelay).UnixNano())
	g.settled.Store(&snapshot{})
	return g, nil
}

// ActivationDelay returns the configured delay.
func (g *Guard) ActivationDelay() time.Duration { return g.activationDelay }

// CanActivate reports whether this node may activate the group now.
//
// It returns nil, [core.ErrNotOwner], or [core.ErrActivationPending].
func (g *Guard) CanActivate(groupKey string) error {
	snap := g.table.current.Load()
	if len(snap.nodes) == 0 {
		// A node that has not learned the membership yet owns nothing.
		// Answering "yes" here would let a starting pod activate the
		// entire keyspace for as long as it took to hear from anyone.
		g.refusals.Add(1)
		return fmt.Errorf("ownership: membership is unknown: %w", core.ErrNotOwner)
	}

	current, ok := owner(snap, groupKey)
	if !ok || current.ID != g.self.ID {
		g.refusals.Add(1)
		return fmt.Errorf("ownership: group %q belongs to %s: %w", groupKey, current.ID, core.ErrNotOwner)
	}
	if g.activationDelay < 0 {
		return nil
	}

	elapsed := g.clk.Now().UnixNano() - g.changedAt.Load()
	if elapsed >= int64(g.activationDelay) {
		return nil
	}

	// Inside the window. Only groups this node did *not* own in the last
	// settled view are deferred: a group it has held all along cannot be
	// active anywhere else, and blocking those would turn every membership
	// change into a brownout for the whole keyspace rather than for the
	// part that moved.
	if before, ok := owner(g.settled.Load(), groupKey); ok && before.ID == g.self.ID {
		return nil
	}

	g.deferrals.Add(1)
	return fmt.Errorf("ownership: group %q was acquired %v ago, waiting %v: %w",
		groupKey, time.Duration(elapsed), g.activationDelay, core.ErrActivationPending)
}

// Update applies a new membership view and starts the deferral window.
//
// The view being replaced becomes the settled reference only if the previous
// window had already closed. A burst of changes — which is what a partition
// looks like, one detection per peer — is therefore treated as a single
// transition away from the last stable view, rather than as several transitions
// between views that are all equally post-change.
func (g *Guard) Update(nodes []core.Node, epoch core.Epoch) {
	now := g.clk.Now().UnixNano()
	if now-g.changedAt.Load() >= int64(g.activationDelay) {
		g.settled.Store(g.table.current.Load())
	}
	g.table.Update(nodes, epoch)
	g.changedAt.Store(now)
}

// Watch applies membership changes from authority until [Guard.Close].
//
// It seeds the table from the authority's current view before returning, so a
// caller that constructs a guard and immediately resolves a key does not race
// the first gossip event.
func (g *Guard) Watch(authority core.MembershipAuthority) {
	if authority == nil {
		return
	}

	g.mu.Lock()
	if g.watching {
		g.mu.Unlock()
		return
	}
	g.watching = true
	g.mu.Unlock()

	changes := authority.Watch()
	members, epoch := authority.Members()
	g.Update(members, epoch)

	go func() {
		defer close(g.stopped)
		for {
			select {
			case <-g.stop:
				return
			case _, open := <-changes:
				if !open {
					return
				}
				// Re-read the authority rather than trusting the
				// event: events are best effort and may be
				// dropped by a slow watcher, so the event is a
				// wake-up and the authority is the truth.
				members, epoch := authority.Members()
				g.Update(members, epoch)
			}
		}
	}()
}

// Counters is a snapshot of guard activity.
type Counters struct {
	// Deferrals counts activations postponed because the group was newly
	// acquired. Expected to spike after a membership change and return to
	// zero.
	Deferrals int64
	// Refusals counts activations refused because this node does not own
	// the key. Sustained non-zero under stable membership means callers
	// disagree with this node about ownership.
	Refusals int64
	// Epoch is the current view's epoch.
	Epoch core.Epoch
	// Members is the size of the current view.
	Members int
}

// Stats returns a snapshot of the counters.
func (g *Guard) Stats() Counters {
	snap := g.table.current.Load()
	return Counters{
		Deferrals: g.deferrals.Load(),
		Refusals:  g.refusals.Load(),
		Epoch:     snap.epoch,
		Members:   len(snap.nodes),
	}
}

// Close stops watching membership. It is idempotent.
func (g *Guard) Close() {
	g.mu.Lock()
	watching := g.watching
	g.watching = false
	g.mu.Unlock()

	if !watching {
		return
	}
	close(g.stop)
	<-g.stopped
}
