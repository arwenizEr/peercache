package ownership

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
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
// It answers three questions, and they fail differently on purpose:
//
//   - Does this node own the key? If not, [core.ErrNotOwner] — the caller
//     should re-resolve and go elsewhere.
//   - Did this node acquire the key so recently that the previous owner may not
//     have noticed? If so, [core.ErrActivationPending] — the caller should
//     retry here shortly.
//   - Did the caller resolve ownership under a view that has since been
//     replaced? If so, [core.ErrStaleEpoch] — see [Guard.CanActivateAt].
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

	// deferrals and refusals are exported as correctness metrics. A
	// non-zero refusal count under steady membership means clients are
	// resolving ownership differently from this node.
	deferrals atomic.Int64
	refusals  atomic.Int64
	// staleEpochs counts activations refused because the caller resolved
	// ownership under a view older than this node's. Only requests that
	// arrive with an epoch attached can raise it.
	staleEpochs atomic.Int64

	// onUpdate is called after each view is applied, on the watcher
	// goroutine.
	//
	// It exists because two independent watchers of the same authority have
	// no ordering between them. Replica promotion has to read an ownership
	// table that already holds the new view — a promoter racing the guard
	// sees the dead node still owning its keys, promotes nothing, and then
	// waits forever for an event that has already been delivered. Hanging
	// the work off the update makes the dependency explicit instead of
	// hoping the scheduler is kind.
	onUpdate func()

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

// OnUpdate registers a function to run after each membership view is applied.
//
// It runs on the watcher goroutine, so it must not block for long: a slow
// callback delays the next view. It is called after the table has been
// updated, which is the entire point.
func (g *Guard) OnUpdate(fn func()) {
	g.mu.Lock()
	g.onUpdate = fn
	g.mu.Unlock()
}

// ActivationDelay returns the configured delay.
func (g *Guard) ActivationDelay() time.Duration { return g.activationDelay }

// CanActivate reports whether this node may activate the group now, under this
// node's own current view.
//
// It returns nil, [core.ErrNotOwner], or [core.ErrActivationPending]. A local
// activation resolves ownership under the current epoch and so can never be
// stale by construction, which is why this form takes no epoch.
func (g *Guard) CanActivate(groupKey string) error {
	return g.CanActivateAt(groupKey, g.table.Epoch())
}

// CanActivateAt reports whether this node may activate the group on behalf of a
// caller that resolved ownership under epoch.
//
// A node refuses a group whose epoch is stale. The
// two directions of disagreement are different problems and get different
// errors, because they call for opposite corrective actions:
//
//   - epoch older than ours — [core.ErrStaleEpoch]. The caller decided under a
//     view that has since been replaced. It should re-resolve; it may be
//     talking to the wrong node entirely.
//   - epoch newer than ours — [core.ErrActivationPending]. *This* node is
//     behind, and cannot confirm ownership under a view it has not seen yet.
//     The caller should ask again here shortly.
//
// Collapsing them into one error would send a caller away from the node that is
// about to be able to serve it.
func (g *Guard) CanActivateAt(groupKey string, epoch core.Epoch) error {
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

	// Ownership is checked before the epoch on purpose. If this node does not
	// own the key under its current view, "go elsewhere" is strictly more
	// useful than "your epoch is old" — and it is the same corrective action
	// the caller would take anyway, one round trip sooner.
	if epoch < snap.epoch {
		g.staleEpochs.Add(1)
		return fmt.Errorf("ownership: group %q was resolved at epoch %d, this node is at %d: %w",
			groupKey, epoch, snap.epoch, core.ErrStaleEpoch)
	}
	if epoch > snap.epoch {
		g.deferrals.Add(1)
		return fmt.Errorf("ownership: group %q was resolved at epoch %d, this node is still at %d: %w",
			groupKey, epoch, snap.epoch, core.ErrActivationPending)
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
// An earlier version skipped updates whose membership was identical, on the
// theory that gossip re-delivering the current view should not restart the
// activation delay. It was added while chasing an unrelated bug, did not fix
// that bug, and was reverted after deliberately breaking the code showed what it
// cost: with
// identical views skipped, the settled-view logic below is exercised so rarely
// that breaking it deliberately no longer fails any test.
//
// The optimisation may still be worth having. It is not worth having untested,
// and it would need its own test proving that a repeated view does not defer
// groups — which is the test that would have caught this immediately.
func (g *Guard) Update(nodes []core.Node, epoch core.Epoch) {
	now := g.clk.Now().UnixNano()
	if now-g.changedAt.Load() >= int64(g.activationDelay) {
		g.settled.Store(g.table.current.Load())
	}
	// The change time is recorded before the new table is published, not
	// after. In the other order a reader could see the new table with the
	// previous change's time, measure the delay from that, and activate a
	// group this node had acquired an instant ago — skipping the delay that
	// narrows the window for two activations. In this order the worst a
	// reader sees is the old table with the new time, which only defers.
	g.changedAt.Store(now)
	g.table.Update(nodes, epoch)

	g.mu.Lock()
	fn := g.onUpdate
	g.mu.Unlock()
	if fn != nil {
		fn()
	}
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
	// StaleEpochs counts activations refused because the caller resolved
	// ownership under an older view. A steady trickle during a membership
	// change is expected; a sustained rate means a caller is caching
	// ownership for too long.
	StaleEpochs int64
	// Epoch is the current view's epoch.
	Epoch core.Epoch
	// Members is the size of the current view.
	Members int
}

// Stats returns a snapshot of the counters.
func (g *Guard) Stats() Counters {
	snap := g.table.current.Load()
	return Counters{
		Deferrals:   g.deferrals.Load(),
		Refusals:    g.refusals.Load(),
		StaleEpochs: g.staleEpochs.Load(),
		Epoch:       snap.epoch,
		Members:     len(snap.nodes),
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

// Settled reports whether the membership view has been stable for longer than
// the activation delay.
//
// It is the same clock the deferral window uses, asked as a question rather
// than applied to a key: if this returns true, no group is being deferred for
// having just moved here, so ownership answers are not about to change under
// the caller's feet.
//
// A negative delay disables deferral, and with it any notion of settling — such
// a node is always settled, which is correct for the single-node configuration
// that setting exists for.
func (g *Guard) Settled() bool {
	if g.activationDelay < 0 {
		return true
	}
	elapsed := g.clk.Now().UnixNano() - g.changedAt.Load()
	return elapsed >= int64(g.activationDelay)
}

// Table returns the ownership table this guard consults.
//
// It is exported so the data path can resolve an owner without going through
// the guard's activation logic: routing a read to a peer is a different
// question from deciding whether to activate a group here, and answering the
// second when the caller asked the first would refuse reads for keys this node
// merely does not own — which is the normal case, not an error.
func (g *Guard) Table() *Table { return g.table }
