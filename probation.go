package peercache

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// probationWatchBuffer is each watcher's buffer. Events are wake-ups, so a
// watcher that falls behind by this many loses nothing it cannot re-read.
const probationWatchBuffer = 64

// probationAuthority hides young nodes from the ranking. See [WithJoinProbation].
//
// # Why it wraps the authority rather than changing the ranking
//
// Rendezvous hashing ranks whatever members it is given (CLAUDE.md decision 1),
// and every consumer of membership — the ownership guard, catch-up, drain,
// readiness — already reads it through [core.MembershipAuthority]. Filtering
// here changes what all of them see at once and changes none of them. A filter
// inside the guard would have left catch-up and readiness disagreeing with it
// about who is a member.
//
// # The rule
//
// A member is ranked if it has been up for at least the probation, measured
// from the start time it advertises under [core.MetaStartedKey]; or if it
// advertises none, which is a peer on an older build; or if no member has been
// up that long, which is a cluster starting cold — excluding everyone would
// leave nothing to own anything. It is a pure function of the member list and
// the time, so every node that sees the same members at the same moment ranks
// the same ones. Nodes do not see them at exactly the same moment, or with
// exactly the same clocks, and the activation delay covers that the same way it
// covers any other membership change.
//
// A young node applies the rule to itself too. It must not rank itself while
// its peers do not, or it would act as owner of keys they route elsewhere.
//
// # The epoch
//
// A probation ending changes the ranked view without the underlying authority
// noticing anything, and a ranked view that changes must carry a new epoch or
// the guard's staleness checks compare unlike things. So the epoch reported
// here is the authority's plus the number of times a probation ending changed
// the ranked view. Both only grow, so the sum does.
type probationAuthority struct {
	inner  core.MembershipAuthority
	clk    clock.Clock
	period time.Duration

	// bumps counts ranked-view changes caused by a probation ending.
	bumps atomic.Uint64

	mu       sync.Mutex
	watchers []chan core.MembershipChange
	timer    clock.Timer
	lastIDs  []string
	closed   bool

	stop    chan struct{}
	stopped chan struct{}
}

func newProbationAuthority(inner core.MembershipAuthority, clk clock.Clock, period time.Duration) *probationAuthority {
	p := &probationAuthority{
		inner:   inner,
		clk:     clk,
		period:  period,
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	changes := inner.Watch()
	p.mu.Lock()
	p.refreshLocked()
	p.mu.Unlock()
	go p.forward(changes)
	return p
}

// Self implements [core.MembershipAuthority].
func (p *probationAuthority) Self() core.Node { return p.inner.Self() }

// Join implements [core.MembershipAuthority].
func (p *probationAuthority) Join(seeds []string) error { return p.inner.Join(seeds) }

// Leave implements [core.MembershipAuthority].
func (p *probationAuthority) Leave(ctx context.Context) error { return p.inner.Leave(ctx) }

// Members implements [core.MembershipAuthority]: the ranked members only.
func (p *probationAuthority) Members() ([]core.Node, core.Epoch) {
	all, epoch := p.inner.Members()
	ranked, _, _ := rankable(all, p.clk.Now().UnixNano(), p.period)
	return ranked, epoch + core.Epoch(p.bumps.Load())
}

// Watch implements [core.MembershipAuthority]. Every event is a wake-up — the
// guard re-reads Members on each — so a probation ending is sent as an update.
func (p *probationAuthority) Watch() <-chan core.MembershipChange {
	ch := make(chan core.MembershipChange, probationWatchBuffer)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		close(ch)
		return ch
	}
	p.watchers = append(p.watchers, ch)
	return ch
}

// onProbation reports how many members are currently hidden by probation.
//
// Computed from the authority now, like Members, rather than read from the
// last refresh: the refresh runs on the relay goroutine, and a figure that lags
// the view it describes reports a dead node as still waiting.
func (p *probationAuthority) onProbation() int {
	all, _ := p.inner.Members()
	_, young, _ := rankable(all, p.clk.Now().UnixNano(), p.period)
	return young
}

// forward relays the underlying authority's events and re-arms the timer.
func (p *probationAuthority) forward(changes <-chan core.MembershipChange) {
	defer close(p.stopped)
	for {
		select {
		case <-p.stop:
			return
		case change, open := <-changes:
			if !open {
				p.closeWatchers()
				return
			}
			p.mu.Lock()
			p.refreshLocked()
			p.broadcastLocked(change)
			p.mu.Unlock()
		}
	}
}

// expire runs when the earliest probation should have ended.
func (p *probationAuthority) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	before := p.lastIDs
	p.refreshLocked()
	if !slices.Equal(before, p.lastIDs) {
		p.bumps.Add(1)
		p.broadcastLocked(core.MembershipChange{Kind: core.ChangeUpdate})
	}
}

// refreshLocked recomputes the ranked view and arms a timer for the next
// probation to end. Callers hold mu.
func (p *probationAuthority) refreshLocked() {
	all, _ := p.inner.Members()
	now := p.clk.Now().UnixNano()
	ranked, _, next := rankable(all, now, p.period)
	p.lastIDs = p.lastIDs[:0]
	for _, n := range ranked {
		p.lastIDs = append(p.lastIDs, n.ID)
	}
	slices.Sort(p.lastIDs)

	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	if next > now && !p.closed {
		p.timer = p.clk.AfterFunc(time.Duration(next-now), p.expire)
	}
}

func (p *probationAuthority) broadcastLocked(change core.MembershipChange) {
	for _, ch := range p.watchers {
		select {
		case ch <- change:
		default:
			// A slow watcher loses the event, not the truth: it reads
			// Members on the next one.
		}
	}
}

func (p *probationAuthority) closeWatchers() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
	}
	for _, ch := range p.watchers {
		close(ch)
	}
	p.watchers = nil
}

// Close stops relaying and closes every watcher. The underlying authority is
// not closed; the application owns it.
func (p *probationAuthority) Close() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	<-p.stopped
	p.closeWatchers()
}

// rankable applies the probation rule. It returns the ranked members, how many
// were hidden, and the earliest instant a hidden one becomes ranked (zero if
// none is hidden).
func rankable(all []core.Node, now int64, period time.Duration) (ranked []core.Node, young int, next int64) {
	established := make([]core.Node, 0, len(all))
	for _, n := range all {
		started, ok := n.StartedAt()
		if !ok || now-started >= int64(period) {
			established = append(established, n)
			continue
		}
		young++
		if until := started + int64(period); next == 0 || until < next {
			next = until
		}
	}
	if len(established) == 0 {
		// A cold start: nobody has lasted the probation, so everybody is
		// ranked. Their probations still end on schedule, and the timer is
		// armed for the first so that the view is re-read then — though by
		// this rule it does not change.
		return all, 0, next
	}
	return established, young, next
}
