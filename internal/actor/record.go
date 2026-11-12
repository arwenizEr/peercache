package actor

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
)

// record is the addressable half of a group: identity, state and timer
// schedule. It outlives every activation.
type record struct {
	id string
	rt *Runtime

	// mu guards act, and serialises enqueueing against deactivation.
	//
	// Enqueueing holds it for the duration of a non-blocking channel send,
	// and deactivation holds it while clearing act. That is what makes the
	// two atomic with respect to each other: without it, a caller can
	// observe a live activation, and post to its mailbox after the goroutine
	// has already decided to exit, and then wait for a reply that nobody
	// will ever send.
	mu  sync.Mutex
	act *activation

	// state and timers belong to whichever goroutine is entitled to touch
	// them, and only one ever is. While active that is the turn goroutine.
	// While inactive nothing touches them except activation, which runs
	// under mu before the goroutine starts.
	state  map[string][]byte
	timers map[string]*groupTimer

	// removed is set, under mu, when the record is taken out of the
	// runtime's directory. A caller that looked the record up just before
	// that must not activate it — its state would diverge from the record
	// that replaces it — so enqueue reports errRecordRemoved and the
	// runtime looks again.
	removed bool

	// generation increments twice per turn: once when the turn's State is
	// minted, once when the turn ends. A State comparing unequal is an
	// escapee.
	generation atomic.Uint64
}

// activation is the ephemeral half: mailbox, goroutine and LRU position.
type activation struct {
	mailbox chan request
	quit    chan struct{}
	done    chan struct{}
	once    sync.Once

	// elem is this activation's LRU position, guarded by Runtime.mu.
	elem *list.Element
}

// request is one unit of work for a group.
type request struct {
	ctx context.Context
	run func(core.GroupState) (any, error)
	// reply is nil for work nobody is waiting on, such as a fired timer.
	reply chan result
}

type result struct {
	val any
	err error
}

// groupTimer is one named timer belonging to a group.
//
// deadline is absolute, so the schedule survives deactivation: the clock timer
// is stopped when the group deactivates and re-armed for the remaining time
// when it activates again.
type groupTimer struct {
	name     string
	deadline time.Time
	fn       string
	arg      []byte
	timer    clock.Timer
}

// enqueue posts work to the group, activating it if necessary.
//
// If wait is false the caller does not receive the result, which is what a
// fired timer wants: blocking the clock's callback goroutine until a turn
// completes would make advancing a fake clock depend on the group making
// progress, and that is a deadlock waiting for a test to find it.
func (r *record) enqueue(ctx context.Context, run func(core.GroupState) (any, error), wait bool) (any, error) {
	r.mu.Lock()
	if r.rt.isClosed() {
		r.mu.Unlock()
		return nil, core.ErrClosed
	}
	if r.removed {
		r.mu.Unlock()
		return nil, errRecordRemoved
	}
	if r.act == nil {
		if err := r.activateLocked(); err != nil {
			// A refused activation on a record that holds nothing leaves
			// nothing worth keeping. Kept, it was one empty record per
			// distinct group ever asked about on a node that does not own
			// it — routing probes, stale callers — and records are never
			// otherwise removed.
			if len(r.state) == 0 && len(r.timers) == 0 {
				r.removed = true
				r.rt.forget(r)
			}
			r.mu.Unlock()
			return nil, err
		}
	}

	req := request{ctx: ctx, run: run}
	if wait {
		req.reply = make(chan result, 1)
	}

	select {
	case r.act.mailbox <- req:
		r.mu.Unlock()
	default:
		r.mu.Unlock()
		r.rt.rejections.Add(1)
		return nil, fmt.Errorf("actor: group %q: %w", r.id, core.ErrGroupOverloaded)
	}

	if !wait {
		return nil, nil
	}
	select {
	case res := <-req.reply:
		return res.val, res.err
	case <-ctx.Done():
		// The turn may still run; it simply has nobody listening. That is
		// deliberate — abandoning a queued turn would leave the group's
		// state depending on whether a caller lost patience.
		return nil, ctx.Err()
	}
}

// activateLocked gives the group a mailbox and a goroutine. Callers must hold
// r.mu and must have established that r.act is nil.
func (r *record) activateLocked() error {
	// Ownership is checked before anything is allocated. A node that does
	// not own this group must not create a mailbox for it, let alone a
	// goroutine — and refusing here, rather than after the turn runs, is
	// what keeps a wrongly-addressed call from mutating state.
	if r.rt.opts.CanActivate != nil {
		if err := r.rt.opts.CanActivate(r.id); err != nil {
			r.rt.refusedActivations.Add(1)
			return err
		}
	}

	a := &activation{
		mailbox: make(chan request, r.rt.opts.MailboxSize),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	// Re-arm the surviving schedule before the goroutine starts, so a timer
	// that came due while the group was deactivated fires promptly.
	r.act = a
	r.rearmTimers()

	if err := r.rt.registerActivation(r, a); err != nil {
		r.stopTimers()
		r.act = nil
		return err
	}

	r.rt.activations.Add(1)
	r.rt.wg.Add(1)
	go r.run(a)
	return nil
}

// run is the turn loop: one goroutine, one group, one request at a time.
func (r *record) run(a *activation) {
	defer r.rt.wg.Done()
	defer close(a.done)

	r.emit(core.Event{Kind: core.EventActivate, GroupID: r.id})

	idle := r.rt.opts.Clock.NewTimer(r.rt.opts.IdleTimeout)
	defer idle.Stop()

	for {
		select {
		case req := <-a.mailbox:
			r.turn(req)
			rearm(idle, r.rt.opts.IdleTimeout)

		case <-idle.C():
			if r.finish(a, false) {
				return
			}
			rearm(idle, r.rt.opts.IdleTimeout)

		case <-a.quit:
			// Drain before exiting: work already accepted must be run,
			// or a caller waits for a reply that never comes.
			for drained := false; !drained; {
				select {
				case req := <-a.mailbox:
					r.turn(req)
				default:
					drained = true
				}
			}
			if r.finish(a, true) {
				return
			}
		}
	}
}

// rearm resets a timer that may or may not have fired.
func rearm(t clock.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C():
		default:
		}
	}
	t.Reset(d)
}

// turn executes one request with exclusive access to the group's state.
func (r *record) turn(req request) {
	var res result

	if err := req.ctx.Err(); err != nil {
		res.err = err
	} else {
		gen := r.generation.Add(1)
		res = r.invoke(req, &State{rec: r, gen: gen, escapes: &r.rt.escapes})
		// Invalidate the State handed to this turn. Anything still holding
		// it is now an escapee and its writes become no-ops.
		r.generation.Add(1)
	}

	r.rt.turns.Add(1)
	if req.reply != nil {
		req.reply <- res
	}
}

// invoke runs the group function, converting a panic into an error.
//
// A panicking group function would otherwise kill the group's only goroutine
// and wedge that group permanently. One bad input would take a key range
// offline until the process restarted.
func (r *record) invoke(req request, s *State) (res result) {
	defer func() {
		if p := recover(); p != nil {
			res = result{err: fmt.Errorf("%w: group %q: %v", core.ErrGroupFuncPanic, r.id, p)}
			r.rt.opts.Logger.Error("group function panicked", "group", r.id, "panic", p)
		}
	}()
	val, err := req.run(s)
	return result{val: val, err: err}
}

// finish tears the activation down if the group is quiescent, reporting whether
// it did.
//
// forced is set when the runtime asked for the group to go, in which case
// pending timers do not save it.
func (r *record) finish(a *activation, forced bool) bool {
	r.mu.Lock()
	if len(a.mailbox) > 0 {
		r.mu.Unlock()
		return false
	}
	if !forced && len(r.timers) > 0 {
		// A group with pending timers is exempt from idle deactivation.
		// Deactivating it would stop its clock timers, and nothing would
		// ever wake it again, so the timer would silently never fire.
		r.rt.timerHeldOpen.Add(1)
		r.mu.Unlock()
		return false
	}

	r.stopTimers()
	r.act = nil
	r.mu.Unlock()

	r.rt.releaseActivation(a)
	r.emit(core.Event{Kind: core.EventDeactivate, GroupID: r.id})
	return true
}

// signalEvict asks the group to deactivate without waiting for it.
//
// Used when enforcing the active cap: making the caller that triggered the cap
// wait for someone else's turn to finish would couple unrelated requests'
// latency together.
func (r *record) signalEvict() {
	r.mu.Lock()
	a := r.act
	r.mu.Unlock()
	if a == nil {
		return
	}
	a.once.Do(func() { close(a.quit) })
}

// evict asks the group to deactivate and waits for its goroutine to exit.
func (r *record) evict() {
	r.mu.Lock()
	a := r.act
	r.mu.Unlock()
	if a == nil {
		return
	}
	a.once.Do(func() { close(a.quit) })
	<-a.done
}

// timerRetryDelay is how long a timer whose turn could not be queued — the
// mailbox was full, or activation was deferred — waits before trying again.
const timerRetryDelay = 50 * time.Millisecond

// scheduleTimer arms a named timer. Only ever called from the turn goroutine.
//
// The schedule is written under r.mu. The turn goroutine is the only one that
// adds or removes entries, but the clock goroutine re-arms a timer whose turn
// could not be queued, so the entries' timer handles are shared.
func (r *record) scheduleTimer(name string, after time.Duration, fn string, arg []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.timers == nil {
		r.timers = make(map[string]*groupTimer, 1)
	}
	if existing, ok := r.timers[name]; ok && existing.timer != nil {
		existing.timer.Stop()
	}

	stored := make([]byte, len(arg))
	copy(stored, arg)

	gt := &groupTimer{
		name:     name,
		deadline: r.rt.opts.Clock.Now().Add(after),
		fn:       fn,
		arg:      stored,
	}
	r.timers[name] = gt
	r.armTimer(gt)
}

// cancelTimer drops a named timer. Only ever called from the turn goroutine.
func (r *record) cancelTimer(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	gt, ok := r.timers[name]
	if !ok {
		return
	}
	if gt.timer != nil {
		gt.timer.Stop()
	}
	delete(r.timers, name)
}

// armTimer starts the clock timer for a schedule entry.
func (r *record) armTimer(gt *groupTimer) {
	remaining := gt.deadline.Sub(r.rt.opts.Clock.Now())
	if remaining < 0 {
		remaining = 0
	}
	gt.timer = r.rt.opts.Clock.AfterFunc(remaining, func() { r.fireTimer(gt) })
}

// rearmTimers re-arms every surviving schedule entry. Called under r.mu during
// activation, before the goroutine exists.
func (r *record) rearmTimers() {
	for _, gt := range r.timers {
		r.armTimer(gt)
	}
}

// stopTimers stops the clock timers but keeps the schedule, which is what lets
// a reactivated group resume them.
func (r *record) stopTimers() {
	for _, gt := range r.timers {
		if gt.timer != nil {
			gt.timer.Stop()
			gt.timer = nil
		}
	}
}

// fireTimer runs when a group timer comes due. It executes on the clock's
// goroutine, so it posts work and does not wait.
//
// The turn it posts checks that gt is still the schedule entry for its name:
// a timer cancelled or replaced after it came due, while its turn sat in the
// mailbox, must neither run nor delete its replacement's entry. That check is
// also what makes a retried or duplicated firing harmless.
//
// A turn that cannot be queued is retried, not dropped. Dropping it left the
// entry in the schedule with no clock timer behind it, so the timer never fired
// and the entry held the group active for good. A timer naming a function that
// is not registered is removed instead, for the same reason.
func (r *record) fireTimer(gt *groupTimer) {
	registered, ok := r.rt.lookup(gt.fn)
	if !ok {
		r.rt.opts.Logger.Warn("group timer names an unregistered function; dropping it",
			"group", r.id, "timer", gt.name, "func", gt.fn)
	}
	ctx := context.Background()
	_, err := r.enqueue(ctx, func(s core.GroupState) (any, error) {
		r.mu.Lock()
		current := r.timers[gt.name] == gt
		if current {
			delete(r.timers, gt.name)
		}
		r.mu.Unlock()
		if !current || !ok {
			return nil, nil
		}
		return registered(ctx, s, gt.arg)
	}, false)
	if err == nil {
		return
	}
	if errors.Is(err, core.ErrClosed) || errors.Is(err, core.ErrNotOwner) {
		// Closed, or the group has moved: its timers will be re-armed
		// wherever, and whenever, it activates next.
		return
	}
	// Mailbox full or activation deferred: try again shortly, if the entry is
	// still this one and the group still has an activation to deliver to. A
	// deactivated group's timers are re-armed by its next activation.
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.timers[gt.name] == gt && r.act != nil {
		gt.timer = r.rt.opts.Clock.AfterFunc(timerRetryDelay, func() { r.fireTimer(gt) })
	}
}

// emit delivers a lifecycle event. Delivery is best effort and nothing may
// depend on it.
func (r *record) emit(e core.Event) {
	if r.rt.opts.OnEvent != nil {
		r.rt.opts.OnEvent(e)
	}
}
