package clock

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"
)

// maxFiresPerAdvance bounds the number of timers a single [Fake.Advance] will
// fire. A callback that re-arms itself for an instant already inside the
// advance window would otherwise spin forever and hang CI for the full test
// timeout. Hitting the bound is recorded as misuse and stops the advance.
const maxFiresPerAdvance = 1 << 20

// Fake is a [Clock] whose time only moves when a test moves it.
//
// It is safe for concurrent use. Timers may be created, stopped and reset from
// any goroutine while another goroutine advances. [Fake.Advance] itself is
// serialised: two concurrent advances run one after the other rather than
// interleaving, so ordering stays deterministic even in a sloppy test.
//
// The zero value is not usable; call [NewFake].
type Fake struct {
	// advanceMu serialises whole advances against each other. It is never
	// held while mu is being acquired by another goroutine's timer call, so
	// the two locks have a strict order: advanceMu, then mu.
	advanceMu sync.Mutex

	mu       sync.Mutex
	now      time.Time
	seq      uint64
	pending  timerHeap
	blockers []*blocker
	misuses  []string
	onMisuse func(string)
}

// blocker is one goroutine parked in [Fake.BlockUntil] waiting for the number
// of scheduled timers to reach n.
type blocker struct {
	n  int
	ch chan struct{}
}

// NewFake returns a [Fake] whose current time is start.
//
// Pass [Epoch] unless the test cares about a specific wall-clock date.
func NewFake(start time.Time) *Fake {
	return &Fake{now: start}
}

// SetMisuseHandler installs fn, called with a description whenever the clock is
// used in a way that would panic on a real clock or that indicates a test bug:
// a negative advance, a non-positive ticker period, or a runaway callback.
//
// The handler runs on the misusing goroutine with no clock lock held. Tests
// normally wire it to t.Errorf. Misuses are recorded regardless of whether a
// handler is installed; see [Fake.Misuses].
func (f *Fake) SetMisuseHandler(fn func(string)) {
	f.mu.Lock()
	f.onMisuse = fn
	f.mu.Unlock()
}

// Misuses returns a copy of every misuse recorded so far. A harness should
// assert this is empty at teardown so that a silently degraded clock cannot
// make a test pass for the wrong reason.
func (f *Fake) Misuses() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.misuses))
	copy(out, f.misuses)
	return out
}

// reportMisuse records msg and invokes the handler, if any, without holding mu.
func (f *Fake) reportMisuse(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	f.mu.Lock()
	f.misuses = append(f.misuses, msg)
	fn := f.onMisuse
	f.mu.Unlock()
	if fn != nil {
		fn(msg)
	}
}

// Now returns the current simulated time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the simulated duration elapsed since t.
func (f *Fake) Since(t time.Time) time.Duration { return f.Now().Sub(t) }

// After implements [Clock].
func (f *Fake) After(d time.Duration) <-chan time.Time { return f.NewTimer(d).C() }

// NewTimer implements [Clock].
func (f *Fake) NewTimer(d time.Duration) Timer {
	return f.schedule(d, 0, nil, make(chan time.Time, 1))
}

// AfterFunc implements [Clock].
//
// fn runs on the goroutine that calls [Fake.Advance], with no clock lock held,
// so it may freely create, stop and reset timers. It must not itself call
// Advance: that deadlocks on the advance lock.
func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	return f.schedule(d, 0, fn, nil)
}

// NewTicker implements [Clock].
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		f.reportMisuse("clock: NewTicker with non-positive period %v", d)
		return deadTicker{}
	}
	return &fakeTicker{t: f.schedule(d, d, nil, make(chan time.Time, 1))}
}

// Sleep implements [Clock]. A non-positive d returns immediately without
// scheduling anything, matching [time.Sleep].
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	t := f.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// schedule creates a timer due d from now and pushes it onto the heap.
func (f *Fake) schedule(d time.Duration, period time.Duration, fn func(), ch chan time.Time) *fakeTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{
		fake:     f,
		deadline: f.now.Add(d),
		period:   period,
		fn:       fn,
		ch:       ch,
		seq:      f.nextSeqLocked(),
		active:   true,
		idx:      -1,
	}
	heap.Push(&f.pending, t)
	f.notifyLocked()
	return t
}

func (f *Fake) nextSeqLocked() uint64 {
	f.seq++
	return f.seq
}

// Pending reports how many timers are currently scheduled. A ticker counts as
// one. Use it to catch timer leaks at the end of a test.
func (f *Fake) Pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pending.Len()
}

// Advance moves simulated time forward by d, firing every timer whose deadline
// falls in the interval, in (deadline, creation sequence) order.
//
// While a timer fires, [Fake.Now] reports that timer's own deadline. Only once
// every due timer has fired does the clock read start+d. A callback that
// schedules a new timer inside the window fires within the same advance.
//
// Advance returns once every due timer has been delivered to. It does not wait
// for goroutines woken by those deliveries; see the package documentation.
func (f *Fake) Advance(d time.Duration) {
	if d < 0 {
		f.reportMisuse("clock: Advance with negative duration %v; time does not run backwards", d)
		return
	}
	f.advanceMu.Lock()
	defer f.advanceMu.Unlock()

	f.mu.Lock()
	target := f.now.Add(d)
	fires := 0
	for {
		t := f.pending.peek()
		if t == nil || t.deadline.After(target) {
			break
		}
		if fires++; fires > maxFiresPerAdvance {
			f.mu.Unlock()
			f.reportMisuse("clock: Advance(%v) fired more than %d timers; a callback is almost certainly re-arming inside the advance window", d, maxFiresPerAdvance)
			f.mu.Lock()
			break
		}
		heap.Pop(&f.pending)
		f.now = t.deadline
		if t.period > 0 {
			// Re-arm before firing so that a callback which stops the
			// ticker actually removes the next tick.
			//
			// Missed ticks are collapsed rather than replayed: the
			// next deadline is the first one strictly after the end
			// of this advance. A real ticker drops ticks its receiver
			// did not drain, and the receiver cannot drain during a
			// synchronous advance, so replaying them would deliver at
			// most one value anyway — while doing O(elapsed/period)
			// work to get there. Advancing an hour past a one-second
			// ticker is 3,600 wasted iterations; advancing a century
			// is three billion, which is the difference between a
			// test and a hang.
			elapsed := target.Sub(t.deadline)
			periods := elapsed/t.period + 1
			t.deadline = t.deadline.Add(periods * t.period)
			t.seq = f.nextSeqLocked()
			heap.Push(&f.pending, t)
		} else {
			t.active = false
		}
		f.notifyLocked()
		at := f.now
		f.mu.Unlock()
		t.fire(at)
		f.mu.Lock()
	}
	f.now = target
	f.notifyLocked()
	f.mu.Unlock()
}

// AdvanceTo moves simulated time forward to t. Moving to an instant in the past
// is misuse and leaves the clock unchanged.
func (f *Fake) AdvanceTo(t time.Time) {
	now := f.Now()
	if t.Before(now) {
		f.reportMisuse("clock: AdvanceTo(%v) is before the current time %v", t, now)
		return
	}
	f.Advance(t.Sub(now))
}

// Flush fires every timer that is already due without moving time forward. It
// is Advance(0), and is what releases a timer created with a non-positive
// duration.
func (f *Fake) Flush() { f.Advance(0) }

// BlockUntil blocks until at least n timers are scheduled.
//
// This is the sanctioned way to synchronise with code under test: park until
// the code has armed the timers you are about to fire, then advance. It is not
// a substitute for observing the effect of firing — use an explicit channel for
// that.
//
// The count is every armed timer on this clock, not only the ones you care
// about. If unrelated components already hold timers, BlockUntil(1) is
// satisfied immediately by one of theirs and synchronises with nothing. Read
// [Fake.Pending] first and block on baseline+n, or arrange for the clock to be
// quiescent before the call.
//
// BlockUntil can block forever if the code under test never arms its timers.
// Prefer [Fake.BlockUntilContext] anywhere a hang would be hard to diagnose.
func (f *Fake) BlockUntil(n int) {
	//nolint:errcheck // context.Background never cancels, so this cannot fail.
	_ = f.BlockUntilContext(context.Background(), n)
}

// BlockUntilContext blocks until at least n timers are scheduled or ctx ends,
// returning ctx.Err() in the latter case.
func (f *Fake) BlockUntilContext(ctx context.Context, n int) error {
	f.mu.Lock()
	if f.pending.Len() >= n {
		f.mu.Unlock()
		return nil
	}
	b := &blocker{n: n, ch: make(chan struct{})}
	f.blockers = append(f.blockers, b)
	f.mu.Unlock()

	select {
	case <-b.ch:
		return nil
	case <-ctx.Done():
		f.removeBlocker(b)
		return ctx.Err()
	}
}

// removeBlocker drops b from the waiting set. It tolerates b having already
// been satisfied and removed.
func (f *Fake) removeBlocker(b *blocker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, other := range f.blockers {
		if other == b {
			f.blockers = append(f.blockers[:i], f.blockers[i+1:]...)
			return
		}
	}
}

// notifyLocked releases every blocker whose threshold the current pending count
// has reached. Callers must hold mu.
func (f *Fake) notifyLocked() {
	if len(f.blockers) == 0 {
		return
	}
	n := f.pending.Len()
	kept := f.blockers[:0]
	for _, b := range f.blockers {
		if n >= b.n {
			close(b.ch)
			continue
		}
		kept = append(kept, b)
	}
	f.blockers = kept
}

// fakeTimer is a scheduled entry on a [Fake]'s heap. A ticker is a fakeTimer
// with a positive period.
type fakeTimer struct {
	fake *Fake

	// The fields below are guarded by fake.mu, except that fn and ch are
	// immutable after construction and fire reads deadline only through the
	// value passed to it.
	deadline time.Time
	period   time.Duration
	seq      uint64
	active   bool
	idx      int

	fn func()
	ch chan time.Time
}

// C implements [Timer]. It is nil for AfterFunc timers.
func (t *fakeTimer) C() <-chan time.Time {
	if t.ch == nil {
		return nil
	}
	return t.ch
}

// Stop implements [Timer].
func (t *fakeTimer) Stop() bool {
	f := t.fake
	f.mu.Lock()
	defer f.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	if t.idx >= 0 {
		heap.Remove(&f.pending, t.idx)
	}
	f.notifyLocked()
	return true
}

// Reset implements [Timer].
func (t *fakeTimer) Reset(d time.Duration) bool {
	f := t.fake
	f.mu.Lock()
	defer f.mu.Unlock()
	was := t.active
	if t.idx >= 0 {
		heap.Remove(&f.pending, t.idx)
	}
	t.deadline = f.now.Add(d)
	t.seq = f.nextSeqLocked()
	t.active = true
	heap.Push(&f.pending, t)
	f.notifyLocked()
	return was
}

// fire delivers the tick. It runs with no clock lock held.
func (t *fakeTimer) fire(at time.Time) {
	if t.fn != nil {
		t.fn()
		return
	}
	select {
	case t.ch <- at:
	default:
		// Receiver has not drained the previous value. Real timers and
		// tickers drop too.
	}
}

// fakeTicker adapts a periodic [fakeTimer] to [Ticker].
type fakeTicker struct{ t *fakeTimer }

func (k *fakeTicker) C() <-chan time.Time { return k.t.C() }

func (k *fakeTicker) Stop() { k.t.Stop() }

func (k *fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		k.t.fake.reportMisuse("clock: Ticker.Reset with non-positive period %v", d)
		k.t.Stop()
		return
	}
	t, f := k.t, k.t.fake
	f.mu.Lock()
	defer f.mu.Unlock()
	// Period and next deadline must change together: a concurrent Advance
	// must never observe the new period with the old deadline.
	if t.idx >= 0 {
		heap.Remove(&f.pending, t.idx)
	}
	t.period = d
	t.deadline = f.now.Add(d)
	t.seq = f.nextSeqLocked()
	t.active = true
	heap.Push(&f.pending, t)
	f.notifyLocked()
}

// timerHeap orders timers by deadline, then by creation sequence so that two
// timers due at the same instant fire in a stable, reproducible order.
type timerHeap []*fakeTimer

func (h timerHeap) Len() int { return len(h) }

func (h timerHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return h[i].seq < h[j].seq
}

func (h timerHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}

func (h *timerHeap) Push(x any) {
	t := x.(*fakeTimer)
	t.idx = len(*h)
	*h = append(*h, t)
}

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	t.idx = -1
	*h = old[:n-1]
	return t
}

// peek returns the earliest timer without removing it, or nil if empty.
func (h timerHeap) peek() *fakeTimer {
	if len(h) == 0 {
		return nil
	}
	return h[0]
}

// Compile-time assertions that the fake satisfies the interfaces it claims.
var (
	_ Clock  = (*Fake)(nil)
	_ Timer  = (*fakeTimer)(nil)
	_ Ticker = (*fakeTicker)(nil)
)
