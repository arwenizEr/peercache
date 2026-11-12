package actor

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// Defaults for [Options].
const (
	// DefaultMailboxSize bounds how many requests may queue for one group.
	// Small on purpose: a deep queue does not make a slow group faster, it
	// only delays the moment the caller finds out.
	DefaultMailboxSize = 64
	// DefaultIdleTimeout is how long an activated group waits with no work
	// before releasing its goroutine.
	DefaultIdleTimeout = 30 * time.Second
	// DefaultMaxActive caps concurrently activated groups. Each costs a
	// goroutine, so this is the knob that decides the runtime's memory
	// floor.
	DefaultMaxActive = 20_000
)

// Options configures a [Runtime].
type Options struct {
	// Clock is the time source for idle and group timers. Required.
	Clock clock.Clock
	// Logger receives diagnostics.
	Logger core.Logger
	// MailboxSize bounds each group's queue. Defaults to
	// [DefaultMailboxSize].
	MailboxSize int
	// IdleTimeout is how long an activated group waits before deactivating.
	// Defaults to [DefaultIdleTimeout].
	IdleTimeout time.Duration
	// MaxActive caps activated groups; zero means [DefaultMaxActive] and a
	// negative value means unlimited.
	MaxActive int
	// CanActivate is consulted before a group is activated, and may refuse.
	//
	// M3 supplies the ownership guard here: a node must not activate a group
	// it does not own, nor one it acquired so recently that the previous
	// owner may not have noticed. A nil hook permits everything, which is
	// correct for a single node with no membership.
	//
	// It runs while the group's lock is held, so it must not block and must
	// not call back into the runtime.
	CanActivate func(groupID string) error

	// OnEvent receives lifecycle events. It is called on the group's turn
	// goroutine and must not block: anything slow belongs on the far side
	// of a channel the handler owns.
	//
	// Events carry no ordering guarantee relative to [Runtime.Stats]. The
	// deactivation event in particular is emitted after the activation has
	// already left the directory, so a handler that observes it and then
	// reads Stats may see a count that has already moved on. Delivery is
	// best effort and nothing may depend on it.
	OnEvent func(core.Event)
}

func (o *Options) withDefaults() {
	if o.MailboxSize <= 0 {
		o.MailboxSize = DefaultMailboxSize
	}
	if o.IdleTimeout <= 0 {
		o.IdleTimeout = DefaultIdleTimeout
	}
	if o.MaxActive == 0 {
		o.MaxActive = DefaultMaxActive
	}
	if o.Logger == nil {
		o.Logger = core.NopLogger{}
	}
}

// Runtime is the group activation directory.
//
// It is safe for concurrent use.
//
// # Lock ordering
//
// There are two levels of lock and the order between them is strict:
//
//	record.mu  ->  Runtime.mu
//
// Runtime.mu is a leaf. Nothing acquires a record's lock while holding it,
// which is why eviction picks its victims under Runtime.mu, releases it, and
// only then touches the victims. Doing it the obvious way — evicting inline
// while already holding the activating record's lock — is a deadlock the first
// time two groups evict each other simultaneously.
type Runtime struct {
	opts Options

	mu      sync.Mutex
	records map[string]*record
	// lru orders activations, most recent at the front. Entries are
	// *record.
	lru    *list.List
	active int
	closed bool

	funcsMu sync.RWMutex
	funcs   map[string]core.GroupFunc

	turns              atomic.Int64
	timerHeldOpen      atomic.Int64
	rejections         atomic.Int64
	refusedActivations atomic.Int64
	escapes            atomic.Int64
	activations        atomic.Int64
	evictions          atomic.Int64

	wg sync.WaitGroup
}

// New returns a [Runtime].
//
// The caller must call [Runtime.Close]; every activated group holds a goroutine
// until then.
func New(opts Options) (*Runtime, error) {
	if opts.Clock == nil {
		return nil, fmt.Errorf("actor: a clock is required: %w", core.ErrInvalidOption)
	}
	opts.withDefaults()
	return &Runtime{
		opts:    opts,
		records: make(map[string]*record),
		lru:     list.New(),
		funcs:   make(map[string]core.GroupFunc),
	}, nil
}

// Register adds fn under name.
//
// Registration is a startup activity: closures never cross the network, so
// every pod is assumed to register the same names from the same binary.
func (rt *Runtime) Register(name string, fn core.GroupFunc) error {
	if name == "" || fn == nil {
		return fmt.Errorf("actor: name and function are both required: %w", core.ErrInvalidOption)
	}
	rt.funcsMu.Lock()
	defer rt.funcsMu.Unlock()
	if _, dup := rt.funcs[name]; dup {
		return fmt.Errorf("actor: %q: %w", name, core.ErrDuplicateRegistration)
	}
	rt.funcs[name] = fn
	return nil
}

// Registered reports whether a function is registered under name.
func (rt *Runtime) Registered(name string) bool {
	rt.funcsMu.RLock()
	defer rt.funcsMu.RUnlock()
	_, ok := rt.funcs[name]
	return ok
}

func (rt *Runtime) lookup(name string) (core.GroupFunc, bool) {
	rt.funcsMu.RLock()
	defer rt.funcsMu.RUnlock()
	fn, ok := rt.funcs[name]
	return fn, ok
}

// record returns the record for id, creating it if necessary.
//
// A record is the addressable half of a group: identity, state and timer
// schedule. It costs no goroutine and survives deactivation.
// errRecordRemoved means a record was taken out of the directory between a
// caller looking it up and enqueuing on it. The caller looks it up again.
var errRecordRemoved = errors.New("actor: record removed")

// forget removes r from the directory. Callers hold r.mu; the runtime lock is a
// leaf, so taking it here keeps the lock order.
func (rt *Runtime) forget(r *record) {
	rt.mu.Lock()
	if rt.records[r.id] == r {
		delete(rt.records, r.id)
	}
	rt.mu.Unlock()
}

func (rt *Runtime) record(id string) (*record, error) {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil, core.ErrClosed
	}
	r, ok := rt.records[id]
	if !ok {
		r = &record{id: id, rt: rt}
		rt.records[id] = r
	}
	rt.mu.Unlock()
	return r, nil
}

// Do runs fn as one turn on the group, returning whatever it returns.
//
// It blocks until the turn completes or ctx is done. A full mailbox is reported
// immediately as [core.ErrGroupOverloaded] rather than waited on: queueing
// behind an already-saturated group only delays the moment the caller learns it
// is saturated.
func (rt *Runtime) Do(ctx context.Context, id string, fn func(core.GroupState) (any, error)) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, err := rt.record(id)
	if err != nil {
		return nil, err
	}
	out, err := r.enqueue(ctx, fn, true)
	if errors.Is(err, errRecordRemoved) {
		// Removed after the lookup, by a refused activation on another
		// goroutine. The fresh record is the one to use.
		if r, err = rt.record(id); err != nil {
			return nil, err
		}
		out, err = r.enqueue(ctx, fn, true)
	}
	// Enforce the cap outside every record lock; see the type comment.
	rt.enforceCap()
	return out, err
}

// registerActivation records a new activation in the directory. Callers hold
// the record's lock; this takes only the runtime lock, which is a leaf.
func (rt *Runtime) registerActivation(r *record, a *activation) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return core.ErrClosed
	}
	a.elem = rt.lru.PushFront(r)
	rt.active++
	return nil
}

// releaseActivation removes an activation from the directory.
func (rt *Runtime) releaseActivation(a *activation) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if a.elem != nil {
		rt.lru.Remove(a.elem)
		a.elem = nil
	}
	rt.active--
}

// Call runs the registered function named fn against the group.
func (rt *Runtime) Call(ctx context.Context, id, fn string, arg []byte) ([]byte, error) {
	registered, ok := rt.lookup(fn)
	if !ok {
		return nil, fmt.Errorf("actor: %q: %w", fn, core.ErrFuncNotRegistered)
	}
	out, err := rt.Do(ctx, id, func(s core.GroupState) (any, error) {
		return registered(ctx, s, arg)
	})
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	val, _ := out.([]byte)
	return val, nil
}

// Deactivate releases a group's activation, keeping its state and timer
// schedule. It returns once the goroutine has exited.
func (rt *Runtime) Deactivate(id string) {
	rt.mu.Lock()
	r, ok := rt.records[id]
	rt.mu.Unlock()
	if !ok {
		return
	}
	r.evict()
}

// DeactivateAll releases every activation, keeping the records and their state.
//
// It is the group half of a graceful drain: the goroutines exit and their
// pending turns complete, but a group remains addressable and keeps what it
// holds, so a read arriving before the process actually stops is still
// answered.
//
// The record list is snapshotted under the lock and evicted outside it. Holding
// the runtime lock while each activation drains would block every other group's
// mailbox behind the slowest one, which during shutdown is exactly when the
// system has the least time to spare.
func (rt *Runtime) DeactivateAll() {
	rt.mu.Lock()
	records := make([]*record, 0, len(rt.records))
	for _, r := range rt.records {
		records = append(records, r)
	}
	rt.mu.Unlock()

	for _, r := range records {
		r.evict()
	}
}

// enforceCap deactivates least-recently-activated groups until the active count
// is within MaxActive.
//
// Victims are chosen under the runtime lock and evicted after releasing it, so
// that no record lock is ever taken while the runtime lock is held.
func (rt *Runtime) enforceCap() {
	if rt.opts.MaxActive < 0 {
		return
	}
	var victims []*record

	rt.mu.Lock()
	for rt.active-len(victims) > rt.opts.MaxActive {
		elem := rt.lru.Back()
		if elem == nil {
			break
		}
		// Move to the front so the same victim is not selected twice by a
		// concurrent enforceCap before it has finished exiting.
		rt.lru.MoveToFront(elem)
		victims = append(victims, elem.Value.(*record))
	}
	rt.mu.Unlock()

	for _, victim := range victims {
		rt.evictions.Add(1)
		// Signal without waiting: making the request that tripped the cap
		// block on somebody else's turn couples unrelated latencies.
		victim.signalEvict()
	}
}

// Counters is a snapshot of runtime activity.
type Counters struct {
	// Addressable is the number of groups with a record: every group ever
	// touched, whether or not it currently holds a goroutine.
	Addressable int64
	// Active is the number of groups holding a goroutine.
	Active int64
	// Turns counts executed turns.
	Turns int64
	// Rejections counts calls refused because the mailbox was full. A
	// rising value is backpressure working, not a fault.
	Rejections int64
	// Escapes counts uses of a GroupState outside its turn. Any non-zero
	// value is an application bug.
	Escapes int64
	// Activations and Evictions count lifecycle transitions.
	Activations int64
	Evictions   int64
	// RefusedActivations counts activations the guard refused, whether
	// because this node does not own the group or because it acquired the
	// group too recently to activate safely.
	RefusedActivations int64
	// TimerHeldOpen counts idle deactivations declined because the group
	// still had a pending timer.
	//
	// It exists as a positive signal. "The group did not deactivate" is not
	// directly observable — a test can only look and see it still active,
	// which is equally true a moment before it deactivates. Counting the
	// decision makes the exemption something that can be waited for rather
	// than inferred from a sample.
	TimerHeldOpen int64
}

// Stats returns a snapshot of the counters.
func (rt *Runtime) Stats() Counters {
	rt.mu.Lock()
	addressable := int64(len(rt.records))
	active := int64(rt.active)
	rt.mu.Unlock()

	return Counters{
		Addressable:        addressable,
		Active:             active,
		Turns:              rt.turns.Load(),
		Rejections:         rt.rejections.Load(),
		Escapes:            rt.escapes.Load(),
		Activations:        rt.activations.Load(),
		Evictions:          rt.evictions.Load(),
		RefusedActivations: rt.refusedActivations.Load(),
		TimerHeldOpen:      rt.timerHeldOpen.Load(),
	}
}

// Close deactivates every group and waits for the goroutines to exit.
//
// It is idempotent. State is not written anywhere, because there is nowhere to
// write it: this runtime is memory only by design.
func (rt *Runtime) Close(ctx context.Context) error {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil
	}
	rt.closed = true
	all := make([]*record, 0, len(rt.records))
	for _, r := range rt.records {
		all = append(all, r)
	}
	rt.mu.Unlock()

	for _, r := range all {
		r.evict()
	}
	rt.wg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (rt *Runtime) isClosed() bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.closed
}
