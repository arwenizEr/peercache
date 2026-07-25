package actor

import (
	"container/list"
	"context"
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
	// OnEvent receives lifecycle events. It is called on the group's turn
	// goroutine and must not block: anything slow belongs on the far side
	// of a channel the handler owns.
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

	turns       atomic.Int64
	rejections  atomic.Int64
	escapes     atomic.Int64
	activations atomic.Int64
	evictions   atomic.Int64

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
}

// Stats returns a snapshot of the counters.
func (rt *Runtime) Stats() Counters {
	rt.mu.Lock()
	addressable := int64(len(rt.records))
	active := int64(rt.active)
	rt.mu.Unlock()

	return Counters{
		Addressable: addressable,
		Active:      active,
		Turns:       rt.turns.Load(),
		Rejections:  rt.rejections.Load(),
		Escapes:     rt.escapes.Load(),
		Activations: rt.activations.Load(),
		Evictions:   rt.evictions.Load(),
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
