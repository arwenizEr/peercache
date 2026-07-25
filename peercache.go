package peercache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/actor"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/store"
)

// Cache is the top-level handle: one per process, constructed at startup and
// closed at shutdown (SPEC.md section 3.1).
//
// A Cache is safe for concurrent use. Namespace and Group return handles, not
// data, and are cheap enough to call on a hot path.
type Cache interface {
	// Namespace returns a handle to a named keyspace, creating it on first
	// use. Calling it again with different options does not reconfigure an
	// existing namespace; the first call wins.
	Namespace(name string, opts ...NamespaceOption) Namespace

	// Group returns a handle to the group with the given id. A group is
	// addressable at all times, whether or not it is currently activated
	// anywhere, so this never fails and never blocks.
	Group(id string) Group

	// RegisterGroupFunc registers fn under name so that it can be invoked
	// by [Group.Call] from any pod.
	//
	// Registration is a startup activity. Closures are never serialized
	// (CLAUDE.md, decision 3): an RPC carries the name and opaque argument
	// bytes, and every pod is assumed to run the same binary and therefore
	// to have registered the same names. Registering the same name twice
	// returns [ErrDuplicateRegistration].
	RegisterGroupFunc(name string, fn GroupFunc) error

	// Stats returns a snapshot of this node's counters and view of the
	// cluster.
	Stats() Stats

	// Close shuts the cache down. With a live context it drains gracefully:
	// deactivate groups, transfer ownership, then stop. If ctx expires
	// first, shutdown becomes ungraceful and this node's owned state is
	// lost, which is the documented consequence of having no durability.
	//
	// Close is idempotent.
	Close(ctx context.Context) error
}

// GroupFunc is a function registered by name and executed on a group's owner
// (SPEC.md section 3.3).
//
// It runs inside that group's turn, so it has exclusive access to s for the
// duration of the call and needs no locking. It must respect ctx: the caller's
// deadline is propagated into the turn, and a function that ignores it holds
// the group's only goroutine hostage while every other request queues behind
// it.
type GroupFunc = core.GroupFunc

// sweepInterval is how often the cache's single sweeper wakes to reclaim
// expired entries. Expiry itself is enforced on read, so this interval affects
// memory, not correctness.
const sweepInterval = time.Second

// sweepShardsPerTick bounds the work one sweep does per namespace. With 256
// shards and a one-second interval a namespace is fully visited every 32
// seconds, and no single tick walks more than a thirty-second of it.
const sweepShardsPerTick = 8

// New constructs a [Cache].
//
// Options are validated once, here, so that a misconfiguration surfaces at
// startup rather than on the first request. [WithNodeID] is required.
//
// The returned Cache owns one background goroutine, released by [Cache.Close].
//
// M1 status: namespaces are fully functional locally. Groups arrive in M2 and
// report [ErrNotImplemented] until then; nothing is distributed until M3.
func New(opts ...Option) (Cache, error) {
	resolved := core.DefaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&resolved)
	}
	if err := resolved.Validate(); err != nil {
		return nil, err
	}

	// Ownership is only meaningful with a membership authority. Without one
	// this is a single node that owns everything, and a guard that always
	// says yes is the correct answer rather than a special case.
	var guard *ownership.Guard
	var canActivate func(string) error
	if resolved.Membership != nil {
		g, err := ownership.NewGuard(ownership.GuardOptions{
			Self:            resolved.Membership.Self(),
			Table:           ownership.NewTable(),
			Clock:           resolved.Clock,
			ActivationDelay: resolved.ActivationDelay,
		})
		if err != nil {
			return nil, err
		}
		guard = g
		guard.Watch(resolved.Membership)
		canActivate = guard.CanActivate
	}

	groups, err := actor.New(actor.Options{
		Clock:       resolved.Clock,
		Logger:      resolved.Logger,
		MailboxSize: resolved.GroupMailboxSize,
		IdleTimeout: resolved.GroupIdleTimeout,
		MaxActive:   resolved.MaxActiveGroups,
		CanActivate: canActivate,
		OnEvent:     resolved.OnEvent,
	})
	if err != nil {
		if guard != nil {
			guard.Close()
		}
		return nil, err
	}

	c := &cache{
		opts:       resolved,
		groups:     groups,
		guard:      guard,
		namespaces: make(map[string]*namespace),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		ticker:     resolved.Clock.NewTicker(sweepInterval),
	}
	go c.sweepLoop()
	return c, nil
}

// cache is the M1 implementation of [Cache].
type cache struct {
	opts core.Options

	// groups is the actor runtime: the activation directory and every
	// group's single goroutine.
	groups *actor.Runtime

	// guard decides whether this node may activate a group. Nil on a single
	// node with no membership, where the answer is always yes.
	guard *ownership.Guard

	// ticker drives one sweeper for every namespace. Per-namespace
	// sweepers would mean a goroutine per namespace for work that is
	// entirely incremental and has no reason to be concurrent.
	ticker  clock.Ticker
	done    chan struct{}
	stopped chan struct{}

	closeOnce sync.Once

	// closed is atomic rather than guarded by mu because every namespace
	// operation consults it. Taking even a read lock on a process-wide
	// RWMutex per Get puts an atomic read-modify-write on a single shared
	// cache line in front of the whole data path, which is measurable
	// against a sub-microsecond budget and would get worse with every core.
	closed atomic.Bool

	mu         sync.RWMutex
	namespaces map[string]*namespace
	// order preserves namespace creation order so that sweeping and stats
	// are deterministic rather than map-iteration order.
	order []string
}

func (c *cache) Namespace(name string, opts ...NamespaceOption) Namespace {
	c.mu.RLock()
	ns, ok := c.namespaces[name]
	c.mu.RUnlock()
	if ok {
		return ns
	}

	resolved := core.DefaultNamespaceOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&resolved)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	// Re-check: two goroutines may have raced past the read lock. The first
	// call wins, as documented — two handles to one namespace with
	// different staleness bounds would be a data-loss bug wearing a
	// convenience-feature costume.
	if ns, ok := c.namespaces[name]; ok {
		return ns
	}
	if c.closed.Load() {
		return &namespace{cache: c, name: name, opts: resolved, err: ErrClosed}
	}

	ns = c.newNamespaceLocked(name, resolved)
	c.namespaces[name] = ns
	c.order = append(c.order, name)
	return ns
}

// newNamespaceLocked builds a namespace, capturing any configuration failure on
// the handle rather than losing it. Callers must hold mu.
func (c *cache) newNamespaceLocked(name string, opts core.NamespaceOptions) *namespace {
	ns := &namespace{cache: c, name: name, opts: opts}

	if err := opts.Validate(); err != nil {
		ns.err = err
		return ns
	}
	if opts.Codec != "" {
		// Validated here rather than in core, which cannot import codec
		// without a cycle. Catching an unregistered codec at startup is
		// the whole point of naming it in configuration.
		if _, err := codec.Get(opts.Codec); err != nil {
			ns.err = err
			return ns
		}
	}
	if opts.Compressor != "" {
		framer, err := codec.NewFramer(opts.Compressor, opts.CompressionThreshold)
		if err != nil {
			ns.err = err
			return ns
		}
		ns.framer = framer
	}

	st, err := store.New(store.Options{
		Shards: opts.Shards,
		Clock:  c.opts.Clock,
		// Sweeping is driven by the cache's single sweeper.
		SweepInterval: 0,
	})
	if err != nil {
		ns.err = err
		return ns
	}
	ns.store = st
	return ns
}

func (c *cache) Group(id string) Group {
	return &group{cache: c, id: id}
}

func (c *cache) RegisterGroupFunc(name string, fn GroupFunc) error {
	if c.closed.Load() {
		return ErrClosed
	}
	return c.groups.Register(name, fn)
}

func (c *cache) Stats() Stats {
	c.mu.RLock()
	namespaces := make([]*namespace, 0, len(c.order))
	for _, name := range c.order {
		namespaces = append(namespaces, c.namespaces[name])
	}
	c.mu.RUnlock()

	groups := c.groups.Stats()
	stats := Stats{
		Self:              Node{ID: c.opts.NodeID, Addr: c.opts.BindAddr},
		Members:           1,
		AddressableGroups: groups.Addressable,
		ActiveGroups:      groups.Active,
	}
	if c.guard != nil {
		owned := c.guard.Stats()
		stats.Epoch = owned.Epoch
		stats.Members = owned.Members
		stats.RefusedActivations = groups.RefusedActivations
		stats.DeferredActivations = owned.Deferrals
		stats.StaleEpochRefusals = owned.StaleEpochs
	}
	for _, ns := range namespaces {
		if ns.store == nil {
			continue
		}
		counters := ns.store.Stats()
		stats.OwnedKeys += counters.Entries
		stats.Hits += counters.Hits
		stats.Misses += counters.Misses
		stats.InflightLoads += ns.flight.Inflight()
		stats.Loads += ns.flight.Loads()
		stats.CollapsedLoads += ns.flight.Collapsed()
	}
	return stats
}

// sweepLoop reclaims expired entries across every namespace until Close.
func (c *cache) sweepLoop() {
	defer close(c.stopped)
	for {
		select {
		case <-c.done:
			return
		case <-c.ticker.C():
			c.sweepOnce()
		}
	}
}

// sweepOnce advances each namespace's sweep cursor by one batch.
func (c *cache) sweepOnce() {
	c.mu.RLock()
	namespaces := make([]*namespace, 0, len(c.order))
	for _, name := range c.order {
		namespaces = append(namespaces, c.namespaces[name])
	}
	c.mu.RUnlock()

	for _, ns := range namespaces {
		if ns.store != nil {
			ns.store.Sweep(sweepShardsPerTick)
		}
	}
}

func (c *cache) Close(ctx context.Context) error {
	var closeErr error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		// Set under mu so that a namespace creation racing with Close
		// either completes before the flag is visible or observes it.
		c.closed.Store(true)
		namespaces := make([]*namespace, 0, len(c.order))
		for _, name := range c.order {
			namespaces = append(namespaces, c.namespaces[name])
		}
		c.mu.Unlock()

		close(c.done)
		c.ticker.Stop()
		<-c.stopped

		// Stop watching membership before draining groups: a view change
		// arriving mid-shutdown would be applied to a runtime that is
		// already closing.
		if c.guard != nil {
			c.guard.Close()
		}

		// Groups first: an activated group holds a goroutine, and its
		// turns may still be running when Close is called.
		if err := c.groups.Close(ctx); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("closing the group runtime: %w", err))
		}

		for _, ns := range namespaces {
			if ns.store == nil {
				continue
			}
			if err := ns.store.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("closing namespace %q: %w", ns.name, err))
			}
		}
	})
	// The context is accepted for the graceful-drain semantics that arrive
	// in M6. There is nothing to drain on a single node with no ownership
	// to transfer, so an expired context changes nothing here — but it is
	// still reported, because silently succeeding on an expired deadline
	// would train callers to ignore it.
	if err := ctx.Err(); err != nil {
		return errors.Join(closeErr, err)
	}
	return closeErr
}

// isClosed reports whether Close has run. It is on the hot path of every
// namespace operation, so it must not take a lock.
func (c *cache) isClosed() bool { return c.closed.Load() }

var _ Cache = (*cache)(nil)
