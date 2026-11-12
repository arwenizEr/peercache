package peercache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/peercache/peercache/internal/actor"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/l1"
	"github.com/peercache/peercache/internal/metrics"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/replication"
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
	// CatchUp pulls the replicas this node should be holding from their
	// owners.
	//
	// A node that has restarted comes back with empty memory, and
	// replication only carries keys written after it returned — so every key
	// that was quiet when it died stays missing until somebody happens to
	// write it. A replica set silently missing its cold keys is one that
	// will fail at the moment it is needed.
	//
	// It is explicit rather than automatic on join, because a node that has
	// just started is usually also serving traffic, and pulling an entire
	// replica set competes with the requests it exists to answer. Call it
	// after readiness, during a quiet period, or not at all if the namespace
	// is reconstructible from a loader.
	//
	// It is safe to call at any time and alongside ordinary traffic:
	// incoming pages are subject to the same version check as live
	// replication, so a page that arrives after a newer write is discarded
	// rather than applied.
	CatchUp(ctx context.Context) error

	// Ready reports whether this node should receive traffic, which is a
	// different question from whether it is a cluster member.
	//
	// A node becomes an owner the instant it joins — peers recompute the
	// ranking and route to it immediately — but it holds nothing until
	// catch-up runs. In between it answers ErrNotFound for data that exists
	// elsewhere, which a caller cannot distinguish from a genuine miss.
	//
	// Wire this to a Kubernetes readiness probe. The pod stays out of the
	// service until it has joined, settled, and caught up, and leaves the
	// service as soon as Drain begins.
	// Drain hands this node's work away and stops accepting more, without
	// closing it.
	//
	// Wire this to a Kubernetes PreStop hook. It marks the node unready,
	// announces its departure so peers stop routing to it, pushes the keys
	// it owns to their new owners, and deactivates its groups — in that
	// order, because every other order leaves a window in which requests
	// arrive at a node that has already given away the ability to serve
	// them.
	//
	// It is idempotent and it is not Close: a drained cache still answers
	// peers that have not yet noticed the departure. Call Close after.
	Drain(ctx context.Context) error

	Ready() ReadyState

	// WaitReady blocks until this node is ready or ctx ends.
	WaitReady(ctx context.Context) error

	// Metrics returns this node's measurements, ready to hand to a
	// Prometheus collector or an OpenTelemetry observer. The library carries
	// no dependency on either; see the method documentation for the adapter,
	// which is about a dozen lines.
	Metrics() []Metric

	// WriteMetrics renders the measurements in the Prometheus text
	// exposition format, so a scrape endpoint needs no dependency at all.
	WriteMetrics(w io.Writer) error

	// MetricsHandler serves the same output over HTTP with the right content
	// type, which is the part everybody gets wrong once.
	MetricsHandler() http.Handler

	// RemoteLatency returns p50, p99 and p99.9 of remote operation latency
	// in nanoseconds, or zeros unless WithLatencyHistograms is set. Values
	// are bucket upper bounds, so each is an over-estimate by at most the
	// bucket width.
	RemoteLatency() (p50, p99, p999 uint64)

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

	// Probation wraps the authority before anything reads it, so that the
	// guard, catch-up, drain and readiness all see the same ranked view.
	var probation *probationAuthority
	if resolved.JoinProbation > 0 && resolved.Membership != nil {
		probation = newProbationAuthority(resolved.Membership, resolved.Clock, resolved.JoinProbation)
		resolved.Membership = probation
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
		transport:  resolved.Transport,
		namespaces: make(map[string]*namespace),
		done:       make(chan struct{}),
		stopped:    make(chan struct{}),
		ticker:     resolved.Clock.NewTicker(sweepInterval),
	}
	c.route = &router{cache: c}
	c.invalidator = newInvalidator(c)
	c.replicas = replication.NewStore()
	c.replicator = newReplicator(c)
	c.handoff = newHandoff(c)
	c.promoter = newPromoter(c)
	c.catchup = newCatchUp(c)
	c.readiness = &readiness{cache: c}
	c.pressure = newPressure(c, resolved.MemoryBudget)
	c.probation = probation
	if resolved.LatencyHistograms {
		c.remoteLatency = &metrics.Histogram{}
	}
	if resolved.Membership == nil {
		// Nothing to catch up from, so that condition is met at birth.
		c.readiness.markCaughtUp()
	}
	c.promoter.start(guard)

	// Serve before returning, so that a caller which constructs a cache and
	// immediately joins the cluster cannot be given ownership of keys it is
	// not yet able to answer for. A node that is a member but not serving
	// looks, from every peer, exactly like a node that is broken.
	if c.transport != nil {
		handler := &peerHandler{cache: c}
		served := make(chan error, 1)
		c.serving.Add(1)
		go func() {
			defer c.serving.Done()
			err := c.transport.Serve(resolved.Listener, handler)
			if err != nil && !c.isClosed() {
				resolved.Logger.Warn("peer transport stopped", "error", err)
			}
			served <- err
		}()

		// Do not return until the transport is answering, when it can
		// say so. Serve runs on its own goroutine, so without this a
		// node can be a member of the cluster — and therefore the owner
		// of part of the keyspace — before it is able to serve a single
		// request. Every peer would see that as a broken node rather
		// than a starting one.
		if r, ok := c.transport.(interface{ Ready() <-chan struct{} }); ok {
			select {
			case <-r.Ready():
			case err := <-served:
				c.serving.Wait()
				if guard != nil {
					guard.Close()
				}
				// The background workers were started above and this
				// path returns no Cache, so nothing else will ever
				// close them. Leaving them running was a leak on a
				// rare path — three goroutines per failed New — which
				// only stayed invisible because no test constructs a
				// cache whose transport refuses to serve and then
				// checks for leaks.
				c.promoter.Close()
				c.invalidator.Close()
				c.replicator.Close()
				c.handoff.Close()
				return nil, fmt.Errorf("peercache: serving peers: %w", err)
			}
		}
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

	// transport carries requests to peers. Nil without one, in which case
	// every key resolves locally and no request ever leaves this process.
	transport core.Transport

	// route resolves ownership and issues peer requests.
	route *router

	// invalidator delivers invalidation hints off the write path. See
	// invalidate.go for why they must not be sent inline.
	invalidator *invalidator

	// replicas holds copies of keys owned elsewhere, kept separately from
	// the owned store because ownership moves and the two have different
	// lifetimes. See internal/replication.
	replicas *replication.Store

	// replicator fans owned writes out to those replicas.
	replicator *replicator

	// handoff moves replicas this node has stopped ranking for to the nodes
	// that now rank for them, before dropping them. See handoff.go for the
	// data loss that happens when they are simply dropped.
	handoff *handoff

	// promoter turns replicas into owned keys when membership moves them
	// here, and hands off the ones it moves away.
	promoter *promoter

	// catchup pulls replicas a restarted node has forgotten it holds.
	catchup *catchUp

	// readiness answers whether this node should receive traffic, which is
	// a different question from whether it is a cluster member.
	readiness *readiness

	// remoteLatency records remote operation latencies when
	// WithLatencyHistograms is set. Nil otherwise, and every call site
	// checks — an unconditional method call on a nil-checked pointer is
	// cheaper than a branch inside a method that does nothing.
	remoteLatency *metrics.Histogram

	// drainRounds and keysHandedOff record graceful shutdown activity.
	drainRounds   atomic.Int64
	keysHandedOff atomic.Int64
	// keysAdopted counts keys taken directly into the owned store because
	// this node already owned them when they arrived — the hand-off path.
	keysAdopted atomic.Int64
	// implicitNamespaces counts namespaces created by an incoming peer write
	// rather than by this node's application. Each one runs with default
	// options, which means no replication.
	implicitNamespaces atomic.Int64

	// splitBrain counts conflicts this node detected as a replica, as
	// distinct from the ones its own fan-out was told about.
	splitBrain atomic.Int64

	// epochSkew counts requests whose caller resolved ownership under a
	// different membership epoch. Expected and harmless during churn; a
	// sustained rate under stable membership means views are not settling.
	epochSkew atomic.Int64

	// degradedWrites counts synchronous writes acknowledged with fewer copies
	// than the namespace's replication factor asked for. See open question 37.
	degradedWrites atomic.Int64

	// underReplicated counts synchronous writes refused because fewer copies
	// than the factor could be placed. See WithRequireReplicationFactor.
	underReplicated atomic.Int64

	// pressure enforces the memory budget. Never nil; with no budget it
	// does nothing.
	pressure *pressure

	// probation hides young members from the ranking. Nil without
	// WithJoinProbation.
	probation *probationAuthority

	// serving tracks the goroutine running Transport.Serve, so that Close
	// can wait for it rather than leaving it to notice on its own. M3 lost a
	// milestone to a Close that only signalled; this one waits.
	serving sync.WaitGroup

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

	// The non-owned tier only exists once there is somewhere else for a key
	// to live. Without a membership authority this node owns the whole
	// keyspace, and caching a copy of a key it already holds would add a
	// staleness window for nothing.
	// A negative L1 TTL means the caller wants no non-owned tier at all.
	if c.guard != nil && opts.L1TTL > 0 {
		cache, err := l1.New(l1.Options{
			Clock:  c.opts.Clock,
			MaxTTL: opts.L1TTL,
		})
		if err != nil {
			ns.err = err
			return ns
		}
		ns.l1 = cache
	}
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
		// Outside the guard block below: a single node with no membership
		// still has groups, still has mailboxes, and can still saturate one.
		MailboxRejections: groups.Rejections,
		StateEscapes:      groups.Escapes,
		// Also outside it, for the same reason. A single node asked for three
		// copies is the plainest case of a write below its factor; inside the
		// block, the counter rose and Stats reported zero.
		DegradedWrites:          c.degradedWrites.Load(),
		UnderReplicatedRefusals: c.underReplicated.Load(),
		ReplicasCurrent:         c.replicasCurrent(),
	}
	stats.MemoryUsed, stats.MemoryBudget, stats.ShedL1, stats.ShedReplicas, stats.ShedOwned = c.pressure.counters()
	if c.probation != nil {
		stats.MembersOnProbation = c.probation.onProbation()
	}
	if c.replicas != nil {
		stats.VersionConflicts = c.replicas.VersionConflicts()
	}
	if c.guard != nil {
		owned := c.guard.Stats()
		stats.Epoch = owned.Epoch
		stats.Members = owned.Members
		stats.RefusedActivations = groups.RefusedActivations
		stats.DeferredActivations = owned.Deferrals
		stats.StaleEpochRefusals = owned.StaleEpochs
		stats.EpochSkew = c.epochSkew.Load()
		sent, dropped, failed := c.invalidator.counters()
		stats.InvalidationsSent = sent
		stats.InvalidationsDropped = dropped
		stats.InvalidationsFailed = failed
		rsent, rdropped, rfailed, conflicts := c.replicator.counters()
		stats.ReplicaKeys = int64(c.replicas.Len())
		stats.ReplicationsSent = rsent
		stats.ReplicationsDropped = rdropped
		stats.ReplicationsFailed = rfailed
		stats.SplitBrainDetections = conflicts + c.splitBrain.Load()
		promoted, evicted, rounds := c.promoter.counters()
		stats.KeysPromoted = promoted
		stats.ReplicasEvicted = evicted
		stats.PromotionRounds = rounds
		pulled, cruns, cfailures := c.catchup.counters()
		stats.ReplicasPulled = pulled
		stats.CatchUpRuns = cruns
		stats.CatchUpFailures = cfailures
		stats.KeysHandedOff = c.keysHandedOff.Load()
		stats.KeysAdopted = c.keysAdopted.Load()
		stats.ImplicitNamespaces = c.implicitNamespaces.Load()
		rehomed, returned := c.handoff.counters()
		stats.ReplicasRehomed = rehomed
		stats.ReplicasRetained = returned
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
		if ns.l1 != nil {
			stats.L1Entries += int64(ns.l1.Len())
		}
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
			// Reconcile on the same tick.
			//
			// Reconciliation is normally driven by membership
			// changes, which is right and enough for the cases M5
			// tested. It is not enough for a hand-off: a draining
			// node announces its departure *before* transferring, so
			// the receivers' membership change has already happened
			// when the keys arrive. If the sender is then killed,
			// there is no further change to react to — the peers
			// already know it is gone — and the keys sit in the
			// replica store, owned by a node that is not looking for
			// them.
			//
			// That lost 53 of 300 keys in a graceful drain at
			// replication factor 3, found by examples/m6 rather than
			// by any test, because the tests exercised the two
			// orderings where a change happened to follow.
			//
			// A periodic pass is the safety net. Reconciliation is
			// idempotent and costs a map scan when there is nothing
			// to do, which is the common case.
			if c.promoter != nil {
				c.promoter.reconcile()
			}
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
	// Replicas expire on the owner's schedule, and nothing else reclaims
	// them: an expiry is not replicated.
	c.replicas.SweepExpired(c.opts.Clock.Now().UnixNano())
	// The budget is checked here as well as on writes, because the L1 tier
	// grows on reads and a read path has no business summing counters.
	c.pressure.check()
}

func (c *cache) Close(ctx context.Context) error {
	ctx = ctxOrBackground(ctx)
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
		if c.probation != nil {
			c.probation.Close()
		}
		// Then the invalidation queue, which holds a reference to the
		// cache and dials peers.
		if c.invalidator != nil {
			c.invalidator.Close()
		}
		if c.promoter != nil {
			c.promoter.Close()
		}
		if c.replicator != nil {
			c.replicator.Close()
		}
		// After the promoter, which is what feeds it: closing the
		// queue's producer first means nothing is enqueued into a
		// worker that has stopped reading.
		if c.handoff != nil {
			c.handoff.Close()
		}
		if c.transport != nil {
			if err := c.transport.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("closing the transport: %w", err))
			}
			c.serving.Wait()
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
