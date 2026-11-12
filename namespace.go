package peercache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/l1"
	"github.com/peercache/peercache/internal/singleflight"
	"github.com/peercache/peercache/internal/store"
)

// Namespace is a handle to a named keyspace (SPEC.md section 3.1).
//
// Values are opaque bytes at this layer; encoding is the codec's business. Keys
// map to groups through the namespace's [Partitioner], and a group maps to
// exactly one owning node, so every operation below either executes locally or
// is forwarded to that owner. The caller cannot tell which, except by latency.
//
// Every method takes a context and honours its deadline. None of them block
// indefinitely.
//
// # Aliasing
//
// [Namespace.Set] copies the value it is given, so a caller may reuse the
// buffer. [Namespace.Get] returns the stored slice without copying, so a caller
// must not modify it. See docs/decisions/0007-get-returns-the-stored-slice.md.
type Namespace interface {
	// Get returns the value for key, or [ErrNotFound]. A missing key never
	// returns a nil value, so that a key holding zero bytes stays
	// distinguishable from a key holding nothing.
	//
	// If the key is not owned locally, the value may come from an L1 copy
	// and be stale by up to the namespace's L1 TTL.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores val under key for ttl. A non-positive ttl means the value
	// does not expire on its own; it can still be lost, because nothing
	// here is durable.
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error

	// Delete removes key. Deleting a key that does not exist is not an
	// error: the postcondition the caller wanted already holds.
	Delete(ctx context.Context, key string) error

	// Exists reports whether key is present without transferring its value.
	// It is not a race-free precondition for anything, and it is a mistake
	// to use it as one.
	Exists(ctx context.Context, key string) (bool, error)

	// BatchGet returns the values for keys, omitting keys that are absent.
	// Keys are grouped by owner and issued as one request per owner, so
	// cost scales with the number of distinct owners rather than the number
	// of keys.
	//
	// A partial failure returns the values that were retrieved along with a
	// non-nil error.
	BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)

	// BatchSet stores every pair in kv with the same ttl. Each pair goes to
	// its key's owner exactly as Set would — routed, replicated and
	// invalidated — one request per key; unlike BatchGet it is not yet
	// coalesced into one request per owner. It is not atomic: a failure
	// returns the first error, and the pairs written before it stay written.
	// There is no cross-group transaction that would make it otherwise.
	BatchSet(ctx context.Context, kv map[string][]byte, ttl time.Duration) error

	// GetOrLoad returns the value for key, invoking the namespace's
	// registered loader on the owner if it is absent.
	//
	// There is no loader argument by design: the loader is registered at
	// namespace construction so that it exists by name on every pod and the
	// owner can run it without shipping the miss back to the caller.
	// Concurrent misses for one key collapse into a single invocation.
	// Without [WithLoader], this returns [ErrNoLoader].
	GetOrLoad(ctx context.Context, key string) ([]byte, error)
}

// namespace is the M1 implementation of [Namespace].
//
// err, when non-nil, is a configuration failure captured at construction —
// [Cache.Namespace] cannot return an error, and reporting the failure from
// every operation is the least surprising alternative to ignoring it.
type namespace struct {
	cache *cache
	name  string
	opts  core.NamespaceOptions
	err   error

	// valueCodec encodes typed values: WithCodec's, or DefaultTypedCodec.
	valueCodec codec.Codec
	// valueType is the T of the first NamespaceOf that opened this
	// namespace, guarded by typeMu. One name holds one type's encoding.
	typeMu    sync.Mutex
	valueType reflect.Type

	store  *store.Store
	framer *codec.Framer
	flight singleflight.Group

	// l1 caches copies of keys this node does not own. Nil when there is no
	// membership authority, because a cluster of one owns everything and a
	// second copy of a local key would only be a way to serve it stale.
	//
	// Built in M1 and connected here in M4, which is when the first
	// non-owned read exists — see docs/decisions/open.md question 13.
	l1 *l1.Cache
}

// status returns the error every operation on this handle should report, or nil
// if the handle is usable.
//
// Ordering matters: a closed cache is reported ahead of a configuration
// problem, because after Close nothing about configuration can still be acted
// on.
func (n *namespace) status() error {
	if n.cache.isClosed() {
		return ErrClosed
	}
	if n.err != nil {
		return n.err
	}
	return nil
}

// groupKeyFor maps a key to the group key that determines its owner.
//
// This is where WithPartitioner takes effect. Grouping related keys under one
// group key is what lets a caller keep a customer's orders on one node and
// compute over them in place, which is the point of the partitioner and the
// reason it was accepted and validated back in M1 while doing nothing.
func (n *namespace) groupKeyFor(key string) string {
	if n.opts.Partitioner == nil {
		return key
	}
	return n.opts.Partitioner.GroupKey(key)
}

// encode prepares a value for storage.
// bindValueType records the type a typed view holds, or reports that this
// namespace already holds another.
func (n *namespace) bindValueType(t reflect.Type) error {
	n.typeMu.Lock()
	defer n.typeMu.Unlock()
	if n.valueType == nil {
		n.valueType = t
		return nil
	}
	if n.valueType != t {
		return fmt.Errorf("peercache: namespace %q holds %v and was opened as %v: %w",
			n.name, n.valueType, t, ErrInvalidOption)
	}
	return nil
}

func (n *namespace) encode(val []byte) []byte {
	if n.framer == nil {
		return val
	}
	return n.framer.Frame(val)
}

// decode reverses encode. It returns a slice that may alias the stored value.
func (n *namespace) decode(stored []byte) ([]byte, error) {
	if n.framer == nil {
		return stored, nil
	}
	out, err := codec.Unframe(stored)
	if err != nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, err)
	}
	return out, nil
}

func (n *namespace) Get(ctx context.Context, key string) ([]byte, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}

	p, err := n.cache.route.locate(n.groupKeyFor(key))
	if err != nil {
		return nil, err
	}
	if p.local {
		return n.localGet(key)
	}
	return n.remoteGet(ctx, p, key, true)
}

// localGet reads from this node's owned store.
func (n *namespace) localGet(key string) ([]byte, error) {
	stored, _, ok := n.store.Get(key)
	if !ok {
		// The bare sentinel, not a wrapped one. A miss is routine — it is
		// most of what GetOrLoad and BatchGet see — and formatting the
		// namespace and key into a message costs four allocations and
		// over a hundred bytes on a path that is supposed to be free.
		// The caller already knows which namespace and key it asked for.
		return nil, ErrNotFound
	}
	return n.decode(stored)
}

// remoteGet serves a key owned elsewhere, from the non-owned tier if a fresh
// copy is there and from the owner otherwise.
//
// retry allows exactly one re-resolution. Ownership can change between
// resolving an owner and that owner receiving the request, and the caller
// should not see a routine race as a failure. It does not loop: a second
// disagreement means membership is genuinely unsettled, and retrying into that
// turns a brownout into a storm.
func (n *namespace) remoteGet(ctx context.Context, p placement, key string, retry bool) ([]byte, error) {
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return n.decode(val)
		}
	}

	value, stamp, err := n.cache.route.remoteGet(ctx, p, n.name, key)
	if err != nil {
		if retry && retryable(err) {
			next, rerr := n.cache.route.locate(n.groupKeyFor(key))
			if rerr != nil {
				return nil, err
			}
			if next.local {
				return n.localGet(key)
			}
			return n.remoteGet(ctx, next, key, false)
		}
		return nil, err
	}

	// Cache the copy under this namespace's L1 bound. The owner may shorten
	// that bound but never extend it, so the ceiling configured here is the
	// one that holds — which is what makes the TTL a real limit on staleness
	// rather than a suggestion the owner can override.
	if n.l1 != nil {
		n.l1.Put(key, value, stamp, n.opts.L1TTL)
	}
	return n.decode(value)
}

func (n *namespace) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}

	p, err := n.cache.route.locate(n.groupKeyFor(key))
	if err != nil {
		return err
	}
	if !p.local {
		if err := n.cache.route.remoteSet(ctx, p, n.name, key, val, ttl); err != nil {
			if !retryable(err) {
				return err
			}
			next, rerr := n.cache.route.locate(n.groupKeyFor(key))
			if rerr != nil || (!next.local && next.owner.ID == p.owner.ID) {
				return err
			}
			if next.local {
				return n.localSet(ctx, key, val, ttl)
			}
			return n.cache.route.remoteSet(ctx, next, n.name, key, val, ttl)
		}
		// Drop any copy this node was holding. It is now known stale,
		// and keeping it would mean this node serves its own write back
		// as the old value.
		if n.l1 != nil {
			n.l1.Invalidate(key)
		}
		return nil
	}
	return n.localSet(ctx, key, val, ttl)
}

// localSet writes to this node's owned store, replicates it and tells the
// cluster.
//
// The order is deliberate. The value is stored first, so a replica that answers
// instantly cannot report a version this node does not yet hold. Replication
// comes next, because it is the part a synchronous caller is waiting for. The
// invalidation hint goes last and is never waited on.
func (n *namespace) localSet(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	version := n.store.Set(key, n.encode(val), ttl)
	stamp := n.stamp(version)
	n.cache.pressure.noteWrite(len(val))

	if err := n.fanOut(ctx, key, n.encode(val), ttl, stamp, false); err != nil {
		// Only synchronous replication can fail here: asynchronous
		// fan-out is queued and returns nil. A synchronous caller asked
		// to know, so it is told — and the value is already stored
		// locally, which is why this reports rather than rolls back.
		// There is no rollback in this system and inventing one for a
		// single key would be a transaction by another name.
		return err
	}
	n.cache.invalidator.enqueue(n.name, []string{key}, stamp)
	return nil
}

// fanOut sends a write or a deletion to this key's replicas.
func (n *namespace) fanOut(ctx context.Context, key string, encoded []byte, ttl time.Duration, stamp core.Stamp, deleted bool) error {
	// The replication factor is checked once, inside replicaTargets, which
	// is the only place that can act on it. Checking here as well would be
	// two conditions that must agree forever, and a mutation audit showed
	// exactly what that buys: removing either one changes nothing, so
	// neither is tested.
	targets, _, err := n.cache.replicaTargets(n.groupKeyFor(key), n.opts.ReplicationFactor)
	if err != nil || len(targets) == 0 {
		// No resolvable replicas is not a write failure by default. It
		// means this node is alone, or membership is not settled, and
		// refusing the write would make a single-node cluster unable to
		// accept one. A namespace that asked for its factor to be honoured
		// gets that refusal anyway; it chose it.
		if n.underFactor(len(targets)) {
			return n.refuseUnderFactor(len(targets))
		}
		n.countDegraded(len(targets))
		return nil
	}
	if err := n.cache.replicator.replicate(ctx, n.opts.SyncReplication, replicaWork{
		namespace: n.name,
		key:       key,
		value:     encoded,
		ttl:       ttl,
		stamp:     stamp,
		deleted:   deleted,
		targets:   targets,
	}); err != nil {
		return err
	}
	// The copies that could be placed are placed before refusing, so a
	// refused write is held as widely as this node's view allowed rather
	// than only by the owner. An unreachable replica fails the write above
	// and takes precedence: it is the more specific diagnosis.
	if n.underFactor(len(targets)) {
		return n.refuseUnderFactor(len(targets))
	}
	n.countDegraded(len(targets))
	return nil
}

// underFactor reports whether a synchronous write that placed `placed` replica
// copies must be refused, because its namespace requires the factor and the
// writer's own copy plus those does not reach it.
func (n *namespace) underFactor(placed int) bool {
	return n.opts.RequireReplicationFactor && n.opts.SyncReplication &&
		1+placed < n.opts.ReplicationFactor
}

// refuseUnderFactor counts and reports a write refused by underFactor.
func (n *namespace) refuseUnderFactor(placed int) error {
	n.cache.underReplicated.Add(1)
	return fmt.Errorf("peercache: namespace %q: %d of %d copies placed (this node sees %d members): %w",
		n.name, 1+placed, n.opts.ReplicationFactor, n.cache.memberCount(), ErrUnderReplicated)
}

// countDegraded records a synchronous write that was acknowledged holding fewer
// copies than the namespace's replication factor asked for.
//
// # What it is counting
//
// Replication targets are the runners-up in the ranking over the *writer's own
// membership view*. A node that has been cut off from the cluster eventually
// detects every peer as failed, its view shrinks to itself, it ranks itself the
// owner of everything, and synchronous replication then places every copy it can
// reach — none — and returns nil. The caller asked for three copies, got one,
// and cannot tell.
//
// That is open question 37, and this counter is its first option: make the
// window visible without changing what any data path does. A caller that reads
// `Set` returning nil as "RF nodes have this" is wrong during a partition, and
// until now nothing said so. Now `Stats().DegradedWrites` and
// `peercache_degraded_writes_total` do.
//
// # Why it is only counted for synchronous writes
//
// Asynchronous replication makes no promise about when a copy lands, so every
// asynchronous write is momentarily below its replication factor by design —
// the window is measured in docs/benchmarks.md rather than counted here.
// Counting them would produce a metric that equals the write rate and means
// nothing. The synchronous promise is the one a caller can be misled by.
//
// # Why the partial-failure case does not reach here
//
// Under synchronous replication a target that refuses or cannot be reached makes
// `fanOut` return an error, so the write is not acknowledged at all. The only
// way to be acknowledged below the factor is for the targets to be missing from
// the ranking in the first place, which is exactly the shrunken-view case.
func (n *namespace) countDegraded(placed int) {
	if !n.opts.SyncReplication || n.opts.ReplicationFactor < 2 {
		return
	}
	// The writer holds one copy itself; `placed` is the rest.
	if 1+placed >= n.opts.ReplicationFactor {
		return
	}
	// Logged once per node, not once per write. A partition degrades every
	// write for as long as it lasts, and a log line per write turns an
	// operational signal into an outage of its own. The counter carries the
	// rate; this line exists so the first one is not silent.
	if n.cache.degradedWrites.Add(1) == 1 {
		n.cache.opts.Logger.Warn(
			"synchronous write acknowledged below its replication factor",
			"namespace", n.name,
			"replication_factor", n.opts.ReplicationFactor,
			"copies", 1+placed,
			"members", n.cache.memberCount(),
			"detail", "this node's membership view is too small to place the copies its namespace asks for; see Stats().DegradedWrites and peercache_degraded_writes_total")
	}
}

// stamp pairs a store version with this node's epoch. See [core.Stamp] for why
// the version alone will not do.
func (n *namespace) stamp(version uint64) core.Stamp {
	var epoch uint64
	if n.cache.guard != nil {
		epoch = uint64(n.cache.guard.Stats().Epoch)
	}
	return core.Stamp{Epoch: epoch, Version: version}
}

func (n *namespace) Delete(ctx context.Context, key string) error {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}

	p, err := n.cache.route.locate(n.groupKeyFor(key))
	if err != nil {
		return err
	}
	if !p.local {
		if err := n.cache.route.remoteDelete(ctx, p, n.name, key); err != nil {
			return err
		}
		if n.l1 != nil {
			n.l1.Invalidate(key)
		}
		return nil
	}

	return n.localDelete(ctx, key)
}

// localDelete removes an owned key, replicates the removal and tells the
// cluster.
//
// It is shared with the peer handler so that a deletion arriving over the
// network does exactly what a local one does. Two write paths that differ is
// how the first version ended up replicating only the writes that happened to
// start on their owner.
func (n *namespace) localDelete(ctx context.Context, key string) error {
	n.store.Delete(key)
	// A deletion invalidates every cached copy. The stamp carries this
	// node's epoch with a version above anything the deleted entry could
	// have held, so a peer holding an older copy drops it; a peer that has
	// somehow already seen a newer write keeps that instead.
	tombstone := n.stamp(math.MaxUint64)
	// Deletions replicate too. A replica that missed one would resurrect
	// the key the moment it was promoted, which is worse than never having
	// replicated it at all.
	if err := n.fanOut(ctx, key, nil, 0, tombstone, true); err != nil {
		return err
	}
	n.cache.invalidator.enqueue(n.name, []string{key}, tombstone)
	if n.l1 != nil {
		n.l1.Invalidate(key)
	}
	return nil
}

func (n *namespace) Exists(ctx context.Context, key string) (bool, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := n.status(); err != nil {
		return false, err
	}

	p, err := n.cache.route.locate(n.groupKeyFor(key))
	if err != nil {
		return false, err
	}
	if p.local {
		return n.store.Exists(key), nil
	}

	// No dedicated RPC: existence is a Get whose value is discarded. Adding
	// a method to save the payload would buy little — the answer is a miss
	// or a value that a caller checking existence usually wants next — and
	// would add a wire method that must then be kept in step forever.
	_, err = n.remoteGet(ctx, p, key, true)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):
		return false, nil
	default:
		return false, err
	}
}

// BatchGet groups keys by owner and issues one request per owner.
//
// This is the whole reason the method exists. A thousand keys spread over three
// nodes cost three round trips, not a thousand — and the grouping is the same
// decision the partitioner already makes, so keys deliberately placed together
// travel together.
//
// A partial failure returns what was collected alongside the error. A caller
// that can use half an answer should not be denied it because one owner was
// slow, and a caller that cannot will check the error.
func (n *namespace) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}

	out := make(map[string][]byte, len(keys))
	byOwner, unresolved, err := n.cache.route.groupKeysByOwner(n, keys)
	if err != nil {
		return out, err
	}

	var failures error
	if len(unresolved) > 0 {
		failures = fmt.Errorf("peercache: %d key(s) have no resolvable owner: %w",
			len(unresolved), ErrNotFound)
	}

	for id, batch := range byOwner {
		if err := ctx.Err(); err != nil {
			return out, errors.Join(failures, err)
		}
		if id == localBucket {
			if err := n.localBatchGet(ctx, batch.keys, out); err != nil {
				failures = errors.Join(failures, err)
			}
			continue
		}
		if err := n.remoteBatchGet(ctx, batch, out); err != nil {
			// One unreachable owner does not invalidate the keys
			// that did come back.
			failures = errors.Join(failures, err)
		}
	}
	return out, failures
}

// localBatchGet reads the keys this node owns straight from its store.
func (n *namespace) localBatchGet(ctx context.Context, keys []string, out map[string][]byte) error {
	for i, key := range keys {
		// A large batch must not outlive the caller's deadline. Checking
		// periodically rather than per key keeps the check off the hot
		// path while still bounding the overrun.
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		stored, _, ok := n.store.Get(key)
		if !ok {
			// Absent keys are omitted, not reported. A batch read
			// where half the keys are cold is the normal case, not
			// an error.
			continue
		}
		val, err := n.decode(stored)
		if err != nil {
			return err
		}
		out[key] = val
	}
	return nil
}

// remoteBatchGet fetches one owner's share, serving whatever the non-owned tier
// already holds and asking only for the rest.
func (n *namespace) remoteBatchGet(ctx context.Context, batch *ownerBatch, out map[string][]byte) error {
	wanted := batch.keys
	if n.l1 != nil {
		wanted = wanted[:0:0]
		for _, key := range batch.keys {
			if val, _, ok := n.l1.Get(key); ok {
				decoded, err := n.decode(val)
				if err != nil {
					return err
				}
				out[key] = decoded
				continue
			}
			wanted = append(wanted, key)
		}
	}
	if len(wanted) == 0 {
		return nil
	}

	entries, err := n.cache.route.remoteBatchGet(ctx, batch.placement, n.name, wanted)
	if err != nil {
		return err
	}
	for _, e := range entries {
		val, err := n.decode(e.Value)
		if err != nil {
			return err
		}
		out[e.Key] = val
		if n.l1 != nil {
			n.l1.Put(e.Key, e.Value, e.Stamp, n.opts.L1TTL)
		}
	}
	return nil
}

func (n *namespace) BatchSet(ctx context.Context, kv map[string][]byte, ttl time.Duration) error {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}

	// Through Set, so that each pair is routed to its owner, replicated,
	// invalidated and counted against the memory budget. Writing straight
	// into this node's store, as an earlier version did, put every key owned
	// elsewhere where no read would look for it.
	for key, val := range kv {
		if err := n.Set(ctx, key, val, ttl); err != nil {
			// Partial application is visible. There is no cross-key
			// atomicity here and never will be.
			return err
		}
	}
	return nil
}

// GetOrLoad returns the value for key, reconstructing it on its owner if it is
// absent.
//
// The loader runs on the owner and nowhere else. That is the whole reason
// SPEC.md §3.1 registers loaders by name rather than passing them per call: if
// every node ran its own, a cold key would send one request per pod to the
// database behind it, which is the stampede single-flighting exists to prevent,
// reintroduced at cluster scale.
func (n *namespace) GetOrLoad(ctx context.Context, key string) ([]byte, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}
	// Checked before routing. The loader is namespace configuration, the
	// same on every pod, so a namespace without one has no loader anywhere —
	// and asking the owner anyway came back as "not found", which told the
	// caller the key did not exist when what was missing was the loader. A
	// test of the remote path found that; the local path always said
	// ErrNoLoader.
	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	p, err := n.cache.route.locate(n.groupKeyFor(key))
	if err != nil {
		return nil, err
	}
	if p.local {
		return n.localGetOrLoad(ctx, key)
	}
	return n.remoteGetOrLoad(ctx, p, key, true)
}

// localGetOrLoad is the single-node path: resident, or loaded under the flight.
func (n *namespace) localGetOrLoad(ctx context.Context, key string) ([]byte, error) {
	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	if stored, _, ok := n.store.Get(key); ok {
		return n.decode(stored)
	}

	result := n.flight.Do(ctx, key, func(loadCtx context.Context) ([]byte, time.Duration, error) {
		// Re-check under the flight. Between the miss above and becoming
		// the leader, a previous load may have completed and populated
		// the key; loading it again would be a pointless round trip to
		// whatever the loader talks to.
		if stored, _, ok := n.store.Get(key); ok {
			val, err := n.decode(stored)
			return val, 0, err
		}

		// The load is bounded by the namespace's timeout, not by
		// whichever caller happened to arrive first. loadCtx already has
		// that caller's cancellation stripped; without a deadline of our
		// own it would have none at all.
		loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()

		val, ttl, err := n.opts.Loader(loadCtx, key)
		if err != nil {
			return nil, 0, err
		}
		// Store inside the flight so that every collapsed caller sees a
		// populated namespace, not just the leader.
		n.store.Set(key, n.encode(val), ttl)
		return val, ttl, nil
	})
	if result.Err != nil {
		return nil, fmt.Errorf("namespace %q key %q: %w", n.name, key, result.Err)
	}
	return result.Value, nil
}

// remoteGetOrLoad asks the owner, which loads if it must.
//
// A cached non-owned copy answers without asking. That is safe for the same
// reason a plain remote Get is: the copy is TTL-bounded, so the staleness it
// can introduce is the documented bound rather than an unbounded one.
func (n *namespace) remoteGetOrLoad(ctx context.Context, p placement, key string, retry bool) ([]byte, error) {
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return n.decode(val)
		}
	}

	value, stamp, err := n.cache.route.remoteLoad(ctx, p, n.name, key)
	if err != nil {
		if retry && retryable(err) {
			next, rerr := n.cache.route.locate(n.groupKeyFor(key))
			if rerr != nil {
				return nil, err
			}
			if next.local {
				return n.localGetOrLoad(ctx, key)
			}
			return n.remoteGetOrLoad(ctx, next, key, false)
		}
		return nil, err
	}
	if n.l1 != nil {
		n.l1.Put(key, value, stamp, n.opts.L1TTL)
	}
	return n.decode(value)
}

var _ Namespace = (*namespace)(nil)
