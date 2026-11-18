package peercache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/codec"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/hotkeys"
	"github.com/arwenizEr/peercache/internal/l1"
	"github.com/arwenizEr/peercache/internal/singleflight"
	"github.com/arwenizEr/peercache/internal/store"
	"github.com/cespare/xxhash/v2"
)

// Namespace is a handle to a named keyspace.
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
//
// Namespace is implemented by this module only and may gain methods in a minor
// release (docs/decisions/0042-what-v1-promises.md). A test that needs one
// should run a real cache with package peercachetest rather than fake this
// interface.
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

	// SetIfAbsent stores val under key for ttl unless a live value is
	// already there, and reports whether it stored. The check and the write
	// happen together on the key's owner, so of many concurrent callers
	// exactly one stores.
	//
	// Like CompareAndSwap and Increment it is not idempotent. An error other
	// than a refusal — ErrNodeUnreachable, an expired deadline — may come
	// after the owner applied it, and retrying could then report false for
	// a value this caller stored. ErrNotOwner, ErrStaleEpoch and
	// ErrActivationPending are refusals; the cache retries those once itself.
	SetIfAbsent(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error)

	// CompareAndSwap stores new under key for ttl only if key currently holds
	// exactly old, and reports whether it did. A missing or expired key never
	// matches; use SetIfAbsent to create one. Values are compared as the
	// bytes Get would return, whatever the namespace's compression.
	//
	// Not idempotent; see SetIfAbsent for what that means for errors.
	CompareAndSwap(ctx context.Context, key string, old, new []byte, ttl time.Duration) (bool, error)

	// Increment adds delta to the integer stored under key and returns the
	// result. The value is a base-10 integer, so Get reads it as text and
	// Set can initialise it. A missing key counts as zero and is created
	// with ttl; an existing key keeps its deadline. A value that is not an
	// integer, or a sum that overflows int64, is ErrNotAnInteger and changes
	// nothing.
	//
	// Not idempotent: an error other than a refusal may come after the owner
	// applied the increment, and retrying would apply it twice. See
	// SetIfAbsent. Like every write it is as durable as the namespace's
	// replication makes it — an asynchronously replicated counter can lose
	// increments acknowledged inside the replication window.
	Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error)

	// Touch gives key a new lifetime of ttl from now without changing its
	// value, and reports whether the key existed. A non-positive ttl removes
	// its deadline. It is safe to retry.
	Touch(ctx context.Context, key string, ttl time.Duration) (bool, error)

	// BatchGet returns the values for keys, omitting keys that are absent.
	// Keys are grouped by owner and issued as one request per owner, so
	// cost scales with the number of distinct owners rather than the number
	// of keys.
	//
	// A partial failure returns the values that were retrieved along with a
	// non-nil error.
	BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)

	// BatchGetOrLoad is BatchGet that loads, on each key's owner, the keys
	// that owner does not hold: one request per owner, and one call to the
	// namespace's WithBatchLoader per owner, or one WithLoader call per
	// missing key without it. Keys that do not exist are omitted. A partial
	// failure returns what was retrieved with a non-nil error. There is no
	// atomicity across keys. Without any loader it reports ErrNoLoader.
	BatchGetOrLoad(ctx context.Context, keys []string) (map[string][]byte, error)

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

// namespace implements [Namespace].
//
// err, when non-nil, is a configuration failure captured at construction —
// [Cache.Namespace] cannot return an error, and reporting the failure from
// every operation is the least surprising alternative to ignoring it.
type namespace struct {
	cache *cache
	name  string
	opts  core.NamespaceOptions
	err   error

	// warnedOptions is set once a call to Namespace has been warned about
	// for options this namespace ignored, so that the warning is logged
	// once rather than on every call of a hot path.
	warnedOptions atomic.Bool

	// valueCodec encodes typed values: WithCodec's, or DefaultTypedCodec.
	valueCodec codec.Codec
	// valueType is the T of the first NamespaceOf that opened this
	// namespace, guarded by typeMu. One name holds one type's encoding.
	typeMu    sync.Mutex
	valueType reflect.Type

	store  *store.Store
	framer *codec.Framer
	flight singleflight.Group
	// readFlight coalesces concurrent L1 misses of one key owned elsewhere
	// into one fetch. It is separate from flight on purpose: a Get must
	// never join a GetOrLoad's load, nor a GetOrLoad a Get's plain read.
	readFlight singleflight.Group
	// refreshing holds the keys with a refresh in flight, so that hits
	// inside the window start one reload, not one goroutine each.
	refreshing sync.Map
	// refreshWG tracks refresh goroutines, so Close neither leaks one nor
	// closes the store under it.
	refreshWG sync.WaitGroup
	// refreshMu orders starting a refresh against Close: a goroutine
	// added to refreshWG after Close began waiting would be a misuse of
	// the WaitGroup and a refresh writing into a closed store.
	refreshMu     sync.Mutex
	refreshClosed bool
	// refreshes and refreshFailures count completed and failed reloads.
	refreshes       atomic.Int64
	refreshFailures atomic.Int64
	// hot estimates the most accessed keys, with WithHotKeyTracking.
	hot *hotkeys.Tracker
	// demotedEpoch is the membership epoch whose ownership the owned store
	// was last demoted to; see promoter.demote.
	demotedEpoch atomic.Uint64
	// promotedEpoch is the membership epoch under which this namespace's
	// replicas were last scanned for promotion; see promoter.reconcileWith.
	promotedEpoch atomic.Uint64
	// negativeHits counts GetOrLoad calls answered from a remembered
	// absence; see [WithNegativeCaching].
	negativeHits atomic.Int64

	// l1 caches copies of keys this node does not own. Nil when there is no
	// membership authority, because a cluster of one owns everything and a
	// second copy of a local key would only be a way to serve it stale.
	//
	// See docs/decisions/open.md question 13.
	l1 *l1.Cache

	// persister writes this namespace's owned changes behind to its blob
	// store. Nil unless the namespace was opened with WithPersistence.
	persister *persister
	// shedHeld reports a key the memory budget shed and storage holds;
	// nil without persistence. Kept as a field so that passing it to the
	// store allocates nothing.
	shedHeld func(key string) bool
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
// reason it is validated at construction.
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
		return n.localGet(ctx, key)
	}
	return n.remoteGet(ctx, p, key, true)
}

// localGet reads from this node's owned store.
func (n *namespace) localGet(ctx context.Context, key string) ([]byte, error) {
	stored, _, _, ok, err := n.lookupResident(ctx, key)
	if err != nil {
		return nil, err
	}
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
	// What crosses the network, and what the L1 tier holds, is the plain
	// value: the owner unframes it before answering. Only the owned and
	// replica stores hold the framed form. Decoding here again, as an earlier
	// version did, failed every remote read of a compressed namespace.
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return val, nil
		}
	}

	value, err := n.sharedFetch(ctx, p, key)
	if err != nil {
		if retry && retryable(err) {
			next, rerr := n.cache.route.locate(n.groupKeyFor(key))
			if rerr != nil {
				return nil, err
			}
			if next.local {
				return n.localGet(ctx, key)
			}
			return n.remoteGet(ctx, next, key, false)
		}
		return nil, err
	}
	return value, nil
}

// sharedFetchAttempts bounds how many times a caller joins a shared fetch that
// ended on someone else's deadline before it fetches alone.
const sharedFetchAttempts = 2

// sharedFetch reads key from its owner, sharing one request among every caller
// on this node that missed L1 for it at the same time.
//
// Without this a hot key's L1 expiry, or the invalidation of it, sent one RPC
// per waiting goroutine to the owner — a thousand callers were a thousand
// requests (measured; docs/decisions/0028). The shared answer is the owner's
// reply to a request sent after every waiter missed, so it is as fresh as any
// of them would have got alone.
//
// # Whose deadline
//
// The fetch runs under the deadline of the caller that started it, because
// deadlines propagate across RPC boundaries and a fetch with none could hang a
// waiter forever. That makes one caller's deadline a risk to the others, so a
// waiter whose own context is still alive when the shared fetch ends on a
// context error does not inherit the failure: it tries again, and after
// sharedFetchAttempts it fetches alone under its own deadline.
func (n *namespace) sharedFetch(ctx context.Context, p placement, key string) ([]byte, error) {
	for attempt := 0; attempt < sharedFetchAttempts; attempt++ {
		res := n.readFlight.Do(ctx, key, func(fetchCtx context.Context) ([]byte, time.Duration, error) {
			if deadline, ok := ctx.Deadline(); ok {
				var cancel context.CancelFunc
				fetchCtx, cancel = clock.WithDeadline(fetchCtx, n.cache.opts.Clock, deadline)
				defer cancel()
			}
			if n.opts.ReplicaReads {
				val, err := n.fetchHolder(fetchCtx, p, key)
				return val, 0, err
			}
			val, err := n.fetchOwner(fetchCtx, p, key)
			return val, 0, err
		})
		if res.Err == nil {
			return res.Value, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(res.Err, context.DeadlineExceeded) && !errors.Is(res.Err, context.Canceled) {
			return nil, res.Err
		}
	}
	return n.fetchOwner(ctx, p, key)
}

// fetchHolder reads key from the holder this node is assigned to -- the owner or
// one of its replicas -- and falls back to the owner whenever a replica cannot
// give an answer this node may use.
//
// The fallback is deliberately unconditional. A replica that declines, fails,
// predates the method, or answers with a copy this node already knows to be
// superseded costs one extra hop, and no outcome of the replica leg is ever
// returned as the read's answer except a served, current copy. In particular a
// replica never produces ErrNotFound: only the owner knows a key is absent.
func (n *namespace) fetchHolder(ctx context.Context, p placement, key string) ([]byte, error) {
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return val, nil
		}
	}
	holder, ok := n.holderFor(key)
	if !ok || holder.ID == p.owner.ID {
		return n.fetchOwner(ctx, p, key)
	}

	var (
		val       []byte
		stamp     core.Stamp
		age, left time.Duration
		served    bool
	)
	if holder.ID == n.cache.selfID() {
		val, stamp, age, left, served = n.replicaCopy(key)
	} else {
		resp, err := n.cache.route.replicaGet(ctx, holder, p.epoch, n.name, key)
		if err != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err == nil && resp.Served {
			val, stamp, age, left, served = resp.Value, resp.Stamp, time.Duration(resp.AgeNanos), time.Duration(resp.TTLRemainingNanos), true
		}
	}
	if !served {
		return n.fetchOwner(ctx, p, key)
	}
	if n.l1 != nil {
		if n.l1.Below(key, stamp) {
			n.cache.replicaFloored.Add(1)
			return n.fetchOwner(ctx, p, key)
		}
		// The copy's life in this tier ends one L1 TTL after the replica
		// received it, not one TTL from now, so the bound holds end to end.
		if remaining := n.opts.L1TTL - age; remaining > 0 {
			n.l1.Put(key, val, stamp, l1Lifetime(remaining, left))
		}
	}
	return val, nil
}

// holderFor picks which of key's holders this node reads it from.
//
// Rendezvous over the holders, seeded by this node's ID: each node always asks
// the same holder for a key, which keeps that holder's own coalescing
// effective, and nodes as a whole divide among all of them.
func (n *namespace) holderFor(key string) (core.Node, bool) {
	if n.cache.guard == nil {
		return core.Node{}, false
	}
	holders, count, _ := n.cache.guard.Table().Holders(n.groupKeyFor(key), n.opts.ReplicationFactor)
	if count == 0 {
		return core.Node{}, false
	}
	// One hash of the key and one of this node, then a mix per holder:
	// hashing a concatenated string per holder allocated on every miss.
	base := xxhash.Sum64String(key) ^ (xxhash.Sum64String(n.cache.selfID()) * 0x9E3779B97F4A7C15)
	best, bestScore := 0, uint64(0)
	for i := 0; i < count; i++ {
		if score := fmix64(base ^ xxhash.Sum64String(holders[i].ID)); i == 0 || score > bestScore {
			best, bestScore = i, score
		}
	}
	return holders[best], true
}

// fmix64 is the murmur3 64-bit finalizer: it spreads every input bit over the
// whole output, so the holder picked for a key does not follow from which IDs
// happen to hash high.
func fmix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// replicaCopy returns this node's replica copy of key, decoded, with its age --
// but only if this node may serve it: it ranks as a replica for key under its
// own current view, the copy is live, and it was received less than one L1 TTL
// ago. It counts what it served and what it declined.
func (n *namespace) replicaCopy(key string) (val []byte, stamp core.Stamp, age, left time.Duration, ok bool) {
	defer func() {
		if ok {
			n.cache.replicaServed.Add(1)
		} else {
			n.cache.replicaReferred.Add(1)
		}
	}()
	if n.cache.guard == nil {
		return nil, core.Stamp{}, 0, 0, false
	}
	holders, count, _ := n.cache.guard.Table().Holders(n.groupKeyFor(key), n.opts.ReplicationFactor)
	self := n.cache.selfID()
	ranked := false
	for i := 1; i < count; i++ {
		if holders[i].ID == self {
			ranked = true
			break
		}
	}
	if !ranked {
		return nil, core.Stamp{}, 0, 0, false
	}

	e, found := n.cache.replicas.Get(n.name, key)
	if !found || e.ReceivedAt == 0 {
		return nil, core.Stamp{}, 0, 0, false
	}
	now := n.cache.opts.Clock.Now().UnixNano()
	if e.ExpiresAt != 0 && now >= e.ExpiresAt {
		return nil, core.Stamp{}, 0, 0, false
	}
	age = time.Duration(now - e.ReceivedAt)
	if age < 0 || age >= n.opts.L1TTL {
		return nil, core.Stamp{}, 0, 0, false
	}
	val, err := n.decode(e.Value)
	if err != nil {
		return nil, core.Stamp{}, 0, 0, false
	}
	if e.ExpiresAt != 0 {
		left = time.Duration(e.ExpiresAt - now)
	}
	return val, e.Stamp, age, left, true
}

// fetchOwner sends one read to the owner and caches the answer in L1.
//
// L1 is checked again first. A caller can miss, lose the race to a fetch that
// then completes and fills L1, and start a fetch of its own; the second look
// turns that into a hit instead of a second request.
func (n *namespace) fetchOwner(ctx context.Context, p placement, key string) ([]byte, error) {
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return val, nil
		}
	}
	value, stamp, left, err := n.cache.route.remoteGet(ctx, p, n.name, key)
	if err != nil {
		return nil, err
	}

	// Cache the copy under this namespace's L1 bound. The owner may shorten
	// that bound but never extend it, so the ceiling configured here is the
	// one that holds — which is what makes the TTL a real limit on staleness
	// rather than a suggestion the owner can override.
	if n.l1 != nil {
		n.l1.Put(key, value, stamp, l1Lifetime(n.opts.L1TTL, left))
	}
	return value, nil
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
	if err := n.awaitRestore(ctx, key); err != nil {
		return err
	}
	// Again after the wait: see localUpdate. A blind write cannot read a
	// wrong base, but applied after the key has left it would sit in an owned
	// store that is no longer the key's, outside every rule.
	if err := n.cache.stillOwner(n.groupKeyFor(key)); err != nil {
		return err
	}
	// Framed once. With compression on every call compresses again, and the
	// store, the persister and the fan-out keep the same bytes anyway: the
	// stored form is what persists and what crosses the wire.
	encoded := n.encode(val)
	written := n.store.Put(key, encoded, ttl)
	stamp := n.stamp(written.Version)
	n.cache.pressure.noteWrite(len(val))

	// The invalidation goes out whether or not the fan-out succeeded. A
	// synchronous write that cannot reach a replica has still been applied
	// here -- there is no rollback -- so every other pod's copy is stale
	// either way, and skipping the hint made a degraded write staler than
	// an asynchronous one. Open question 45.
	if n.persister != nil {
		n.persister.record(key, encoded, written.ExpiresAt, written.Order, false)
	}
	err := n.fanOut(ctx, key, encoded, ttl, stamp, written.Order, written.Prev, false)
	n.hint(key, stamp)
	if err != nil {
		// Only synchronous replication can fail here: asynchronous
		// fan-out is queued and returns nil. A synchronous caller asked
		// to know, so it is told — and the value is already stored
		// locally, which is why this reports rather than rolls back.
		// There is no rollback in this system and inventing one for a
		// single key would be a transaction by another name.
		return err
	}
	return nil
}

// fanOut sends a write or a deletion to this key's replicas.
func (n *namespace) fanOut(ctx context.Context, key string, encoded []byte, ttl time.Duration, stamp core.Stamp, order, prev uint64, deleted bool) error {
	// The replication factor is checked once, inside replicaTargets, which
	// is the only place that can act on it. Checking here as well would be
	// two conditions that must agree forever, and breaking either on purpose
	// showed
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
		n.countDegraded(len(targets), degradedByView)
		return nil
	}
	if err := n.cache.replicator.replicate(ctx, n.opts.SyncReplication, replicaWork{
		namespace: n.name,
		key:       key,
		value:     encoded,
		ttl:       ttl,
		stamp:     stamp,
		order:     order,
		prev:      prev,
		deleted:   deleted,
		targets:   targets,
	}); err != nil {
		// Only synchronous replication fails here, and by then the write is
		// applied on this node: there is no rollback. So it is reported as
		// what it is — held, by fewer nodes than its factor — and counted
		// like any other degraded write. It was an opaque failure until
		// M16's real-world run showed an application unable to tell an
		// applied Increment from a refused one, and so decrementing stock
		// twice on a retry (docs/decisions/open.md question 37, option 2).
		n.countDegraded(0, degradedByFailure)
		return fmt.Errorf("%w: %w", ErrUnderReplicated, err)
	}
	// The copies that could be placed are placed before refusing, so a
	// refused write is held as widely as this node's view allowed rather
	// than only by the owner.
	if n.underFactor(len(targets)) {
		return n.refuseUnderFactor(len(targets))
	}
	n.countDegraded(len(targets), degradedByView)
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
// That is docs/decisions/open.md question 37, and this counter is its first
// option: make the
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
// Why a synchronous write was held below its factor, for the one log line
// countDegraded writes. On kind the line blamed a small membership view for
// every one, including writes whose replicas a rolling restart had made
// briefly unreachable while the view held all five members.
const (
	degradedByView    = "this node's membership view is too small to place the copies its namespace asks for"
	degradedByFailure = "replicas in this node's view did not take the copy; it is held on this node until they are written again or catch up"
)

func (n *namespace) countDegraded(placed int, why string) {
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
			"detail", why+"; see Stats().DegradedWrites and peercache_degraded_writes_total")
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
	if err := n.awaitRestore(ctx, key); err != nil {
		return err
	}
	if err := n.cache.stillOwner(n.groupKeyFor(key)); err != nil {
		return err
	}
	_, order := n.store.Remove(key)
	if n.persister != nil {
		n.persister.record(key, nil, 0, order, true)
	}
	// A deletion invalidates every cached copy. The stamp carries this
	// node's epoch with a version above anything the deleted entry could
	// have held, so a peer holding an older copy drops it; a peer that has
	// somehow already seen a newer write keeps that instead.
	tombstone := n.stamp(math.MaxUint64)
	// Deletions replicate too. A replica that missed one would resurrect
	// the key the moment it was promoted, which is worse than never having
	// replicated it at all.
	// Invalidated whether or not the fan-out succeeded; see localSet.
	err := n.fanOut(ctx, key, nil, 0, tombstone, order, 0, true)
	n.hint(key, tombstone)
	if err != nil {
		return err
	}
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
		_, _, _, ok, err := n.lookupResident(ctx, key)
		return ok, err
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
		stored, _, _, ok, err := n.lookupResident(ctx, key)
		if err != nil {
			return err
		}
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
				out[key] = val // plain; see remoteGet
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
		out[e.Key] = e.Value // plain; see remoteGet
		if n.l1 != nil {
			n.l1.Put(e.Key, e.Value, e.Stamp, l1Lifetime(n.opts.L1TTL, time.Duration(e.TTLRemainingNanos)))
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
// loaders are registered by name rather than passed per call: if
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
	stored, version, left, ok, err := n.lookupResident(ctx, key)
	if err != nil {
		return nil, err
	}
	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}
	if ok {
		if n.opts.RefreshAhead > 0 {
			n.maybeRefresh(key, version, left)
		}
		return n.decode(stored)
	}
	if n.knownAbsent(key) {
		// Bare, as Get's miss is: a probe answered from memory should not
		// allocate an error per call.
		return nil, ErrNotFound
	}

	result := n.flight.Do(ctx, key, func(loadCtx context.Context) ([]byte, time.Duration, error) {
		// Re-check under the flight. Between the miss above and becoming
		// the leader, a previous load may have completed and populated
		// the key — or found nothing, and remembered that; loading it
		// again would be a pointless round trip to whatever the loader
		// talks to.
		stored, _, _, ok, held := n.store.LookupGuarded(key, n.shedHeld)
		if held {
			return nil, 0, n.shedSinceRestore(key)
		}
		if ok {
			val, err := n.decode(stored)
			return val, 0, err
		}
		if n.knownAbsent(key) {
			return nil, 0, ErrNotFound
		}

		// The load is bounded by the namespace's timeout, not by
		// whichever caller happened to arrive first. loadCtx already has
		// that caller's cancellation stripped; without a deadline of our
		// own it would have none at all.
		loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()

		loadCtx, span := n.cache.traceLoad(loadCtx, n.name, 1)
		val, ttl, err := n.opts.Loader(loadCtx, key)
		span.End(err)
		if err != nil {
			// Only an absence is remembered. Any other error, an outage
			// above all, must not be served as "not found" once the
			// source is back. MarkAbsent declines if a write landed
			// while the loader ran.
			if n.opts.NegativeTTL > 0 && errors.Is(err, ErrNotFound) {
				n.store.MarkAbsent(key, n.opts.NegativeTTL)
			}
			return nil, 0, err
		}
		// Store inside the flight so that every collapsed caller sees a
		// populated namespace, not just the leader — but only if the key is
		// still absent. A Set that landed while the loader ran is newer than
		// what the loader read, and storing over it unconditionally, as an
		// earlier version did, lost that write until the key's TTL. In that
		// case the newer value is the answer. Guarded too: a key the budget
		// shed while the loader ran is in storage, newer than what the
		// loader read, and must not be stored over.
		resident, wrote, shed := n.store.SetIfAbsentGuarded(key, n.encode(val), ttl, n.shedHeld)
		if shed {
			return nil, 0, n.shedSinceRestore(key)
		}
		if !wrote {
			current, err := n.decode(resident)
			return current, 0, err
		}
		n.cache.pressure.noteWrite(len(val))
		return val, ttl, nil
	})
	if result.Err != nil {
		return nil, fmt.Errorf("namespace %q key %q: %w", n.name, key, result.Err)
	}
	return result.Value, nil
}

// knownAbsent reports whether the owner remembers that key's loader found
// nothing, counting the load it saved. See [WithNegativeCaching].
func (n *namespace) knownAbsent(key string) bool {
	if n.opts.NegativeTTL <= 0 || !n.store.KnownAbsent(key) {
		return false
	}
	n.negativeHits.Add(1)
	return true
}

// remoteGetOrLoad asks the owner, which loads if it must.
//
// A cached non-owned copy answers without asking. That is safe for the same
// reason a plain remote Get is: the copy is TTL-bounded, so the staleness it
// can introduce is the documented bound rather than an unbounded one.
func (n *namespace) remoteGetOrLoad(ctx context.Context, p placement, key string, retry bool) ([]byte, error) {
	if n.l1 != nil {
		if val, _, ok := n.l1.Get(key); ok {
			return val, nil // plain; see remoteGet
		}
	}

	value, stamp, left, err := n.cache.route.remoteLoad(ctx, p, n.name, key)
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
		n.l1.Put(key, value, stamp, l1Lifetime(n.opts.L1TTL, left))
	}
	return value, nil
}

var _ Namespace = (*namespace)(nil)

// lookupResident reads key from the owned store once its restore is done,
// reading it back first if the memory budget shed it, including when the shed
// lands between the restore and the read (open question 62). ok false is an
// absence storage agrees with.
func (n *namespace) lookupResident(ctx context.Context, key string) (val []byte, version uint64, left time.Duration, ok bool, err error) {
	for attempt := 0; ; attempt++ {
		if err := n.awaitRestore(ctx, key); err != nil {
			return nil, 0, 0, false, err
		}
		val, version, left, ok, held := n.store.LookupGuarded(key, n.shedHeld)
		if !held {
			return val, version, left, ok, nil
		}
		if attempt == shedRetries {
			return nil, 0, 0, false, n.shedSinceRestore(key)
		}
	}
}

// shedSinceRestore is the refusal for a key the budget keeps shedding faster
// than it can be read back.
func (n *namespace) shedSinceRestore(key string) error {
	return fmt.Errorf("peercache: namespace %q: %q is being shed faster than it "+
		"can be read back; ask again shortly: %w", n.name, key, ErrActivationPending)
}

// awaitRestore waits, in a persistent namespace, for key's partition to be
// restored since the last membership change; see persister.await. Elsewhere it
// is free.
func (n *namespace) awaitRestore(ctx context.Context, key string) error {
	// Every operation the owner serves on a key passes here, which makes
	// it the one place hot-key tracking needs to watch.
	if n.hot != nil {
		n.hot.Seen(key)
	}
	if n.persister == nil {
		return nil
	}
	err := n.persister.await(ctx, key)
	if errors.Is(err, errPartitionUnrestored) {
		if n.store.Exists(key) {
			return nil // memory holds it: degraded, not wrong
		}
		return fmt.Errorf("peercache: namespace %q: %q's partition could not be restored from storage "+
			"and this node does not hold it, so it cannot say what it is; ask again shortly: %w",
			n.name, key, ErrActivationPending)
	}
	return err
}

// l1Lifetime is how long a copy may live in the non-owned tier: the
// namespace's bound, cut to the key's own remaining lifetime when the key
// expires sooner. A copy must not outlive its key — a session that expired on
// its owner must not stay readable elsewhere for up to an L1 TTL. Zero left
// means the key does not expire.
func l1Lifetime(bound, left time.Duration) time.Duration {
	if left > 0 && left < bound {
		return left
	}
	return bound
}
