package peercache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/replication"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// peerHandler is the server side of the peer protocol.
//
// It is the only place a request from another node turns into local work. Every
// method here runs against *this* node's owned store or group runtime and never
// forwards: a request that arrives at the wrong node is refused, not relayed.
// Relaying would let one stale caller drag a request around the cluster,
// multiplying a single misrouted call into a chain of them, and would make the
// deadline budget disappear into hops nobody asked for.
type peerHandler struct {
	cache *cache
}

// Handle implements [core.Handler].
//
// The context arrives carrying the caller's deadline — gRPC turns grpc-timeout
// back into a deadline — and is passed onward unchanged into the store and the
// group turn. Nothing here extends it and nothing invents one.
func (h *peerHandler) Handle(ctx context.Context, from core.Node, method string, req []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if h.cache.isClosed() {
		return nil, ErrClosed
	}
	ctx, span := h.cache.traceServe(ctx, method, from)
	out, err := h.handle(ctx, from, method, req)
	span.End(err)
	return out, err
}

// handle dispatches one request by method.
func (h *peerHandler) handle(ctx context.Context, from core.Node, method string, req []byte) ([]byte, error) {
	switch method {
	case transport.MethodGet:
		return h.get(ctx, req)
	case transport.MethodReplicaGet:
		return h.replicaGet(ctx, req)
	case transport.MethodLoad:
		return h.load(ctx, req)
	case transport.MethodSet:
		return h.set(ctx, req)
	case transport.MethodDelete:
		return h.delete(ctx, req)
	case transport.MethodCondSet:
		return h.condSet(ctx, req)
	case transport.MethodIncrement:
		return h.increment(ctx, req)
	case transport.MethodTouch:
		return h.touch(ctx, req)
	case transport.MethodBatchGet:
		return h.batchGet(ctx, req)
	case transport.MethodCall:
		return h.call(ctx, req)
	case transport.MethodSubmit:
		return h.submit(ctx, req)
	case transport.MethodReplicate:
		return h.replicate(ctx, req)
	case transport.MethodCatchUp:
		return h.catchUp(ctx, req)
	case transport.MethodInvalidate:
		return h.invalidate(ctx, from, req)
	case transport.MethodDescribe:
		return h.describe(ctx, req)
	default:
		return nil, fmt.Errorf("peercache: unknown method %q from %s: %w",
			method, from.ID, ErrNotImplemented)
	}
}

// noteEpoch records that a caller resolved ownership under a different view.
//
// It does not refuse. That was the first design and it was wrong, in a way that
// only showed up once two nodes had genuinely diverged: a reader at epoch 6 was
// refused by an owner at epoch 4, and *both were right*.
//
// An epoch counts the membership transitions one node has observed. The design
// §3.5 makes membership AP and [core.Epoch] says so directly — epochs are
// per-view, not globally agreed, and comparing them "establishes staleness,
// never consensus". Two nodes that agree exactly about who is in the cluster
// can still hold different epochs, because they counted different numbers of
// intermediate views getting there. Demanding equality across a hop therefore
// fails requests between nodes that agree, and once the counters have drifted
// it fails them permanently.
//
// What actually protects correctness is [peerHandler.assertOwned], which asks
// whether *this* node owns the key under *its own* current view. That question
// is answerable locally and is the only one that matters: if this node owns the
// key, serving it is right whatever the caller believed; if it does not, the
// caller is told to go elsewhere whatever epoch it quoted.
//
// The epoch still travels, because the owner stamps its replies with its own
// and a caller needs both halves to order values. Divergence is counted so it
// is visible rather than silent.
func (h *peerHandler) noteEpoch(callerEpoch uint64) {
	if h.cache.guard == nil {
		return
	}
	if callerEpoch != uint64(h.cache.guard.Stats().Epoch) {
		h.cache.epochSkew.Add(1)
	}
}

// localNamespace returns the namespace by name without creating it.
//
// A request for a namespace this node has never heard of is a miss rather than
// an error. Namespaces are created lazily on first use, so a peer that has
// written to "orders" and a peer that has not are both correct; inventing the
// namespace here would allocate a store on every node for every name any peer
// happened to mention.
func (h *peerHandler) localNamespace(name string) (*namespace, bool) {
	h.cache.mu.RLock()
	ns, ok := h.cache.namespaces[name]
	h.cache.mu.RUnlock()
	return ns, ok
}

// misconfigured reports a namespace whose options failed validation on this
// node. Such a namespace is registered — so the application's handle can keep
// returning the error — but has no store, and a peer request that reached it
// used to dereference that nil store on the handler's goroutine and take the
// process down. The error wraps the validation failure, so a caller sees
// ErrInvalidOption from the owner rather than a dropped connection.
func (h *peerHandler) misconfigured(ns *namespace) error {
	return fmt.Errorf("peercache: namespace %q is misconfigured on %s: %w", ns.name, h.cache.self().ID, ns.err)
}

func (h *peerHandler) get(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.GetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}

	ns, ok := h.localNamespace(in.Namespace)
	if !ok {
		return wire.Encode(&wire.GetResponse{})
	}
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}

	if err := ns.awaitRestore(ctx, in.Key); err != nil {
		return nil, err
	}
	stored, version, left, found := ns.store.Lookup(in.Key)
	if !found {
		return wire.Encode(&wire.GetResponse{})
	}
	value, err := ns.decode(stored)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.GetResponse{
		Found: true,
		Value: value,
		Stamp: h.stamp(version),
		// How long the key has left, so the caller's L1 copy does not
		// outlive it. The caller still clamps to its own L1 TTL: an
		// owner can only ever shorten the bound. Zero: no expiry.
		TTLRemainingNanos: int64(left),
	})
}

// replicaGet answers a read from this node's replica copy, or declines.
//
// There is no ownership check: the caller chose this node because it ranks as
// a replica. Whether it still does is decided here, under this node's own view,
// for the same reason [peerHandler.assertOwned] uses its own: epochs are
// per-view and cannot be compared across nodes. A decline is never an error,
// so a reader treats it exactly like a peer that predates this method.
func (h *peerHandler) replicaGet(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.ReplicaGetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)

	ns, ok := h.localNamespace(in.Namespace)
	if !ok || ns.err != nil || !ns.opts.ReplicaReads {
		h.cache.replicaReferred.Add(1)
		return wire.Encode(&wire.ReplicaGetResponse{})
	}
	val, stamp, age, left, ok := ns.replicaCopy(in.Key)
	if !ok {
		return wire.Encode(&wire.ReplicaGetResponse{})
	}
	return wire.Encode(&wire.ReplicaGetResponse{
		Served:            true,
		Value:             val,
		Stamp:             stamp,
		AgeNanos:          int64(age),
		TTLRemainingNanos: int64(left),
	})
}

// load reconstructs a key here, on its owner, under this node's single flight.
//
// Every caller in the cluster that wants this key arrives here, so one flight
// collapses the whole cluster's concurrent misses into one call to the loader —
// which is the property that makes registering loaders by name worth the
// constraint it imposes.
func (h *peerHandler) load(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.LoadRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}

	// Created if absent, unlike Get: a load has to be able to populate a
	// namespace this node has not been asked about before.
	ns := h.cache.Namespace(in.Namespace).(*namespace)
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}
	if ns.opts.Loader == nil {
		// No loader configured here. Report absence rather than an
		// error: from the caller's side this is the same "not there"
		// that a plain Get would give, and inventing a failure would
		// make a missing key indistinguishable from a broken owner.
		return wire.Encode(&wire.LoadResponse{})
	}

	value, err := ns.localGetOrLoad(ctx, in.Key)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return wire.Encode(&wire.LoadResponse{})
		}
		return nil, err
	}
	_, version, _ := ns.store.Get(in.Key)
	left, _ := ns.store.Remaining(in.Key)
	return wire.Encode(&wire.LoadResponse{
		Found:             true,
		Value:             value,
		Stamp:             h.stamp(version),
		TTLRemainingNanos: int64(left),
	})
}

func (h *peerHandler) set(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.SetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}

	// Unlike Get, a write creates the namespace; see writableNamespace.
	ns, err := h.writableNamespace(in.Namespace)
	if err != nil {
		return nil, err
	}

	// Through the namespace's own write path, not straight into the store.
	//
	// Writing to the store directly is what the first version did, and it
	// meant a write that arrived over the network was never replicated and
	// never invalidated: only keys written *through* their owner got copies.
	// The promotion acceptance test found it, because two thirds of the
	// keyspace had no replica to promote. A local write and a remote write
	// must be the same write once they reach the owner.
	if err := ns.localSet(ctx, in.Key, in.Value, time.Duration(in.TTLNanos)); err != nil {
		return nil, err
	}
	_, version, _ := ns.store.Get(in.Key)
	return wire.Encode(&wire.SetResponse{Stamp: h.stamp(version)})
}

// writableNamespace returns the namespace an incoming write lands in,
// creating it if this node's application has not opened it.
//
// The creation happens because refusing would mean a key could never be
// written from anywhere but the node that happened to read it first. But a
// namespace created here gets *default* options — replication factor 1 — and
// options are fixed at creation, so an owner whose application has not yet
// opened the namespace silently replicates nothing. That presented as 53 of
// 300 keys destroyed by a node failure at RF=3, and took a bisect to find
// because every counter said the write succeeded. So it is counted and logged:
// a non-zero ImplicitNamespaces means some node is running with configuration
// its application never chose.
func (h *peerHandler) writableNamespace(name string) (*namespace, error) {
	_, existed := h.cache.namespaceIfExists(name)
	ns := h.cache.Namespace(name).(*namespace)
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}
	if !existed {
		h.cache.implicitNamespaces.Add(1)
		h.cache.opts.Logger.Warn(
			"namespace created by an incoming write, with default options; "+
				"replication is disabled for it until this node's application "+
				"opens it with the options it wants",
			"namespace", name,
			"replicationFactor", ns.opts.ReplicationFactor)
	}
	return ns, nil
}

func (h *peerHandler) delete(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.DeleteRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}
	// Same reasoning as set: a deletion arriving over the network has to
	// replicate its tombstone, or a replica that missed it resurrects the
	// key the moment it is promoted.
	if ns, ok := h.localNamespace(in.Namespace); ok {
		if ns.err != nil {
			return nil, h.misconfigured(ns)
		}
		if err := ns.localDelete(ctx, in.Key); err != nil {
			return nil, err
		}
	}
	return wire.Encode(&wire.DeleteResponse{})
}

func (h *peerHandler) batchGet(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.BatchGetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)

	ns, ok := h.localNamespace(in.Namespace)
	if !ok {
		return wire.Encode(&wire.BatchGetResponse{})
	}
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}

	out := wire.BatchGetResponse{Entries: make([]wire.BatchEntry, 0, len(in.Keys))}
	var missing []string
	for _, key := range in.Keys {
		// The context is checked inside the loop rather than only at
		// entry. A large batch against a slow node would otherwise run
		// to completion after the caller had already given up, spending
		// this node's time on an answer nobody will read.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := h.assertOwned(in.Namespace, key); err != nil {
			// One misrouted key does not spoil the batch: the
			// caller resolved it, so the caller can re-resolve it.
			// Omitting it looks like a miss, which is the outcome
			// a BatchGet caller must already handle; a load must
			// not, because "missing" there means "does not exist",
			// so it is named.
			if in.Load {
				out.Misrouted = append(out.Misrouted, key)
			}
			continue
		}
		if err := ns.awaitRestore(ctx, key); err != nil {
			return nil, err
		}
		stored, version, left, found := ns.store.Lookup(key)
		if !found {
			if in.Load {
				missing = append(missing, key)
			}
			continue
		}
		value, err := ns.decode(stored)
		if err != nil {
			return nil, err
		}
		out.Entries = append(out.Entries, wire.BatchEntry{
			Key:               key,
			Value:             value,
			Stamp:             h.stamp(version),
			TTLRemainingNanos: int64(left),
		})
	}

	if len(missing) > 0 {
		// The keys this owner does not hold, loaded in one call — the
		// point of BatchGetOrLoad. A failed load fails this owner's
		// share; the caller keeps every other owner's.
		if ns.opts.Loader == nil && ns.opts.BatchLoader == nil {
			return nil, fmt.Errorf("namespace %q: %w", in.Namespace, ErrNoLoader)
		}
		loaded := make(map[string][]byte, len(missing))
		if err := ns.loadMissing(ctx, missing, loaded); err != nil {
			return nil, err
		}
		for _, key := range missing {
			value, ok := loaded[key]
			if !ok {
				continue
			}
			// The stamp the value was stored under, for the caller's
			// L1. A value that was already gone again carries none.
			_, version, _ := ns.store.Get(key)
			left, _ := ns.store.Remaining(key)
			out.Entries = append(out.Entries, wire.BatchEntry{
				Key:               key,
				Value:             value,
				Stamp:             h.stamp(version),
				TTLRemainingNanos: int64(left),
			})
		}
	}
	return wire.Encode(&out)
}

func (h *peerHandler) call(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.CallRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)

	// No explicit ownership check: the group runtime consults the guard on
	// activation, so a group this node does not own already fails with
	// ErrNotOwner. Checking twice would let the two answers disagree.
	result, err := h.cache.groups.Call(ctx, in.Group, in.FuncName, in.Args)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.CallResponse{Result: result})
}

// submit applies a router flush: many groups, each in its own actor turn.
//
// Failures are per group and collected rather than fatal. A flush spans groups
// that have nothing to do with each other, and there is no cross-group
// atomicity in this system — so one group's handler returning an error must not
// discard the work of the groups beside it, which have already been applied.
func (h *peerHandler) submit(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.SubmitRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)

	out := wire.SubmitResponse{}
	for _, batch := range in.Batches {
		// Checked per group rather than once: a large flush against a
		// slow node would otherwise keep starting turns after the caller
		// had already given up waiting for them.
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		arg, err := wire.Encode(&batch.Messages)
		if err != nil {
			out.Failures = append(out.Failures,
				wire.GroupFailure{Group: batch.Group, Error: err.Error()})
			continue
		}
		// The group runtime consults the ownership guard on activation,
		// so a group this node does not own fails here with ErrNotOwner
		// rather than needing a second check that could disagree.
		if _, err := h.cache.groups.Call(ctx, batch.Group, in.FuncName, arg); err != nil {
			out.Failures = append(out.Failures,
				wire.GroupFailure{Group: batch.Group, Error: err.Error()})
		}
	}
	return wire.Encode(&out)
}

// replicate accepts a copy of a key this node does not own.
//
// There is deliberately no ownership check. A replica is by definition not the
// owner, so asserting ownership here would reject every legitimate copy. What
// protects against a confused sender is the stamp: an older copy is refused,
// and a copy from a second writer in the same epoch is reported as the split
// brain it is.
func (h *peerHandler) replicate(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.ReplicateRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}

	if in.TTLNanos < 0 {
		// Expired before it was sent. Zero means "does not expire", so a
		// negative remainder must not be read as zero: stored, it would
		// never expire anywhere. The senders already skip expired keys;
		// this is the receiver not trusting them to.
		return wire.Encode(&wire.ReplicateResponse{})
	}

	if in.Deleted {
		// A tombstone, ordered like any copy: it removes what is held only
		// if what is held is not newer. A deletion from an owner on an
		// older epoch must not remove a write from the key's current owner.
		// The comment here once said a stamp check above already refused
		// older tombstones; there was none, and a stale one removed newer
		// data. Removing outright rather than storing a marker is safe
		// once ordered: a replica that is promoted rebuilds from what it
		// holds, and an absent key and a key marked absent are the same
		// answer.
		h.cache.replicas.DeleteUnlessNewer(in.Namespace, in.Key, in.Stamp, in.Order)
		return wire.Encode(&wire.ReplicateResponse{})
	}

	// If this node already owns the key, take it straight into the owned
	// store instead of filing it as a replica.
	//
	// This is the hand-off path. A draining node announces its departure
	// first, so by the time its keys arrive the ranking already names the
	// receiver as owner — and a key filed as a replica would sit there until
	// the *next* membership change reconciled it. At RF=1 there is no next
	// change and no replica set to belong to, so it would be evicted
	// instead: a graceful shutdown would lose exactly the data it was trying
	// to save. Breaking it on purpose found that, by way of a test that passed
	// while drain did nothing at all.
	if ns, exists := h.cache.namespaceIfExists(in.Namespace); exists && ns.err == nil && h.ownsKey(ns, in.Key) {
		// in.Value is the sender's stored form, framed already, as every
		// replicated copy is; framing it again stored a frame inside a
		// frame and handed the header back to the next reader.
		h.cache.observeOrder(in.Order)
		if ns.store.Restore(in.Key, in.Value, time.Duration(in.TTLNanos), in.Stamp.Version, in.Order) {
			h.cache.keysAdopted.Add(1)
			if ns.persister != nil && in.Order != 0 {
				ns.persister.record(in.Key, in.Value,
					replication.DeadlineFrom(h.cache.opts.Clock.Now().UnixNano(), in.TTLNanos), in.Order, false)
			}
		}
		h.cache.pressure.noteWrite(len(in.Value))
		return wire.Encode(&wire.ReplicateResponse{})
	}

	now := h.cache.opts.Clock.Now().UnixNano()
	h.cache.observeOrder(in.Order)
	_, conflict := h.cache.replicas.Put(in.Namespace, in.Key, replication.Entry{
		Value:      in.Value,
		Stamp:      in.Stamp,
		ExpiresAt:  replication.DeadlineFrom(now, in.TTLNanos),
		ReceivedAt: now,
		Order:      in.Order,
		From:       in.From,
		Prev:       in.Prev,
	})
	h.cache.pressure.noteWrite(len(in.Value))
	if conflict {
		h.cache.splitBrain.Add(1)
		h.cache.opts.Logger.Warn("conflicting replicated write",
			"namespace", in.Namespace, "key", in.Key, "from", in.From)
	}
	// Ownership is checked again after the copy is filed. The check above
	// and the Put are not atomic with a membership change: if this node's
	// view moved between them, the reconciliation that the move triggered
	// may already have run and found nothing to promote, and the copy would
	// sit in the replica store, unreadable, until the next tick. A hand-off
	// from a draining node is exactly that race.
	if ns, exists := h.cache.namespaceIfExists(in.Namespace); exists && ns.err == nil && h.ownsKey(ns, in.Key) {
		h.cache.promoter.promoteKey(ns, in.Key)
	}
	return wire.Encode(&wire.ReplicateResponse{Conflict: conflict})
}

// catchUp answers a returning node with the keys it should be replicating.
//
// The requester does not say which keys it wants — it has forgotten, which is
// why it is asking. It names itself and this node works out the answer from the
// same rendezvous ranking both compute, which is also what makes the answer
// trustworthy: a node cannot ask for keys it does not rank for.
func (h *peerHandler) catchUp(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.CatchUpRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	if in.Requester == "" || in.ReplicationFactor < 2 {
		return wire.Encode(&wire.CatchUpResponse{})
	}
	limit := in.Limit
	if limit <= 0 || limit > catchUpPageSize {
		limit = catchUpPageSize
	}

	ns, ok := h.localNamespace(in.Namespace)
	if !ok {
		return wire.Encode(&wire.CatchUpResponse{})
	}
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}

	if in.Replicas {
		return h.catchUpReplicas(ctx, ns, in, limit)
	}

	// One pass per asker and namespace: its key list is sorted once, when
	// it asks for the first page, not once per page.
	pass := in.Requester + "\x00" + ns.name + "\x00owned"
	entries, more := ns.store.SnapshotFrom(h.cache.pages, pass, in.After, limit, func(key string) bool {
		if h.cache.guard == nil {
			return false
		}
		// Send a key if the asker ranks anywhere in its replica set —
		// which includes ranking *first*, as its owner.
		//
		// Restricting this to keys the sender still owns was the first
		// version, and it left the worst case unhandled: a node that
		// restarts and reclaims ownership of keys it held before. While
		// it was away another node owned them; on its return that node
		// is demoted and still holds the data, but by the old rule it
		// was no longer entitled to send it, so the returning owner came
		// back to keys that existed elsewhere and could not be fetched.
		//
		// What still bounds this is the ranking: a node cannot obtain
		// keys it has no business holding by asking nicely, because the
		// sender computes the answer rather than trusting the request.
		// Holders rather than Replicas: this runs once per key scanned, and
		// Replicas allocates per call. It also clamps the asker's factor to
		// the maximum, which a well-formed asker never exceeds.
		ranked, n, _ := h.cache.guard.Table().Holders(ns.groupKeyFor(key), in.ReplicationFactor)
		for _, node := range ranked[:n] {
			if node.ID == in.Requester {
				return true
			}
		}
		return false
	})

	out := wire.CatchUpResponse{More: more, Entries: make([]wire.CatchUpEntry, 0, len(entries))}
	now := h.cache.opts.Clock.Now().UnixNano()
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := ns.decode(e.Value)
		if err != nil {
			return nil, err
		}
		// The remaining TTL, not the original. A key three seconds from
		// expiry must not be handed over with a fresh full lifetime, or
		// a slow transfer would keep resurrecting it.
		var ttl int64
		if e.ExpiresAt != 0 {
			ttl = e.ExpiresAt - now
			if ttl <= 0 {
				continue
			}
		}
		out.Entries = append(out.Entries, wire.CatchUpEntry{
			Key:      e.Key,
			Value:    ns.encode(value),
			Stamp:    h.stamp(e.Version),
			TTLNanos: ttl,
			Order:    e.Order,
		})
	}
	return wire.Encode(&out)
}

// catchUpReplicas answers a catch-up for this node's replica copies of keys
// the asker owns. See [wire.CatchUpRequest.Replicas].
//
// Only keys the asker owns, by this node's ranking: a replica asking for other
// replicas' copies would be asking for something its owner already sends. The
// copies go out as held — the stored form, and the stamp their owner issued —
// so the asker's stamp and version checks see exactly what this node saw.
func (h *peerHandler) catchUpReplicas(ctx context.Context, ns *namespace, in wire.CatchUpRequest, limit int) ([]byte, error) {
	if h.cache.guard == nil {
		return wire.Encode(&wire.CatchUpResponse{})
	}
	now := h.cache.opts.Clock.Now().UnixNano()
	pass := in.Requester + "\x00" + ns.name + "\x00replicas"
	entries, more := h.cache.replicas.SnapshotFrom(h.cache.pages, pass, ns.name, in.After, limit, now, func(key string) bool {
		owner, _, err := h.cache.guard.Table().Owner(ns.groupKeyFor(key))
		return err == nil && owner.ID == in.Requester
	})
	out := wire.CatchUpResponse{More: more, Entries: make([]wire.CatchUpEntry, 0, len(entries))}
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var ttl int64
		if e.ExpiresAt != 0 {
			ttl = e.ExpiresAt - now
			if ttl <= 0 {
				continue
			}
		}
		out.Entries = append(out.Entries, wire.CatchUpEntry{
			Key:      e.Key,
			Value:    e.Value,
			Stamp:    e.Stamp,
			TTLNanos: ttl,
			Order:    e.Order,
			From:     e.From,
			Prev:     e.Prev,
		})
	}
	return wire.Encode(&out)
}

func (h *peerHandler) invalidate(ctx context.Context, from core.Node, req []byte) ([]byte, error) {
	var in wire.InvalidateRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	// Deliberately no epoch check. An invalidation is a hint, and refusing
	// a hint because the sender's view has moved on would discard exactly
	// the messages sent during a membership change, which is when stale
	// copies are most likely. Correctness does not depend on it either way.
	if ns, ok := h.localNamespace(in.Namespace); ok && ns.l1 != nil {
		for _, key := range in.Keys {
			// With replica reads the next refill may come from a
			// replica that has not yet received in.Stamp, so the
			// copy is replaced by a floor rather than deleted. See
			// docs/decisions/0028-hot-key-reads.md.
			if ns.opts.ReplicaReads {
				ns.l1.Floor(key, in.Stamp, ns.opts.L1TTL)
				continue
			}
			ns.l1.InvalidateOlderThan(key, in.Stamp)
		}
	}
	return wire.Encode(&wire.InvalidateResponse{})
}

// ownsKey reports whether this node owns a key under the current view.
func (h *peerHandler) ownsKey(ns *namespace, key string) bool {
	if h.cache.guard == nil {
		return true
	}
	owner, _, err := h.cache.guard.Table().Owner(ns.groupKeyFor(key))
	return err == nil && owner.ID == h.cache.self().ID
}

// assertOwned refuses a key this node does not own.
//
// The check is by key, not by namespace: ownership is per group, and a
// namespace's keys are spread across every node in the cluster.
//
// Ownership is decided by the key's group, which the namespace's partitioner
// derives from it. Callers route by that group key, so the check must use it
// too: checked by the raw key, a partitioned namespace's owner refused every
// remote request whose key happened to hash elsewhere. A namespace this node
// has not opened falls back to the key itself, which is the default
// partitioner and what an implicitly created namespace would use.
func (h *peerHandler) assertOwned(namespace, key string) error {
	if h.cache.guard == nil {
		return nil
	}
	groupKey := key
	ns, ok := h.localNamespace(namespace)
	if ok && ns.err == nil {
		groupKey = ns.groupKeyFor(key)
	}
	if err := h.cache.guard.CanActivate(groupKey); err != nil {
		return err
	}
	// With WithCatchUpGate: a node that has not finished a catch-up since it
	// started owns its keys
	// but may not hold them: their newest copies are on its replicas. Serving
	// a peer's write now would apply it to an empty or persisted-but-older
	// base — an Increment counting from the wrong value, a SetIfAbsent
	// succeeding over a key that exists — and the catch-up that follows
	// would then discard the acknowledged write as older than the copy it
	// brings. So until the first catch-up completes, a namespace with
	// replicas to catch up from answers "ask again here shortly", which every
	// caller already retries. Readiness waits for the same event; this
	// closes the path readiness does not gate, peers forwarding to an owner.
	// Opt-in, because a node whose application never calls CatchUp would
	// refuse its peers indefinitely. Found by M16's kill -9 on kind; open
	// question 49, part two.
	if h.cache.opts.CatchUpGate && ok && ns.err == nil && ns.opts.ReplicationFactor >= 2 &&
		!h.cache.readiness.caughtUp.Load() {
		return fmt.Errorf("peercache: %q is owned here, but this node has not caught up since it started: %w",
			key, core.ErrActivationPending)
	}
	return nil
}

// stamp pairs a store version with this node's current epoch.
//
// Both halves are needed. A version orders writes by one owner and means
// nothing across an ownership change — it restarts when an entry is reclaimed
// (docs/decisions/open.md question 18) — so the epoch is what makes the pair
// comparable at all.
func (h *peerHandler) stamp(version uint64) wire.Stamp {
	var epoch uint64
	if h.cache.guard != nil {
		epoch = uint64(h.cache.guard.Stats().Epoch)
	}
	return wire.Stamp{Epoch: epoch, Version: version}
}

var _ core.Handler = (*peerHandler)(nil)
