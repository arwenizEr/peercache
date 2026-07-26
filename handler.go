package peercache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/replication"
	"github.com/peercache/peercache/internal/transport"
	"github.com/peercache/peercache/internal/wire"
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

	switch method {
	case transport.MethodGet:
		return h.get(ctx, req)
	case transport.MethodLoad:
		return h.load(ctx, req)
	case transport.MethodSet:
		return h.set(ctx, req)
	case transport.MethodDelete:
		return h.delete(ctx, req)
	case transport.MethodBatchGet:
		return h.batchGet(ctx, req)
	case transport.MethodCall:
		return h.call(ctx, req)
	case transport.MethodSubmit:
		return h.submit(ctx, req)
	case transport.MethodReplicate:
		return h.replicate(ctx, req)
	case transport.MethodInvalidate:
		return h.invalidate(ctx, from, req)
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
// An epoch counts the membership transitions one node has observed. SPEC.md
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

func (h *peerHandler) get(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.GetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}

	ns, ok := h.localNamespace(in.Namespace)
	if !ok {
		return wire.Encode(&wire.GetResponse{})
	}

	stored, version, found := ns.store.Get(in.Key)
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
		// The remaining TTL is not currently readable from the store,
		// so the owner grants nothing and the caller falls back to its
		// own L1 maximum. That is the safe direction: the caller's
		// bound always applies, and an owner can only ever shorten it.
		TTLRemainingNanos: 0,
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
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}

	// Created if absent, unlike Get: a load has to be able to populate a
	// namespace this node has not been asked about before.
	ns := h.cache.Namespace(in.Namespace).(*namespace)
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
	return wire.Encode(&wire.LoadResponse{
		Found: true,
		Value: value,
		Stamp: h.stamp(version),
	})
}

func (h *peerHandler) set(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.SetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}

	// Unlike Get, a write creates the namespace: the owner is where the
	// value lives, so refusing would mean a key could never be written from
	// anywhere but the node that happened to read it first.
	ns := h.cache.Namespace(in.Namespace).(*namespace)

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

func (h *peerHandler) delete(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.DeleteRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}
	// Same reasoning as set: a deletion arriving over the network has to
	// replicate its tombstone, or a replica that missed it resurrects the
	// key the moment it is promoted.
	if ns, ok := h.localNamespace(in.Namespace); ok {
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

	out := wire.BatchGetResponse{Entries: make([]wire.BatchEntry, 0, len(in.Keys))}
	for _, key := range in.Keys {
		// The context is checked inside the loop rather than only at
		// entry. A large batch against a slow node would otherwise run
		// to completion after the caller had already given up, spending
		// this node's time on an answer nobody will read.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := h.assertOwned(key); err != nil {
			// One misrouted key does not spoil the batch: the
			// caller resolved it, so the caller can re-resolve it.
			// Omitting it looks like a miss, which is the outcome
			// the caller must already handle.
			continue
		}
		stored, version, found := ns.store.Get(key)
		if !found {
			continue
		}
		value, err := ns.decode(stored)
		if err != nil {
			return nil, err
		}
		out.Entries = append(out.Entries, wire.BatchEntry{
			Key:   key,
			Value: value,
			Stamp: h.stamp(version),
		})
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

	if in.Deleted {
		// A tombstone. Removing outright rather than storing a marker
		// is safe because the stamp check above it has already refused
		// anything older, and a replica that is promoted rebuilds from
		// what it holds — an absent key and a key marked absent are the
		// same answer.
		h.cache.replicas.Delete(in.Namespace, in.Key)
		return wire.Encode(&wire.ReplicateResponse{})
	}

	_, conflict := h.cache.replicas.Put(in.Namespace, in.Key, replication.Entry{
		Value:    in.Value,
		Stamp:    in.Stamp,
		TTLNanos: in.TTLNanos,
		From:     in.From,
	})
	if conflict {
		h.cache.splitBrain.Add(1)
		h.cache.opts.Logger.Warn("conflicting replicated write",
			"namespace", in.Namespace, "key", in.Key, "from", in.From)
	}
	return wire.Encode(&wire.ReplicateResponse{Conflict: conflict})
}

func (h *peerHandler) invalidate(ctx context.Context, from core.Node, req []byte) ([]byte, error) {
	var in wire.InvalidateRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	// Deliberately no epoch check. An invalidation is a hint, and refusing
	// a hint because the sender's view has moved on would discard exactly
	// the messages sent during a membership change, which is when stale
	// copies are most likely. Correctness does not depend on it either way
	// (CLAUDE.md non-negotiable 6).
	if ns, ok := h.localNamespace(in.Namespace); ok && ns.l1 != nil {
		for _, key := range in.Keys {
			ns.l1.InvalidateOlderThan(key, in.Stamp)
		}
	}
	return wire.Encode(&wire.InvalidateResponse{})
}

// assertOwned refuses a key this node does not own.
//
// The check is by key, not by namespace: ownership is per group, and a
// namespace's keys are spread across every node in the cluster.
func (h *peerHandler) assertOwned(key string) error {
	if h.cache.guard == nil {
		return nil
	}
	return h.cache.guard.CanActivate(key)
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
