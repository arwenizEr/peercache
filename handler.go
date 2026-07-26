package peercache

import (
	"context"
	"fmt"
	"time"

	"github.com/peercache/peercache/internal/core"
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
	case transport.MethodSet:
		return h.set(ctx, req)
	case transport.MethodDelete:
		return h.delete(ctx, req)
	case transport.MethodBatchGet:
		return h.batchGet(ctx, req)
	case transport.MethodCall:
		return h.call(ctx, req)
	case transport.MethodInvalidate:
		return h.invalidate(ctx, from, req)
	default:
		return nil, fmt.Errorf("peercache: unknown method %q from %s: %w",
			method, from.ID, ErrNotImplemented)
	}
}

// checkEpoch refuses a request resolved under a membership view this node has
// already replaced.
//
// It is the same asymmetry the activation guard uses, for the same reason: a
// caller behind us should re-resolve and go elsewhere, while a caller ahead of
// us has seen a view we have not, and this node cannot confirm ownership under
// a view it does not hold. See docs/decisions/0012-the-activation-guard.md.
func (h *peerHandler) checkEpoch(callerEpoch uint64) error {
	if h.cache.guard == nil {
		// No membership authority means a cluster of one, where every
		// epoch is this node's own and there is nothing to disagree with.
		return nil
	}
	ours := uint64(h.cache.guard.Stats().Epoch)
	switch {
	case callerEpoch < ours:
		return fmt.Errorf("peercache: request resolved at epoch %d, this node is at %d: %w",
			callerEpoch, ours, ErrStaleEpoch)
	case callerEpoch > ours:
		return fmt.Errorf("peercache: request resolved at epoch %d, this node is still at %d: %w",
			callerEpoch, ours, ErrActivationPending)
	}
	return nil
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
	if err := h.checkEpoch(in.Epoch); err != nil {
		return nil, err
	}
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

func (h *peerHandler) set(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.SetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	if err := h.checkEpoch(in.Epoch); err != nil {
		return nil, err
	}
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}

	// Unlike Get, a write creates the namespace: the owner is where the
	// value lives, so refusing would mean a key could never be written from
	// anywhere but the node that happened to read it first.
	ns := h.cache.Namespace(in.Namespace).(*namespace)
	version := ns.store.Set(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos))
	return wire.Encode(&wire.SetResponse{Stamp: h.stamp(version)})
}

func (h *peerHandler) delete(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.DeleteRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	if err := h.checkEpoch(in.Epoch); err != nil {
		return nil, err
	}
	if err := h.assertOwned(in.Key); err != nil {
		return nil, err
	}
	if ns, ok := h.localNamespace(in.Namespace); ok {
		ns.store.Delete(in.Key)
	}
	return wire.Encode(&wire.DeleteResponse{})
}

func (h *peerHandler) batchGet(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.BatchGetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	if err := h.checkEpoch(in.Epoch); err != nil {
		return nil, err
	}

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
	if err := h.checkEpoch(in.Epoch); err != nil {
		return nil, err
	}

	// No explicit ownership check: the group runtime consults the guard on
	// activation, so a group this node does not own already fails with
	// ErrNotOwner. Checking twice would let the two answers disagree.
	result, err := h.cache.groups.Call(ctx, in.Group, in.FuncName, in.Args)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.CallResponse{Result: result})
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
