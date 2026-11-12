package peercache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// router resolves ownership and issues peer requests.
//
// It is the client half of the peer protocol, and it is deliberately thin: it
// decides *where* a request goes and nothing about what the answer means. The
// namespace decides whether an answer may be cached; the handler decides
// whether a request may be served.
type router struct {
	cache *cache
}

// placement is the answer to "who owns this key, and is that us".
type placement struct {
	owner core.Node
	epoch uint64
	local bool
}

// locate resolves the owner of a group key.
//
// A cache with no membership authority is a cluster of one and owns everything,
// which is the correct answer rather than a special case: the single-node
// configuration is real and must not pay for a lookup that can only ever say
// "you".
func (r *router) locate(groupKey string) (placement, error) {
	if r.cache.guard == nil {
		return placement{local: true}, nil
	}
	owner, epoch, err := r.cache.guard.Table().Owner(groupKey)
	if err != nil {
		return placement{}, err
	}
	return placement{
		owner: owner,
		epoch: uint64(epoch),
		local: owner.ID == r.cache.self().ID,
	}, nil
}

// invoke sends one request to a peer and returns its response body.
//
// The context is passed through untouched. Every deadline in this system
// belongs to the caller who set it: this layer never extends one to give a slow
// peer another chance, because that spends time the caller has already
// budgeted elsewhere.
func (r *router) invoke(ctx context.Context, node core.Node, method string, req any, resp any) error {
	if r.cache.transport == nil {
		return fmt.Errorf("peercache: %s owns this key but no transport is configured: %w",
			node.ID, ErrNotImplemented)
	}

	body, err := wire.Encode(req)
	if err != nil {
		return err
	}
	client, err := r.cache.transport.Dial(node)
	if err != nil {
		return fmt.Errorf("peercache: dialling %s: %w", node.ID, err)
	}
	// Timed only when histograms are enabled. The clock read is cheap
	// (~6ns) but pointless on the local path, and this is the remote one
	// where a millisecond round trip makes it free by comparison.
	var started int64
	if r.cache.remoteLatency != nil {
		started = r.cache.opts.Clock.Now().UnixNano()
	}
	out, err := client.Invoke(ctx, method, body)
	if r.cache.remoteLatency != nil {
		if elapsed := r.cache.opts.Clock.Now().UnixNano() - started; elapsed >= 0 {
			r.cache.remoteLatency.Observe(uint64(elapsed))
		}
	}
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	return wire.Decode(out, resp)
}

// self returns this node's identity, or the zero node when there is no
// membership authority.
func (c *cache) self() core.Node {
	if c.opts.Membership == nil {
		return core.Node{ID: c.opts.NodeID}
	}
	return c.opts.Membership.Self()
}

// memberCount returns the size of this node's membership view, itself included.
//
// It reads the guard's cached view rather than the membership layer directly,
// because that is the view ownership and replication were resolved from — a
// diagnostic that reported a different number from the one the decision used
// would send an operator looking in the wrong place.
func (c *cache) memberCount() int {
	if c.guard == nil {
		return 1
	}
	return c.guard.Stats().Members
}

// remoteGet fetches one key from its owner and returns the value with the
// owner's stamp.
func (r *router) remoteGet(ctx context.Context, p placement, ns, key string) ([]byte, core.Stamp, error) {
	var resp wire.GetResponse
	err := r.invoke(ctx, p.owner, transport.MethodGet, &wire.GetRequest{
		Namespace: ns,
		Key:       key,
		Epoch:     p.epoch,
	}, &resp)
	if err != nil {
		return nil, core.Stamp{}, err
	}
	if !resp.Found {
		return nil, core.Stamp{}, ErrNotFound
	}
	return resp.Value, resp.Stamp, nil
}

// remoteLoad asks an owner for a key, letting it reconstruct if absent.
func (r *router) remoteLoad(ctx context.Context, p placement, ns, key string) ([]byte, core.Stamp, error) {
	var resp wire.LoadResponse
	err := r.invoke(ctx, p.owner, transport.MethodLoad, &wire.LoadRequest{
		Namespace: ns,
		Key:       key,
		Epoch:     p.epoch,
	}, &resp)
	if err != nil {
		return nil, core.Stamp{}, err
	}
	if !resp.Found {
		return nil, core.Stamp{}, ErrNotFound
	}
	return resp.Value, resp.Stamp, nil
}

// remoteSet writes one key on its owner.
func (r *router) remoteSet(ctx context.Context, p placement, ns, key string, val []byte, ttl time.Duration) error {
	return r.invoke(ctx, p.owner, transport.MethodSet, &wire.SetRequest{
		Namespace: ns,
		Key:       key,
		Value:     val,
		TTLNanos:  int64(ttl),
		Epoch:     p.epoch,
	}, &wire.SetResponse{})
}

// remoteDelete removes one key on its owner.
func (r *router) remoteDelete(ctx context.Context, p placement, ns, key string) error {
	return r.invoke(ctx, p.owner, transport.MethodDelete, &wire.DeleteRequest{
		Namespace: ns,
		Key:       key,
		Epoch:     p.epoch,
	}, &wire.DeleteResponse{})
}

// remoteCall runs a registered function on a group's owner.
func (r *router) remoteCall(ctx context.Context, p placement, group, fn string, arg []byte) ([]byte, error) {
	var resp wire.CallResponse
	err := r.invoke(ctx, p.owner, transport.MethodCall, &wire.CallRequest{
		Group:    group,
		FuncName: fn,
		Args:     arg,
		Epoch:    p.epoch,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Result, nil
}

// groupKeysByOwner splits keys into one bucket per owning node.
//
// This is what makes BatchGet cost one request per owner rather than one per
// key, and it is the same grouping [Router] applies to messages. Keys whose
// owner cannot be resolved are returned separately rather than dropped: a
// caller that asked about a key deserves to know its fate, and silently
// omitting it is indistinguishable from a miss.
func (r *router) groupKeysByOwner(ns *namespace, keys []string) (map[string]*ownerBatch, []string, error) {
	byOwner := make(map[string]*ownerBatch)
	var unresolved []string

	for _, key := range keys {
		p, err := r.locate(ns.groupKeyFor(key))
		if err != nil {
			unresolved = append(unresolved, key)
			continue
		}
		id := p.owner.ID
		if p.local {
			id = localBucket
		}
		batch, ok := byOwner[id]
		if !ok {
			batch = &ownerBatch{placement: p}
			byOwner[id] = batch
		}
		batch.keys = append(batch.keys, key)
	}
	return byOwner, unresolved, nil
}

// localBucket is the map key for keys this node owns. A node ID cannot collide
// with it: an empty ID is rejected at construction, and this is not empty.
const localBucket = "\x00local"

// ownerBatch is the set of keys destined for one node.
type ownerBatch struct {
	placement placement
	keys      []string
}

// remoteBatchGet fetches many keys from one owner in a single round trip.
func (r *router) remoteBatchGet(ctx context.Context, p placement, ns string, keys []string) ([]wire.BatchEntry, error) {
	var resp wire.BatchGetResponse
	err := r.invoke(ctx, p.owner, transport.MethodBatchGet, &wire.BatchGetRequest{
		Namespace: ns,
		Keys:      keys,
		Epoch:     p.epoch,
	}, &resp)
	if err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// retryable reports whether an error means "ask again, possibly elsewhere".
//
// Ownership answers go stale in flight: a key can move between resolving its
// owner and the owner receiving the request. Rather than surface that to the
// caller as a failure, the data path re-resolves once. It does not loop —
// a second disagreement means membership is genuinely unsettled, and spinning
// would turn a brownout into a storm.
func retryable(err error) bool {
	return errors.Is(err, ErrNotOwner) ||
		errors.Is(err, ErrStaleEpoch) ||
		errors.Is(err, ErrActivationPending)
}
