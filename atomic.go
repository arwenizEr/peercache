package peercache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/arwenizEr/peercache/internal/store"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// This file holds the conditional operations on namespace keys — SetIfAbsent,
// CompareAndSwap, Increment and Touch — from the public method to the owner's
// store. See docs/decisions/0027-atomic-key-operations.md.
//
// Each one runs on the key's owner as a single [store.Store.Update], so the
// check and the write cannot be separated by another writer, and then
// replicates and invalidates exactly as Set does. None of them is idempotent
// except Touch, which is why routeWrite retries only on refusals that
// guarantee nothing was applied.

func (n *namespace) SetIfAbsent(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error) {
	return n.condSet(ctxOrBackground(ctx), key, val, false, nil, ttl)
}

func (n *namespace) CompareAndSwap(ctx context.Context, key string, old, new []byte, ttl time.Duration) (bool, error) {
	return n.condSet(ctxOrBackground(ctx), key, new, true, old, ttl)
}

func (n *namespace) Increment(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	ctx = ctxOrBackground(ctx)
	var result int64
	err := n.routeWrite(ctx, key,
		func() (err error) {
			result, err = n.localIncrement(ctx, key, delta, ttl)
			return err
		},
		func(p placement) error {
			var resp wire.IncrementResponse
			err := n.cache.route.invoke(ctx, p.owner, transport.MethodIncrement, &wire.IncrementRequest{
				Namespace: n.name,
				Key:       key,
				Delta:     delta,
				TTLNanos:  int64(ttl),
				Epoch:     p.epoch,
			}, &resp)
			result = resp.Value
			return heldRemotely(err, p, resp.UnderReplicated)
		})
	return result, err
}

func (n *namespace) Touch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ctx = ctxOrBackground(ctx)
	var existed bool
	err := n.routeWrite(ctx, key,
		func() (err error) {
			existed, err = n.localTouch(ctx, key, ttl)
			return err
		},
		func(p placement) error {
			var resp wire.TouchResponse
			err := n.cache.route.invoke(ctx, p.owner, transport.MethodTouch, &wire.TouchRequest{
				Namespace: n.name,
				Key:       key,
				TTLNanos:  int64(ttl),
				Epoch:     p.epoch,
			}, &resp)
			existed = resp.Existed
			return heldRemotely(err, p, resp.UnderReplicated)
		})
	return existed, err
}

// condSet carries SetIfAbsent (compare false) and CompareAndSwap.
func (n *namespace) condSet(ctx context.Context, key string, val []byte, compare bool, old []byte, ttl time.Duration) (bool, error) {
	var stored bool
	err := n.routeWrite(ctx, key,
		func() (err error) {
			stored, err = n.localCondSet(ctx, key, val, compare, old, ttl)
			return err
		},
		func(p placement) error {
			var resp wire.CondSetResponse
			err := n.cache.route.invoke(ctx, p.owner, transport.MethodCondSet, &wire.CondSetRequest{
				Namespace: n.name,
				Key:       key,
				Value:     val,
				Compare:   compare,
				Old:       old,
				TTLNanos:  int64(ttl),
				Epoch:     p.epoch,
			}, &resp)
			stored = resp.Stored
			return heldRemotely(err, p, resp.UnderReplicated)
		})
	return stored, err
}

// routeWrite runs a write on key's owner: local here, or remote there.
//
// It re-resolves the owner once if the owner refused with ErrNotOwner,
// ErrStaleEpoch or ErrActivationPending, because those are answered before
// anything is applied. It does not retry anything else. An unreachable owner
// or an expired deadline may have applied the operation before the reply was
// lost, and retrying an Increment or a CompareAndSwap would apply it twice.
func (n *namespace) routeWrite(ctx context.Context, key string, local func() error, remote func(placement) error) error {
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
	if p.local {
		return local()
	}
	err = remote(p)
	if err != nil && retryable(err) {
		next, rerr := n.cache.route.locate(n.groupKeyFor(key))
		if rerr != nil || (!next.local && next.owner.ID == p.owner.ID) {
			return err
		}
		if next.local {
			return local()
		}
		err = remote(next)
	}
	if err == nil && n.l1 != nil {
		// As for Set: a copy this node held is now known stale.
		n.l1.Invalidate(key)
	}
	return err
}

// localUpdate applies fn to key's value on this node, the owner, then
// replicates and invalidates the result as localSet does. fn sees and returns
// plain values; the namespace's framing is applied and removed here. It runs
// under the store shard's lock, so it must be quick and must not block.
//
// It reports whether anything was written. A write that stored locally and
// then failed synchronous replication reports true with the error, for the
// same reason localSet does not roll back.
func (n *namespace) localUpdate(ctx context.Context, key string, fn func(cur []byte, ok bool) (store.Mutation, bool, error)) (bool, error) {
	if n.shedHeld == nil {
		return n.localUpdateOnce(ctx, key, fn) // nothing shed to read back
	}
	for attempt := 0; ; attempt++ {
		wrote, err := n.localUpdateOnce(ctx, key, fn)
		if !errors.Is(err, errShedSinceRestore) {
			return wrote, err
		}
		if attempt == shedRetries {
			return false, n.shedSinceRestore(key)
		}
	}
}

// errShedSinceRestore reports that the memory budget shed key between the
// wait for its restore and the update itself; the update reads it back and
// tries again.
var errShedSinceRestore = errors.New("peercache: key shed since its restore")

// shedRetries bounds how often one update reads back a key the budget keeps
// shedding. Each retry is a shed racing this very update, so a few suffice.
const shedRetries = 8

func (n *namespace) localUpdateOnce(ctx context.Context, key string, fn func(cur []byte, ok bool) (store.Mutation, bool, error)) (bool, error) {
	if err := n.awaitRestore(ctx, key); err != nil {
		return false, err
	}
	var encoded []byte
	var plainLen int
	groupKey := n.groupKeyFor(key)
	res, err := n.store.Update(key, func(stored []byte, ok bool) (store.Mutation, bool, error) {
		// Ownership is checked again here, under the key's shard lock,
		// because the check the caller passed came before awaitRestore,
		// which can wait seconds. Demotion takes the key out of this store
		// under the same lock, so the two are ordered: either this update
		// runs first and the key leaves with its new value, or the key has
		// left and this refuses. Without it an Increment that waited out a
		// restore while the key moved back to its owner ran on the store the
		// key had just left, counted from nothing, and replicated that to the
		// owner with a newer order than the real count (open question 58).
		if err := n.cache.stillOwner(groupKey); err != nil {
			return store.Mutation{}, false, err
		}
		// And the shed mark, under the same lock for the same reason: the
		// budget can shed the key after awaitRestore read it back, and it
		// marks a key under this lock before removing it. Absent and
		// marked is "not in memory", not "not there"; counted from zero,
		// the write outranked the stored value (open question 62).
		if !ok && n.shedHeld != nil && n.shedHeld(key) {
			return store.Mutation{}, false, errShedSinceRestore
		}
		var cur []byte
		if ok {
			// Compared and parsed decoded: two framings of one value need
			// not be equal bytes.
			plain, err := n.decode(stored)
			if err != nil {
				return store.Mutation{}, false, err
			}
			cur = plain
		}
		m, write, err := fn(cur, ok)
		if err != nil || !write {
			return store.Mutation{}, false, err
		}
		plainLen = len(m.Value)
		encoded = n.encode(m.Value)
		m.Value = encoded
		return m, true, nil
	})
	if err != nil || !res.Wrote {
		return false, err
	}
	n.cache.pressure.noteWrite(plainLen)

	// Replicas are sent a lifetime and hold the owner's deadline, so a kept
	// deadline is sent as what remains of it.
	var ttl time.Duration
	if res.ExpiresAt != 0 {
		ttl = time.Duration(res.ExpiresAt - n.cache.opts.Clock.Now().UnixNano())
		if ttl <= 0 {
			ttl = 1
		}
	}
	stamp := n.stamp(res.Version)
	if n.cache.traced(n.name, key) {
		n.cache.record(n.name, key, keytraceWrite, res.Order, n.cache.selfID(), res.Version, encoded, "update")
	}
	// Invalidated whether or not the fan-out succeeded; see localSet.
	if n.persister != nil {
		n.persister.record(key, encoded, res.ExpiresAt, res.Order, false)
	}
	err = n.fanOut(ctx, key, encoded, ttl, stamp, res.Order, res.Prev, false)
	n.hint(key, stamp)
	if err != nil {
		return true, err
	}
	return true, nil
}

func (n *namespace) localCondSet(ctx context.Context, key string, val []byte, compare bool, old []byte, ttl time.Duration) (bool, error) {
	return n.localUpdate(ctx, key, func(cur []byte, ok bool) (store.Mutation, bool, error) {
		if compare {
			if !ok || !bytes.Equal(cur, old) {
				return store.Mutation{}, false, nil
			}
		} else if ok {
			return store.Mutation{}, false, nil
		}
		return store.Mutation{Value: val, TTL: ttl}, true, nil
	})
}

func (n *namespace) localIncrement(ctx context.Context, key string, delta int64, ttl time.Duration) (int64, error) {
	var result int64
	_, err := n.localUpdate(ctx, key, func(cur []byte, ok bool) (store.Mutation, bool, error) {
		var current int64
		if ok {
			v, err := strconv.ParseInt(string(cur), 10, 64)
			if err != nil {
				return store.Mutation{}, false, fmt.Errorf("namespace %q: key %q holds %q: %w",
					n.name, key, truncateForError(cur), ErrNotAnInteger)
			}
			current = v
		}
		sum := current + delta
		if (delta > 0 && sum < current) || (delta < 0 && sum > current) {
			return store.Mutation{}, false, fmt.Errorf("namespace %q: key %q: %d %+d overflows int64: %w",
				n.name, key, current, delta, ErrNotAnInteger)
		}
		result = sum
		m := store.Mutation{Value: strconv.AppendInt(nil, sum, 10)}
		if ok {
			m.KeepDeadline = true
		} else {
			m.TTL = ttl
		}
		return m, true, nil
	})
	return result, err
}

func (n *namespace) localTouch(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	var existed bool
	_, err := n.localUpdate(ctx, key, func(cur []byte, ok bool) (store.Mutation, bool, error) {
		if !ok {
			return store.Mutation{}, false, nil
		}
		existed = true
		return store.Mutation{Value: cur, TTL: ttl}, true, nil
	})
	return existed, err
}

// truncateForError keeps an error message about a value from carrying the
// whole value.
func truncateForError(b []byte) string {
	const max = 32
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "…"
}

func (h *peerHandler) condSet(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.CondSetRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}
	ns, err := h.writableNamespace(in.Namespace)
	if err != nil {
		return nil, err
	}
	stored, err := ns.localCondSet(ctx, in.Key, in.Value, in.Compare, in.Old, time.Duration(in.TTLNanos))
	held, err := heldLocally(err)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.CondSetResponse{Stored: stored, UnderReplicated: held})
}

func (h *peerHandler) increment(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.IncrementRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}
	ns, err := h.writableNamespace(in.Namespace)
	if err != nil {
		return nil, err
	}
	v, err := ns.localIncrement(ctx, in.Key, in.Delta, time.Duration(in.TTLNanos))
	held, err := heldLocally(err)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.IncrementResponse{Value: v, UnderReplicated: held})
}

func (h *peerHandler) touch(ctx context.Context, req []byte) ([]byte, error) {
	var in wire.TouchRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	h.noteEpoch(in.Epoch)
	if err := h.assertOwned(in.Namespace, in.Key); err != nil {
		return nil, err
	}
	// Like delete, not like set: a namespace this node has never opened holds
	// nothing to touch, and creating it for a touch would only count a
	// misconfiguration that wrote nothing.
	ns, ok := h.localNamespace(in.Namespace)
	if !ok {
		return wire.Encode(&wire.TouchResponse{})
	}
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}
	existed, err := ns.localTouch(ctx, in.Key, time.Duration(in.TTLNanos))
	held, err := heldLocally(err)
	if err != nil {
		return nil, err
	}
	return wire.Encode(&wire.TouchResponse{Existed: existed, UnderReplicated: held})
}

// heldLocally splits an owner's ErrUnderReplicated off the error it returns to
// a forwarding peer. That write happened, and its result must reach the caller
// — an Increment's new value, a SetIfAbsent's true — but an RPC error carries
// no body. So it travels as a field of the response, and every other error as
// an error.
func heldLocally(err error) (string, error) {
	if err != nil && errors.Is(err, ErrUnderReplicated) {
		return err.Error(), nil
	}
	return "", err
}

// heldRemotely is heldLocally's other half, on the forwarding node: an owner's
// report that the write is held below its factor becomes ErrUnderReplicated
// again, beside the result the response carried.
func heldRemotely(err error, p placement, held string) error {
	if err != nil || held == "" {
		return err
	}
	return &remoteHeld{owner: string(p.owner.ID), detail: held}
}

// remoteHeld is ErrUnderReplicated as reported by a write's owner.
type remoteHeld struct {
	owner, detail string
}

// Error returns the owner's report and names the owner.
func (e *remoteHeld) Error() string { return e.detail + " (on owner " + e.owner + ")" }

// Is makes errors.Is(err, ErrUnderReplicated) hold.
func (e *remoteHeld) Is(target error) bool { return target == ErrUnderReplicated }
