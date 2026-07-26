package peercache

import (
	"context"
	"errors"
	"fmt"

	"github.com/peercache/peercache/internal/transport"
	"github.com/peercache/peercache/internal/wire"
)

// Router routes typed messages to the owner of the group each belongs to
// (SPEC.md section 3.4).
//
// This is the queue-workload shape of the same idea as [Partitioner], and it is
// a first-class API rather than an internal optimisation because the batching
// it enables is the difference between one RPC and ten thousand. Submit groups
// messages by owner, issues one request per owner, and the owner applies every
// message for a given group inside a single actor turn.
type Router[T any] interface {
	// Submit routes msgs to their owners. It returns once every owner has
	// accepted its batch, or with an error if any batch failed; a partial
	// failure is reported as an error even though the successful batches
	// have already been applied. There is no cross-owner atomicity and
	// there is no rollback.
	Submit(ctx context.Context, msgs ...T) error
}

// NewRouter returns a [Router] that maps each message to a group with groupOf
// and applies it on the owner via the registered function named handler.
//
// groupOf must be pure and consistent across pods, exactly as a [Partitioner]
// must be: it is the same decision, made over a message instead of a key.
//
// # What the handler receives
//
// The handler is an ordinary registered group function, so it is named rather
// than serialised (CLAUDE.md non-negotiable 3). Its argument is not one message
// but the whole batch for that group, encoded — unpack it with [Batch]. Handing
// it the batch is what makes the affinity real: the function runs once per
// group per flush, inside one turn, instead of once per message.
//
//	cache.RegisterGroupFunc("apply", func(ctx context.Context, s GroupState, arg []byte) ([]byte, error) {
//	    msgs, err := peercache.Batch[Order](arg)
//	    if err != nil {
//	        return nil, err
//	    }
//	    for _, m := range msgs {
//	        // ... apply m to s
//	    }
//	    return nil, nil
//	})
func NewRouter[T any](c Cache, groupOf func(T) string, handler string) Router[T] {
	return &messageRouter[T]{cache: c, groupOf: groupOf, handler: handler}
}

// Batch decodes a router batch inside a handler.
//
// It is the counterpart of [Router.Submit]: Submit encodes each message, the
// owner receives one group's messages as a single argument, and this turns them
// back into values in submission order.
func Batch[T any](arg []byte) ([]T, error) {
	var raw [][]byte
	if err := wire.Decode(arg, &raw); err != nil {
		return nil, err
	}
	out := make([]T, 0, len(raw))
	for i, b := range raw {
		var msg T
		if err := wire.Decode(b, &msg); err != nil {
			return nil, fmt.Errorf("peercache: decoding batch message %d: %w", i, err)
		}
		out = append(out, msg)
	}
	return out, nil
}

// messageRouter is the implementation of [Router].
type messageRouter[T any] struct {
	cache   Cache
	groupOf func(T) string
	handler string
}

func (r *messageRouter[T]) Submit(ctx context.Context, msgs ...T) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.cache == nil || r.groupOf == nil || r.handler == "" {
		return ErrInvalidOption
	}
	if len(msgs) == 0 {
		// Submitting nothing succeeds. The caller's postcondition, that
		// every supplied message has been routed, already holds.
		return nil
	}

	c, ok := r.cache.(*cache)
	if !ok {
		return fmt.Errorf("peercache: router needs a cache from New: %w", ErrInvalidOption)
	}
	if c.isClosed() {
		return ErrClosed
	}

	// Group by group key first, preserving submission order within each
	// group. Order matters: these are queue messages, and a handler applying
	// a state machine cares which arrived first.
	byGroup := make(map[string][][]byte)
	var groupOrder []string
	for i := range msgs {
		encoded, err := wire.Encode(&msgs[i])
		if err != nil {
			return fmt.Errorf("peercache: encoding message %d: %w", i, err)
		}
		g := r.groupOf(msgs[i])
		if _, seen := byGroup[g]; !seen {
			groupOrder = append(groupOrder, g)
		}
		byGroup[g] = append(byGroup[g], encoded)
	}

	// Then group the groups by owner. This second grouping is what bounds
	// the request count by the number of owners rather than by the number of
	// groups or messages, which is the acceptance criterion in SPEC.md §4.
	byOwner := make(map[string]*ownerWork)
	var ownerOrder []string
	var failures error

	for _, g := range groupOrder {
		p, err := c.route.locate(g)
		if err != nil {
			failures = errors.Join(failures, fmt.Errorf("peercache: group %q: %w", g, err))
			continue
		}
		id := p.owner.ID
		if p.local {
			id = localBucket
		}
		work, seen := byOwner[id]
		if !seen {
			work = &ownerWork{placement: p}
			byOwner[id] = work
			ownerOrder = append(ownerOrder, id)
		}
		work.batches = append(work.batches, wire.GroupBatch{Group: g, Messages: byGroup[g]})
	}

	for _, id := range ownerOrder {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures, err)
		}
		work := byOwner[id]
		if id == localBucket {
			failures = errors.Join(failures, r.applyLocal(ctx, c, work.batches))
			continue
		}
		failures = errors.Join(failures, r.applyRemote(ctx, c, work.placement, work.batches))
	}
	return failures
}

// ownerWork is every group destined for one node in a single flush.
type ownerWork struct {
	placement placement
	batches   []wire.GroupBatch
}

// applyLocal runs the batches this node owns, one turn per group.
func (r *messageRouter[T]) applyLocal(ctx context.Context, c *cache, batches []wire.GroupBatch) error {
	var failures error
	for _, b := range batches {
		arg, err := wire.Encode(&b.Messages)
		if err != nil {
			failures = errors.Join(failures, err)
			continue
		}
		if _, err := c.groups.Call(ctx, b.Group, r.handler, arg); err != nil {
			failures = errors.Join(failures,
				fmt.Errorf("peercache: group %q: %w", b.Group, err))
		}
	}
	return failures
}

// applyRemote sends one request carrying every group destined for one peer.
func (r *messageRouter[T]) applyRemote(ctx context.Context, c *cache, p placement, batches []wire.GroupBatch) error {
	var resp wire.SubmitResponse
	err := c.route.invoke(ctx, p.owner, transport.MethodSubmit, &wire.SubmitRequest{
		FuncName: r.handler,
		Batches:  batches,
		Epoch:    p.epoch,
	}, &resp)
	if err != nil {
		return fmt.Errorf("peercache: submitting to %s: %w", p.owner.ID, err)
	}

	// Per-group failures come back individually rather than as one error,
	// because the groups that did apply have already applied and the caller
	// needs to know which ones to resubmit.
	var failures error
	for _, f := range resp.Failures {
		failures = errors.Join(failures,
			fmt.Errorf("peercache: group %q on %s: %s", f.Group, p.owner.ID, f.Error))
	}
	return failures
}

var _ Router[int] = (*messageRouter[int])(nil)
