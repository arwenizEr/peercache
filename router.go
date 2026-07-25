package peercache

import (
	"context"
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
// M0 status: the returned router reports [ErrNotImplemented] from Submit.
func NewRouter[T any](c Cache, groupOf func(T) string, handler string) Router[T] {
	return &noopRouter[T]{cache: c, groupOf: groupOf, handler: handler}
}

// noopRouter is the M0 placeholder implementation of [Router].
type noopRouter[T any] struct {
	cache   Cache
	groupOf func(T) string
	handler string
}

func (r *noopRouter[T]) Submit(ctx context.Context, msgs ...T) error {
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
	return ErrNotImplemented
}

var _ Router[int] = (*noopRouter[int])(nil)
