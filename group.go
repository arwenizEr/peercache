package peercache

import (
	"context"
	"errors"

	"github.com/peercache/peercache/internal/core"
)

// Group is a handle to a named collection of keys guaranteed to share an owner
// (SPEC.md section 3.2).
//
// A group is the unit of activation and of serialization. It is addressable at
// all times, but it is activated lazily on its owner and deactivated after an
// idle period. While activated, every operation on it runs on a single
// goroutine fed by a bounded mailbox.
//
// That is why there is no Lock method and never will be. Serialization is
// structural: it comes from there being one goroutine, not from callers
// agreeing to take a lock. A lock API would also be a distributed lock API,
// with all the liveness problems that implies, on top of a membership layer
// that is explicitly AP.
//
// The serialization guarantee holds while the group has one activation. A
// partition can briefly produce two, which is detected through epochs and
// counted, not prevented.
type Group interface {
	// ID returns the group's identifier.
	ID() string

	// Get returns the value for a key within the group, or [ErrNotFound].
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores a value within the group. Group entries have no
	// independent TTL; they live as long as the group is activated or until
	// deleted.
	Set(ctx context.Context, key string, val []byte) error

	// Delete removes a key from the group. Removing an absent key is not an
	// error.
	Delete(ctx context.Context, key string) error

	// Keys returns the group's keys. The result is a snapshot taken inside
	// one turn, so it is internally consistent but stale the instant it is
	// returned.
	Keys(ctx context.Context) ([]string, error)

	// Items returns the group's full contents as a snapshot. Cost is
	// proportional to the group's size; prefer [Group.Call] to compute over
	// a large group in place rather than shipping it to the caller.
	Items(ctx context.Context) (map[string][]byte, error)

	// Call runs the registered function fn against this group's state,
	// on the group's owner, inside a single turn.
	//
	// This is the primary API. Moving the computation to the data is the
	// point of the library: a counter increment, an aggregate update or a
	// state-machine transition costs one round trip and no state transfer.
	// If the mailbox is full, Call returns [ErrGroupOverloaded] rather than
	// blocking past the caller's deadline.
	Call(ctx context.Context, fn string, arg []byte) ([]byte, error)

	// Flush deactivates the group, releasing its memory after any pending
	// turns complete. Because nothing is durable, flushing a group whose
	// namespace has no loader discards its state.
	Flush(ctx context.Context) error
}

// GroupState is the actor-turn view of a group's memory (SPEC.md section 3.3).
//
// Its methods take no context and return no error because they are plain local
// map accesses running on the group's single activation goroutine. There is
// nothing to fail and nothing to wait for. That is the whole payoff of moving
// computation to the data.
//
// A GroupState is valid only for the turn that produced it. Capturing one in a
// goroutine and using it later is a use-after-free in all but name. The
// implementation stamps each state with a generation and turns a violation into
// [ErrStateEscaped] on the calling layer, rather than a panic or silent
// corruption.
type GroupState = core.GroupState

// noopGroup is the M0 placeholder implementation of [Group].
type noopGroup struct {
	cache *noopCache
	id    string
}

func (g *noopGroup) ID() string { return g.id }

// status mirrors noopNamespace.status: a closed cache is reported ahead of
// unimplemented scaffolding.
func (g *noopGroup) status() error {
	if g.cache.isClosed() {
		return ErrClosed
	}
	return ErrNotImplemented
}

func (g *noopGroup) Get(ctx context.Context, _ string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, g.status()
}

func (g *noopGroup) Set(ctx context.Context, _ string, _ []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.status()
}

func (g *noopGroup) Delete(ctx context.Context, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.status()
}

func (g *noopGroup) Keys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, g.status()
}

func (g *noopGroup) Items(ctx context.Context) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, g.status()
}

func (g *noopGroup) Call(ctx context.Context, fn string, _ []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.status(); !errors.Is(err, ErrNotImplemented) {
		return nil, err
	}
	// Registration is real even in M0, so an unregistered name is reported
	// as such. Anything else would make the harness unable to distinguish a
	// wiring mistake from missing scaffolding.
	if !g.cache.hasFunc(fn) {
		return nil, ErrFuncNotRegistered
	}
	return nil, ErrNotImplemented
}

func (g *noopGroup) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return g.status()
}

var _ Group = (*noopGroup)(nil)
