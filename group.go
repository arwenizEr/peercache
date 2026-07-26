package peercache

import (
	"context"

	"github.com/peercache/peercache/internal/core"
)

// Group is a handle to a named collection of keys guaranteed to share an owner
// (SPEC.md section 3.2).
//
// A group is a virtual actor: addressable at all times, activated lazily, and
// deactivated after an idle period. While activated it owns exactly one
// goroutine, fed by a bounded mailbox, and every operation below runs as one
// turn on it.
//
// **There is no Lock and there will not be one.** Serialization is structural —
// it comes from there being one goroutine, not from callers agreeing to take a
// lock. A lock API here would also be a *distributed* lock API, with all the
// liveness problems that implies, layered on a membership system that is
// explicitly AP.
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
	// independent TTL; they live as long as the group's record does.
	Set(ctx context.Context, key string, val []byte) error

	// Delete removes a key from the group. Removing an absent key is not an
	// error.
	Delete(ctx context.Context, key string) error

	// Keys returns the group's keys, sorted. The result is a snapshot taken
	// inside one turn, so it is internally consistent but stale the instant
	// it is returned.
	Keys(ctx context.Context) ([]string, error)

	// Items returns the group's full contents as a snapshot. Cost is
	// proportional to the group's size; prefer [Group.Call] to compute over
	// a large group in place rather than shipping it to the caller.
	Items(ctx context.Context) (map[string][]byte, error)

	// Call runs the registered function fn against this group's state,
	// inside a single turn.
	//
	// This is the primary API. Moving the computation to the data is the
	// point of the library: a counter increment, an aggregate update or a
	// state-machine transition costs one round trip and no state transfer.
	// If the mailbox is full, Call returns [ErrGroupOverloaded] rather than
	// blocking past the caller's deadline.
	Call(ctx context.Context, fn string, arg []byte) ([]byte, error)

	// Flush deactivates the group, releasing its goroutine once pending
	// turns complete. State and pending timers survive; the next operation
	// reactivates it.
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
// goroutine and using it later is a use-after-free in all but name, and it is
// what the generation stamp exists to stop: methods on an escaped state do
// nothing and are counted, so the escape can neither corrupt a later turn nor
// race with one. It does not panic, because the goroutine holding it is not
// inside a Call and there is nobody to return an error to.
type GroupState = core.GroupState

// group is the M2 implementation of [Group], delegating to the actor runtime.
type group struct {
	cache *cache
	id    string
}

func (g *group) ID() string { return g.id }

// status reports why the group cannot be used, or nil.
func (g *group) status() error {
	if g.cache.isClosed() {
		return ErrClosed
	}
	return nil
}

func (g *group) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.status(); err != nil {
		return nil, err
	}

	out, err := g.cache.groups.Do(ctx, g.id, func(s GroupState) (any, error) {
		val, ok := s.Get(key)
		if !ok {
			return nil, ErrNotFound
		}
		return val, nil
	})
	if err != nil {
		return nil, err
	}
	val, _ := out.([]byte)
	return val, nil
}

func (g *group) Set(ctx context.Context, key string, val []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.status(); err != nil {
		return err
	}
	_, err := g.cache.groups.Do(ctx, g.id, func(s GroupState) (any, error) {
		s.Set(key, val)
		return nil, nil
	})
	return err
}

func (g *group) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.status(); err != nil {
		return err
	}
	_, err := g.cache.groups.Do(ctx, g.id, func(s GroupState) (any, error) {
		s.Delete(key)
		return nil, nil
	})
	return err
}

func (g *group) Keys(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.status(); err != nil {
		return nil, err
	}
	out, err := g.cache.groups.Do(ctx, g.id, func(s GroupState) (any, error) {
		return s.Keys(), nil
	})
	if err != nil {
		return nil, err
	}
	keys, _ := out.([]string)
	return keys, nil
}

func (g *group) Items(ctx context.Context) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.status(); err != nil {
		return nil, err
	}
	out, err := g.cache.groups.Do(ctx, g.id, func(s GroupState) (any, error) {
		keys := s.Keys()
		items := make(map[string][]byte, len(keys))
		for _, key := range keys {
			if val, ok := s.Get(key); ok {
				// Copied: this snapshot leaves the turn, so it
				// cannot alias state a later turn may rewrite.
				// Every other read in this library aliases; this
				// one cannot.
				dup := make([]byte, len(val))
				copy(dup, val)
				items[key] = dup
			}
		}
		return items, nil
	})
	if err != nil {
		return nil, err
	}
	items, _ := out.(map[string][]byte)
	return items, nil
}

// Call runs fn on the group's owner, wherever that is.
//
// This is the API SPEC.md §3.2 calls primary, and the only group operation that
// crosses the network. The rest — Get, Set, Keys, Items — read or write group
// state directly and are answered only by the owner, because shipping a group's
// state to a caller is the thing this design exists to avoid. A caller that
// wants to work with a group it does not own uses Call and the work happens
// there.
func (g *group) Call(ctx context.Context, fn string, arg []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := g.status(); err != nil {
		return nil, err
	}

	p, err := g.cache.route.locate(g.id)
	if err != nil {
		return nil, err
	}
	if p.local {
		return g.cache.groups.Call(ctx, g.id, fn, arg)
	}

	out, err := g.cache.route.remoteCall(ctx, p, g.id, fn, arg)
	if err == nil || !retryable(err) {
		return out, err
	}

	// Re-resolve once. A group can move between resolving its owner and the
	// owner receiving the call, and that race is routine rather than a
	// failure. It is not a loop: a second disagreement means membership is
	// genuinely unsettled, and retrying into that makes a brownout worse.
	next, rerr := g.cache.route.locate(g.id)
	if rerr != nil {
		return nil, err
	}
	if next.local {
		return g.cache.groups.Call(ctx, g.id, fn, arg)
	}
	if next.owner.ID == p.owner.ID {
		return nil, err
	}
	return g.cache.route.remoteCall(ctx, next, g.id, fn, arg)
}

func (g *group) Flush(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.status(); err != nil {
		return err
	}
	g.cache.groups.Deactivate(g.id)
	return nil
}

var _ Group = (*group)(nil)
