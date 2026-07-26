package peercache

import (
	"context"
	"fmt"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/l1"
	"github.com/peercache/peercache/internal/singleflight"
	"github.com/peercache/peercache/internal/store"
)

// Namespace is a handle to a named keyspace (SPEC.md section 3.1).
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

	// BatchGet returns the values for keys, omitting keys that are absent.
	// Keys are grouped by owner and issued as one request per owner, so
	// cost scales with the number of distinct owners rather than the number
	// of keys.
	//
	// A partial failure returns the values that were retrieved along with a
	// non-nil error.
	BatchGet(ctx context.Context, keys []string) (map[string][]byte, error)

	// BatchSet stores every pair in kv with the same ttl, batched per
	// owner. It is not atomic across owners, and there is no cross-group
	// transaction that would make it so.
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

// namespace is the M1 implementation of [Namespace].
//
// err, when non-nil, is a configuration failure captured at construction —
// [Cache.Namespace] cannot return an error, and reporting the failure from
// every operation is the least surprising alternative to ignoring it.
type namespace struct {
	cache *cache
	name  string
	opts  core.NamespaceOptions
	err   error

	store  *store.Store
	framer *codec.Framer
	flight singleflight.Group

	// l1 caches copies of keys this node does not own. Nil when there is no
	// membership authority, because a cluster of one owns everything and a
	// second copy of a local key would only be a way to serve it stale.
	//
	// Built in M1 and connected here in M4, which is when the first
	// non-owned read exists — see docs/decisions/open.md question 13.
	l1 *l1.Cache
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

// encode prepares a value for storage.
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}

	stored, _, ok := n.store.Get(key)
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

func (n *namespace) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}
	n.store.Set(key, n.encode(val), ttl)
	return nil
}

func (n *namespace) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}
	n.store.Delete(key)
	return nil
}

func (n *namespace) Exists(ctx context.Context, key string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if err := n.status(); err != nil {
		return false, err
	}
	return n.store.Exists(key), nil
}

func (n *namespace) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}

	out := make(map[string][]byte, len(keys))
	for i, key := range keys {
		// A large batch must not outlive the caller's deadline. Checking
		// periodically rather than per key keeps the check off the hot
		// path while still bounding the overrun.
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return out, err
			}
		}
		stored, _, ok := n.store.Get(key)
		if !ok {
			// Absent keys are omitted, not reported. A batch read
			// where half the keys are cold is the normal case, not
			// an error.
			continue
		}
		val, err := n.decode(stored)
		if err != nil {
			return out, err
		}
		out[key] = val
	}
	return out, nil
}

func (n *namespace) BatchSet(ctx context.Context, kv map[string][]byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := n.status(); err != nil {
		return err
	}

	i := 0
	for key, val := range kv {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				// Partial application is visible. There is no
				// cross-key atomicity here and never will be.
				return err
			}
		}
		n.store.Set(key, n.encode(val), ttl)
		i++
	}
	return nil
}

func (n *namespace) GetOrLoad(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}
	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	if stored, _, ok := n.store.Get(key); ok {
		return n.decode(stored)
	}

	result := n.flight.Do(ctx, key, func(loadCtx context.Context) ([]byte, time.Duration, error) {
		// Re-check under the flight. Between the miss above and becoming
		// the leader, a previous load may have completed and populated
		// the key; loading it again would be a pointless round trip to
		// whatever the loader talks to.
		if stored, _, ok := n.store.Get(key); ok {
			val, err := n.decode(stored)
			return val, 0, err
		}

		// The load is bounded by the namespace's timeout, not by
		// whichever caller happened to arrive first. loadCtx already has
		// that caller's cancellation stripped; without a deadline of our
		// own it would have none at all.
		loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()

		val, ttl, err := n.opts.Loader(loadCtx, key)
		if err != nil {
			return nil, 0, err
		}
		// Store inside the flight so that every collapsed caller sees a
		// populated namespace, not just the leader.
		n.store.Set(key, n.encode(val), ttl)
		return val, ttl, nil
	})
	if result.Err != nil {
		return nil, fmt.Errorf("namespace %q key %q: %w", n.name, key, result.Err)
	}
	return result.Value, nil
}

var _ Namespace = (*namespace)(nil)
