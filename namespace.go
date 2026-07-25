package peercache

import (
	"context"
	"time"

	"github.com/peercache/peercache/internal/core"
)

// Namespace is a handle to a named keyspace (SPEC.md section 3.1).
//
// Values are opaque bytes at this layer; encoding is the codec's business. Keys
// map to groups through the namespace's [Partitioner], and a group maps to
// exactly one owning node, so every operation below either executes locally or
// is forwarded to that owner. The caller cannot tell which, except by latency.
//
// Every method takes a context and honours its deadline across the RPC
// boundary. None of them block indefinitely.
type Namespace interface {
	// Get returns the value for key. A missing key returns [ErrNotFound],
	// never a nil value, so that a key holding zero bytes stays
	// distinguishable from a key holding nothing.
	//
	// If the key is not owned locally, the value may come from an L1 copy
	// and be stale by up to the namespace's L1 TTL.
	Get(ctx context.Context, key string) ([]byte, error)

	// Set stores val under key for ttl. A zero ttl means the value does not
	// expire on its own; it can still be lost, because nothing here is
	// durable.
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

	// BatchSet stores every pair in kv with the same ttl, batched per owner.
	// It is not atomic across owners, and there is no cross-group
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

// noopNamespace is the M0 placeholder implementation of [Namespace].
//
// err, when non-nil, is an option validation failure captured by
// [Cache.Namespace], which cannot itself return an error. Reporting it from
// every operation is the least surprising alternative to ignoring it.
type noopNamespace struct {
	cache *noopCache
	name  string
	opts  core.NamespaceOptions
	err   error
}

// status returns the error every operation on this handle should report, or nil
// if the handle is usable. Ordering matters: a closed cache is reported ahead of
// a configuration problem, because after Close nothing about configuration can
// still be acted on.
func (n *noopNamespace) status() error {
	if n.cache.isClosed() {
		return ErrClosed
	}
	if n.err != nil {
		return n.err
	}
	return ErrNotImplemented
}

func (n *noopNamespace) Get(ctx context.Context, _ string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, n.status()
}

func (n *noopNamespace) Set(ctx context.Context, _ string, _ []byte, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.status()
}

func (n *noopNamespace) Delete(ctx context.Context, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.status()
}

func (n *noopNamespace) Exists(ctx context.Context, _ string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return false, n.status()
}

func (n *noopNamespace) BatchGet(ctx context.Context, _ []string) (map[string][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, n.status()
}

func (n *noopNamespace) BatchSet(ctx context.Context, _ map[string][]byte, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return n.status()
}

func (n *noopNamespace) GetOrLoad(ctx context.Context, _ string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n.err == nil && !n.cache.isClosed() && n.opts.Loader == nil {
		return nil, ErrNoLoader
	}
	return nil, n.status()
}

var _ Namespace = (*noopNamespace)(nil)
