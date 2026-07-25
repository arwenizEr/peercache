package peercache

import (
	"context"
	"sync"

	"github.com/peercache/peercache/internal/core"
)

// Cache is the top-level handle: one per process, constructed at startup and
// closed at shutdown (SPEC.md section 3.1).
//
// A Cache is safe for concurrent use. Namespace and Group return handles, not
// data, and are cheap enough to call on a hot path.
type Cache interface {
	// Namespace returns a handle to a named keyspace, creating its
	// configuration on first use. Calling it again with different options
	// does not reconfigure an existing namespace; the first call wins.
	Namespace(name string, opts ...NamespaceOption) Namespace

	// Group returns a handle to the group with the given id. A group is
	// addressable at all times, whether or not it is currently activated
	// anywhere, so this never fails and never blocks.
	Group(id string) Group

	// RegisterGroupFunc registers fn under name so that it can be invoked
	// by [Group.Call] from any pod.
	//
	// Registration is a startup activity. Closures are never serialized
	// (CLAUDE.md, decision 3): an RPC carries the name and opaque argument
	// bytes, and every pod is assumed to run the same binary and therefore
	// to have registered the same names. Registering the same name twice
	// returns [ErrDuplicateRegistration].
	RegisterGroupFunc(name string, fn GroupFunc) error

	// Stats returns a snapshot of this node's counters and view of the
	// cluster.
	Stats() Stats

	// Close shuts the cache down. With a live context it drains gracefully:
	// deactivate groups, transfer ownership, then stop. If ctx expires
	// first, shutdown becomes ungraceful and this node's owned state is
	// lost, which is the documented consequence of having no durability.
	//
	// Close is idempotent.
	Close(ctx context.Context) error
}

// GroupFunc is a function registered by name and executed on a group's owner
// (SPEC.md section 3.3).
//
// It runs inside that group's turn, so it has exclusive access to s for the
// duration of the call and needs no locking. It must respect ctx: the caller's
// deadline is propagated into the turn, and a function that ignores it holds
// the group's only goroutine hostage while every other request queues behind
// it.
type GroupFunc = core.GroupFunc

// New constructs a [Cache].
//
// Options are validated once, here, so that a misconfiguration surfaces at
// startup rather than on the first request. [WithNodeID] is required.
//
// M0 status: the returned Cache is a no-op whose data-path methods report
// [ErrNotImplemented]. Construction, option validation and Close are real, so
// that the test harness has something to build on.
func New(opts ...Option) (Cache, error) {
	resolved := core.DefaultOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&resolved)
	}
	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	return &noopCache{opts: resolved}, nil
}

// noopCache is the M0 placeholder implementation of [Cache].
//
// It exists so that every interface in SPEC.md section 3 compiles and can be
// wired into the test harness before any distributed logic exists. Every method
// that would touch data reports [ErrNotImplemented]; every method that concerns
// lifecycle or configuration behaves correctly, because the harness depends on
// those and a lie there would be load-bearing.
type noopCache struct {
	opts core.Options

	mu         sync.Mutex
	closed     bool
	funcs      map[string]GroupFunc
	namespaces map[string]*noopNamespace
}

func (c *noopCache) Namespace(name string, opts ...NamespaceOption) Namespace {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The first call wins, as documented. Returning a reconfigured handle
	// on a later call would mean two callers holding handles to one
	// namespace with different replication factors and different staleness
	// bounds, which is a data-loss bug wearing a convenience-feature
	// costume.
	if ns, ok := c.namespaces[name]; ok {
		return ns
	}

	resolved := core.DefaultNamespaceOptions()
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(&resolved)
	}
	// Namespace cannot return an error, and returning a handle that
	// silently ignores a bad option would hide the misconfiguration until
	// production. Instead the error is captured and reported by every
	// operation on the handle.
	ns := &noopNamespace{cache: c, name: name, opts: resolved, err: resolved.Validate()}
	if c.namespaces == nil {
		c.namespaces = make(map[string]*noopNamespace)
	}
	c.namespaces[name] = ns
	return ns
}

func (c *noopCache) Group(id string) Group {
	return &noopGroup{cache: c, id: id}
}

func (c *noopCache) RegisterGroupFunc(name string, fn GroupFunc) error {
	if name == "" || fn == nil {
		return ErrInvalidOption
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	if _, dup := c.funcs[name]; dup {
		return ErrDuplicateRegistration
	}
	if c.funcs == nil {
		c.funcs = make(map[string]GroupFunc)
	}
	c.funcs[name] = fn
	return nil
}

func (c *noopCache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Self:    Node{ID: c.opts.NodeID, Addr: c.opts.BindAddr},
		Members: 1,
	}
}

func (c *noopCache) Close(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// isClosed reports whether Close has run. Handles consult it so that operations
// after Close report [ErrClosed] rather than [ErrNotImplemented], which keeps
// the lifecycle distinguishable from the scaffolding.
func (c *noopCache) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// hasFunc reports whether a group function is registered under name. Group
// handles ask through this rather than reaching into the cache's lock
// themselves.
func (c *noopCache) hasFunc(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.funcs[name]
	return ok
}

var _ Cache = (*noopCache)(nil)
