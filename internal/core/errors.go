package core

import "errors"

// Sentinel errors.
//
// CLAUDE.md requires sentinel errors to live in the root package. They are
// declared here because internal packages must be able to return them without
// importing the root, and the root re-exports each one by assignment:
//
//	var ErrNotFound = core.ErrNotFound
//
// Assignment copies the pointer, not the value, so peercache.ErrNotFound and
// core.ErrNotFound are the same error object and errors.Is behaves identically
// through either name. The public surface still reads as though the root
// package owns them.
var (
	// ErrNotImplemented is returned by every no-op implementation created
	// in M0. Its presence in a returned error means the code path exists
	// but the milestone that fills it in has not run yet. It must not
	// survive past the milestone that implements the path.
	ErrNotImplemented = errors.New("peercache: not implemented")

	// ErrNotFound means the key or group does not exist. It is distinct
	// from a nil value: a key may legitimately hold zero bytes.
	ErrNotFound = errors.New("peercache: not found")

	// ErrClosed means the cache has been closed. Every operation returns
	// it after Close, and it is never retryable.
	ErrClosed = errors.New("peercache: cache is closed")

	// ErrGroupOverloaded means a group's mailbox was full and the request
	// was rejected rather than queued (SPEC.md §4, M2). This is
	// backpressure working as designed, and it is retryable.
	ErrGroupOverloaded = errors.New("peercache: group mailbox full")

	// ErrStaleEpoch means an operation was attempted under a membership
	// epoch older than the one the node currently holds. The caller should
	// re-resolve ownership and retry.
	ErrStaleEpoch = errors.New("peercache: stale membership epoch")

	// ErrNotOwner means this node does not own the group key. It carries no
	// hint about who does; the caller re-resolves through the ownership
	// table.
	ErrNotOwner = errors.New("peercache: not the owner of this key")

	// ErrStateEscaped means a GroupState was used outside the actor turn
	// that produced it. SPEC.md §3.3 requires this be caught rather than
	// left as undefined behaviour, and caught without a panic.
	ErrStateEscaped = errors.New("peercache: group state used outside its turn")

	// ErrNoLoader means GetOrLoad was called on a namespace constructed
	// without WithLoader.
	ErrNoLoader = errors.New("peercache: namespace has no registered loader")

	// ErrFuncNotRegistered means Call named a group function that was not
	// registered on this node. Because all pods run the same binary
	// (CLAUDE.md, decision 3), this indicates a rolling upgrade in progress
	// or a registration that did not happen at startup.
	ErrFuncNotRegistered = errors.New("peercache: group function not registered")

	// ErrDuplicateRegistration means RegisterGroupFunc was called twice for
	// one name. Registration is startup-only and must be deterministic.
	ErrDuplicateRegistration = errors.New("peercache: group function already registered")

	// ErrLoaderPanic means a registered loader panicked. The panic is
	// contained and converted rather than propagated: a loader is
	// application code, and a bug in it must not become a panic in every
	// unrelated goroutine that happened to be waiting for the same key.
	ErrLoaderPanic = errors.New("peercache: loader panicked")

	// ErrLoaderFailed means a load could not complete for a reason that is
	// not the loader's own error, such as the loader exiting without
	// returning.
	ErrLoaderFailed = errors.New("peercache: loader did not complete")

	// ErrInvalidOption means a supplied option is outside its documented
	// range or contradicts another. Configuration is validated once, at
	// construction, so this never surfaces on a data path.
	ErrInvalidOption = errors.New("peercache: invalid option")

	// ErrNodeUnreachable means the transport could not deliver to a peer.
	// It wraps nothing about why; the cause is attached with %w by the
	// transport.
	ErrNodeUnreachable = errors.New("peercache: node unreachable")
)
