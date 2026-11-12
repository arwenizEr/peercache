package core

import "errors"

// Sentinel errors.
//
// Sentinel errors belong to the root package. They are
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
	// ErrNotImplemented reports an RPC method this binary does not
	// recognise, or a cache asked to reach another node with no transport
	// configured. See the root package's documentation of it.
	ErrNotImplemented = errors.New("peercache: not implemented")

	// ErrNotFound means the key or group does not exist. It is distinct
	// from a nil value: a key may legitimately hold zero bytes.
	ErrNotFound = errors.New("peercache: not found")

	// ErrClosed means the cache has been closed. Every operation returns
	// it after Close, and it is never retryable.
	ErrClosed = errors.New("peercache: cache is closed")

	// ErrGroupOverloaded means a group's mailbox was full and the request
	// was rejected rather than queued. This is
	// backpressure working as designed, and it is retryable.
	ErrGroupOverloaded = errors.New("peercache: group mailbox full")

	// ErrActivationPending means this node owns the group but acquired it
	// too recently to activate safely: the previous owner may not have
	// noticed the membership change yet, and activating immediately would
	// widen the window in which one group has two activations.
	//
	// It is retryable, and retrying against the same node is correct — the
	// wait is short and bounded by the configured activation delay. This is
	// the brief unavailability that narrowing the split-brain window costs.
	ErrActivationPending = errors.New("peercache: activation deferred after a membership change")

	// ErrStaleEpoch means an operation was attempted under a membership
	// epoch older than the one the node currently holds. The caller should
	// re-resolve ownership and retry.
	ErrStaleEpoch = errors.New("peercache: stale membership epoch")

	// ErrNotOwner means this node does not own the group key. It carries no
	// hint about who does; the caller re-resolves through the ownership
	// table.
	ErrNotOwner = errors.New("peercache: not the owner of this key")

	// ErrNoLoader means GetOrLoad was called on a namespace constructed
	// without WithLoader.
	ErrNoLoader = errors.New("peercache: namespace has no registered loader")

	// ErrFuncNotRegistered means Call named a group function that was not
	// registered on this node. Because all pods run the same binary,
	// this indicates a rolling upgrade in progress
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

	// ErrGroupFuncPanic means a registered group function panicked. The
	// panic is contained and converted rather than propagated: it would
	// otherwise kill the group's only goroutine and wedge that group
	// permanently, turning one bad input into a permanently unavailable
	// key range.
	ErrGroupFuncPanic = errors.New("peercache: group function panicked")

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

	// ErrUnderReplicated means a synchronous write could not be placed on as
	// many nodes as its namespace's replication factor asks for, and the
	// namespace was configured to refuse rather than acknowledge it.
	ErrUnderReplicated = errors.New("peercache: write held by fewer nodes than its replication factor")

	// ErrNotAnInteger means Increment found a value that is not a base-10
	// int64, or a sum that would overflow one. See the root package.
	ErrNotAnInteger = errors.New("peercache: value is not an integer, or the sum overflows")

	// ErrOverMemoryBudget means an operation that would add bulk data was
	// stopped because this node is over its memory budget.
	ErrOverMemoryBudget = errors.New("peercache: over memory budget")

	// ErrCodec means a typed value could not be encoded, or stored bytes
	// could not be decoded, with the namespace's codec.
	ErrCodec = errors.New("peercache: value does not encode or decode with the namespace's codec")
)
