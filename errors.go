package peercache

import "github.com/peercache/peercache/internal/core"

// Sentinel errors. Test with errors.Is, never by comparing strings.
//
// Each name below is bound to the same error object the internal packages
// return, so errors.Is is unaffected by the indirection. Errors returned from
// this package wrap their cause with %w; unwrap to reach transport or codec
// detail.
var (
	// ErrNotImplemented means the code path exists but the milestone that
	// fills it in has not run yet. It is scaffolding, not a runtime
	// condition, and must not survive the milestone that implements the
	// path.
	ErrNotImplemented = core.ErrNotImplemented

	// ErrNotFound means the key or group does not exist. It is distinct
	// from an empty value: a key may legitimately hold zero bytes, and
	// Get returning (nil, nil) would make those indistinguishable.
	ErrNotFound = core.ErrNotFound

	// ErrClosed means the cache has been closed. Every operation returns it
	// afterwards, and it is never retryable.
	ErrClosed = core.ErrClosed

	// ErrGroupOverloaded means a group's mailbox was full and the request
	// was rejected instead of queued. This is backpressure working as
	// designed: blocking indefinitely would convert a slow group into a
	// cluster-wide stall. Retryable, ideally after a backoff.
	ErrGroupOverloaded = core.ErrGroupOverloaded

	// ErrStaleEpoch means the operation was attempted under a membership
	// epoch older than the one the node now holds. Re-resolve ownership and
	// retry; do not assume the previous owner is still correct.
	ErrStaleEpoch = core.ErrStaleEpoch

	// ErrNotOwner means this node does not own the key's group. It
	// deliberately carries no hint about who does, because a hint computed
	// under a stale epoch is worse than no hint.
	ErrNotOwner = core.ErrNotOwner

	// ErrStateEscaped means a [GroupState] was used outside the actor turn
	// that produced it. SPEC.md section 3.3 requires this be detected
	// rather than left as undefined behaviour, and detected without a
	// panic.
	ErrStateEscaped = core.ErrStateEscaped

	// ErrNoLoader means [Namespace.GetOrLoad] was called on a namespace
	// constructed without [WithLoader].
	ErrNoLoader = core.ErrNoLoader

	// ErrFuncNotRegistered means [Group.Call] named a function this node
	// does not have. Since every pod runs the same binary, this normally
	// means a rolling upgrade is in flight.
	ErrFuncNotRegistered = core.ErrFuncNotRegistered

	// ErrDuplicateRegistration means [Cache.RegisterGroupFunc] was called
	// twice for one name. Registration happens once, at startup, and must
	// be deterministic across pods.
	ErrDuplicateRegistration = core.ErrDuplicateRegistration

	// ErrLoaderPanic means a registered loader panicked. The panic is
	// contained and converted rather than propagated, because a bug in
	// application code must not become a panic in every unrelated goroutine
	// that happened to be waiting for the same key. Every concurrent caller
	// receives this error.
	ErrLoaderPanic = core.ErrLoaderPanic

	// ErrLoaderFailed means a load could not complete for a reason that is
	// not an error the loader returned — for instance a loader that exited
	// without returning.
	ErrLoaderFailed = core.ErrLoaderFailed

	// ErrInvalidOption means a supplied option is outside its documented
	// range or contradicts another. Configuration is validated once, at
	// construction, so this never surfaces on a data path.
	ErrInvalidOption = core.ErrInvalidOption

	// ErrNodeUnreachable means the transport could not deliver to a peer.
	// The underlying cause is wrapped.
	ErrNodeUnreachable = core.ErrNodeUnreachable
)
