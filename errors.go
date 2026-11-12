package peercache

import "github.com/arwenizEr/peercache/internal/core"

// Sentinel errors. Test with errors.Is, never by comparing strings.
//
// Each name below is bound to the same error object the internal packages
// return, so errors.Is is unaffected by the indirection. Errors returned from
// this package wrap their cause with %w; unwrap to reach transport or codec
// detail.
var (
	// ErrNotImplemented means the code path exists but the work that
	// fills it in has not run yet. It is scaffolding, not a runtime
	// condition, and must not survive the change that implements the
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

	// ErrActivationPending means this node owns the group but acquired it
	// too recently to activate safely. The previous owner may not have
	// noticed the membership change yet.
	//
	// Retryable, and retrying against the same node is correct: the wait is
	// bounded by [WithActivationDelay]. This brief unavailability is what
	// narrowing the double-activation window costs, and it applies only to
	// groups whose ownership actually moved.
	ErrActivationPending = core.ErrActivationPending

	// ErrStaleEpoch means the operation was attempted under a membership
	// epoch older than the one the node now holds. Re-resolve ownership and
	// retry; do not assume the previous owner is still correct.
	ErrStaleEpoch = core.ErrStaleEpoch

	// ErrNotOwner means this node does not own the key's group. It
	// deliberately carries no hint about who does, because a hint computed
	// under a stale epoch is worse than no hint.
	ErrNotOwner = core.ErrNotOwner

	// ErrStateEscaped names the condition where a [GroupState] is used
	// outside the actor turn that produced it.
	//
	// Nothing returns it, and nothing can. The design requires the
	// condition be caught without a panic, and it is — but the goroutine
	// committing the violation is not inside a Call, so there is no caller to
	// hand an error to. The offending method does nothing and
	// Stats().StateEscapes counts it; peercache_state_escapes_total exports
	// it.
	//
	// It remains exported because removing a sentinel breaks anyone who
	// matched on it, and because it is the canonical name for a condition the
	// documentation has to be able to refer to. Do not write code that waits
	// for it. See docs/decisions/open.md question 36.
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

	// ErrGroupFuncPanic means a registered group function panicked. The
	// panic is contained and converted rather than propagated: it would
	// otherwise kill the group's only goroutine and wedge that group
	// permanently, so one bad input would take a key range offline until the
	// process restarted. The group remains usable and keeps its state.
	ErrGroupFuncPanic = core.ErrGroupFuncPanic

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

	// ErrUnderReplicated means a synchronous write reached fewer nodes than
	// its namespace's replication factor, in a namespace opened with
	// [WithRequireReplicationFactor]. Without that option the same write is
	// acknowledged and counted in Stats().DegradedWrites instead.
	//
	// The value is held by the owner, and by whichever replicas this node
	// could place it on. Like every synchronous failure it is reported, not
	// rolled back: a Get on the owner may return it. Retrying is safe for
	// Set and Delete, which are idempotent, and will keep failing until this
	// node's membership view is large enough for the factor again.
	//
	// It is distinct from [ErrNodeUnreachable]. That means a replica this
	// node believes in did not answer; this means this node does not believe
	// in enough replicas — usually because it has been cut off from the
	// cluster, or because the cluster is smaller than the factor. See
	// docs/decisions/open.md question 37.
	ErrUnderReplicated = core.ErrUnderReplicated

	// ErrOverMemoryBudget means [Cache.CatchUp] stopped pulling because this
	// node is over the budget set by [WithMemoryBudget].
	//
	// Pulling on would only feed the shedding that the budget is already
	// doing, so catch-up stops instead and says so. The replication factor is
	// then below what the namespace asks for on this node, and
	// Ready().ReplicasCurrent stays false — which is the truth. Retry once
	// memory is available; a budget that is always exceeded is a budget
	// smaller than this node's share of the working set.
	ErrOverMemoryBudget = core.ErrOverMemoryBudget

	// ErrCodec means a [TypedNamespace] could not encode a value with its
	// namespace's codec, or the bytes stored under a key did not decode as
	// the namespace's type. The latter usually means the bytes were written
	// some other way — through Raw, or by a pod running a build with a
	// different type for this namespace. Not retryable: the bytes will not
	// change by asking again.
	ErrCodec = core.ErrCodec
)
