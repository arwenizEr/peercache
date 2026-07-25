package peercache

import (
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

// The types below are aliases, not wrappers. peercache.Node and the internal
// definition are the same type, so the public API is exactly what SPEC.md
// section 3 specifies while internal packages remain free of an import cycle
// back to this one. See docs/decisions/0001-core-types-location.md.
//
// SPEC.md section 2 gives these types no file of their own; types.go is a
// deliberate, documented deviation from that layout.

// Epoch is a monotonically increasing membership version.
//
// Every ownership decision is stamped with the epoch it was made under, so a
// node returning from a partition can recognise its own conclusions as stale
// and refuse to act on them. Epochs are per-view: because membership is gossip
// based and therefore AP, two nodes may hold different epochs at the same
// instant. Comparing epochs establishes staleness. It never establishes
// consensus.
type Epoch = core.Epoch

// Node identifies one member of the cluster.
//
// ID must be stable for the process lifetime and unique cluster-wide, because
// rendezvous hashing derives ownership from it: changing a node's ID moves its
// share of the keyspace. Addr is the address peers dial. Meta carries small,
// gossip-sized annotations such as the node's current epoch.
type Node = core.Node

// ChangeKind classifies a [MembershipChange] as a join, a deliberate leave, a
// detected failure, or a metadata update.
type ChangeKind = core.ChangeKind

// Membership change kinds.
const (
	// ChangeJoin means a node entered the local view.
	ChangeJoin = core.ChangeJoin
	// ChangeLeave means a node announced its departure. If shutdown was
	// graceful its ownership and state were transferred first.
	ChangeLeave = core.ChangeLeave
	// ChangeFail means the failure detector declared a node dead. The node
	// may still be alive on the other side of a partition; this is exactly
	// the case that epoch-guarded activation exists to contain.
	ChangeFail = core.ChangeFail
	// ChangeUpdate means an existing node's metadata changed.
	ChangeUpdate = core.ChangeUpdate
)

// MembershipChange is one transition in this node's view of the cluster.
//
// Epoch is the view's epoch after the change is applied, so a watcher can stamp
// subsequent ownership decisions with it directly.
type MembershipChange = core.MembershipChange

// MembershipAuthority is the source of truth for cluster membership
// (SPEC.md section 3.5).
//
// The default implementation is gossip over memberlist and is AP: it will
// report a partitioned peer as failed rather than block. A CP implementation
// may be substituted later; this interface exists so that substitution is not a
// rewrite.
//
// Watch returns a channel per call. A slow watcher must not be able to stall
// membership processing, so implementations drop rather than block.
type MembershipAuthority = core.MembershipAuthority

// EventKind classifies an [Event].
type EventKind = core.EventKind

// Event kinds.
const (
	// EventCreate is emitted when a key appears for the first time.
	EventCreate = core.EventCreate
	// EventUpdate is emitted when an existing key is overwritten.
	EventUpdate = core.EventUpdate
	// EventDelete is emitted when a key is removed by a caller.
	EventDelete = core.EventDelete
	// EventEvict is emitted when a key is removed by TTL or capacity
	// pressure rather than by a caller.
	EventEvict = core.EventEvict
	// EventActivate is emitted when a group activates on this node.
	EventActivate = core.EventActivate
	// EventDeactivate is emitted when a group deactivates, whether by idle
	// timeout, capacity pressure, or ownership transfer.
	EventDeactivate = core.EventDeactivate
	// EventCustom is emitted by a registered group function through
	// [GroupState.Emit].
	EventCustom = core.EventCustom
)

// Event is a lifecycle or state-change notification.
//
// Events are informational and delivery is best effort. Nothing in PeerCache
// may depend on an event being observed, for the same reason nothing may depend
// on an invalidation broadcast arriving.
type Event = core.Event

// Stats is a point-in-time snapshot of a node's view of itself and the cluster.
// Reading it never blocks a data path.
type Stats = core.Stats

// Clock is the injected time source (SPEC.md section 3.6).
//
// Nothing in the library reads the wall clock directly. Supply
// [github.com/peercache/peercache/internal/clock.Real] in production; the test
// harness supplies a fake whose time only moves when a test moves it.
type Clock = clock.Clock

// Timer is a one-shot alarm produced by a [Clock].
type Timer = clock.Timer

// Ticker is a repeating alarm produced by a [Clock].
type Ticker = clock.Ticker

// Transport is the injected peer-to-peer message fabric (SPEC.md section 3.6).
//
// Implementations must guarantee that a request returns when its context is
// done, whatever the state of the network. A hang is a worse failure than an
// error: it consumes the caller's deadline budget silently and leaves no trace
// in metrics.
type Transport = core.Transport

// PeerClient is a handle to one remote node. Invoke carries the caller's
// deadline to the remote handler.
type PeerClient = core.PeerClient

// Handler is the server side of a [Transport]: it executes a request that
// arrived from a peer. Implementations must not retain the request slice after
// returning.
type Handler = core.Handler

// HandlerFunc adapts an ordinary function to [Handler].
type HandlerFunc = core.HandlerFunc

// Logger is the logging surface of the library.
//
// The library never writes to a global logger: that decision belongs to the
// application embedding it. The default is [NopLogger], which discards
// everything, so configuring nothing costs nothing. Variadic arguments are
// alternating keys and values, as in log/slog.
type Logger = core.Logger

// NopLogger discards every message. It is the default [Logger].
type NopLogger = core.NopLogger
