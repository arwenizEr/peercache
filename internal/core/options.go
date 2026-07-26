package core

import (
	"fmt"
	"net"
	"time"

	"github.com/peercache/peercache/internal/clock"
)

// Options is the resolved configuration of a Cache.
//
// Only the fields M0 can honestly act on are present. Later milestones add
// their own rather than reserving space now, because an option that exists and
// does nothing is worse documentation than an option that does not exist.
type Options struct {
	// NodeID uniquely identifies this node. Required.
	NodeID string
	// Listener is what the peer transport serves on.
	//
	// It is separate from Transport because SPEC.md §3.6 fixes
	// Transport.Serve to take a net.Listener, and because the two have
	// different lifetimes in practice: an application often wants to bind
	// its port before constructing the cache so that a bind failure is
	// reported at startup rather than from a goroutine.
	//
	// Nil is valid for a transport with nothing to listen on, such as the
	// in-process one in internal/testkit.
	Listener net.Listener
	// BindAddr is the address peers dial. It may be empty for an
	// in-process transport.
	BindAddr string
	// Seeds are addresses used to join an existing cluster.
	Seeds []string

	// Clock is the time source. Defaults to [clock.Real].
	Clock clock.Clock
	// Logger receives diagnostics. Defaults to [NopLogger].
	Logger Logger
	// Transport is the peer fabric. A nil transport means single-node
	// operation with no networking.
	Transport Transport
	// Membership is the source of cluster membership. A nil authority
	// means single-node operation.
	Membership MembershipAuthority

	// GroupMailboxSize bounds how many requests may queue for one group
	// before further calls are refused with ErrGroupOverloaded.
	GroupMailboxSize int
	// GroupIdleTimeout is how long an activated group waits with no work
	// before releasing its goroutine.
	GroupIdleTimeout time.Duration
	// MaxActiveGroups caps concurrently activated groups. Zero takes the
	// default; a negative value means unlimited.
	MaxActiveGroups int
	// OnEvent receives lifecycle events. It runs on the emitting group's
	// goroutine and must not block.
	OnEvent func(Event)

	// ActivationDelay is how long this node waits, after a membership
	// change, before activating a group it has just acquired. Zero takes
	// the default; negative disables deferral.
	ActivationDelay time.Duration
}

// Option mutates [Options]. Options are applied in order, so a later option
// wins over an earlier one.
type Option func(*Options)

// DefaultOptions returns the configuration used when no options are supplied.
func DefaultOptions() Options {
	return Options{
		Clock:  clock.Real(),
		Logger: NopLogger{},
	}
}

// Validate reports whether the resolved options are internally consistent.
func (o *Options) Validate() error {
	if o.NodeID == "" {
		return fmt.Errorf("peercache: NodeID is required: %w", ErrInvalidOption)
	}
	if o.Clock == nil {
		return fmt.Errorf("peercache: Clock must not be nil: %w", ErrInvalidOption)
	}
	if o.Logger == nil {
		return fmt.Errorf("peercache: Logger must not be nil: %w", ErrInvalidOption)
	}
	return nil
}

// NamespaceOptions is the resolved configuration of one namespace
// (SPEC.md section 3.1).
type NamespaceOptions struct {
	// Loader reconstructs missing values. Nil means GetOrLoad returns
	// [ErrNoLoader].
	Loader Loader
	// Partitioner maps keys to group keys. Defaults to [PartitionByKey].
	Partitioner Partitioner
	// SyncReplication makes a write wait for its replicas before returning.
	//
	// Off by default (CLAUDE.md non-negotiable 7). Turning it on trades
	// latency for a closed data-loss window: the write returns only once
	// every replica has the value, so losing the owner immediately
	// afterwards loses nothing. It also bounds that group's write rate by
	// its slowest replica, because the group's single activation holds
	// while the fan-out completes.
	SyncReplication bool
	// ReplicationFactor is the number of copies, between 1 and 3. 1 means
	// the owner only.
	ReplicationFactor int
	// Codec names the registered codec used for typed values.
	Codec string
	// L1TTL bounds how long a non-owned copy may be served. Negative
	// disables the non-owned tier entirely, so every read of a key owned
	// elsewhere is a round trip and nothing is ever served stale. It is a
	// correctness bound, not a tuning knob: CLAUDE.md decision 6 makes it
	// the only thing standing between a stale read and an unbounded one.
	L1TTL time.Duration

	// Compressor names the compressor applied to stored values at or above
	// CompressionThreshold bytes. Empty means values are stored verbatim,
	// which is the default: a decompress on every read would put a
	// compression codec on the hot path of a cache whose premise is
	// sub-microsecond local reads.
	//
	// SPEC.md section 3.1 does not define a compression option; section 4
	// (M1) requires compression above a configurable threshold. This is
	// that knob. See docs/decisions/0008-compression-is-opt-in.md.
	Compressor string
	// CompressionThreshold is the size at or above which compression is
	// attempted. Ignored when Compressor is empty.
	CompressionThreshold int

	// LoaderTimeout bounds one invocation of the loader.
	//
	// It cannot be the calling context's deadline: concurrent misses
	// collapse into one load, and taking whichever caller arrived first as
	// the bound would let a caller with five milliseconds left abort a load
	// that a thousand callers with a second left were depending on.
	LoaderTimeout time.Duration

	// Shards is the number of independently locked partitions in this
	// namespace's store. Zero takes the store's default.
	Shards int
}

// NamespaceOption mutates [NamespaceOptions].
type NamespaceOption func(*NamespaceOptions)

// Namespace option defaults.
const (
	// DefaultReplicationFactor is 1: owner only, no replicas.
	DefaultReplicationFactor = 1
	// DefaultL1TTL bounds non-owned copies by default. Short on purpose.
	DefaultL1TTL = 5 * time.Second
	// MaxReplicationFactor is the ceiling from SPEC.md section 3.1.
	MaxReplicationFactor = 3
	// DefaultLoaderTimeout bounds one loader invocation. Generous, because
	// a loader usually talks to a database; finite, because a load that
	// never returns holds a goroutine and every caller waiting on that key.
	DefaultLoaderTimeout = 30 * time.Second
)

// DefaultNamespaceOptions returns the configuration used when no namespace
// options are supplied.
func DefaultNamespaceOptions() NamespaceOptions {
	return NamespaceOptions{
		Partitioner:       PartitionByKey(),
		ReplicationFactor: DefaultReplicationFactor,
		L1TTL:             DefaultL1TTL,
		LoaderTimeout:     DefaultLoaderTimeout,
	}
}

// Validate reports whether the resolved namespace options are usable.
func (o *NamespaceOptions) Validate() error {
	if o.Partitioner == nil {
		return fmt.Errorf("peercache: Partitioner must not be nil: %w", ErrInvalidOption)
	}
	if o.ReplicationFactor < 1 || o.ReplicationFactor > MaxReplicationFactor {
		return fmt.Errorf("peercache: replication factor %d out of range 1..%d: %w",
			o.ReplicationFactor, MaxReplicationFactor, ErrInvalidOption)
	}
	if o.L1TTL == 0 {
		return fmt.Errorf("peercache: L1 TTL must be positive, got %v: %w", o.L1TTL, ErrInvalidOption)
	}
	if o.LoaderTimeout <= 0 {
		return fmt.Errorf("peercache: loader timeout must be positive, got %v: %w",
			o.LoaderTimeout, ErrInvalidOption)
	}
	if o.CompressionThreshold < 0 {
		return fmt.Errorf("peercache: compression threshold %d must not be negative: %w",
			o.CompressionThreshold, ErrInvalidOption)
	}
	if o.Shards < 0 {
		return fmt.Errorf("peercache: shard count %d must not be negative: %w", o.Shards, ErrInvalidOption)
	}
	return nil
}
