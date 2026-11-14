package core

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// Options is the resolved configuration of a Cache.
//
// Only fields the library acts on are present. Nothing is reserved ahead of
// its implementation, because an option that exists and does nothing is worse
// documentation than an option that does not exist.
type Options struct {
	// LatencyHistograms records the distribution of remote operation
	// latencies, exported as Prometheus histograms.
	//
	// Off by default, and the reason is measurement rather than caution. An
	// observation costs 15.8ns uncontended and 36.9ns under concurrency,
	// which is ~7% of the measured 515ns local read path — and on a
	// platform whose monotonic clock resolves to ~500us, timing a
	// sub-microsecond operation records zeros anyway. Paying 7% for zeros is
	// not a trade worth making silently.
	//
	// Remote operations are a different case: a 1.7ms round trip is far
	// above any platform's clock resolution and 37ns is noise against it.
	// So this instruments the remote path only, and says so.
	LatencyHistograms bool
	// NodeID uniquely identifies this node. Required.
	NodeID string
	// Listener is what the peer transport serves on.
	//
	// It is separate from Transport because Transport.Serve takes a
	// net.Listener, and because the two have
	// different lifetimes in practice: an application often wants to bind
	// its port before constructing the cache so that a bind failure is
	// reported at startup rather than from a goroutine.
	//
	// Nil is valid for a transport with nothing to listen on, such as the
	// in-process one in internal/testkit.
	Listener net.Listener
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
	// MemoryBudget is the number of bytes this node's owned, replica and L1
	// tiers may hold before it sheds. Zero means no budget.
	MemoryBudget int64
	// JoinProbation is how long a node must have been up before its peers
	// rank it. Zero means no probation.
	JoinProbation time.Duration
	// OnEvent receives lifecycle events. It runs on the emitting group's
	// goroutine and must not block.
	OnEvent func(Event)

	// ActivationDelay is how long this node waits, after a membership
	// change, before activating a group it has just acquired. Zero takes
	// the default; negative disables deferral.
	ActivationDelay time.Duration

	// CatchUpGate refuses peers' operations on this node's keys, in
	// namespaces with replicas, until its first catch-up completes. See
	// WithCatchUpGate.
	CatchUpGate bool
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
	if len(o.Seeds) > 0 && o.Membership == nil {
		return fmt.Errorf("peercache: seeds %v given with no membership authority to join through: %w",
			o.Seeds, ErrInvalidOption)
	}
	if o.JoinProbation < 0 {
		return fmt.Errorf("peercache: join probation %v must not be negative: %w", o.JoinProbation, ErrInvalidOption)
	}
	if o.MemoryBudget < 0 {
		return fmt.Errorf("peercache: memory budget %d must not be negative: %w", o.MemoryBudget, ErrInvalidOption)
	}
	return nil
}

// NamespaceOptions is the resolved configuration of one namespace.
type NamespaceOptions struct {
	// Loader reconstructs missing values. Nil means GetOrLoad returns
	// [ErrNoLoader].
	Loader Loader
	// Partitioner maps keys to group keys. Defaults to [PartitionByKey].
	Partitioner Partitioner
	// SyncReplication makes a write wait for its replicas before returning.
	//
	// Off by default. Turning it on trades
	// latency for a closed data-loss window: the write returns only once
	// every replica has the value, so losing the owner immediately
	// afterwards loses nothing. It also bounds that group's write rate by
	// its slowest replica, because the group's single activation holds
	// while the fan-out completes.
	SyncReplication bool
	// RequireReplicationFactor makes a synchronous write that cannot be
	// placed on ReplicationFactor nodes fail with [ErrUnderReplicated]
	// instead of succeeding below the factor. Requires SyncReplication.
	// See docs/decisions/open.md question 37.
	RequireReplicationFactor bool
	// ReplicationFactor is the number of copies, between 1 and 3. 1 means
	// the owner only.
	ReplicationFactor int
	// Codec names the registered codec used for typed values. Empty means
	// the typed view's default, MessagePack.
	Codec string
	// TypedLoader is a loader that returns a typed value; see
	// [TypedLoaderSpec]. It conflicts with Loader.
	TypedLoader TypedLoaderSpec
	// L1TTL bounds how long a non-owned copy may be served. Negative
	// disables the non-owned tier entirely, so every read of a key owned
	// elsewhere is a round trip and nothing is ever served stale. It is a
	// correctness bound, not a tuning knob: invalidation is best-effort, so
	// this is the only thing standing between a stale read and an unbounded one.
	L1TTL time.Duration

	// Compressor names the compressor applied to stored values at or above
	// CompressionThreshold bytes. Empty means values are stored verbatim,
	// which is the default: a decompress on every read would put a
	// compression codec on the hot path of a cache whose premise is
	// sub-microsecond local reads.
	//
	// Compression applies above a configurable threshold; this is that
	// knob. See docs/decisions/0008-compression-is-opt-in.md.
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

	// NegativeTTL is how long the owner remembers that the loader reported
	// ErrNotFound for a key, answering GetOrLoad of it with ErrNotFound
	// without loading. Zero, the default, remembers nothing. See
	// docs/decisions/0034-negative-caching.md.
	NegativeTTL time.Duration

	// Shards is the number of independently locked partitions in this
	// namespace's store. Zero takes the store's default.
	Shards int

	// ReplicaReads lets a read of a key owned elsewhere be answered by one
	// of the key's replicas. Requires ReplicationFactor of at least 2 and an
	// enabled L1 tier. See docs/decisions/0028-hot-key-reads.md.
	ReplicaReads bool

	// Persistence, if set, makes the namespace persistent: every write is
	// ordered by the cache's hybrid logical clock and written behind to this
	// store. See docs/decisions/0029-persistence-as-a-segment-log.md.
	Persistence BlobStore
	// PersistFlushInterval is how often a persistent namespace flushes.
	// Zero means the default; negative is refused.
	PersistFlushInterval time.Duration
}

// BlobStore is where a persistent namespace keeps its segments and snapshots.
// It is declared here, and aliased as persist.BlobStore, because the namespace
// options hold one and package persist depends on this package rather than the
// other way round. See package persist for the full contract.
type BlobStore interface {
	// Put stores data under name atomically, replacing any object there.
	Put(ctx context.Context, name string, data []byte) error
	// Get returns the object under name, or an error wrapping ErrNotFound.
	Get(ctx context.Context, name string) ([]byte, error)
	// List returns the names starting with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// Delete removes name; an absent name is not an error.
	Delete(ctx context.Context, name string) error
}

// NamespaceOption mutates [NamespaceOptions].
type NamespaceOption func(*NamespaceOptions)

// Namespace option defaults.
const (
	// DefaultReplicationFactor is 1: owner only, no replicas.
	DefaultReplicationFactor = 1
	// DefaultL1TTL bounds non-owned copies by default. Short on purpose.
	DefaultL1TTL = 5 * time.Second
	// MaxReplicationFactor is the highest replication factor a namespace
	// may ask for.
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
	if o.TypedLoader != nil && o.Loader != nil {
		return fmt.Errorf("peercache: a namespace has one loader; WithLoader and WithTypedLoader were both given: %w",
			ErrInvalidOption)
	}
	if o.ReplicationFactor < 1 || o.ReplicationFactor > MaxReplicationFactor {
		return fmt.Errorf("peercache: replication factor %d out of range 1..%d: %w",
			o.ReplicationFactor, MaxReplicationFactor, ErrInvalidOption)
	}
	if o.RequireReplicationFactor && !o.SyncReplication {
		return fmt.Errorf("peercache: requiring the replication factor needs synchronous replication; "+
			"an asynchronous write returns before it knows how many copies landed: %w", ErrInvalidOption)
	}
	if o.L1TTL == 0 {
		return fmt.Errorf("peercache: L1 TTL must be positive, got %v: %w", o.L1TTL, ErrInvalidOption)
	}
	if o.NegativeTTL < 0 {
		return fmt.Errorf("peercache: negative caching TTL must not be negative, got %v: %w",
			o.NegativeTTL, ErrInvalidOption)
	}
	if o.LoaderTimeout <= 0 {
		return fmt.Errorf("peercache: loader timeout must be positive, got %v: %w",
			o.LoaderTimeout, ErrInvalidOption)
	}
	if o.CompressionThreshold < 0 {
		return fmt.Errorf("peercache: compression threshold %d must not be negative: %w",
			o.CompressionThreshold, ErrInvalidOption)
	}
	if o.ReplicaReads && o.ReplicationFactor < 2 {
		return fmt.Errorf("peercache: replica reads need a replication factor of at least 2, got %d: %w",
			o.ReplicationFactor, ErrInvalidOption)
	}
	if o.ReplicaReads && o.L1TTL < 0 {
		return fmt.Errorf("peercache: replica reads are bounded by the L1 TTL, and this namespace "+
			"disables the L1 tier: %w", ErrInvalidOption)
	}
	if o.PersistFlushInterval < 0 {
		return fmt.Errorf("peercache: a persistence flush interval must be positive: %w", ErrInvalidOption)
	}
	if o.Shards < 0 {
		return fmt.Errorf("peercache: shard count %d must not be negative: %w", o.Shards, ErrInvalidOption)
	}
	return nil
}
