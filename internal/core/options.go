package core

import (
	"fmt"
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
	// ReplicationFactor is the number of copies, between 1 and 3. 1 means
	// the owner only.
	ReplicationFactor int
	// Codec names the registered codec used for typed values.
	Codec string
	// L1TTL bounds how long a non-owned copy may be served. It is a
	// correctness bound, not a tuning knob: CLAUDE.md decision 6 makes it
	// the only thing standing between a stale read and an unbounded one.
	L1TTL time.Duration
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
)

// DefaultNamespaceOptions returns the configuration used when no namespace
// options are supplied.
func DefaultNamespaceOptions() NamespaceOptions {
	return NamespaceOptions{
		Partitioner:       PartitionByKey(),
		ReplicationFactor: DefaultReplicationFactor,
		L1TTL:             DefaultL1TTL,
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
	if o.L1TTL <= 0 {
		return fmt.Errorf("peercache: L1 TTL must be positive, got %v: %w", o.L1TTL, ErrInvalidOption)
	}
	return nil
}
