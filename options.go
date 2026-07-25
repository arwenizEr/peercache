package peercache

import (
	"time"

	"github.com/peercache/peercache/internal/core"
)

// Option configures a [Cache] at construction. Options apply in order, so a
// later option overrides an earlier one.
type Option = core.Option

// NamespaceOption configures one namespace (SPEC.md section 3.1).
type NamespaceOption = core.NamespaceOption

// WithNodeID sets this node's cluster-unique identifier. It is required, and it
// must be stable for the lifetime of the process: ownership is derived from it,
// so a node that restarts under a new ID takes a different share of the
// keyspace and forces an avoidable rebalance.
func WithNodeID(id string) Option {
	return func(o *core.Options) { o.NodeID = id }
}

// WithBindAddr sets the address peers dial. It may be left empty when the cache
// runs on an in-process transport.
func WithBindAddr(addr string) Option {
	return func(o *core.Options) { o.BindAddr = addr }
}

// WithSeeds sets the addresses used to join an existing cluster. An empty list
// means this node forms a cluster of one and waits to be joined.
func WithSeeds(seeds ...string) Option {
	return func(o *core.Options) { o.Seeds = seeds }
}

// WithClock injects the time source. The default is the real clock; the test
// harness injects a fake so that timeouts cost nothing and always interleave
// the same way.
func WithClock(c Clock) Option {
	return func(o *core.Options) { o.Clock = c }
}

// WithLogger injects a logger. The default discards everything.
func WithLogger(l Logger) Option {
	return func(o *core.Options) { o.Logger = l }
}

// WithTransport injects the peer fabric. A nil transport means single-node
// operation with no networking at all, which is a supported configuration and
// not an error.
func WithTransport(t Transport) Option {
	return func(o *core.Options) { o.Transport = t }
}

// WithMembership injects the membership authority. A nil authority means
// single-node operation.
func WithMembership(m MembershipAuthority) Option {
	return func(o *core.Options) { o.Membership = m }
}

// WithLoader registers the function that reconstructs a missing value
// (SPEC.md section 3.1).
//
// The loader belongs to the namespace rather than to each call, so that it
// exists by name on every pod and the key's owner can execute it locally
// instead of shipping the miss back to the caller. Concurrent misses for one
// key collapse into a single invocation.
func WithLoader(l Loader) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Loader = l }
}

// WithPartitioner sets how keys map to groups (SPEC.md section 3.4). The
// default is [PartitionByKey].
//
// The partitioner must be pure and identical on every pod. Two nodes
// disagreeing about a key's group is indistinguishable from a split brain, and
// far harder to diagnose.
func WithPartitioner(p Partitioner) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Partitioner = p }
}

// WithReplicationFactor sets the number of copies held for each key, from 1 to
// 3. One means the owner only. A value outside the range is rejected at
// construction with [ErrInvalidOption].
func WithReplicationFactor(rf int) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.ReplicationFactor = rf }
}

// WithCodec names the registered codec used for typed values.
func WithCodec(name string) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Codec = name }
}

// WithL1TTL bounds how long a copy of a non-owned key may be served from local
// memory.
//
// This is a correctness bound, not a tuning knob. Invalidation broadcasts are a
// latency optimisation and may be lost; this TTL is the only thing that turns
// an unbounded stale read into a bounded one. Longer means faster and staler,
// and the staleness is what a caller must reason about.
func WithL1TTL(d time.Duration) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.L1TTL = d }
}

// Loader reconstructs a namespace value that is not resident
// (SPEC.md section 3.1). Returning a zero ttl means the namespace default
// applies.
type Loader = core.Loader

// Partitioner maps a cache key to the group key that decides its owner
// (SPEC.md section 3.4).
type Partitioner = core.Partitioner

// PartitionerFunc adapts an ordinary function to [Partitioner].
type PartitionerFunc = core.PartitionerFunc

// PartitionByKey makes every key its own group. This is the default: maximum
// spread across the cluster, and no batch affinity.
func PartitionByKey() Partitioner { return core.PartitionByKey() }

// PartitionByPrefix groups keys by the portion before the first occurrence of
// sep, so that "user:123/cart" and "user:123/profile" share an owner and can be
// updated in a single actor turn.
//
// A key not containing sep forms its own group. An empty sep is treated as
// [PartitionByKey] rather than silently making every key its own group by a
// different route.
func PartitionByPrefix(sep string) Partitioner { return core.PartitionByPrefix(sep) }

// PartitionByFunc groups keys with an arbitrary pure function. A nil function
// falls back to [PartitionByKey] instead of failing on the first Get.
func PartitionByFunc(f func(string) string) Partitioner { return core.PartitionByFunc(f) }
