package peercache

import (
	"net"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/persist"
)

// Option configures a [Cache] at construction. Options apply in order, so a
// later option overrides an earlier one.
type Option = core.Option

// NamespaceOption configures one namespace.
type NamespaceOption = core.NamespaceOption

// WithNodeID sets this node's cluster-unique identifier. It is required, and it
// must be stable for the lifetime of the process: ownership is derived from it,
// so a node that restarts under a new ID takes a different share of the
// keyspace and forces an avoidable rebalance.
func WithNodeID(id string) Option {
	return func(o *core.Options) { o.NodeID = id }
}

// WithSeeds sets the addresses used to join an existing cluster. New passes them
// to the membership authority's Join once the peer transport is serving, and
// fails if the join does. An empty list means this node forms a cluster of one
// and waits to be joined. Seeds with no [WithMembership] are rejected with
// [ErrInvalidOption]: there is nothing to join through.
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
// WithLatencyHistograms records the distribution of remote operation latencies
// and exports them as Prometheus histograms.
//
// It is off by default because the cost was measured rather than assumed: an
// observation is 15.8ns uncontended and 36.9ns under concurrency, against a
// measured local read path of 515ns. That is ~7%, and on a platform whose
// clock resolves to ~500us a sub-microsecond operation times as zero regardless
// — so the local path would pay for nothing.
//
// Only remote operations are instrumented, where a millisecond round trip
// dwarfs the observation and the clock can actually resolve it.
func WithLatencyHistograms() Option {
	return func(o *core.Options) { o.LatencyHistograms = true }
}

// WithListener supplies the listener the peer transport serves on.
//
// Bind the port yourself and pass it in: a bind failure then surfaces where the
// application can report it, rather than from a goroutine inside the library.
// Nil is valid for a transport that has nothing to listen on.
func WithListener(l net.Listener) Option {
	return func(o *core.Options) { o.Listener = l }
}

// WithTransport supplies the fabric this node reaches its peers over.
//
// Nil is a supported configuration rather than an error: it means single-node
// operation with no networking, in which case every key is owned locally and
// nothing is ever routed. A cache configured that way still answers
// [ErrNotImplemented] if something asks it to reach another node, which is the
// only way that sentinel is still reachable.
//
// Pair it with [WithMembership]. A transport without a membership authority has
// nobody to talk to, and a membership authority without a transport can see
// peers it cannot reach.
func WithTransport(t Transport) Option {
	return func(o *core.Options) { o.Transport = t }
}

// WithMembership injects the membership authority. A nil authority means
// single-node operation.
func WithMembership(m MembershipAuthority) Option {
	return func(o *core.Options) { o.Membership = m }
}

// WithActivationDelay sets how long this node waits, after a membership change,
// before activating a group it has just acquired. The default is 3 seconds.
//
// The delay gives the previous owner time to notice the change, narrowing the
// window in which one group has two activations. It is paid only by groups
// whose ownership actually moved: a group this node has held all along
// activates immediately, because it cannot be active anywhere else.
//
// Calls for a deferred group return [ErrActivationPending], which is retryable
// against the same node. This is the brief, visible unavailability that
// narrowing the split-brain window costs. A negative value disables deferral
// entirely, which is only sane on a single node.
//
// It narrows the window; it does not close it. Membership is gossip based and
// therefore AP, and no setting here changes that.
func WithActivationDelay(d time.Duration) Option {
	return func(o *core.Options) { o.ActivationDelay = d }
}

// WithCatchUpGate makes this node refuse its peers' operations on keys it owns,
// in namespaces with a replication factor of 2 or more, until its first
// [Cache.CatchUp] has completed. They are answered with [ErrActivationPending],
// "ask again here shortly", which the forwarding node retries and an
// application already treats as refused before anything was applied.
//
// It closes a window readiness cannot. A restarted node owns its keys from the
// moment it joins, but their newest copies are on its replicas until catch-up
// brings them back. The readiness gate keeps clients away in that window; it
// does not keep peers from forwarding writes, and a write applied to an empty
// or persisted-but-older base is wrong — an Increment counts from the wrong
// value, a SetIfAbsent succeeds over a key that exists — and the catch-up that
// follows discards it as older. On kind, under kill -9, that was 1 to 3 lost
// decrements on 5 of 50 stock counters.
//
// Turn it on wherever the application calls CatchUp before serving, as the
// Kubernetes deployment in deploy/ does. Off by default because a node whose
// application never calls CatchUp would refuse its peers indefinitely. Only
// forwarded operations are held: this node's own calls are the application's,
// and readiness already tells it when to make them. See docs/decisions/open.md
// question 49.
func WithCatchUpGate() Option {
	return func(o *core.Options) { o.CatchUpGate = true }
}

// WithGroupMailboxSize bounds how many requests may queue for one group before
// further calls are refused with [ErrGroupOverloaded]. The default is 64.
//
// Small on purpose. A deep queue does not make a slow group faster; it only
// delays the moment the caller finds out, and it converts one slow group into
// unbounded memory.
func WithGroupMailboxSize(n int) Option {
	return func(o *core.Options) { o.GroupMailboxSize = n }
}

// WithGroupIdleTimeout sets how long an activated group waits with no work
// before releasing its goroutine. The default is 30 seconds.
//
// Deactivation keeps the group's state and timer schedule; only the goroutine
// and mailbox are released. A group with pending timers is exempt, because
// nothing else would wake it and its timer would silently never fire.
func WithGroupIdleTimeout(d time.Duration) Option {
	return func(o *core.Options) { o.GroupIdleTimeout = d }
}

// WithMemoryBudget caps the bytes this node's data tiers may hold, shedding
// entries when it is exceeded. Zero, the default, means no cap.
//
// It counts the owned stores, the replica store and the L1 tier, at their keys'
// and values' sizes plus a per-entry overhead estimated on the high side. It
// does not count activated groups, which [WithMaxActiveGroups] bounds, or
// buffers in flight. Set it below the container's memory limit with room for
// those and for the Go runtime: a budget near the limit is an OOM kill with
// extra steps.
//
// Over budget, the node sheds to nine tenths of it, cheapest tier first: the L1
// tier whole, then replicas, then owned data. Shedding a replica lowers that
// key's replication factor; shedding owned data loses the key, which a loader
// rebuilds. Neither is silent: Stats().ShedL1, ShedReplicas and ShedOwned count
// them, and peercache_memory_used_bytes shows how close the node runs.
//
// The case it exists for is a failover cascade. A node dies, its share moves to
// the survivors, and a survivor sized for its steady state is now over its
// container limit. Without a budget it is killed, its share moves on, and the
// next one follows. With one, it sheds and keeps serving.
//
// A negative value is rejected with [ErrInvalidOption].
func WithMemoryBudget(bytes int64) Option {
	return func(o *core.Options) { o.MemoryBudget = bytes }
}

// WithJoinProbation keeps a node out of the ownership ranking until it has been
// up for d. Zero, the default, ranks a node as soon as it joins.
//
// It exists for a pod in CrashLoopBackOff. Without it every restart is a join:
// every peer ranks the new node at once, a share of the keyspace moves to a node
// holding nothing, and reads of that share report ErrNotFound until the node
// catches up — which a crashlooping node never does, because it dies first. Then
// the share moves back, and the next restart moves it again. With a probation
// longer than the pod survives, its restarts move nothing.
//
// A node's age is measured from the start time its membership authority
// publishes; the built-in gossip and the test harness both do. A node that
// publishes none — a peer on an older build — is treated as established, so a
// rolling upgrade is not held up by it. A cluster where every node is younger
// than d, which is a cold start, ranks them all, since excluding them all would
// leave nothing to own anything.
//
// The cost is that a new pod takes no share of the keyspace for its first d.
// Readiness reflects that: the node sees a ranked view without itself and
// serves by routing. Set d longer than a crashing pod lives and shorter than
// you are willing to wait for a new one to carry load; 30 seconds suits most.
// Stats().MembersOnProbation counts the nodes currently waiting.
//
// It requires every node to have a roughly synchronised clock, which Kubernetes
// nodes do; skew shows up as a node being ranked a little earlier on some peers
// than others, which the activation delay already absorbs. A negative value is
// rejected with [ErrInvalidOption].
func WithJoinProbation(d time.Duration) Option {
	return func(o *core.Options) { o.JoinProbation = d }
}

// WithMaxActiveGroups caps concurrently activated groups, evicting the
// least-recently-activated when the cap is exceeded. The default is 20,000; a
// negative value means unlimited.
//
// Each activation costs a goroutine, so this is the knob that bounds the
// runtime's memory. Eviction releases the goroutine and keeps the state, so an
// evicted group is indistinguishable from an idle one except by latency.
func WithMaxActiveGroups(n int) Option {
	return func(o *core.Options) { o.MaxActiveGroups = n }
}

// WithEventHandler registers a receiver for lifecycle events: activation,
// deactivation, and the create/update/delete of group keys.
//
// The handler runs on the emitting group's turn goroutine and must not block —
// anything slow belongs on the far side of a channel the handler owns. Delivery
// is best effort: nothing in this library may depend on an event being
// observed, for the same reason nothing may depend on an invalidation arriving.
func WithEventHandler(fn func(Event)) Option {
	return func(o *core.Options) { o.OnEvent = fn }
}

// WithLoader registers the function that reconstructs a missing value.
//
// The loader belongs to the namespace rather than to each call, so that it
// exists by name on every pod and the key's owner can execute it locally
// instead of shipping the miss back to the caller. Concurrent misses for one
// key collapse into a single invocation.
func WithLoader(l Loader) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Loader = l }
}

// WithPartitioner sets how keys map to groups. The
// default is [PartitionByKey].
//
// The partitioner must be pure and identical on every pod. Two nodes
// disagreeing about a key's group is indistinguishable from a split brain, and
// far harder to diagnose.
func WithPartitioner(p Partitioner) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Partitioner = p }
}

// WithSyncReplication makes writes in this namespace wait for their replicas.
//
// The default is asynchronous, which returns as soon as the owner has the
// value and leaves a measured window in which it exists on one node only. This
// closes that window: a write returns only when every replica holds it, so an
// owner lost immediately afterwards loses nothing.
//
// The cost is real and worth stating. A group's writes are serialised through
// its single activation, and a synchronous fan-out holds that activation while
// the network round-trips — so the group's write rate becomes its slowest
// replica's round trip. It buys durability with throughput on the group, not
// with throughput on the cluster.
//
// It has no effect at replication factor 1, where there is nothing to wait for.
func WithSyncReplication() NamespaceOption {
	return func(o *core.NamespaceOptions) { o.SyncReplication = true }
}

// WithRequireReplicationFactor makes a synchronous write that cannot reach the
// namespace's replication factor fail with [ErrUnderReplicated] rather than
// succeed with fewer copies.
//
// Without it, synchronous replication places every copy this node's membership
// view allows and reports success. Normally that is every copy. But a node cut
// off from the cluster eventually sees itself alone, ranks itself the owner of
// everything, and acknowledges each write holding the single copy it has — for
// as long as the partition lasts. Stats().DegradedWrites counts those, and the
// caller cannot tell them apart from a healthy write.
//
// With it, those writes fail instead. That is a choice of correctness over
// availability for this namespace, and it has two costs worth stating before
// turning it on:
//
//   - A cluster smaller than the factor refuses every write. That includes one
//     node running alone in development, and a pod that has started but not
//     yet joined anyone.
//   - An isolated node stops accepting writes for this namespace until it can
//     see enough peers again, rather than accepting them and losing them if
//     it dies.
//
// A refused write is still held by its owner. There is no rollback, which is
// also true of a synchronous write refused because a replica did not answer.
//
// This does not make writes agree on anything across nodes. A node that can
// see enough peers to place its copies accepts the write whether or not those
// peers agree with each other about membership, and the two-activation window
// of a partition is unchanged. What it changes is that a nil return now always
// means the factor was reached from where the writer stood.
//
// It requires [WithSyncReplication], because an asynchronous write returns
// before it can know how many copies landed. Using it without that is rejected
// with [ErrInvalidOption]. At replication factor 1 it has no effect.
func WithRequireReplicationFactor() NamespaceOption {
	return func(o *core.NamespaceOptions) { o.RequireReplicationFactor = true }
}

// WithReplicationFactor sets the number of copies held for each key, from 1 to
// 3. One means the owner only. A value outside the range is rejected at
// construction with [ErrInvalidOption].
func WithReplicationFactor(rf int) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.ReplicationFactor = rf }
}

// WithReplicaReads lets a read of a key owned elsewhere be answered by one of
// the key's replicas instead of only its owner, spreading a hot key's refills
// across every node that holds it.
//
// Each node always asks the same holder for a given key -- the owner or one of
// its replicas, chosen by rendezvous hash -- so a key's readers divide among its
// holders. A node that is itself a replica answers from its own copy.
//
// A replica answers only with a copy it received less than the namespace's L1
// TTL ago, and only while it still ranks as a replica; the reader caches the
// copy for what is left of that TTL. So a value read this way is stale by at
// most the L1 TTL plus the replication delay, and the replication delay is
// measured (docs/benchmarks.md), not bounded. A replica that declines, lacks
// the key, or answers with a copy this node already knows to be superseded
// sends the read to the owner. A key quiet for longer than the TTL is
// therefore always read from its owner.
//
// It applies to Get only. GetOrLoad, BatchGet, Exists and every write go to
// the owner as before.
//
// It requires a replication factor of at least 2 and an enabled L1 tier;
// otherwise the namespace reports [ErrInvalidOption]. See
// docs/decisions/0028-hot-key-reads.md.
func WithReplicaReads() NamespaceOption {
	return func(o *core.NamespaceOptions) { o.ReplicaReads = true }
}

// WithPersistence makes the namespace persistent: its owners write every change
// behind to store, so that keys survive the loss of every node holding them.
//
// Writes are recorded in memory and flushed every 100 ms — or as set by
// persist.WithFlushInterval — as one immutable log object per flush, which a
// background splitter later moves into per-partition segments (decision 0030).
// A write is acknowledged before its flush, so a crash loses at most the last
// flush interval. A node that comes to own
// keys it does not hold — at start, or because every holder of them died —
// restores them from store on first access, keeping whichever of memory and
// storage is newer. Each partition's compactor folds its segments into
// snapshots and drops deletions after an hour's grace. Shedding under
// WithMemoryBudget drops a key from memory, never from store; it comes back on
// its next access.
//
// Writes are ordered by a hybrid logical clock carried through replication and
// promotion. Two writes to one key by owners that never exchanged an order,
// closer together than their clocks' skew, may be restored in either order, and
// conflicting writes from a network partition are resolved last writer wins.
//
// store must be the same on every node. persist.Memory is for tests; real
// backends live in package persist and in separate modules. See
// docs/decisions/0029-persistence-as-a-segment-log.md.
func WithPersistence(store persist.BlobStore, opts ...persist.Option) NamespaceOption {
	var cfg persist.Config
	for _, opt := range opts {
		opt(&cfg)
	}
	return func(o *core.NamespaceOptions) {
		o.Persistence = store
		o.PersistFlushInterval = cfg.FlushInterval
	}
}

// WithCodec names the registered codec used for typed values.
func WithCodec(name string) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Codec = name }
}

// WithNegativeCaching makes a key's owner remember, for ttl, that the
// namespace's loader reported the key does not exist — returned an error
// matching ErrNotFound — and answer GetOrLoad of it with ErrNotFound without
// calling the loader again. Off by default; zero turns it off and a negative
// ttl is ErrInvalidOption.
//
// It stops a stream of lookups for keys that do not exist from reaching the
// loader's database once per request. The absence is not a value: Get, Exists,
// Keys, replication and persistence never see it, and any write of the key —
// Set, Delete, an atomic operation — forgets it, so a Delete is how an
// application says "load this again". Only ErrNotFound is remembered; any
// other loader error, an outage included, is returned and forgotten. Absences
// stay on the owner, at most 256 per shard: losing one costs one load.
func WithNegativeCaching(ttl time.Duration) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.NegativeTTL = ttl }
}

// WithTTLJitter shortens every TTL this namespace's owners assign by up to
// fraction, 0 <= fraction < 1, so that keys written together do not expire
// together. Keys loaded in one burst — a warm-up, a batch load, a restore, a
// loader that always answers one TTL — otherwise all expire in the same instant
// and their reloads reach the database at once.
//
// The amount is fixed per key, a function of the key alone: a key rewritten
// keeps the same lifetime, and replicas and persistence carry the owner's
// deadline, so no two nodes disagree. TTLs only get shorter, never longer,
// because a TTL is often a bound on how stale a value may be. A fraction of
// 0.1 turns a one-hour TTL into one between 54 and 60 minutes. Zero, the
// default, assigns TTLs exactly; outside [0, 1) is ErrInvalidOption. See
// docs/decisions/0037-ttl-jitter.md.
func WithTTLJitter(fraction float64) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.TTLJitter = fraction }
}

// WithL1TTL bounds how long a copy of a non-owned key may be served from local
// memory.
//
// This is a correctness bound, not a tuning knob. Invalidation broadcasts are a
// latency optimisation and may be lost; this TTL is the only thing that turns
// an unbounded stale read into a bounded one. Longer means faster and staler,
// and the staleness is what a caller must reason about.
// A negative duration disables the non-owned tier for this namespace. Every
// read of a key owned elsewhere then costs a round trip and nothing is ever
// served stale — which is the right trade for a namespace whose values must
// never lag, and the wrong one for almost everything else.
func WithL1TTL(d time.Duration) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.L1TTL = d }
}

// WithL1MaxBytes bounds the memory this namespace's non-owned tier may hold on
// each node. Default 64 MiB.
//
// Each copy is charged its key and value bytes plus a fixed overhead, the same
// charge the L1 bytes gauge and the memory budget see, so the bound and the
// gauge agree. Past it the least valuable copies are evicted, which costs round
// trips, never data. A copy larger than the whole bound is not cached at all.
// The bound is in bytes, not entries, because a count says nothing about
// memory once values differ in size. Zero or negative is ErrInvalidOption.
func WithL1MaxBytes(n int64) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.L1MaxBytes = n }
}

// WithCompression compresses stored values of at least threshold bytes using
// the named compressor. "lz4" is registered by default.
//
// Compression is off unless this is set. A decompress on every read would put a
// compression codec on the hot path of a cache whose entire premise is
// sub-microsecond local reads, and most cached values are far too small to
// benefit. The threshold exists because compressing a forty-byte counter costs
// more than it saves.
//
// Compression applies above a configurable threshold, and this is that knob.
// See
// docs/decisions/0008-compression-is-opt-in.md.
func WithCompression(name string, threshold int) NamespaceOption {
	return func(o *core.NamespaceOptions) {
		o.Compressor = name
		o.CompressionThreshold = threshold
	}
}

// WithLoaderTimeout bounds one invocation of the namespace's loader. The
// default is 30 seconds.
//
// This cannot be the calling context's deadline. Concurrent misses collapse
// into a single load, so taking whichever caller arrived first as the bound
// would let a caller with five milliseconds left abort a load that a thousand
// callers with a second left were depending on.
func WithLoaderTimeout(d time.Duration) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.LoaderTimeout = d }
}

// WithShards sets the number of independently locked partitions in this
// namespace's store, rounded up to a power of two. Zero takes the default of
// 256.
//
// The default is well above any realistic core count, so contention comes from
// key collisions rather than from the shard map. Lower it only for a namespace
// known to hold very few keys, where 256 mostly-empty maps are pure overhead.
func WithShards(n int) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.Shards = n }
}

// Loader reconstructs a namespace value that is not resident.
//
// The returned ttl is applied exactly as [Namespace.Set] would apply it: a
// non-positive value means the loaded value does not expire on its own. A
// loader that returns zero is asking for a value reconstructed once and never
// refreshed, which is occasionally what is wanted and more often an oversight.
type Loader = core.Loader

// BatchLoader reconstructs many missing keys in one call; see
// [WithBatchLoader].
type BatchLoader = core.BatchLoader

// WithBatchLoader registers the loader BatchGetOrLoad calls on each key's owner
// with every key in the batch that owner is missing — one call, usually one
// database query, instead of one per key. A key absent from the returned map
// does not exist and is left out of the result; the TTL applies to every value
// returned; an error fails that owner's share of the batch and stores nothing.
// Each value is stored only if no write landed while the loader ran.
//
// It runs where the per-key loader does, on the owner, bounded by
// WithLoaderTimeout and not by the caller's context. A namespace without one
// still answers BatchGetOrLoad with its WithLoader loader, once per missing
// key. It is byte-valued; a NamespaceOf view's BatchGetOrLoad uses it or its
// typed loader. See docs/decisions/0035-batch-loads.md.
func WithBatchLoader(fn BatchLoader) NamespaceOption {
	return func(o *core.NamespaceOptions) { o.BatchLoader = fn }
}

// Partitioner maps a cache key to the group key that decides its owner.
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
