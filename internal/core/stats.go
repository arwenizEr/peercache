package core

// Stats is a point-in-time snapshot of a node's view of itself and the cluster.
//
// SPEC.md names Stats in §3.1 but never defines it. This shape covers what M0
// through M3 can populate honestly; M7 extends it with the correctness counters
// (split-brain detections, stale-epoch rejections, version conflicts). It is a
// plain struct rather than an interface so that adding a field is not a
// breaking change for implementers.
//
// Every field is a snapshot, not a live view. Reading Stats never blocks a data
// path.
type Stats struct {
	// Self is this node.
	Self Node
	// Members is the number of nodes in the local membership view,
	// including Self.
	Members int
	// Epoch is the membership epoch the view was taken under.
	Epoch Epoch

	// OwnedKeys is the number of keys this node owns and holds.
	OwnedKeys int64
	// ReplicaKeys is the number of keys this node holds as a replica.
	ReplicaKeys int64
	// L1Entries is the number of non-owned, TTL-bounded cached entries.
	L1Entries int64

	// AddressableGroups is the number of groups this node knows of. A group
	// is addressable at all times; activation is a separate, smaller set.
	AddressableGroups int64
	// ActiveGroups counts groups with a live actor goroutine on this node.
	ActiveGroups int64

	// Hits and Misses count namespace reads served from local memory and
	// not, respectively. An expired key counts as a miss, because that is
	// what the caller experienced.
	Hits   int64
	Misses int64

	// InflightLoads is the number of callers currently waiting on a loader.
	// It is the observable evidence that concurrent misses are collapsing
	// rather than stampeding.
	InflightLoads int64
	// Loads counts loader invocations; CollapsedLoads counts GetOrLoad
	// calls that attached to a load already in flight instead of starting
	// one. A healthy ratio under load is what singleflight is for.
	Loads          int64
	CollapsedLoads int64

	// RefusedActivations counts group activations the ownership guard
	// refused, and DeferredActivations the subset postponed because the
	// group had just been acquired.
	//
	// These are correctness metrics rather than performance ones. Deferrals
	// should spike after a membership change and return to zero; sustained
	// refusals under stable membership mean callers disagree with this node
	// about who owns what.
	RefusedActivations  int64
	DeferredActivations int64
	// StaleEpochRefusals counts activations refused because the caller
	// resolved ownership under a view older than this node's. It stays zero
	// until M4, when requests start arriving with an epoch attached; a
	// sustained rate after that means a caller is caching ownership for
	// longer than membership stays still.
	StaleEpochRefusals int64
	// EpochSkew counts peer requests whose caller had resolved ownership
	// under a different membership epoch from this node's.
	//
	// It is an observation, not a failure. Epochs are per-view and two
	// nodes that agree about membership can still hold different ones, so
	// this is expected during churn. A sustained rate under stable
	// membership means views are not settling.
	EpochSkew int64

	// InvalidationsSent, InvalidationsDropped and InvalidationsFailed
	// describe the best-effort invalidation broadcast.
	//
	// None of them is an error condition. Dropped counts hints discarded
	// because the queue was full, which is the designed behaviour under
	// write pressure; Failed counts peers that did not accept one. Both
	// mean some node will serve a stale copy until its TTL expires, which
	// is the documented bound — see CLAUDE.md non-negotiable 6. They are
	// worth watching as a ratio, not alerting on individually.
	InvalidationsSent    int64
	InvalidationsDropped int64
	InvalidationsFailed  int64

	// ReplicationsSent, ReplicationsDropped and ReplicationsFailed describe
	// replica fan-out.
	//
	// Unlike the invalidation counters, a non-zero Dropped or Failed here
	// is a real reduction in redundancy: a key that should exist on three
	// nodes exists on fewer until it is next written. Under asynchronous
	// replication that is the expected cost of a peer being briefly
	// unreachable; sustained, it means the replication factor is nominal
	// rather than actual.
	ReplicationsSent    int64
	ReplicationsDropped int64
	ReplicationsFailed  int64

	// SplitBrainDetections counts writes a replica saw from two different
	// nodes under one membership epoch.
	//
	// One group has one owner, so this cannot happen unless two nodes
	// believed they owned the same key — the partition case SPEC.md §4
	// requires be detected and counted. It is never merged: conflicting
	// writes are reported, and choosing between them is the application's
	// business.
	//
	// Any non-zero value deserves investigation. It does not mean data was
	// lost, but it means two writers disagreed about who was in charge.
	SplitBrainDetections int64

	// KeysPromoted counts keys this node took over from a failed owner:
	// replicas it already held that it now owns. ReplicasEvicted counts
	// copies it dropped because membership moved them elsewhere.
	//
	// Both spike on a membership change and are flat otherwise. Movement
	// under stable membership means the view is not settling.
	KeysPromoted    int64
	ReplicasEvicted int64
	// PromotionRounds counts reconciliations run. It exists so the two
	// counters above can be read as a rate, and so a test can wait for a
	// membership change to have been *acted on* rather than merely observed.
	PromotionRounds int64

	// ReplicasPulled counts keys obtained by catch-up, CatchUpRuns the
	// number of times it ran, and CatchUpFailures the peers that could not
	// be reached during one.
	//
	// A restarted node returns with empty memory, and replication only
	// carries keys written after it came back — so without catch-up, every
	// key that was quiet when it died stays missing. Failures here mean the
	// replica set is thinner than its configured factor claims.
	ReplicasPulled  int64
	CatchUpRuns     int64
	CatchUpFailures int64

	// KeysHandedOff counts keys pushed to a new owner during a graceful
	// drain. Zero on a node that has never drained; on one that has, it
	// should equal what it owned when the drain began.
	KeysHandedOff int64
	// KeysAdopted counts keys received from a draining peer that this node
	// already owned, and so took directly into its owned store rather than
	// filing as replicas.
	KeysAdopted int64

	// ImplicitNamespaces counts namespaces this node created in response to
	// an incoming peer write, rather than because its own application asked
	// for one.
	//
	// Any non-zero value is a misconfiguration worth alerting on. Such a
	// namespace runs with default options — replication factor 1 — and
	// namespace options cannot be changed afterwards, so this node will
	// replicate nothing for it however the application configures it later.
	// Losing the node then loses its entire share of that namespace.
	//
	// The remedy is to open every namespace, with the options you want, on
	// every node before serving traffic.
	ImplicitNamespaces int64

	// MailboxRejections counts calls refused because a group's mailbox was
	// full, reported to the caller as ErrGroupOverloaded.
	//
	// SPEC.md's M7 bullet names this as a correctness metric. A rising value
	// is backpressure working as designed, not a fault — but it is the
	// signal that a group is saturated, and the only one that arrives before
	// the latency does.
	MailboxRejections int64

	// VersionConflicts counts replicated copies rejected because a newer one
	// was already held.
	//
	// Distinct from SplitBrainDetections, which counts two nodes writing one
	// key under one epoch. This counts one node's writes arriving out of
	// order, which asynchronous replication makes ordinary and the stamp
	// ordering handles. A steady low rate is normal; a rising one means
	// reordering under load.
	VersionConflicts int64
}
