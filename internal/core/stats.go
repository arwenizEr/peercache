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
}
