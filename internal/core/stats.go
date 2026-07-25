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
	// not, respectively.
	Hits   int64
	Misses int64
}
