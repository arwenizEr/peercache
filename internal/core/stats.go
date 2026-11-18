package core

// Stats is a point-in-time snapshot of a node's view of itself and the cluster.
//
// Alongside throughput it carries the correctness counters (split-brain
// detections, stale-epoch rejections, version conflicts). It is a plain struct rather than an interface so that adding a field is not a
// breaking change for implementers.
//
// Every field is a snapshot, not a live view. Reading Stats never blocks a data
// path.
type Stats struct {
	// Self is this node.
	Self Node `json:"self"`
	// Members is the number of nodes in the local membership view,
	// including Self.
	Members int `json:"members"`
	// Epoch is the membership epoch the view was taken under.
	Epoch Epoch `json:"epoch"`

	// OwnedKeys is the number of keys this node owns and holds.
	OwnedKeys int64 `json:"ownedKeys"`
	// ReplicaKeys is the number of keys this node holds as a replica.
	ReplicaKeys int64 `json:"replicaKeys"`
	// L1Entries is the number of non-owned, TTL-bounded cached entries.
	L1Entries int64 `json:"l1Entries"`

	// AddressableGroups is the number of groups this node knows of. A group
	// is addressable at all times; activation is a separate, smaller set.
	AddressableGroups int64 `json:"addressableGroups"`
	// ActiveGroups counts groups with a live actor goroutine on this node.
	ActiveGroups int64 `json:"activeGroups"`

	// Hits and Misses count namespace reads served from local memory and
	// not, respectively. An expired key counts as a miss, because that is
	// what the caller experienced.
	Hits   int64 `json:"hits"`
	Misses int64 `json:"misses"`

	// InflightLoads is the number of callers currently waiting on a loader.
	// It is the observable evidence that concurrent misses are collapsing
	// rather than stampeding.
	InflightLoads int64 `json:"inflightLoads"`
	// Loads counts loader invocations; CollapsedLoads counts GetOrLoad
	// calls that attached to a load already in flight instead of starting
	// one. A healthy ratio under load is what singleflight is for.
	Loads          int64 `json:"loads"`
	CollapsedLoads int64 `json:"collapsedLoads"`
	// CoalescedReads counts reads of keys owned elsewhere that missed L1
	// and joined a fetch from the owner already in flight on this node,
	// instead of sending their own. See docs/decisions/0028.
	CoalescedReads int64 `json:"coalescedReads"`
	// NegativeHits counts GetOrLoad calls the owner answered ErrNotFound
	// from a remembered absence, without calling the loader, in namespaces
	// opened with WithNegativeCaching. See docs/decisions/0034.
	NegativeHits int64 `json:"negativeHits"`
	// Refreshes counts background reloads WithRefreshAhead completed, stored
	// or dropped because a write landed first; RefreshFailures counts the
	// ones whose loader failed, which left the current value in place.
	Refreshes       int64 `json:"refreshes"`
	RefreshFailures int64 `json:"refreshFailures"`

	// ReplicaReadsServed counts reads this node answered from its replica
	// copy, for a peer or for itself, in namespaces opened with
	// WithReplicaReads. ReplicaReadsReferred counts the ones it declined --
	// no copy, a copy older than the L1 TTL, or no longer a replica -- which
	// the reader then sent to the owner. A rising referred count with a
	// steady served count is what keys quieter than the TTL look like.
	ReplicaReadsServed   int64 `json:"replicaReadsServed"`
	ReplicaReadsReferred int64 `json:"replicaReadsReferred"`
	// ReplicaReadsFloored counts replica answers this node discarded as a
	// reader because an invalidation had already told it of a newer stamp.
	ReplicaReadsFloored int64 `json:"replicaReadsFloored"`

	// PersistPending is how many keys of persistent namespaces are waiting
	// for the next flush — the keys a crash now would lose.
	// PersistFlushes counts flushes, PersistRecordsWritten the records they
	// wrote, and PersistFailures segments that could not be written and
	// were kept for the next flush. See
	// docs/decisions/0029-persistence-as-a-segment-log.md.
	PersistPending        int64 `json:"persistPending"`
	PersistFlushes        int64 `json:"persistFlushes"`
	PersistRecordsWritten int64 `json:"persistRecordsWritten"`
	PersistFailures       int64 `json:"persistFailures"`
	// PersistRestored counts keys a restore brought back into memory, and
	// PersistRestoreFailures restores that could not read their partition —
	// a store outage or a corrupt object — after which the partition is
	// served from memory alone until a retry succeeds.
	PersistRestored        int64 `json:"persistRestored"`
	PersistRestoreFailures int64 `json:"persistRestoreFailures"`
	// PersistCompactions counts partitions folded into a snapshot by this
	// node as their compactor, and PersistCompactFailures compactions that
	// could not finish; their objects are folded by the next one.
	PersistCompactions     int64 `json:"persistCompactions"`
	PersistCompactFailures int64 `json:"persistCompactFailures"`
	// PersistSplits counts log sets this node moved into partition segments
	// as its namespace's splitter, and PersistSplitFailures splits that
	// stopped part-way; the next one completes them. See decision 0030.
	PersistSplits        int64 `json:"persistSplits"`
	PersistSplitFailures int64 `json:"persistSplitFailures"`

	// RefusedActivations counts group activations the ownership guard
	// refused, and DeferredActivations the subset postponed because the
	// group had just been acquired.
	//
	// These are correctness metrics rather than performance ones. Deferrals
	// should spike after a membership change and return to zero; sustained
	// refusals under stable membership mean callers disagree with this node
	// about who owns what.
	RefusedActivations  int64 `json:"refusedActivations"`
	DeferredActivations int64 `json:"deferredActivations"`
	// StaleEpochRefusals counts activations refused because the caller
	// resolved ownership under a view older than this node's. A sustained
	// rate means a caller is caching ownership for
	// longer than membership stays still.
	StaleEpochRefusals int64 `json:"staleEpochRefusals"`
	// EpochSkew counts peer requests whose caller had resolved ownership
	// under a different membership epoch from this node's.
	//
	// It is an observation, not a failure. Epochs are per-view and two
	// nodes that agree about membership can still hold different ones, so
	// this is expected during churn. A sustained rate under stable
	// membership means views are not settling.
	EpochSkew int64 `json:"epochSkew"`

	// InvalidationsSent, InvalidationsDropped and InvalidationsFailed
	// describe the best-effort invalidation broadcast.
	//
	// None of them is an error condition. Sent and Failed count messages
	// to peers, and a message carries every hint queued for its peer and
	// namespace, so under write pressure one message stands for many
	// hints. Dropped counts hints discarded because a queue was full — the
	// node's, once, or one peer's, once for that peer — which is the
	// designed behaviour under write pressure; Failed counts messages a
	// peer did not accept. Both
	// mean some node will serve a stale copy until its TTL expires, which
	// is the documented bound: correctness never depends on an
	// invalidation arriving. They are
	// worth watching as a ratio, not alerting on individually.
	InvalidationsSent    int64 `json:"invalidationsSent"`
	InvalidationsDropped int64 `json:"invalidationsDropped"`
	InvalidationsFailed  int64 `json:"invalidationsFailed"`

	// ReplicationsSent, ReplicationsDropped and ReplicationsFailed describe
	// replica fan-out.
	//
	// Unlike the invalidation counters, a non-zero Dropped or Failed here
	// is a real reduction in redundancy: a key that should exist on three
	// nodes exists on fewer until it is next written. Under asynchronous
	// replication that is the expected cost of a peer being briefly
	// unreachable; sustained, it means the replication factor is nominal
	// rather than actual.
	ReplicationsSent    int64 `json:"replicationsSent"`
	ReplicationsDropped int64 `json:"replicationsDropped"`
	ReplicationsFailed  int64 `json:"replicationsFailed"`
	// RestoreReplicationsDropped counts restored keys never sent to their
	// replicas because the backlog of restored keys waiting to go was at its
	// guard bound, four million. Zero in any ordinary restart: a restored
	// key waits for the network rather than being dropped, because after a
	// rolling restart its replicas restarted too and hold nothing. Counted
	// apart from ReplicationsDropped, which counts live writes.
	RestoreReplicationsDropped int64 `json:"restoreReplicationsDropped"`

	// SplitBrainDetections counts forks a replica saw in a key's history:
	// copies from two different writers, neither of which replaced the
	// other's write (replication.Forked).
	//
	// One group has one owner, so this means two nodes believed they owned
	// the same key — the partition case — or a new owner wrote over a value
	// it never received. Either way acknowledged writes diverged, which must
	// be detected and counted. It is never merged: conflicting
	// writes are reported, and choosing between them is the application's
	// business.
	//
	// Any non-zero value deserves investigation. It does not mean data was
	// lost, but it means two writers disagreed about who was in charge.
	SplitBrainDetections int64 `json:"splitBrainDetections"`

	// KeysPromoted counts keys this node took over from a failed owner:
	// replicas it already held that it now owns. ReplicasEvicted counts
	// copies it dropped because membership moved them elsewhere.
	//
	// Both spike on a membership change and are flat otherwise. Movement
	// under stable membership means the view is not settling.
	KeysPromoted    int64 `json:"keysPromoted"`
	ReplicasEvicted int64 `json:"replicasEvicted"`

	// ReplicasRehomed counts replicas this node handed to the nodes that now
	// rank for them before dropping its own copy. ReplicasRetained counts the
	// ones it could not place and kept instead.
	//
	// Together they account for every replica ReplicasEvicted counts: a
	// membership change moves keys, and each moved copy is either placed
	// elsewhere or held onto. Dropping it because this node stopped ranking
	// for it loses the key whenever the
	// nodes that now rank for it hold nothing, which is the ordinary case
	// after a partition heals. See docs/decisions/open.md question 38.
	//
	// A rising ReplicasRetained under stable membership means hand-offs are
	// failing and this node is accumulating copies it does not rank for. It
	// is memory rather than data loss, which is the safe direction, but it
	// means the cluster is not converging.
	ReplicasRehomed  int64 `json:"replicasRehomed"`
	ReplicasRetained int64 `json:"replicasRetained"`
	// PromotionRounds counts reconciliations run. It exists so the two
	// counters above can be read as a rate, and so a test can wait for a
	// membership change to have been *acted on* rather than merely observed.
	PromotionRounds int64 `json:"promotionRounds"`
	// PromotionScans counts passes that ranked every replica of a namespace
	// for ownership: one per namespace on a membership change, on every
	// tick while a view settles, and at the end of a catch-up. It is flat
	// under stable membership even while PromotionRounds rises, because a
	// round that runs only to hand off out-of-place copies does not scan.
	// Rising under stable membership means the view is not settling.
	PromotionScans int64 `json:"promotionScans"`

	// ReplicasPulled counts keys obtained by catch-up, CatchUpRuns the
	// number of times it ran, and CatchUpFailures the peers that could not
	// be reached during one.
	//
	// A restarted node returns with empty memory, and replication only
	// carries keys written after it came back — so without catch-up, every
	// key that was quiet when it died stays missing. Failures here mean the
	// replica set is thinner than its configured factor claims.
	ReplicasPulled  int64 `json:"replicasPulled"`
	CatchUpRuns     int64 `json:"catchUpRuns"`
	CatchUpFailures int64 `json:"catchUpFailures"`

	// ReplicasCurrent mirrors ReadyState.ReplicasCurrent: this node has
	// completed a catch-up, with no failure, under the membership view it
	// holds now. It is here as well so the metrics surface can carry it.
	ReplicasCurrent bool `json:"replicasCurrent"`

	// KeysHandedOff counts keys pushed to a new owner during a graceful
	// drain. Zero on a node that has never drained; on one that has, it
	// should equal what it owned when the drain began.
	KeysHandedOff int64 `json:"keysHandedOff"`
	// KeysAdopted counts keys received from a draining peer that this node
	// already owned, and so took directly into its owned store rather than
	// filing as replicas.
	KeysAdopted int64 `json:"keysAdopted"`

	// ImplicitNamespaces counts namespaces this node created in response to
	// an incoming peer write, rather than because its own application asked
	// for one. Since peer requests for an unopened namespace are refused
	// (UnopenedNamespaceRefusals), nothing on the request paths creates one;
	// the counter stays, because a metric is never removed before v2.
	//
	// Any non-zero value is a misconfiguration worth alerting on. Such a
	// namespace runs with default options — replication factor 1 — and
	// namespace options cannot be changed afterwards, so this node will
	// replicate nothing for it however the application configures it later.
	// Losing the node then loses its entire share of that namespace.
	//
	// The remedy is to open every namespace, with the options you want, on
	// every node before serving traffic.
	ImplicitNamespaces int64 `json:"implicitNamespaces"`
	// UnopenedNamespaceRefusals counts peer requests this node refused with
	// ErrActivationPending because they named a namespace its application
	// has not opened. A node that is starting refuses some, briefly, before
	// it opens its namespaces; a rate that does not fall means this node's
	// application never opens a namespace its peers use, and its share of
	// that namespace cannot be served.
	UnopenedNamespaceRefusals int64 `json:"unopenedNamespaceRefusals"`

	// DegradedWrites counts synchronous writes this node acknowledged while
	// holding fewer copies than the namespace's replication factor asked for.
	//
	// Replication targets come from the ranking over this node's own
	// membership view. A node cut off from the cluster eventually detects
	// every peer as failed, ranks itself the owner of everything, places
	// every copy it can reach — one, its own — and returns success. The
	// caller asked for three copies and cannot tell it got one.
	//
	// Any non-zero value means synchronous replication was not synchronous
	// with anything: for that write, the promise that a returned Set has
	// reached the replication factor did not hold. During a partition it
	// rises for as long as the partition lasts, which is what makes it worth
	// alerting on rather than merely watching — it is the only signal that
	// distinguishes a degraded acknowledgement from a healthy one.
	//
	// Asynchronous writes are not counted. They promise nothing about when a
	// copy lands, so counting them would produce a number equal to the write
	// rate; the async window is measured in docs/benchmarks.md instead.
	//
	// Counted, not refused, unless the namespace was opened with
	// WithRequireReplicationFactor; such writes are refused and counted in
	// UnderReplicatedRefusals instead. See docs/decisions/open.md question 37.
	DegradedWrites int64 `json:"degradedWrites"`

	// UnderReplicatedRefusals counts synchronous writes refused with
	// [ErrUnderReplicated] because this node could not place as many copies
	// as the namespace's replication factor asked for, in a namespace that
	// requires it. It is the refusing counterpart of DegradedWrites: the same
	// event, on a namespace configured to fail it rather than acknowledge it.
	//
	// The caller was told, so this is not a silent failure. It is still worth
	// watching: a sustained rate means a node is cut off, or the cluster is
	// smaller than the factor, and every write to those namespaces is failing.
	UnderReplicatedRefusals int64 `json:"underReplicatedRefusals"`

	// MemoryBudget is the configured budget in bytes, zero for none, and
	// MemoryUsed the accounted size of the owned, replica and L1 tiers. The
	// accounting is reported with or without a budget, so that one can be
	// sized from it.
	MemoryBudget int64 `json:"memoryBudget"`
	MemoryUsed   int64 `json:"memoryUsed"`
	// ShedL1, ShedReplicas and ShedOwned count entries dropped to stay within
	// the budget, by tier. ShedOwned is data loss, by design and on purpose;
	// a sustained rate means the budget is smaller than this node's share.
	ShedL1       int64 `json:"shedL1"`
	ShedReplicas int64 `json:"shedReplicas"`
	ShedOwned    int64 `json:"shedOwned"`

	// MembersOnProbation counts members this node can see but does not yet
	// rank, because they have been up for less than WithJoinProbation. A node
	// that stays here and keeps reappearing is a pod in CrashLoopBackOff.
	MembersOnProbation int `json:"membersOnProbation"`

	// StateEscapes counts uses of a GroupState outside the turn that issued
	// it. Any non-zero value is an application bug.
	//
	// The runtime cannot report this as an error: the goroutine holding an
	// escaped state is not inside a Call, so there is nobody to return to.
	// The methods no-op and the violation is counted instead — silence would
	// be wrong and a panic in someone else's goroutine would be worse — which
	// makes this counter the only way the bug is ever visible.
	StateEscapes int64 `json:"stateEscapes"`

	// MailboxRejections counts calls refused because a group's mailbox was
	// full, reported to the caller as ErrGroupOverloaded.
	//
	// This is a correctness metric. A rising value
	// is backpressure working as designed, not a fault — but it is the
	// signal that a group is saturated, and the only one that arrives before
	// the latency does.
	MailboxRejections int64 `json:"mailboxRejections"`

	// VersionConflicts counts replicated copies rejected because a newer one
	// was already held.
	//
	// Distinct from SplitBrainDetections, which counts two nodes writing one
	// key under one epoch. This counts one node's writes arriving out of
	// order, which asynchronous replication makes ordinary and the stamp
	// ordering handles. A steady low rate is normal; a rising one means
	// reordering under load.
	VersionConflicts int64 `json:"versionConflicts"`
}
