package wire

import (
	"fmt"

	"github.com/arwenizEr/peercache/internal/codec"
	"github.com/arwenizEr/peercache/internal/core"
)

// Encoding is the codec used for message envelopes.
//
// It is deliberately fixed rather than configurable. WithCodec chooses how an
// application's *values* are encoded; this is the encoding of the protocol
// itself, and letting two pods disagree about it would mean they could not
// exchange the message that says which codec to use.
const Encoding = codec.NameMsgPack

// Stamp is an owner-issued (epoch, version) pair.
//
// It is [core.Stamp] under a local name, so that the protocol and the storage
// tiers compare versions by the same rule rather than by two implementations
// that agree until one of them is edited.
type Stamp = core.Stamp

// GetRequest asks an owner for one key.
type GetRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	// Epoch is the membership epoch the caller resolved ownership under.
	// The owner refuses the call if it is stale, which is what stops a
	// caller acting on a view the cluster has moved on from.
	Epoch uint64 `msgpack:"epoch"`
}

// GetResponse is an owner's answer.
type GetResponse struct {
	Found bool   `msgpack:"found"`
	Value []byte `msgpack:"value"`
	Stamp Stamp  `msgpack:"stamp"`
	// TTLRemainingNanos bounds how long the caller may keep this value in
	// its non-owned tier. Zero means the owner set no TTL, in which case
	// the caller applies its own maximum. The caller always clamps: an
	// owner cannot grant a longer cache lifetime than the caller's own
	// policy allows.
	TTLRemainingNanos int64 `msgpack:"ttl_remaining_ns"`
}

// ReplicaGetRequest asks a replica for its copy of a key. See
// docs/decisions/0028-hot-key-reads.md.
type ReplicaGetRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Epoch     uint64 `msgpack:"epoch"`
}

// ReplicaGetResponse is a replica's answer. Served false means it declined and
// the caller must ask the owner; a replica never reports a key absent.
type ReplicaGetResponse struct {
	Served bool   `msgpack:"served"`
	Value  []byte `msgpack:"value"`
	Stamp  Stamp  `msgpack:"stamp"`
	// AgeNanos is how long ago the replica received this copy, by its own
	// clock. The caller caches the value for its L1 TTL minus this, so the
	// copy's whole life stays inside one TTL of its arrival.
	AgeNanos int64 `msgpack:"age_ns"`
	// TTLRemainingNanos is how long the key itself has left, as
	// GetResponse's is; zero for no expiry. A copy must not outlive its key.
	TTLRemainingNanos int64 `msgpack:"ttl_remaining_ns"`
}

// LoadRequest asks an owner for a key, reconstructing it if absent.
type LoadRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Epoch     uint64 `msgpack:"epoch"`
}

// LoadResponse is the owner's answer, after loading if it had to.
//
// Found is false only when the namespace has no loader and the key is absent.
// A loader that fails returns an error rather than a negative answer, because
// "could not reconstruct" and "does not exist" call for different responses
// from the caller.
type LoadResponse struct {
	Found             bool   `msgpack:"found"`
	Value             []byte `msgpack:"value"`
	Stamp             Stamp  `msgpack:"stamp"`
	TTLRemainingNanos int64  `msgpack:"ttl_remaining_ns"`
}

// SetRequest writes one key on its owner.
type SetRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Value     []byte `msgpack:"value"`
	TTLNanos  int64  `msgpack:"ttl_ns"`
	Epoch     uint64 `msgpack:"epoch"`
}

// SetResponse returns the version the owner assigned.
type SetResponse struct {
	Stamp Stamp `msgpack:"stamp"`
}

// DeleteRequest removes one key on its owner.
type DeleteRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Epoch     uint64 `msgpack:"epoch"`
}

// CondSetRequest writes one key on its owner only if a condition holds there:
// the key is absent, or, with Compare, it holds exactly Old. It carries both
// SetIfAbsent and CompareAndSwap.
type CondSetRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Value     []byte `msgpack:"value"`
	Compare   bool   `msgpack:"compare"`
	Old       []byte `msgpack:"old"`
	TTLNanos  int64  `msgpack:"ttl_ns"`
	Epoch     uint64 `msgpack:"epoch"`
}

// CondSetResponse reports whether the owner stored the value.
type CondSetResponse struct {
	Stored bool `msgpack:"stored"`
	// UnderReplicated, when set, is the owner's ErrUnderReplicated: the write
	// happened and the result above is its result, but replication fell short.
	// It travels in the body rather than as an RPC error because an error
	// carries no body, and the caller needs the result to act on the write.
	UnderReplicated string `msgpack:"under_replicated"`
}

// IncrementRequest adds Delta to the integer under one key on its owner.
// TTLNanos applies only if the increment creates the key.
type IncrementRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Delta     int64  `msgpack:"delta"`
	TTLNanos  int64  `msgpack:"ttl_ns"`
	Epoch     uint64 `msgpack:"epoch"`
}

// IncrementResponse returns the integer after the increment.
type IncrementResponse struct {
	Value int64 `msgpack:"value"`
	// UnderReplicated, when set, is the owner's ErrUnderReplicated: the write
	// happened and the result above is its result, but replication fell short.
	// It travels in the body rather than as an RPC error because an error
	// carries no body, and the caller needs the result to act on the write.
	UnderReplicated string `msgpack:"under_replicated"`
}

// TouchRequest gives one key a new lifetime on its owner.
type TouchRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	TTLNanos  int64  `msgpack:"ttl_ns"`
	Epoch     uint64 `msgpack:"epoch"`
}

// TouchResponse reports whether the key existed.
type TouchResponse struct {
	Existed bool `msgpack:"existed"`
	// UnderReplicated, when set, is the owner's ErrUnderReplicated: the write
	// happened and the result above is its result, but replication fell short.
	// It travels in the body rather than as an RPC error because an error
	// carries no body, and the caller needs the result to act on the write.
	UnderReplicated string `msgpack:"under_replicated"`
}

// DeleteResponse is empty. It exists so that the method has a response type
// like every other, rather than a special case in the dispatcher.
type DeleteResponse struct{}

// BatchGetRequest reads many keys from one namespace in one round trip.
//
// The caller has already grouped keys by owner, so every key here belongs to
// the node being asked. That grouping is the entire point: it turns N keys
// spread over three owners into three requests rather than N.
type BatchGetRequest struct {
	Namespace string   `msgpack:"namespace"`
	Keys      []string `msgpack:"keys"`
	Epoch     uint64   `msgpack:"epoch"`
	// Load asks the owner to load the keys it does not hold, as
	// BatchGetOrLoad does (docs/decisions/0035-batch-loads.md). It is the
	// fourth field of the array: a peer on an earlier release reads three
	// and answers what it holds, which is BatchGet.
	Load bool `msgpack:"load"`
}

// BatchGetResponse carries whatever the owner had.
//
// Entries is sparse: a key absent on the owner has no entry. Callers must match
// by key rather than by index, because the response is not aligned with the
// request and a future owner is free to reorder.
type BatchGetResponse struct {
	// Misrouted lists, for a request with Load, the keys this node does
	// not own, so the caller asks their owner rather than taking them as
	// missing. The second field of the array; empty for a plain BatchGet.
	Misrouted []string     `msgpack:"misrouted"`
	Entries   []BatchEntry `msgpack:"entries"`
}

// BatchEntry is one key's worth of a [BatchGetResponse].
type BatchEntry struct {
	Key               string `msgpack:"key"`
	Value             []byte `msgpack:"value"`
	Stamp             Stamp  `msgpack:"stamp"`
	TTLRemainingNanos int64  `msgpack:"ttl_remaining_ns"`
}

// CallRequest runs a registered function on a group's owner.
type CallRequest struct {
	Group string `msgpack:"group"`
	// FuncName selects a function registered at startup. Closures never
	// cross the network, so this is a name
	// and a byte payload rather than anything executable.
	FuncName string `msgpack:"func_name"`
	Args     []byte `msgpack:"args"`
	Epoch    uint64 `msgpack:"epoch"`
}

// CallResponse carries the function's encoded result.
type CallResponse struct {
	Result []byte `msgpack:"result"`
}

// SubmitRequest applies many messages to many groups on one owner.
//
// One request carries every group that owner is responsible for, which is what
// makes the cost of a router flush proportional to the number of owners rather
// than to the number of messages.
type SubmitRequest struct {
	// FuncName is the registered handler applied to every batch here. A
	// router carries one handler, so it is named once rather than per group.
	FuncName string       `msgpack:"func_name"`
	Batches  []GroupBatch `msgpack:"batches"`
	Epoch    uint64       `msgpack:"epoch"`
}

// GroupBatch is every message destined for one group, in submission order.
type GroupBatch struct {
	Group string `msgpack:"group"`
	// Messages are encoded payloads. The owner hands the whole list to the
	// handler as a single argument, so that a group's batch is applied in
	// one actor turn rather than one turn per message.
	Messages [][]byte `msgpack:"messages"`
}

// SubmitResponse reports which groups did not apply.
//
// Failures is sparse: a group that applied cleanly has no entry. Partial
// failure is a real outcome rather than an edge case — there is no cross-group
// atomicity in this system and never will be — so the response names the groups
// that did not take rather than collapsing everything into one error.
type SubmitResponse struct {
	Failures []GroupFailure `msgpack:"failures"`
}

// GroupFailure is one group's error, as text.
//
// The error crosses as a string because a sentinel per group would need a
// second error-mapping scheme alongside the transport's, and the caller's
// remedy — resubmit that group — is the same whatever the cause.
type GroupFailure struct {
	Group string `msgpack:"group"`
	Error string `msgpack:"error"`
}

// ReplicateRequest carries one owned key to one of its replicas.
type ReplicateRequest struct {
	Namespace string `msgpack:"namespace"`
	Key       string `msgpack:"key"`
	Value     []byte `msgpack:"value"`
	TTLNanos  int64  `msgpack:"ttl_ns"`
	// Stamp is the owner's (epoch, version). The replica keeps it so that a
	// promotion continues the version sequence rather than restarting it,
	// which would make every cached copy in the cluster look newer than the
	// truth.
	Stamp Stamp `msgpack:"stamp"`
	// From is the sending node. Two senders writing one key under one epoch
	// is a split brain, and the replica is the only party that sees both.
	From string `msgpack:"from"`
	// Deleted marks a removal. Deletions must replicate: a replica that
	// missed one would resurrect the key the moment it was promoted.
	Deleted bool `msgpack:"deleted"`
	// Order is the write's order from its owner's hybrid logical clock, in
	// every namespace; a release before M16 sent it for persistent ones only,
	// and zero means that. It orders replica copies, and the replica keeps it
	// so that a promotion continues it, and advances its own clock past it.
	Order uint64 `msgpack:"order"`
	// Prev is the order of the value this write replaced on its owner, zero
	// if none or unknown. A replica holding a copy from another writer
	// compares the two to tell a handover — this write descends from the
	// copy — from a fork, two writers each unaware of the other's write.
	Prev uint64 `msgpack:"prev"`
}

// ReplicateResponse reports whether the copy collided with another writer's.
type ReplicateResponse struct {
	Conflict bool `msgpack:"conflict"`
}

// CatchUpRequest asks an owner for the keys this node should be replicating.
//
// A restarted node comes back with empty memory and no record of what it was
// responsible for. It cannot enumerate what it wants, so it names itself and
// lets each owner work out the answer from the ownership ranking both of them
// compute identically.
type CatchUpRequest struct {
	Namespace string `msgpack:"namespace"`
	Requester string `msgpack:"requester"`
	// ReplicationFactor is the *asker's* configured factor. Namespace
	// options are per-node configuration, so the owner must not assume its
	// own matches.
	ReplicationFactor int `msgpack:"replication_factor"`
	// After and Limit page the response. Without them a catch-up on a large
	// namespace would build one message proportional to the whole keyspace,
	// which is the shape of every out-of-memory bug in this file's history.
	After string `msgpack:"after"`
	Limit int    `msgpack:"limit"`
	// Replicas asks for the sender's *replica* copies of keys the asker
	// owns, instead of the keys the sender owns. A node restarted faster
	// than failure detection was never declared dead, so nobody was promoted
	// in its place, and the newest copies of what it owns are replicas on its
	// peers. A sender on an earlier release ignores the field and answers
	// with its owned keys again, which the stamp check makes harmless.
	Replicas bool `msgpack:"replicas"`
}

// CatchUpResponse carries one page of keys.
type CatchUpResponse struct {
	Entries []CatchUpEntry `msgpack:"entries"`
	More    bool           `msgpack:"more"`
}

// CatchUpEntry is one key's worth of a catch-up page.
type CatchUpEntry struct {
	Key      string `msgpack:"key"`
	Value    []byte `msgpack:"value"`
	Stamp    Stamp  `msgpack:"stamp"`
	TTLNanos int64  `msgpack:"ttl_ns"`
	// Order is the entry's persistence order; see ReplicateRequest.Order.
	Order uint64 `msgpack:"order"`
	// From is the node that wrote the value, set when the sender relays a
	// copy it holds as a replica. Empty means the sender wrote it. The
	// receiver records the writer, not the relay, for the same reason a
	// hand-off does: two relays of one write are not two writers, and filing
	// them as such counts a split brain that never happened.
	From string `msgpack:"from"`
	// Prev is a relayed copy's Prev, so a fork stays visible to its
	// receiver; zero for a key the sender owns.
	Prev uint64 `msgpack:"prev"`
}

// InvalidateRequest tells a peer that keys it may be caching have changed.
//
// It is a latency optimisation and nothing else. Correctness never depends on
// one of these arriving, which is why
// non-owned entries are TTL-bounded independently of it.
type InvalidateRequest struct {
	Namespace string   `msgpack:"namespace"`
	Keys      []string `msgpack:"keys"`
	// Stamp is the owner's stamp after the change. A receiver drops a
	// cached copy only if what it holds is older, so an invalidation that
	// arrives late cannot evict a value that is newer than it.
	Stamp Stamp `msgpack:"stamp"`
	// Entries are more keys of the same namespace, each with its own stamp:
	// the hints a sender batched into one message. A release before them
	// reads Keys and Stamp alone and ignores the rest, so a sender puts one
	// hint there and the others here.
	Entries []InvalidateEntry `msgpack:"entries"`
}

// InvalidateEntry is one key of an [InvalidateRequest] with its own stamp.
type InvalidateEntry struct {
	Key   string `msgpack:"key"`
	Stamp Stamp  `msgpack:"stamp"`
}

// InvalidateResponse is empty.
type InvalidateResponse struct{}

// DescribeRequest asks a peer for its Describe, and with Digests for its
// replica digests. See docs/decisions/0039-cluster-view.md.
type DescribeRequest struct {
	Digests bool `msgpack:"digests"`
}

// DescribeResponse is a peer's Describe, as the JSON Cache.Describe would
// serve, and its digests when asked for.
type DescribeResponse struct {
	Description []byte            `msgpack:"description"`
	Digests     []NamespaceDigest `msgpack:"digests"`
}

// NamespaceDigest is one namespace's running digests on one node, per
// partition of the keyspace: Owned over its owned store, Held over the replica
// copies it holds. Partitions with nothing in them are left out.
type NamespaceDigest struct {
	Namespace string       `msgpack:"namespace"`
	Owned     []PartDigest `msgpack:"owned"`
	Held      []PartDigest `msgpack:"held"`
}

// PartDigest is one partition's count of keys and the wrapping sum of a hash of
// each key and its version. It carries no key.
type PartDigest struct {
	Part  uint32 `msgpack:"part"`
	Count uint64 `msgpack:"count"`
	Sum   uint64 `msgpack:"sum"`
}

// Encode marshals a message.
func Encode(v any) ([]byte, error) {
	c, err := codec.Get(Encoding)
	if err != nil {
		return nil, fmt.Errorf("wire: %w", err)
	}
	b, err := c.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("wire: encoding %T: %w", v, err)
	}
	return b, nil
}

// Decode unmarshals a message into v, which must be a non-nil pointer.
//
// A decode failure is [core.ErrInvalidOption] rather than a bare codec error,
// because from the receiver's point of view a message it cannot parse is a
// malformed request, and the transport maps that to InvalidArgument.
//
// The frame is structurally validated before the decoder sees it, so that no
// length prefix the decoder acts on can be larger than the bytes behind it. See
// validate.go for why that check cannot live in the decoders themselves.
func Decode(data []byte, v any) error {
	c, err := codec.Get(Encoding)
	if err != nil {
		return fmt.Errorf("wire: %w", err)
	}
	if err := validate(data); err != nil {
		return fmt.Errorf("wire: decoding %T: %w", v, err)
	}
	if err := c.Unmarshal(data, v); err != nil {
		return fmt.Errorf("wire: decoding %T: %w: %w", v, err, core.ErrInvalidOption)
	}
	return nil
}
