package wire

import (
	"fmt"

	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
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
// Version alone is not a cluster-wide monotonic counter: a key's version
// restarts at 1 when its store entry is reclaimed, so a version can decrease
// over a key's lifetime (docs/decisions/open.md question 18). Comparison is
// therefore lexicographic on (Epoch, Version), and even that is only an
// optimisation — a non-owned copy's TTL is the real staleness bound.
type Stamp struct {
	Epoch   uint64 `msgpack:"epoch"`
	Version uint64 `msgpack:"version"`
}

// NewerThan reports whether s supersedes other.
//
// Epoch dominates: a stamp issued under a later membership view wins whatever
// the versions say, because the version sequences either side of an ownership
// change are unrelated. Within one epoch, the version orders writes by the same
// owner, which is the only situation where it means anything.
func (s Stamp) NewerThan(other Stamp) bool {
	if s.Epoch != other.Epoch {
		return s.Epoch > other.Epoch
	}
	return s.Version > other.Version
}

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
}

// BatchGetResponse carries whatever the owner had.
//
// Entries is sparse: a key absent on the owner has no entry. Callers must match
// by key rather than by index, because the response is not aligned with the
// request and a future owner is free to reorder.
type BatchGetResponse struct {
	Entries []BatchEntry `msgpack:"entries"`
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
	// cross the network (CLAUDE.md non-negotiable 3), so this is a name
	// and a byte payload rather than anything executable.
	FuncName string `msgpack:"func_name"`
	Args     []byte `msgpack:"args"`
	Epoch    uint64 `msgpack:"epoch"`
}

// CallResponse carries the function's encoded result.
type CallResponse struct {
	Result []byte `msgpack:"result"`
}

// InvalidateRequest tells a peer that keys it may be caching have changed.
//
// It is a latency optimisation and nothing else. CLAUDE.md non-negotiable 6
// requires that correctness never depend on one of these arriving, which is why
// non-owned entries are TTL-bounded independently of it.
type InvalidateRequest struct {
	Namespace string   `msgpack:"namespace"`
	Keys      []string `msgpack:"keys"`
	// Stamp is the owner's stamp after the change. A receiver drops a
	// cached copy only if what it holds is older, so an invalidation that
	// arrives late cannot evict a value that is newer than it.
	Stamp Stamp `msgpack:"stamp"`
}

// InvalidateResponse is empty.
type InvalidateResponse struct{}

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
func Decode(data []byte, v any) error {
	c, err := codec.Get(Encoding)
	if err != nil {
		return fmt.Errorf("wire: %w", err)
	}
	if err := c.Unmarshal(data, v); err != nil {
		return fmt.Errorf("wire: decoding %T: %w: %w", v, err, core.ErrInvalidOption)
	}
	return nil
}
