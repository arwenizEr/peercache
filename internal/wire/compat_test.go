package wire_test

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/arwenizEr/peercache/internal/wire"
)

// earlierBatchGetRequest is BatchGetRequest as the release before Load
// encoded and decoded it: three fields, the decoder reading exactly three.
type earlierBatchGetRequest struct {
	Namespace string
	Keys      []string
	Epoch     uint64
}

func (r earlierBatchGetRequest) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := enc.EncodeString(r.Namespace); err != nil {
		return err
	}
	if err := enc.Encode(r.Keys); err != nil {
		return err
	}
	return enc.EncodeUint(r.Epoch)
}

func (r *earlierBatchGetRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if r.Namespace, err = dec.DecodeString(); err != nil {
		return err
	}
	if err = dec.Decode(&r.Keys); err != nil {
		return err
	}
	r.Epoch, err = dec.DecodeUint64()
	return err
}

// TestABatchLoadRequestCrossesARollingUpgrade: during a rolling upgrade a
// BatchGetOrLoad request reaches a peer on the release before Load, and a
// BatchGet from such a peer reaches this one. Each side must read the other —
// the earlier peer answering what it holds, as BatchGet does
// (docs/decisions/0035-batch-loads.md) — rather than fail the batch.
func TestABatchLoadRequestCrossesARollingUpgrade(t *testing.T) {
	data, err := wire.Encode(&wire.BatchGetRequest{Namespace: "n", Keys: []string{"a", "b"}, Epoch: 7, Load: true})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var old earlierBatchGetRequest
	if err := msgpack.Unmarshal(data, &old); err != nil {
		t.Fatalf("the earlier release cannot read a load request: %v", err)
	}
	if old.Namespace != "n" || len(old.Keys) != 2 || old.Epoch != 7 {
		t.Fatalf("the earlier release read %+v", old)
	}

	data, err = msgpack.Marshal(earlierBatchGetRequest{Namespace: "n", Keys: []string{"a"}, Epoch: 3})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var now wire.BatchGetRequest
	if err := wire.Decode(data, &now); err != nil {
		t.Fatalf("this release cannot read the earlier one's batch get: %v", err)
	}
	if now.Namespace != "n" || len(now.Keys) != 1 || now.Epoch != 3 || now.Load {
		t.Fatalf("this release read %+v from an earlier batch get", now)
	}
}

// earlierReplicateRequest is ReplicateRequest as v1.1 encoded and decoded it,
// before Durable.
type earlierReplicateRequest struct {
	Namespace string     `msgpack:"namespace"`
	Key       string     `msgpack:"key"`
	Value     []byte     `msgpack:"value"`
	TTLNanos  int64      `msgpack:"ttl_ns"`
	Stamp     wire.Stamp `msgpack:"stamp"`
	From      string     `msgpack:"from"`
	Deleted   bool       `msgpack:"deleted"`
	Order     uint64     `msgpack:"order"`
	Prev      uint64     `msgpack:"prev"`
}

// TestAReplicateRequestsDurableBoundCrossesARollingUpgrade: a copy carrying
// its writer's durable bound reaches a peer on the release before it, which
// must read the copy and ignore the bound; a copy from such a peer arrives
// with no bound, which reads as zero, "says nothing" (open question 68).
func TestAReplicateRequestsDurableBoundCrossesARollingUpgrade(t *testing.T) {
	data, err := wire.Encode(&wire.ReplicateRequest{Namespace: "n", Key: "k", Value: []byte("v"),
		From: "a", Order: 9, Durable: 7})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var old earlierReplicateRequest
	if err := wire.Decode(data, &old); err != nil {
		t.Fatalf("the earlier release cannot read a copy with a bound: %v", err)
	}
	if old.Key != "k" || old.Order != 9 || string(old.Value) != "v" {
		t.Fatalf("the earlier release read %+v", old)
	}

	data, err = wire.Encode(&earlierReplicateRequest{Namespace: "n", Key: "k", Value: []byte("v"), From: "a", Order: 9})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var now wire.ReplicateRequest
	if err := wire.Decode(data, &now); err != nil {
		t.Fatalf("this release cannot read the earlier one's copy: %v", err)
	}
	if now.Durable != 0 || now.Order != 9 {
		t.Fatalf("read %+v, want Order 9 and no bound", now)
	}
}
