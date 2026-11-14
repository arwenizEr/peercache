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
