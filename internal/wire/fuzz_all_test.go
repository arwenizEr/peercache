package wire_test

import (
	"testing"

	"github.com/arwenizEr/peercache/internal/wire"
)

// FuzzDecodeEveryMessage is the exhaustive companion to FuzzDecode.
//
// FuzzDecode covers five message types. That was a reasonable sample when it
// was written and it is now a liability: the wire package has twenty-seven, a peer can
// send any of them, and the length-prefix defect — six bytes that wedged the
// process for two minutes by pre-allocating from a declared length — lived in a
// decoder that nothing was fuzzing at the time.
//
// docs/design.md asks that no input from a peer may panic a node. "No input" means
// every message, not a representative subset, so this decodes each arbitrary
// byte string into all twenty-seven and requires none of them to panic, hang, or
// allocate from a length it has not read.
//
// A decoder that errors is behaving correctly. The only acceptable outcomes are
// an error or a value; a panic is a remotely triggerable crash, and an
// allocation proportional to a declared rather than delivered length is a
// remotely triggerable OOM.
func FuzzDecodeEveryMessage(f *testing.F) {
	// Seed with a valid encoding of each type, so the fuzzer starts from
	// structurally plausible inputs rather than having to discover msgpack.
	seeds := []any{
		&wire.GetRequest{Namespace: "n", Key: "k", Epoch: 1},
		&wire.GetResponse{Found: true, Value: []byte("v")},
		&wire.ReplicaGetRequest{Namespace: "n", Key: "k", Epoch: 1},
		&wire.ReplicaGetResponse{Served: true, Value: []byte("v"), AgeNanos: 1},
		&wire.LoadRequest{Namespace: "n", Key: "k"},
		&wire.LoadResponse{Found: true, Value: []byte("v")},
		&wire.SetRequest{Namespace: "n", Key: "k", Value: []byte("v")},
		&wire.SetResponse{},
		&wire.DeleteRequest{Namespace: "n", Key: "k"},
		&wire.DeleteResponse{},
		&wire.CondSetRequest{Namespace: "n", Key: "k", Value: []byte("v"), Compare: true, Old: []byte("o")},
		&wire.CondSetResponse{Stored: true},
		&wire.IncrementRequest{Namespace: "n", Key: "k", Delta: 1},
		&wire.IncrementResponse{Value: 1},
		&wire.TouchRequest{Namespace: "n", Key: "k", TTLNanos: 1},
		&wire.TouchResponse{Existed: true},
		&wire.BatchGetRequest{Namespace: "n", Keys: []string{"a", "b"}},
		&wire.BatchGetResponse{Entries: []wire.BatchEntry{{Key: "a", Value: []byte("v")}}},
		&wire.CallRequest{Group: "g", FuncName: "f", Args: []byte{1}},
		&wire.CallResponse{Result: []byte("r")},
		&wire.ReplicateRequest{Namespace: "n", Key: "k", Value: []byte("v")},
		&wire.ReplicateResponse{},
		&wire.CatchUpRequest{Namespace: "n"},
		&wire.CatchUpResponse{},
		&wire.InvalidateRequest{Namespace: "n", Keys: []string{"a"}},
		&wire.InvalidateResponse{},
		// The submit family was missing from this list while the fuzz body
		// below already decoded into two of the three, which is the same shape
		// of gap this target exists to close: a sample that has quietly fallen
		// behind the thing it samples. GroupBatch was the third, seeded here
		// and decoded nowhere until someone noticed the counts disagreed.
		&wire.SubmitRequest{FuncName: "f", Batches: []wire.GroupBatch{
			{Group: "g", Messages: [][]byte{[]byte("m")}},
		}},
		&wire.SubmitResponse{Failures: []wire.GroupFailure{{Group: "g", Error: "boom"}}},
		&wire.GroupBatch{Group: "g", Messages: [][]byte{[]byte("m")}},
	}
	for _, s := range seeds {
		if b, err := wire.Encode(s); err == nil {
			f.Add(b)
		}
	}

	// Shapes that have historically broken decoders: nothing, a reserved
	// byte, a declared-huge array, a declared-huge map, a declared-huge
	// string. The last three are the length-prefix family.
	f.Add([]byte{})
	f.Add([]byte{0xc1})
	f.Add([]byte{0xdd, 0xff, 0xff, 0xff, 0xff}) // array32, 4294967295 elements
	f.Add([]byte{0xdf, 0xff, 0xff, 0xff, 0xff}) // map32, 4294967295 pairs
	f.Add([]byte{0xdb, 0xff, 0xff, 0xff, 0xff}) // str32, 4294967295 bytes
	f.Add([]byte{0xc6, 0xff, 0xff, 0xff, 0xff}) // bin32, 4294967295 bytes

	// The two that actually found defects, kept as permanent seeds. Both are
	// nested one level, which is what put the hostile header on a *field*
	// rather than on the message envelope: the outer array satisfies the
	// message's own decoder, and the inner one is read by whatever decodes
	// field zero.
	//
	// The first reached SubmitResponse.Failures, the only slice of structs with
	// no bounded decoder: 4,294,967,295 GroupFailure values, 137 GB. The second
	// reached CallResponse.Result and every other []byte field in the protocol,
	// through a msgpack path that bypasses the library's own allocation limit:
	// 4 GiB from six bytes.
	f.Add([]byte{0x91, 0xdd, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x91, 0xc6, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Each into its own value. A byte string harmless for one shape may
		// not be for another, which is the reason this is exhaustive.
		var (
			getReq   wire.GetRequest
			getResp  wire.GetResponse
			rgReq    wire.ReplicaGetRequest
			rgResp   wire.ReplicaGetResponse
			loadReq  wire.LoadRequest
			loadResp wire.LoadResponse
			setReq   wire.SetRequest
			setResp  wire.SetResponse
			delReq   wire.DeleteRequest
			delResp  wire.DeleteResponse
			csReq    wire.CondSetRequest
			csResp   wire.CondSetResponse
			incReq   wire.IncrementRequest
			incResp  wire.IncrementResponse
			touReq   wire.TouchRequest
			touResp  wire.TouchResponse
			bgReq    wire.BatchGetRequest
			bgResp   wire.BatchGetResponse
			callReq  wire.CallRequest
			callResp wire.CallResponse
			replReq  wire.ReplicateRequest
			replResp wire.ReplicateResponse
			cuReq    wire.CatchUpRequest
			cuResp   wire.CatchUpResponse
			invReq   wire.InvalidateRequest
			invResp  wire.InvalidateResponse
			subReq   wire.SubmitRequest
			subResp  wire.SubmitResponse
			// GroupBatch is a field of SubmitRequest rather than an RPC
			// payload, and it was seeded here without ever being decoded
			// into -- a one-type gap in the target whose whole premise is
			// that a sample is not enough. It has its own custom decoder,
			// so it is its own way to get this wrong.
			batch wire.GroupBatch
		)
		_ = wire.Decode(data, &getReq)
		_ = wire.Decode(data, &getResp)
		_ = wire.Decode(data, &rgReq)
		_ = wire.Decode(data, &rgResp)
		_ = wire.Decode(data, &loadReq)
		_ = wire.Decode(data, &loadResp)
		_ = wire.Decode(data, &setReq)
		_ = wire.Decode(data, &setResp)
		_ = wire.Decode(data, &delReq)
		_ = wire.Decode(data, &delResp)
		_ = wire.Decode(data, &csReq)
		_ = wire.Decode(data, &csResp)
		_ = wire.Decode(data, &incReq)
		_ = wire.Decode(data, &incResp)
		_ = wire.Decode(data, &touReq)
		_ = wire.Decode(data, &touResp)
		_ = wire.Decode(data, &bgReq)
		_ = wire.Decode(data, &bgResp)
		_ = wire.Decode(data, &callReq)
		_ = wire.Decode(data, &callResp)
		_ = wire.Decode(data, &replReq)
		_ = wire.Decode(data, &replResp)
		_ = wire.Decode(data, &cuReq)
		_ = wire.Decode(data, &cuResp)
		_ = wire.Decode(data, &invReq)
		_ = wire.Decode(data, &invResp)
		_ = wire.Decode(data, &subReq)
		_ = wire.Decode(data, &subResp)
		_ = wire.Decode(data, &batch)

		// The length-prefix invariant, applied to every variable-length
		// field. A decoded collection cannot legitimately contain more
		// elements than the input had bytes: each element costs at least
		// one byte to encode. Anything larger means the decoder sized an
		// allocation from a number it read rather than from data it
		// received, which is the length-prefix defect and a remote OOM.
		limit := len(data) + 1
		for name, n := range map[string]int{
			"BatchGetRequest.Keys":     len(bgReq.Keys),
			"BatchGetResponse.Entries": len(bgResp.Entries),
			"InvalidateRequest.Keys":   len(invReq.Keys),
			"CatchUpResponse.Entries":  len(cuResp.Entries),
			"SubmitRequest.Batches":    len(subReq.Batches),
			"SubmitResponse.Failures":  len(subResp.Failures),
			"GroupBatch.Messages":      len(batch.Messages),
		} {
			if n > limit {
				t.Fatalf("%s decoded %d elements from %d bytes; the decoder sized "+
					"an allocation from a declared length rather than the "+
					"delivered one", name, n, len(data))
			}
		}
	})
}
