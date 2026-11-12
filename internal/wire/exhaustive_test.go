package wire_test

import (
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache/internal/wire"
)

// everyMessage returns a fresh zero value of each message type the protocol can
// receive, together with the length of whatever variable-length field it has.
//
// The length accessor is the point of the table. A decoder that sizes an
// allocation from a declared length is invisible from the outside — it returns
// an error either way — so the assertion has to be that the *elements* were
// never conjured, and that needs a per-type accessor.
//
// Types with no variable-length field return zero. They are still decoded,
// because "no input from a peer may panic a node" covers them too, and a nested
// field can be reached through a message that has no slice of its own.
func everyMessage() []struct {
	name string
	make func() (any, func() int)
} {
	return []struct {
		name string
		make func() (any, func() int)
	}{
		{"GetRequest", func() (any, func() int) { v := new(wire.GetRequest); return v, func() int { return 0 } }},
		{"GetResponse", func() (any, func() int) { v := new(wire.GetResponse); return v, func() int { return 0 } }},
		{"LoadRequest", func() (any, func() int) { v := new(wire.LoadRequest); return v, func() int { return 0 } }},
		{"LoadResponse", func() (any, func() int) { v := new(wire.LoadResponse); return v, func() int { return 0 } }},
		{"SetRequest", func() (any, func() int) { v := new(wire.SetRequest); return v, func() int { return 0 } }},
		{"SetResponse", func() (any, func() int) { v := new(wire.SetResponse); return v, func() int { return 0 } }},
		{"DeleteRequest", func() (any, func() int) { v := new(wire.DeleteRequest); return v, func() int { return 0 } }},
		{"DeleteResponse", func() (any, func() int) { v := new(wire.DeleteResponse); return v, func() int { return 0 } }},
		{"CallRequest", func() (any, func() int) { v := new(wire.CallRequest); return v, func() int { return 0 } }},
		{"CallResponse", func() (any, func() int) { v := new(wire.CallResponse); return v, func() int { return 0 } }},
		{"ReplicateRequest", func() (any, func() int) { v := new(wire.ReplicateRequest); return v, func() int { return 0 } }},
		{"ReplicateResponse", func() (any, func() int) { v := new(wire.ReplicateResponse); return v, func() int { return 0 } }},
		{"CatchUpRequest", func() (any, func() int) { v := new(wire.CatchUpRequest); return v, func() int { return 0 } }},
		{"InvalidateResponse", func() (any, func() int) { v := new(wire.InvalidateResponse); return v, func() int { return 0 } }},

		// The ones with a variable-length field. Each of these is a place a
		// declared length could have bought an allocation.
		{"BatchGetRequest", func() (any, func() int) {
			v := new(wire.BatchGetRequest)
			return v, func() int { return len(v.Keys) }
		}},
		{"BatchGetResponse", func() (any, func() int) {
			v := new(wire.BatchGetResponse)
			return v, func() int { return len(v.Entries) }
		}},
		{"InvalidateRequest", func() (any, func() int) {
			v := new(wire.InvalidateRequest)
			return v, func() int { return len(v.Keys) }
		}},
		{"CatchUpResponse", func() (any, func() int) {
			v := new(wire.CatchUpResponse)
			return v, func() int { return len(v.Entries) }
		}},
		{"SubmitRequest", func() (any, func() int) {
			v := new(wire.SubmitRequest)
			return v, func() int { return len(v.Batches) }
		}},
		{"SubmitResponse", func() (any, func() int) {
			v := new(wire.SubmitResponse)
			return v, func() int { return len(v.Failures) }
		}},
		{"GroupBatch", func() (any, func() int) {
			v := new(wire.GroupBatch)
			return v, func() int { return len(v.Messages) }
		}},
	}
}

// hostileInputs are the length-prefix family, in the shapes that have actually
// broken this package.
//
// The nested form is the one that mattered. A bare array32 header is the first
// thing anyone tries and every decoder here already rejected it; wrapping it in
// a one-element array so it lands on a *field* rather than on the message
// envelope is what reached the unguarded path — the outer header satisfies the
// message's own DecodeArrayLen, and the inner one is then read by whatever
// decodes field zero.
var hostileInputs = []struct {
	name string
	in   []byte
}{
	{"bare array32 of 2^32-1", []byte{0xdd, 0xff, 0xff, 0xff, 0xff}},
	{"bare array32 of 2^31-1", []byte{0xdd, 0x7f, 0xff, 0xff, 0xff}},
	{"bare map32 of 2^32-1", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
	{"bare str32 of 2^32-1", []byte{0xdb, 0xff, 0xff, 0xff, 0xff}},
	{"bare bin32 of 2^32-1", []byte{0xc6, 0xff, 0xff, 0xff, 0xff}},
	{"nested array32 of 2^32-1", []byte{0x91, 0xdd, 0xff, 0xff, 0xff, 0xff}},
	{"nested array32 of 2^31-1", []byte{0x91, 0xdd, 0x7f, 0xff, 0xff, 0xff}},
	{"nested array32 of 2^24", []byte{0x91, 0xdd, 0x01, 0x00, 0x00, 0x00}},
	{"nested map32 of 2^32-1", []byte{0x91, 0xdf, 0xff, 0xff, 0xff, 0xff}},
	{"nested bin32 of 2^32-1", []byte{0x91, 0xc6, 0xff, 0xff, 0xff, 0xff}},
	{"twice nested array32", []byte{0x91, 0x91, 0xdd, 0xff, 0xff, 0xff, 0xff}},
	{"reserved byte", []byte{0xc1}},
	{"empty", []byte{}},
}

// TestNoMessageAllocatesFromADeclaredLength is the exhaustive form of
// TestALengthPrefixCannotBuyAnAllocation, and it exists because the
// representative form was not enough.
//
// The first hardening pass fixed the decoders it knew about and fuzzed five
// message types as a sample. [wire.SubmitResponse] was not one of the five, and
// it was the one message with a slice of structs and no custom decoder — so the
// defect the pass was written to eliminate survived it in the single place
// nobody looked.
//
// Six bytes reproduced it: 91 dd ff ff ff ff decoded into a SubmitResponse asks
// the allocator for 4,294,967,295 GroupFailure values, 137 GB, before reading a
// single element. Under the race detector the process aborts; without it the
// machine swaps.
//
// This test therefore takes the opposite approach to sampling: every message
// type, every hostile shape, one budget. Adding a message type without adding it
// to [everyMessage] is caught by TestProtoDeclaresEveryMessage, so the table
// cannot silently fall behind the protocol.
func TestNoMessageAllocatesFromADeclaredLength(t *testing.T) {
	// Generous by three orders of magnitude against any real decode of these
	// inputs, and eight orders below the defect. A budget that has to be
	// tuned is a budget that will be tuned until it passes.
	const budgetBytes = 1 << 20

	for _, msg := range everyMessage() {
		for _, hostile := range hostileInputs {
			t.Run(msg.name+"/"+hostile.name, func(t *testing.T) {
				target, count := msg.make()

				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)

				// The return value is deliberately ignored. An error is
				// the correct outcome and so is a zero value; what is
				// never correct is having allocated for elements that
				// were not delivered.
				_ = wire.Decode(hostile.in, target)

				runtime.ReadMemStats(&after)
				used := after.TotalAlloc - before.TotalAlloc

				if used > budgetBytes {
					t.Fatalf("decoding %d bytes into %s allocated %d KiB; a length prefix "+
						"must not buy an allocation, or a peer can exhaust this node's "+
						"memory with a handful of bytes",
						len(hostile.in), msg.name, used/1024)
				}
				if n := count(); n > len(hostile.in)+1 {
					t.Fatalf("%s decoded %d elements from %d bytes; the decoder sized an "+
						"allocation from a declared length rather than a delivered one",
						msg.name, n, len(hostile.in))
				}
			})
		}
	}
}

// TestSubmitResponseStillRoundTripsAfterBounding is the overcorrection guard for
// the decoder added above: refusing everything is also safe, and useless.
//
// It is separate from TestEveryMessageRoundTrips because that test predates the
// custom encoder pair, and a round-trip that passed before the pair existed
// proves nothing about the pair. This one is written against the bounded path
// specifically, at a size large enough that a per-element append is doing real
// work.
func TestSubmitResponseStillRoundTripsAfterBounding(t *testing.T) {
	failures := make([]wire.GroupFailure, 0, 2000)
	for i := 0; i < 2000; i++ {
		failures = append(failures, wire.GroupFailure{
			Group: "group/" + string(rune('a'+i%26)),
			Error: "peercache: group " + string(rune('0'+i%10)) + " did not apply",
		})
	}
	sent := &wire.SubmitResponse{Failures: failures}

	encoded, err := wire.Encode(sent)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got wire.SubmitResponse
	if err := wire.Decode(encoded, &got); err != nil {
		t.Fatalf("Decode of a legitimate 2000-failure response: %v", err)
	}
	if len(got.Failures) != len(failures) {
		t.Fatalf("decoded %d failures, want %d", len(got.Failures), len(failures))
	}
	for i := range failures {
		if got.Failures[i] != failures[i] {
			t.Fatalf("failure %d round-tripped as %+v, want %+v", i, got.Failures[i], failures[i])
		}
	}

	// Empty is the common case — most submits apply cleanly — and a decoder
	// that turns "no failures" into a non-nil empty slice would make callers
	// that test `len(...) == 0` and callers that test `!= nil` disagree.
	roundTripped := wire.SubmitResponse{}
	encoded, err = wire.Encode(&wire.SubmitResponse{})
	if err != nil {
		t.Fatalf("Encode(empty): %v", err)
	}
	if err := wire.Decode(encoded, &roundTripped); err != nil {
		t.Fatalf("Decode(empty): %v", err)
	}
	if roundTripped.Failures != nil {
		t.Fatalf("an empty SubmitResponse decoded to %v, want nil", roundTripped.Failures)
	}
}
