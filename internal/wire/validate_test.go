package wire

import (
	"encoding/binary"
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
)

// These tests are in package wire rather than wire_test because validate is the
// unexported half of the fix and testing it only through Decode would leave the
// two halves indistinguishable: a decoder that happens to reject an input looks
// exactly like a validator that rejected it first.

// TestValidateRejectsALengthPrefixNothingBacks is the core property.
//
// Every entry here declares more than the frame contains. None of them can be
// honest, whatever the message type behind them is, which is why this check can
// live in one place instead of in each decoder.
func TestValidateRejectsALengthPrefixNothingBacks(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
	}{
		{"bin32 declaring 4 GiB", []byte{0xc6, 0xff, 0xff, 0xff, 0xff}},
		{"bin16 declaring 64 KiB", []byte{0xc5, 0xff, 0xff}},
		{"bin8 declaring 255", []byte{0xc4, 0xff}},
		{"str32 declaring 4 GiB", []byte{0xdb, 0xff, 0xff, 0xff, 0xff}},
		{"str16 declaring 64 KiB", []byte{0xda, 0xff, 0xff}},
		{"str8 declaring 255", []byte{0xd9, 0xff}},
		{"fixstr declaring 31", []byte{0xbf}},
		{"array32 declaring 4 G elements", []byte{0xdd, 0xff, 0xff, 0xff, 0xff}},
		{"array16 declaring 65535", []byte{0xdc, 0xff, 0xff}},
		{"fixarray declaring 15", []byte{0x9f}},
		{"map32 declaring 4 G pairs", []byte{0xdf, 0xff, 0xff, 0xff, 0xff}},
		{"fixmap declaring 15", []byte{0x8f}},
		{"ext32 declaring 4 GiB", []byte{0xc9, 0xff, 0xff, 0xff, 0xff, 0x01}},

		// Nested: the outer value is satisfiable and the inner one is not.
		// This is the shape that reached the unguarded path, because the
		// outer header is consumed by the message's own decoder and the
		// inner one lands on a field.
		{"nested bin32", []byte{0x91, 0xc6, 0xff, 0xff, 0xff, 0xff}},
		{"nested array32", []byte{0x91, 0xdd, 0xff, 0xff, 0xff, 0xff}},
		{"twice nested bin32", []byte{0x91, 0x91, 0xc6, 0xff, 0xff, 0xff, 0xff}},

		// Truncated in the length prefix itself.
		{"bin32 with a short prefix", []byte{0xc6, 0xff, 0xff}},
		{"array32 with a short prefix", []byte{0xdd, 0x00}},

		// Truncated in a fixed-width scalar.
		{"uint64 with four bytes", []byte{0xcf, 0x01, 0x02, 0x03, 0x04}},
		{"fixext16 with nothing", []byte{0xd8}},

		{"empty", []byte{}},
		{"reserved 0xc1", []byte{0xc1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validate(tc.in)
			if err == nil {
				t.Fatalf("validate(%x) accepted a frame whose declared sizes it cannot "+
					"contain; the decoder would then allocate from that declaration", tc.in)
			}
			if !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("validate(%x) returned %v, which does not wrap ErrInvalidOption; "+
					"the transport maps that sentinel to InvalidArgument, so an "+
					"unwrapped error is reported to the peer as the wrong kind of "+
					"failure", tc.in, err)
			}
		})
	}
}

// TestValidateAcceptsEveryRealEncoding is the overcorrection guard, and it is
// the reason this validator can sit on the hot path at all.
//
// A validator that rejects a legitimate message is a self-inflicted outage: the
// encoder and decoder in this package are the only ones that speak this
// protocol, so anything Encode produces must survive it.
func TestValidateAcceptsEveryRealEncoding(t *testing.T) {
	messages := []any{
		&GetRequest{Namespace: "n", Key: "k", Epoch: 7},
		&GetResponse{Found: true, Value: []byte("v"), Stamp: Stamp{Epoch: 1, Version: 2}},
		&LoadRequest{Namespace: "n", Key: "k"},
		&LoadResponse{Found: true, Value: make([]byte, 4096)},
		&SetRequest{Namespace: "n", Key: "k", Value: make([]byte, 70000)},
		&SetResponse{Stamp: Stamp{Epoch: 3, Version: 4}},
		&DeleteRequest{Namespace: "n", Key: "k"},
		&DeleteResponse{},
		&BatchGetRequest{Namespace: "n", Keys: []string{"a", "b", "c"}},
		&BatchGetResponse{Entries: []BatchEntry{{Key: "a", Value: []byte("v")}, {Key: "b"}}},
		&CallRequest{Group: "g", FuncName: "f", Args: make([]byte, 1024)},
		&CallResponse{Result: make([]byte, 300000)},
		&ReplicateRequest{Namespace: "n", Key: "k", Value: []byte("v"), From: "n1"},
		&ReplicateResponse{Conflict: true},
		&CatchUpRequest{Namespace: "n", After: "k", Limit: 100},
		&CatchUpResponse{Entries: []CatchUpEntry{{Key: "a", Value: []byte("v")}}, More: true},
		&InvalidateRequest{Namespace: "n", Keys: []string{"a", "b"}},
		&InvalidateResponse{},
		&SubmitRequest{FuncName: "f", Batches: []GroupBatch{
			{Group: "g1", Messages: [][]byte{[]byte("m1"), []byte("m2")}},
			{Group: "g2", Messages: [][]byte{make([]byte, 5000)}},
		}},
		&SubmitResponse{Failures: []GroupFailure{{Group: "g1", Error: "boom"}}},
		&GroupBatch{Group: "g", Messages: [][]byte{[]byte("m")}},
	}

	for _, m := range messages {
		encoded, err := Encode(m)
		if err != nil {
			t.Fatalf("Encode(%T): %v", m, err)
		}
		if err := validate(encoded); err != nil {
			t.Fatalf("validate rejected a message this package encoded: %T: %v", m, err)
		}
	}
}

// TestValidateRejectsUnboundedNesting covers the validator's own failure mode.
//
// The walk is recursive, so a frame that nests as deep as its byte count allows
// would put an attacker in charge of this node's stack. Rejecting past a fixed
// depth turns that into a parse error, and the depth is far above anything the
// protocol's own shapes reach — asserted here rather than assumed.
func TestValidateRejectsUnboundedNesting(t *testing.T) {
	deep := make([]byte, 0, 4096)
	for i := 0; i < 4096; i++ {
		deep = append(deep, 0x91) // fixarray of one
	}
	err := validate(deep)
	if err == nil {
		t.Fatal("validate accepted 4096 levels of nesting; the walk is recursive, so " +
			"an unbounded depth is a remotely triggerable stack overflow")
	}
	if !strings.Contains(err.Error(), "nests deeper") {
		t.Fatalf("validate rejected deep nesting with %v, which does not name the depth "+
			"bound; an operator reading this needs to know which limit fired", err)
	}

	// And the bound is not so tight that it catches the protocol. The deepest
	// real shape is a SubmitRequest: fields, batches, one batch, its messages.
	encoded, err := Encode(&SubmitRequest{FuncName: "f", Batches: []GroupBatch{
		{Group: "g", Messages: [][]byte{[]byte("m")}},
	}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := validate(encoded); err != nil {
		t.Fatalf("the depth bound rejected the protocol's own deepest message: %v", err)
	}
}

// TestValidateAllocatesNothing pins the property that lets this run on every
// received message.
//
// The walk carries no state beyond offsets, so it has no reason to allocate. If
// it ever starts to, the cost of validating moves from "a linear read" to
// "a linear read plus garbage per request", and the honest response would be to
// find out why rather than to accept it quietly.
func TestValidateAllocatesNothing(t *testing.T) {
	encoded, err := Encode(&SubmitRequest{FuncName: "f", Batches: []GroupBatch{
		{Group: "g1", Messages: [][]byte{[]byte("m1"), []byte("m2")}},
		{Group: "g2", Messages: [][]byte{make([]byte, 512)}},
	}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	allocs := testing.AllocsPerRun(200, func() {
		_ = validate(encoded)
	})
	if allocs != 0 {
		t.Fatalf("validate allocated %.1f times per call; it walks offsets and should "+
			"allocate nothing", allocs)
	}
}

// BenchmarkValidate measures what the check costs on the messages the remote
// path actually carries.
//
// The number belongs in docs/benchmarks.md rather than in a comment, because a
// cost recorded next to the code it defends is a cost nobody re-measures.
func BenchmarkValidate(b *testing.B) {
	cases := []struct {
		name string
		msg  any
	}{
		{"GetRequest", &GetRequest{Namespace: "orders", Key: "order/12345", Epoch: 7}},
		{"GetResponse", &GetResponse{Found: true, Value: make([]byte, 256), Stamp: Stamp{Epoch: 1, Version: 9}}},
		{"BatchGetResponse/100", &BatchGetResponse{Entries: make([]BatchEntry, 100)}},
		{"SubmitRequest/10x10", &SubmitRequest{FuncName: "apply", Batches: func() []GroupBatch {
			out := make([]GroupBatch, 10)
			for i := range out {
				out[i] = GroupBatch{Group: "g", Messages: make([][]byte, 10)}
			}
			return out
		}()}},
	}
	for _, tc := range cases {
		encoded, err := Encode(tc.msg)
		if err != nil {
			b.Fatalf("Encode: %v", err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			for i := 0; i < b.N; i++ {
				if err := validate(encoded); err != nil {
					b.Fatalf("validate: %v", err)
				}
			}
		})
	}
}

// minimalValue returns the shortest well-formed msgpack value beginning with the
// type byte c, or nil if no such value exists.
//
// The payloads are zero bytes and the collection elements are positive fixints,
// because the shape is what is under test and not the contents.
func minimalValue(c byte) []byte {
	fill := func(header []byte, n int) []byte {
		return append(header, make([]byte, n)...)
	}
	// values appends n minimal values, each one byte.
	values := func(header []byte, n int) []byte {
		for i := 0; i < n; i++ {
			header = append(header, 0x00)
		}
		return header
	}

	switch {
	case c <= 0x7f, c >= 0xe0: // fixint
		return []byte{c}
	case c&0xe0 == 0xa0: // fixstr
		return fill([]byte{c}, int(c&0x1f))
	case c&0xf0 == 0x90: // fixarray
		return values([]byte{c}, int(c&0x0f))
	case c&0xf0 == 0x80: // fixmap
		return values([]byte{c}, 2*int(c&0x0f))
	}

	switch c {
	case 0xc0, 0xc2, 0xc3: // nil, false, true
		return []byte{c}
	case 0xc1: // reserved, never emitted, never valid
		return nil
	case 0xcc, 0xd0:
		return fill([]byte{c}, 1)
	case 0xcd, 0xd1:
		return fill([]byte{c}, 2)
	case 0xca, 0xce, 0xd2:
		return fill([]byte{c}, 4)
	case 0xcb, 0xcf, 0xd3:
		return fill([]byte{c}, 8)
	case 0xd4: // fixext1: type byte + 1
		return fill([]byte{c}, 1+1)
	case 0xd5:
		return fill([]byte{c}, 1+2)
	case 0xd6:
		return fill([]byte{c}, 1+4)
	case 0xd7:
		return fill([]byte{c}, 1+8)
	case 0xd8:
		return fill([]byte{c}, 1+16)
	case 0xd9, 0xc4: // str8, bin8 — one byte of payload
		return fill([]byte{c, 0x01}, 1)
	case 0xda, 0xc5: // str16, bin16
		return fill([]byte{c, 0x00, 0x01}, 1)
	case 0xdb, 0xc6: // str32, bin32
		return fill([]byte{c, 0x00, 0x00, 0x00, 0x01}, 1)
	case 0xc7: // ext8: length, then a type byte, then the payload
		return fill([]byte{c, 0x01, 0x7f}, 1)
	case 0xc8: // ext16
		return fill([]byte{c, 0x00, 0x01, 0x7f}, 1)
	case 0xc9: // ext32
		return fill([]byte{c, 0x00, 0x00, 0x00, 0x01, 0x7f}, 1)
	case 0xdc: // array16 of two
		return values([]byte{c, 0x00, 0x02}, 2)
	case 0xdd: // array32 of two
		return values([]byte{c, 0x00, 0x00, 0x00, 0x02}, 2)
	case 0xde: // map16 of one pair
		return values([]byte{c, 0x00, 0x01}, 2)
	case 0xdf: // map32 of one pair
		return values([]byte{c, 0x00, 0x00, 0x00, 0x01}, 2)
	}
	return nil
}

// TestValidateHandlesEveryTypeByte walks all 256 leading bytes msgpack defines.
//
// # Why exhaustively rather than by example
//
// The validator's whole job is to be right about *sizes*, and it is right about
// them one type byte at a time: each family has its own arithmetic for how many
// bytes follow. A family whose arithmetic is wrong lets a declared length
// through, which is the defect the file exists to prevent, and it does so
// silently — the wrong size is only visible when something downstream allocates
// from it.
//
// The protocol's own encoder emits perhaps a third of these. A peer can send any
// of them, and a fuzzer will eventually try all of them; testing only the third
// we emit would leave the arithmetic for the rest asserted by nothing. Twelve
// branches were uncovered before this test existed.
//
// Each byte is checked twice: the shortest well-formed value beginning with it
// must be accepted, and that same value one byte short must be rejected. The
// second half is what makes the first meaningful — a validator that accepts
// everything would pass the first check on all 256.
func TestValidateHandlesEveryTypeByte(t *testing.T) {
	for i := 0; i < 256; i++ {
		c := byte(i)
		v := minimalValue(c)

		if v == nil {
			// 0xc1 is the only byte msgpack reserves and never emits.
			if c != 0xc1 {
				t.Fatalf("no minimal value defined for type byte 0x%02x; the table in "+
					"minimalValue has fallen behind the format", c)
			}
			if err := validate([]byte{c}); err == nil {
				t.Fatalf("validate accepted the reserved byte 0x%02x", c)
			}
			continue
		}

		if err := validate(v); err != nil {
			t.Fatalf("validate rejected a well-formed value for type byte 0x%02x (%x): %v",
				c, v, err)
		}

		// One byte short. Every family must notice, because "the frame ends
		// before the value does" is the same question as "is this declared
		// length satisfiable".
		if len(v) > 1 {
			if err := validate(v[:len(v)-1]); err == nil {
				t.Fatalf("validate accepted a truncated value for type byte 0x%02x (%x); "+
					"a length that runs past the end of the frame is exactly what this "+
					"check exists to refuse", c, v[:len(v)-1])
			}
		}
	}
}

// TestEveryTypeByteIsClassified pins that the type switch is total.
//
// `scan` ends with an "unknown msgpack type byte" error that nothing can reach:
// msgpack assigns all 256 leading bytes, and every one of them is handled above
// it. That makes the branch dead code, which is worth keeping — a future format
// extension would land there rather than in a silent misparse — but only if it
// is honest about being unreachable.
//
// This asserts it: no byte, well-formed or truncated, produces that message.
func TestEveryTypeByteIsClassified(t *testing.T) {
	for i := 0; i < 256; i++ {
		c := byte(i)
		for _, in := range [][]byte{{c}, minimalValue(c)} {
			if in == nil {
				continue
			}
			if err := validate(in); err != nil && strings.Contains(err.Error(), "unknown msgpack type byte") {
				t.Fatalf("type byte 0x%02x reached the unreachable default branch; the "+
					"switch above it is not total", c)
			}
		}
	}
}

// TestABoundedDecoderStillAllocatesOnlyForWhatArrives covers the bounded
// decoders now that the frame validator sits in front of them.
//
// # Why this test had to be written
//
// The M8 mutation audit returned VACUOUS for the M4 entry that guarded
// bounded.go: preallocating `make([]BatchEntry, 0, n)` from the declared length
// no longer fails `TestALengthPrefixCannotBuyAnAllocation`, because the inputs
// that test uses are refused by `validate` before any decoder sees them. The
// protection moved; the test guarding the old layer stopped guarding anything.
//
// That is a coverage regression introduced by a fix, which is the same hazard
// recorded against the hand-off's `rf < 2` branch, and it deserved a test rather
// than a note because this one is reachable.
//
// # The input that still reaches the decoder
//
// `validate` guarantees a declared count is no larger than the bytes behind it.
// It does not, and cannot cheaply, guarantee those bytes decode into the element
// type. So an array32 declaring 100,000 elements followed by exactly 100,000
// one-byte values is *structurally* honest, passes validation, and then fails on
// the first element because a fixint is not a [BatchEntry].
//
// Appending as elements arrive costs nothing for that input: the first element
// fails and nothing was ever appended. Sizing the slice from the declared count
// costs 100,000 × sizeof(BatchEntry) — megabytes — before discovering the same
// thing. That difference is the property, and it is what the budget below
// measures.
func TestABoundedDecoderStillAllocatesOnlyForWhatArrives(t *testing.T) {
	const elements = 100_000

	// An outer one-element array (the message envelope BatchGetResponse
	// decodes), then an array32 of `elements`, then that many fixints.
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, elements)
	frame := append([]byte{0x91, 0xdd}, header...)
	frame = append(frame, make([]byte, elements)...) // fixint zero, valid msgpack

	// Every declared length is satisfiable, so the validator must let it past —
	// otherwise this test is measuring the validator again rather than the
	// decoder behind it.
	if err := validate(frame); err != nil {
		t.Fatalf("the validator rejected a structurally honest frame (%v), so this "+
			"test no longer reaches the decoder it is about", err)
	}

	// Comfortably above what appending nothing costs and far below the
	// megabytes a declared-length preallocation needs.
	const budgetBytes = 1 << 20

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	var resp BatchGetResponse
	err := Decode(frame, &resp)

	runtime.ReadMemStats(&after)
	used := after.TotalAlloc - before.TotalAlloc

	if err == nil {
		t.Fatal("a frame of fixints decoded into BatchEntry values; the elements are " +
			"not entries and the decoder must say so")
	}
	if used > budgetBytes {
		t.Fatalf("decoding a frame declaring %d elements allocated %d KiB before "+
			"failing on the first one. The declared count is satisfiable, so the "+
			"validator cannot refuse it — only appending as elements arrive keeps "+
			"the cost proportional to what was delivered",
			elements, used/1024)
	}
	if n := len(resp.Entries); n != 0 {
		t.Fatalf("%d entries survived a decode that failed on element 0", n)
	}
}
