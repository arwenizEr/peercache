package wire

import (
	"fmt"

	"github.com/arwenizEr/peercache/internal/core"
)

// MaxNestingDepth caps how deeply a message may nest.
//
// The protocol's deepest shape is a [SubmitRequest]: an array of fields holding
// an array of [GroupBatch], each holding an array of byte strings — four levels.
// Sixteen is far beyond anything the encoders here produce and far below
// anything that costs the validator real work, which is the whole range a bound
// like this needs to sit in.
//
// It exists because the validator is recursive, and a recursive function walking
// attacker-supplied structure without a depth bound is a stack overflow with a
// network trigger. That would be a panic, which CLAUDE.md forbids in library
// paths, and one no `recover` could usefully catch.
//
// It bounds the error message too, which is not incidental. Each level wraps the
// one below it, so the rejection for a frame of 4096 nested arrays is 4096 nested
// error strings — 88 KB, measured, from seven bytes of input, and headed for
// somebody's log. Sixteen levels of wrapping is a sentence.
const MaxNestingDepth = 16

// validate checks that a message's declared lengths are satisfiable by the bytes
// that actually arrived.
//
// # Why this exists on top of the bounded decoders
//
// bounded.go handles the collection types by appending as they go, so a declared
// length costs nothing until elements arrive. That was believed to cover the
// hazard. It does not, and the gap is in the library rather than in this
// package:
//
// vmihailenco/msgpack v5 guards allocations behind a `disableAllocLimitFlag`
// check in Decoder.readN, which caps growth at 1 MiB per step. Its byte-string
// paths — `Decoder.bytes` and `Decoder.bytesPtr`, which every `[]byte` struct
// field goes through — do not call that method. They call the package-level
// `readN` directly, whose first act is:
//
//	b = append(b, make([]byte, n-len(b))...)
//
// with n taken from the wire. Five bytes, `c6 ff ff ff ff`, declare a 4 GiB byte
// string and get a 4 GiB allocation before a single byte of payload is read.
// Every message in this package with a `[]byte` field is reachable that way:
// values, call arguments, call results, replicated payloads, catch-up entries.
//
// Writing a bounded decoder for each of them would work and would need eight
// more encoder/decoder pairs kept in exact agreement by hand, with a new one
// required every time a message gains a byte-string field — a rule enforced by
// remembering it. Validating the frame once, before the decoder sees it, covers
// every existing message and every future one in a single place.
//
// # What it guarantees
//
// After validate returns nil, every length prefix in the message is no larger
// than the bytes remaining behind it. A decoder can then allocate from a
// declared length as much as it likes: the declaration is bounded by the frame,
// and the frame is bounded by the transport. That is the property that turns a
// remotely triggerable out-of-memory into a parse error.
//
// It deliberately does not check that the message *means* anything — types,
// field names and required fields are the decoder's business. This answers one
// question only: can this message's claims about its own size be true?
//
// # What it costs
//
// One linear pass over the frame, no allocation: 25 ns for a GetRequest, 51 ns
// for a 256-byte GetResponse, 4.1 µs for a hundred-entry BatchGetResponse —
// about 9% of what decoding the same message costs, and three orders of
// magnitude below the 175 µs remote round trip it defends. BenchmarkValidate
// measures it; docs/benchmarks.md records it.
func validate(data []byte) error {
	off, err := scan(data, 0, 0)
	if err != nil {
		return err
	}
	// Trailing bytes are not an error. The decoder reads one value and stops,
	// and a stricter rule here would reject frames a future version of the
	// protocol might legitimately extend.
	_ = off
	return nil
}

// scan validates one msgpack value starting at off and returns the offset just
// past it.
func scan(data []byte, off, depth int) (int, error) {
	if depth > MaxNestingDepth {
		return 0, fmt.Errorf("wire: message nests deeper than %d levels: %w",
			MaxNestingDepth, core.ErrInvalidOption)
	}
	if off >= len(data) {
		return 0, fmt.Errorf("wire: message ends where a value was expected: %w",
			core.ErrInvalidOption)
	}

	c := data[off]
	off++

	// The four inline-length families first, and by high bits rather than by
	// range comparisons. Between them they cover almost every byte this
	// protocol actually emits — small integers, short strings, and the field
	// arrays every message is wrapped in — so putting them behind the
	// twenty-odd equality tests below would pay for the rare shapes on every
	// common one. BenchmarkValidate measures what is left: 25 ns for a
	// GetRequest, 51 ns for a 256-byte GetResponse, zero allocations.
	switch {
	case c <= 0x7f, c >= 0xe0: // positive and negative fixint
		return off, nil
	case c&0xe0 == 0xa0: // fixstr, 0xa0-0xbf
		return skip(data, off, int(c&0x1f))
	case c&0xf0 == 0x90: // fixarray, 0x90-0x9f
		return scanN(data, off, depth, int(c&0x0f))
	case c&0xf0 == 0x80: // fixmap, 0x80-0x8f
		return scanN(data, off, depth, 2*int(c&0x0f))
	}

	switch {
	case c == 0xc0, c == 0xc2, c == 0xc3: // nil, false, true
		return off, nil

	// The one byte the format reserves and never emits. Rejecting it by name
	// costs nothing and makes a corrupt frame report what it is.
	case c == 0xc1:
		return 0, fmt.Errorf("wire: reserved byte 0xc1 in message: %w", core.ErrInvalidOption)

	// Fixed-width scalars with a payload of known size.
	case c == 0xcc, c == 0xd0: // uint8, int8
		return skip(data, off, 1)
	case c == 0xcd, c == 0xd1: // uint16, int16
		return skip(data, off, 2)
	case c == 0xca, c == 0xce, c == 0xd2: // float32, uint32, int32
		return skip(data, off, 4)
	case c == 0xcb, c == 0xcf, c == 0xd3: // float64, uint64, int64
		return skip(data, off, 8)

	// Fixed-size extensions: one type byte, then the payload.
	case c == 0xd4:
		return skip(data, off, 1+1)
	case c == 0xd5:
		return skip(data, off, 1+2)
	case c == 0xd6:
		return skip(data, off, 1+4)
	case c == 0xd7:
		return skip(data, off, 1+8)
	case c == 0xd8:
		return skip(data, off, 1+16)

	// Length-prefixed byte strings. This family is the reason the file exists.
	case c == 0xd9, c == 0xc4: // str8, bin8
		return skipPrefixed(data, off, 1)
	case c == 0xda, c == 0xc5: // str16, bin16
		return skipPrefixed(data, off, 2)
	case c == 0xdb, c == 0xc6: // str32, bin32
		return skipPrefixed(data, off, 4)

	// Length-prefixed extensions: the length covers the payload, and the type
	// byte sits between the length and it.
	case c == 0xc7: // ext8
		return skipPrefixed(data, off, 1, 1)
	case c == 0xc8: // ext16
		return skipPrefixed(data, off, 2, 1)
	case c == 0xc9: // ext32
		return skipPrefixed(data, off, 4, 1)

	// Length-prefixed collections. The count is a promise about how many
	// values follow, and every value costs at least one byte, so a count
	// larger than the bytes remaining cannot be honoured whatever those
	// values turn out to be.
	case c == 0xdc: // array16
		return scanCounted(data, off, depth, 2, 1)
	case c == 0xdd: // array32
		return scanCounted(data, off, depth, 4, 1)
	case c == 0xde: // map16
		return scanCounted(data, off, depth, 2, 2)
	case c == 0xdf: // map32
		return scanCounted(data, off, depth, 4, 2)
	}

	// Unreachable, and deliberately kept.
	//
	// msgpack assigns all 256 leading bytes and every one is handled above, so
	// nothing can arrive here — `TestEveryTypeByteIsClassified` asserts exactly
	// that, by walking all 256 and requiring none of them to produce this
	// message. It stays because a future extension to the format should land on
	// a named refusal rather than on whichever branch happens to be nearest,
	// and because a total switch that says so is easier to review than one the
	// reader has to enumerate by hand.
	return 0, fmt.Errorf("wire: unknown msgpack type byte 0x%02x at offset %d: %w",
		c, off-1, core.ErrInvalidOption)
}

// skip advances past n payload bytes, refusing to run off the end.
func skip(data []byte, off, n int) (int, error) {
	if n > len(data)-off {
		return 0, fmt.Errorf("wire: value declares %d bytes with %d remaining: %w",
			n, len(data)-off, core.ErrInvalidOption)
	}
	return off + n, nil
}

// skipPrefixed reads a big-endian length of lenBytes and skips that many bytes,
// plus any fixed extra that sits between the length and the payload.
func skipPrefixed(data []byte, off, lenBytes int, extra ...int) (int, error) {
	n, off, err := readLen(data, off, lenBytes)
	if err != nil {
		return 0, err
	}
	for _, e := range extra {
		n += e
	}
	return skip(data, off, n)
}

// scanCounted reads a big-endian element count and validates that many values.
//
// perElement is 1 for arrays and 2 for maps, because a map's count is pairs.
func scanCounted(data []byte, off, depth, lenBytes, perElement int) (int, error) {
	n, off, err := readLen(data, off, lenBytes)
	if err != nil {
		return 0, err
	}
	// Multiplication before the bound check would overflow on a hostile
	// count; the check is therefore made against the unmultiplied value
	// first, using the same "at least one byte each" reasoning.
	//
	// This bound is a fast path, not a correctness guard, and saying so
	// matters. Removing it entirely changes no outcome: scanN calls scan for
	// each element and the first one runs out of bytes, so the frame is
	// rejected either way. A mutation audit proved that — deleting the check
	// left the whole package passing — which is the same result replicate.go
	// records for its own `rf < 2` branch.
	//
	// It stays for two reasons. It turns a loop over a declared four billion
	// into one comparison when the elements are absent, and it names the
	// problem in the error instead of reporting a truncation several levels
	// down. Neither is correctness; both are worth one comparison.
	if n > len(data)-off {
		return 0, fmt.Errorf("wire: collection declares %d elements with %d bytes "+
			"remaining; each element costs at least one byte: %w",
			n, len(data)-off, core.ErrInvalidOption)
	}
	return scanN(data, off, depth, n*perElement)
}

// scanN validates n consecutive values.
func scanN(data []byte, off, depth, n int) (int, error) {
	var err error
	for i := 0; i < n; i++ {
		if off, err = scan(data, off, depth+1); err != nil {
			return 0, fmt.Errorf("wire: element %d of %d: %w", i, n, err)
		}
	}
	return off, nil
}

// readLen reads a big-endian unsigned length of lenBytes.
//
// It returns an int, and the widest length msgpack can express is 2^32-1, which
// fits. On a 32-bit platform it would not — so the value is assembled in a
// uint64 and rejected if it does not fit, rather than silently wrapping into a
// negative length that every bound check below would then pass.
//
// The overflow branch is therefore dead on every platform this is tested on and
// live on the one it protects, which is why it shows as uncovered and has no
// test. Deleting it would be safe today and wrong the first time someone builds
// for GOARCH=386.
func readLen(data []byte, off, lenBytes int) (int, int, error) {
	if lenBytes > len(data)-off {
		return 0, 0, fmt.Errorf("wire: message ends inside a %d-byte length prefix: %w",
			lenBytes, core.ErrInvalidOption)
	}
	var n uint64
	for i := 0; i < lenBytes; i++ {
		n = n<<8 | uint64(data[off+i])
	}
	off += lenBytes
	if n > uint64(maxInt) {
		return 0, 0, fmt.Errorf("wire: length %d does not fit in an int on this platform: %w",
			n, core.ErrInvalidOption)
	}
	return int(n), off, nil
}

// maxInt is the largest value an int holds on this platform.
const maxInt = int(^uint(0) >> 1)
