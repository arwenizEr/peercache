package codec_test

import (
	"bytes"
	"math"
	"testing"
	"unicode/utf8"

	"github.com/arwenizEr/peercache/internal/codec"
)

// The frame parser is the only place in this library that interprets bytes it
// did not write. They come from its own store and over the network, and
// library paths never panic. Six hand-written
// corrupt frames are not evidence of that; a fuzzer is.
//
// Run longer with:
//
//	go test ./internal/codec -run '^$' -fuzz FuzzUnframe -fuzztime 60s

// FuzzUnframe asserts that no input, however malformed, can panic the parser or
// make it allocate without bound. Any error is acceptable; a crash is not.
func FuzzUnframe(f *testing.F) {
	framer, err := codec.NewFramer(codec.NameLZ4, 16)
	if err != nil {
		f.Fatalf("NewFramer: %v", err)
	}

	// Seed with well-formed frames and with the corrupt shapes already known
	// to matter, so the fuzzer starts from the interesting boundary rather
	// than from nothing.
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add([]byte{99, 1, 2, 3})
	f.Add(framer.Frame(nil))
	f.Add(framer.Frame([]byte("small")))
	f.Add(framer.Frame(bytes.Repeat([]byte("peercache-"), 100)))
	f.Add(framer.Frame(incompressible(1024)))
	// A compressed frame declaring an enormous decompressed size.
	f.Add([]byte{1, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		out, err := codec.Unframe(data)
		if err != nil {
			return
		}
		// A successful unframe must not claim more data than could
		// plausibly have come from the input. LZ4's maximum expansion
		// ratio is 255:1, so anything beyond that means the size field
		// was trusted further than it should have been.
		const maxRatio = 255
		if limit := len(data)*maxRatio + 64; len(out) > limit {
			t.Fatalf("Unframe expanded %d bytes into %d, beyond any legitimate ratio",
				len(data), len(out))
		}
	})
}

// FuzzFrameRoundTrip asserts the property that actually matters: whatever goes
// in comes back out, byte for byte, at every size and compressibility.
func FuzzFrameRoundTrip(f *testing.F) {
	f.Add([]byte{}, 0)
	f.Add([]byte("small"), 16)
	f.Add([]byte("small"), 1024)
	f.Add(bytes.Repeat([]byte("peercache-"), 200), 16)
	f.Add(incompressible(2048), 16)
	f.Add(bytes.Repeat([]byte{0}, 4096), 0)

	f.Fuzz(func(t *testing.T, payload []byte, threshold int) {
		// Keep the threshold in a sane range; negative is rejected at
		// construction and is covered by an ordinary test.
		if threshold < 0 {
			threshold = -threshold
		}
		threshold %= 1 << 20

		framer, err := codec.NewFramer(codec.NameLZ4, threshold)
		if err != nil {
			t.Fatalf("NewFramer(%d): %v", threshold, err)
		}

		framed := framer.Frame(payload)
		if len(framed) == 0 {
			t.Fatal("Frame produced no output; every frame carries at least a header")
		}

		got, err := codec.Unframe(framed)
		if err != nil {
			t.Fatalf("Unframe of a frame this package just wrote: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip changed the value: %d bytes in, %d out", len(payload), len(got))
		}
	})
}

// codecProbe is the value FuzzCodecRoundTrip encodes. Its fields cover the
// shapes a cached application value actually takes and that the two codecs
// disagree about most: a string, raw bytes (base64 in JSON, native in MsgPack),
// both signed and unsigned integers at their extremes, a float, and a map.
//
// The json tags are load-bearing. Both codecs are configured to key on them —
// see TestCodecsAgreeOnFieldNames — so a probe without them would be testing
// the default naming of each library rather than this package's configuration.
type codecProbe struct {
	S  string            `json:"s"`
	B  []byte            `json:"b"`
	I  int64             `json:"i"`
	U  uint64            `json:"u"`
	F  float64           `json:"f"`
	OK bool              `json:"ok"`
	M  map[string]string `json:"m"`
}

// FuzzCodecRoundTrip asserts the property the [codec.Codec] interface states in
// prose: for any value a codec accepts, Unmarshal(Marshal(v)) reproduces v.
//
// TestCodecRoundTrip exists and does not check this. It compares three
// hand-written values and, for the two composite fields, compares only their
// *lengths* — a codec that replaced every map value with the empty string would
// pass it. That is the shape this file's other targets were written to catch, so
// it is worth naming: the test asserted that something came back, not that the
// right thing did.
//
// The JSON codec does not hold the property for one input class, which this
// target found on its first run and question 41 records. That case is excluded
// here by name, narrowly, so the remaining property is still enforced for JSON
// rather than the whole codec being exempted.
func FuzzCodecRoundTrip(f *testing.F) {
	f.Add("user:123", []byte{0, 1, 2}, int64(42), uint64(7), 1.5, true, "region", "eu")
	f.Add("", []byte(nil), int64(0), uint64(0), 0.0, false, "", "")
	// The integer extremes. A codec that routes everything through a float64
	// loses the low bits here and nowhere else.
	f.Add("", []byte{}, int64(math.MinInt64), uint64(math.MaxUint64), 0.0, false, "k", "v")
	f.Add("", []byte{}, int64(math.MaxInt64), uint64(1)<<63, math.SmallestNonzeroFloat64, true, "k", "v")
	// Invalid UTF-8, in both a string field and a map, which is the case
	// question 41 is about.
	f.Add("\xff\xfe", []byte{0xff}, int64(-1), uint64(1), math.Copysign(0, -1), true, "\xff", "\xfe")
	// Characters JSON escapes and MsgPack does not.
	f.Add("\"\\\n\t<>&", []byte("peercache"), int64(1), uint64(1), math.Pi, false, "a\x00b", "c\x00d")

	f.Fuzz(func(t *testing.T, s string, b []byte, i int64, u uint64, fl float64, ok bool, mk, mv string) {
		want := codecProbe{S: s, B: b, I: i, U: u, F: fl, OK: ok}
		if mk != "" {
			want.M = map[string]string{mk: mv}
		}

		// The two built-in codecs by name, not codec.Names().
		//
		// The registry is a package-level global and several tests in this
		// package register stubs into it — one of which returns nil from
		// Marshal and ignores Unmarshal entirely. Iterating the registry
		// made this target pass under `-fuzz`, which runs no other test,
		// and fail under a plain `go test`, which runs them first. The
		// property belongs to the codecs this package ships; a stub
		// registered by a neighbouring test is not one of them.
		for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
			c, err := codec.Get(name)
			if err != nil {
				t.Fatalf("Get(%q): %v", name, err)
			}
			if name == codec.NameJSON && !probeIsValidUTF8(want) {
				// Question 41: encoding/json substitutes U+FFFD for
				// invalid UTF-8 rather than failing, so the round
				// trip cannot hold and no wrapper can make it. The
				// substitution itself is pinned by
				// TestJSONCodecReplacesInvalidUTF8; skipping it here
				// keeps this target enforcing everything else about
				// the JSON codec instead of nothing.
				continue
			}

			encoded, err := c.Marshal(want)
			if err != nil {
				// A refusal is allowed by the contract — JSON
				// rejects NaN and the infinities. Silent damage is
				// not, which is what the rest of this checks.
				continue
			}
			var got codecProbe
			if err := c.Unmarshal(encoded, &got); err != nil {
				t.Fatalf("%s: Unmarshal of bytes this codec just wrote: %v", name, err)
			}
			if got.S != want.S {
				t.Fatalf("%s: string changed: %q -> %q", name, want.S, got.S)
			}
			if !bytes.Equal(got.B, want.B) {
				t.Fatalf("%s: bytes changed: %v -> %v", name, want.B, got.B)
			}
			if got.I != want.I || got.U != want.U || got.OK != want.OK {
				t.Fatalf("%s: scalars changed: %+v -> %+v", name, want, got)
			}
			if !sameFloat(got.F, want.F) {
				t.Fatalf("%s: float changed: %v -> %v", name, want.F, got.F)
			}
			// A nil map and an empty one are not distinguished: JSON
			// writes null for the first and {} for the second, and
			// decoding the second yields a non-nil empty map. The
			// distinction is not one this library carries meaning in.
			if len(got.M) != len(want.M) {
				t.Fatalf("%s: map size changed: %v -> %v", name, want.M, got.M)
			}
			for k, v := range want.M {
				if got.M[k] != v {
					t.Fatalf("%s: map[%q] changed: %q -> %q", name, k, v, got.M[k])
				}
			}
		}
	})
}

// probeIsValidUTF8 reports whether every string in p is valid UTF-8, including
// map keys and values. Byte slices are exempt: both codecs carry them as opaque
// bytes, JSON by base64, so they are not affected by question 41.
func probeIsValidUTF8(p codecProbe) bool {
	if !utf8.ValidString(p.S) {
		return false
	}
	for k, v := range p.M {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			return false
		}
	}
	return true
}

// sameFloat compares two float64s including NaN, which is not equal to itself.
//
// The distinction matters here because the codecs differ: JSON refuses NaN at
// Marshal, MsgPack carries it, and a naive == would report the MsgPack round
// trip as a corruption every time the fuzzer produced one.
func sameFloat(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b
}
