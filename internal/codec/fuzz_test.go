package codec_test

import (
	"bytes"
	"testing"

	"github.com/peercache/peercache/internal/codec"
)

// The frame parser is the only place in this library that interprets bytes it
// did not write. Today they come from its own store; from M4 they arrive over
// the network, and CLAUDE.md forbids panics in library paths. Six hand-written
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
