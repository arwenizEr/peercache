package codec_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
)

// compressible returns a payload LZ4 can shrink.
func compressible(n int) []byte {
	return bytes.Repeat([]byte("peercache-"), n/10+1)[:n]
}

// incompressible returns a payload LZ4 cannot shrink, generated without
// randomness so the test is reproducible.
func incompressible(n int) []byte {
	out := make([]byte, n)
	// A simple LCG: high enough entropy that LZ4 finds no matches, but
	// entirely deterministic.
	state := uint32(2463534242)
	for i := range out {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		out[i] = byte(state)
	}
	return out
}

func newFramer(t *testing.T, threshold int) *codec.Framer {
	t.Helper()
	f, err := codec.NewFramer(codec.NameLZ4, threshold)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}
	return f
}

func TestFramerRoundTrip(t *testing.T) {
	f := newFramer(t, 64)

	payloads := map[string][]byte{
		"empty":                  {},
		"below threshold":        []byte("small"),
		"exactly at threshold":   compressible(64),
		"well above threshold":   compressible(64 * 1024),
		"incompressible, large":  incompressible(64 * 1024),
		"incompressible, medium": incompressible(200),
	}

	for name, want := range payloads {
		t.Run(name, func(t *testing.T) {
			framed := f.Frame(want)
			got, err := codec.Unframe(framed)
			if err != nil {
				t.Fatalf("Unframe: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("round trip lost data: got %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

// TestFramerCompressesOnlyAboveThreshold is the point of having a threshold:
// compressing a forty-byte counter costs more than it saves.
func TestFramerCompressesOnlyAboveThreshold(t *testing.T) {
	f := newFramer(t, 1024)

	small := compressible(1023)
	if framed := f.Frame(small); len(framed) != len(small)+1 {
		t.Fatalf("a value below the threshold was compressed: %d bytes framed from %d", len(framed), len(small))
	}

	large := compressible(4096)
	if framed := f.Frame(large); len(framed) >= len(large) {
		t.Fatalf("a value above the threshold was not compressed: %d bytes framed from %d", len(framed), len(large))
	}
}

// TestFramerStoresIncompressiblePayloadsRaw covers the case that would
// otherwise cost twice: paying to make a value bigger, then paying again to
// decompress it.
func TestFramerStoresIncompressiblePayloadsRaw(t *testing.T) {
	f := newFramer(t, 16)
	payload := incompressible(4096)

	framed := f.Frame(payload)
	if len(framed) != len(payload)+1 {
		t.Fatalf("incompressible payload framed to %d bytes from %d; expected raw storage",
			len(framed), len(payload))
	}

	got, err := codec.Unframe(framed)
	if err != nil {
		t.Fatalf("Unframe: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("round trip lost data")
	}
}

func TestFramerThresholdZeroCompressesEverything(t *testing.T) {
	f := newFramer(t, 0)
	if f.Threshold() != 0 {
		t.Fatalf("Threshold() = %d", f.Threshold())
	}
	if f.Compressor() != codec.NameLZ4 {
		t.Fatalf("Compressor() = %q", f.Compressor())
	}

	payload := compressible(512)
	framed := f.Frame(payload)
	if len(framed) >= len(payload) {
		t.Fatalf("threshold 0 did not compress: %d from %d", len(framed), len(payload))
	}
}

func TestNewFramerRejectsBadInput(t *testing.T) {
	if _, err := codec.NewFramer(codec.NameLZ4, -1); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("negative threshold = %v, want ErrInvalidOption", err)
	}
	if _, err := codec.NewFramer("nope", 64); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("unknown compressor = %v, want ErrInvalidOption", err)
	}
}

// TestUnframeRejectsCorruptInput enumerates every way a frame can be wrong. A
// cache that returns half a value is indistinguishable from one that returns
// the wrong value, so all of these must fail rather than improvise.
func TestUnframeRejectsCorruptInput(t *testing.T) {
	f := newFramer(t, 0)
	good := f.Frame(compressible(4096))

	oversized := []byte{1}
	oversized = binary.AppendUvarint(oversized, 1<<40)
	oversized = append(oversized, good[len(good)-8:]...)

	// A uvarint that never terminates: ten continuation bytes overflow.
	unreadableSize := []byte{1}
	unreadableSize = append(unreadableSize, bytes.Repeat([]byte{0x80}, 11)...)

	tests := map[string][]byte{
		"empty":             {},
		"unknown code":      {99, 1, 2, 3},
		"declared too big":  oversized,
		"unreadable size":   unreadableSize,
		"truncated payload": good[:len(good)/2],
		"size disagrees":    append(append([]byte{1}, binary.AppendUvarint(nil, 7)...), good[3:]...),
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := codec.Unframe(input)
			if err == nil {
				t.Fatalf("Unframe accepted corrupt input and returned %d bytes", len(got))
			}
			if !codec.IsCorruptFrame(err) && !strings.Contains(err.Error(), "lz4") {
				t.Fatalf("error %v is neither a corrupt-frame report nor a decompression failure", err)
			}
		})
	}
}

// TestUnframeRawIsZeroCopy documents the aliasing contract. The store hands out
// its own slices, so the framer doing the same is consistent rather than
// surprising — but it has to be deliberate and tested, not accidental.
func TestUnframeRawIsZeroCopy(t *testing.T) {
	f := newFramer(t, 1<<20) // effectively never compress
	payload := []byte("hello")

	framed := f.Frame(payload)
	got, err := codec.Unframe(framed)
	if err != nil {
		t.Fatalf("Unframe: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("empty result")
	}
	framed[1] = 'H'
	if got[0] != 'H' {
		t.Fatal("Unframe copied an uncompressed payload; the contract says it aliases")
	}
}

func TestDecompressSized(t *testing.T) {
	c, err := codec.GetCompressor(codec.NameLZ4)
	if err != nil {
		t.Fatalf("GetCompressor: %v", err)
	}

	original := compressible(8192)
	packed, err := c.Compress(original)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}

	got, err := codec.DecompressSized(packed, len(original))
	if err != nil {
		t.Fatalf("DecompressSized: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("round trip lost data")
	}

	if _, err := codec.DecompressSized(packed, -1); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("negative size = %v, want ErrInvalidOption", err)
	}
	if got, err := codec.DecompressSized(nil, 0); err != nil || len(got) != 0 {
		t.Fatalf("zero size = %v, %v", got, err)
	}
}

// TestCompressorReportsIncompressible pins the internal signal the framer
// depends on. If Compress ever started returning a larger buffer instead of an
// error, the framer would silently start storing inflated values.
func TestCompressorReportsIncompressible(t *testing.T) {
	c, err := codec.GetCompressor(codec.NameLZ4)
	if err != nil {
		t.Fatalf("GetCompressor: %v", err)
	}
	if _, err := c.Compress(nil); err == nil {
		t.Fatal("Compress of an empty payload succeeded; expected an incompressible report")
	}
	if _, err := c.Compress(incompressible(4096)); err == nil {
		t.Fatal("Compress of an incompressible payload succeeded")
	}
}

func TestCompressorDecompressWithoutKnownSize(t *testing.T) {
	c, err := codec.GetCompressor(codec.NameLZ4)
	if err != nil {
		t.Fatalf("GetCompressor: %v", err)
	}
	original := compressible(64 * 1024)
	packed, err := c.Compress(original)
	if err != nil {
		t.Fatalf("Compress: %v", err)
	}
	got, err := c.Decompress(packed)
	if err != nil {
		t.Fatalf("Decompress: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatal("round trip lost data")
	}
}
