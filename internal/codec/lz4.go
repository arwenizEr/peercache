package codec

import (
	"fmt"

	"github.com/pierrec/lz4/v4"

	"github.com/arwenizEr/peercache/internal/core"
)

// NameLZ4 is the registry key for the LZ4 compressor.
const NameLZ4 = "lz4"

// lz4Compressor uses LZ4 block format, not the frame format.
//
// Block format has no per-stream header, no checksum and no magic bytes, which
// for values measured in kilobytes is the difference between compression paying
// for itself and not. The framing this package adds already records whether a
// payload is compressed, so the format's own header would be redundant.
//
// The consequence is that decompression needs the original size. It is stored
// in the frame header; see frame.go.
type lz4Compressor struct{}

// Name implements [Compressor].
func (lz4Compressor) Name() string { return NameLZ4 }

// Compress implements [Compressor].
//
// A payload that does not compress is returned as an incompressible error from
// the LZ4 block API; callers of this type go through [Framer], which treats
// that as "store it raw" rather than as a failure.
func (lz4Compressor) Compress(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, errIncompressible
	}
	dst := make([]byte, lz4.CompressBlockBound(len(src)))
	n, err := lz4.CompressBlock(src, dst, nil)
	if err != nil {
		return nil, fmt.Errorf("codec: lz4 compress: %w", err)
	}
	if n == 0 || n >= len(src) {
		// n == 0 means LZ4 could not compress it at all. n >= len(src)
		// means it could, but the result is no smaller, which is the
		// same thing from the caller's point of view.
		return nil, errIncompressible
	}
	return dst[:n], nil
}

// Decompress implements [Compressor].
//
// LZ4 block format does not record the original length, so this cannot be used
// without knowing it. [Framer] does know it; a direct caller should use
// [DecompressSized].
func (c lz4Compressor) Decompress(src []byte) ([]byte, error) {
	// Without a known original size the only safe strategy is to grow until
	// it fits. Start at 4x, which covers the overwhelming majority of real
	// payloads, and give up at a bound rather than growing without limit on
	// corrupt input.
	capacity := len(src) * 4
	if capacity < 64 {
		capacity = 64
	}
	const maxGrowth = 1 << 26 // 64 MiB
	for {
		out, err := decompressInto(src, capacity)
		if err == nil {
			return out, nil
		}
		if capacity >= maxGrowth {
			return nil, fmt.Errorf("codec: lz4 decompress: output exceeds %d bytes or input is corrupt: %w",
				maxGrowth, err)
		}
		capacity *= 4
	}
}

// DecompressSized decompresses src, which must expand to exactly size bytes.
//
// This is the fast path, used when the original size is recorded in the frame.
// The exact-length check is not paranoia: LZ4 block format carries no checksum,
// so a length that disagrees with the frame header is the only evidence of
// corruption available.
func DecompressSized(src []byte, size int) ([]byte, error) {
	if size < 0 {
		return nil, fmt.Errorf("codec: negative decompressed size %d: %w", size, core.ErrInvalidOption)
	}
	if size == 0 {
		return []byte{}, nil
	}
	out, err := decompressInto(src, size)
	if err != nil {
		return nil, err
	}
	if len(out) != size {
		return nil, fmt.Errorf("codec: lz4 decompress produced %d bytes, expected %d: %w",
			len(out), size, errCorruptFrame)
	}
	return out, nil
}

// decompressInto decompresses src into a buffer of capacity bytes and returns
// however much was actually produced. A capacity smaller than the original
// length is reported as an error by the LZ4 block decoder.
func decompressInto(src []byte, capacity int) ([]byte, error) {
	dst := make([]byte, capacity)
	n, err := lz4.UncompressBlock(src, dst)
	if err != nil {
		return nil, fmt.Errorf("codec: lz4 decompress: %w", err)
	}
	return dst[:n], nil
}

func init() { compressors[NameLZ4] = lz4Compressor{} }
