package codec

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/peercache/peercache/internal/core"
)

// Frame header codes. These are wire format: a value framed by one pod is
// unframed by another, so a code's meaning must never change. Add codes; do not
// renumber them.
const (
	// frameRaw means the payload follows verbatim.
	frameRaw byte = 0
	// frameLZ4 means a uvarint original size follows, then an LZ4 block.
	frameLZ4 byte = 1
)

// codeByName maps a compressor to its frame code. A compressor with no code
// cannot be used for framing, which is deliberate: adding one is a wire-format
// change and should require editing this table.
var codeByName = map[string]byte{
	NameLZ4: frameLZ4,
}

var nameByCode = map[byte]string{
	frameLZ4: NameLZ4,
}

// errIncompressible reports that compression did not make the payload smaller.
// It is an internal signal, not a failure: [Framer.Frame] stores the value raw.
var errIncompressible = errors.New("codec: payload is incompressible")

// errCorruptFrame reports a frame that cannot be read. It is exported through
// wrapping only, because there is nothing a caller can do differently.
var errCorruptFrame = errors.New("codec: corrupt frame")

// Framer applies compression above a size threshold and records, in the
// value itself, whether it did.
//
// A Framer is immutable after construction and safe for concurrent use.
//
// Framing is all-or-nothing per namespace. A namespace without compression
// stores values verbatim with no header at all, so the default read path costs
// nothing; a namespace with compression frames every value, including the ones
// too small to compress. Mixing the two within one namespace would mean
// guessing at read time whether a leading zero byte is a header or data, and a
// cache that guesses is a cache that eventually guesses wrong.
type Framer struct {
	compressor Compressor
	code       byte
	threshold  int
}

// NewFramer returns a Framer using the named compressor for values of at least
// threshold bytes.
//
// A threshold of zero compresses everything, which is almost never what anyone
// wants; the namespace layer treats a missing compressor as "no framing at all"
// rather than as a threshold of zero.
func NewFramer(name string, threshold int) (*Framer, error) {
	if threshold < 0 {
		return nil, fmt.Errorf("codec: negative compression threshold %d: %w", threshold, core.ErrInvalidOption)
	}
	c, err := GetCompressor(name)
	if err != nil {
		return nil, err
	}
	code, ok := codeByName[name]
	if !ok {
		return nil, fmt.Errorf("codec: compressor %q has no frame code and cannot be used for framing: %w",
			name, core.ErrInvalidOption)
	}
	return &Framer{compressor: c, code: code, threshold: threshold}, nil
}

// Threshold returns the size at or above which compression is attempted.
func (f *Framer) Threshold() int { return f.threshold }

// Compressor returns the name of the compressor in use.
func (f *Framer) Compressor() string { return f.compressor.Name() }

// Frame returns src wrapped for storage.
//
// Compression is attempted only at or above the threshold, and only kept if it
// actually made the value smaller. A payload that grows under compression is
// stored raw: paying to make something bigger and then paying again to
// decompress it is a pure loss, and incompressible payloads are common.
func (f *Framer) Frame(src []byte) []byte {
	if len(src) >= f.threshold {
		if packed, err := f.compressor.Compress(src); err == nil {
			out := make([]byte, 0, 1+binary.MaxVarintLen64+len(packed))
			out = append(out, f.code)
			out = binary.AppendUvarint(out, uint64(len(src)))
			return append(out, packed...)
		}
		// Either incompressible or the compressor failed. Neither is a
		// reason to lose the value; store it raw.
	}
	out := make([]byte, 0, 1+len(src))
	out = append(out, frameRaw)
	return append(out, src...)
}

// Unframe reverses [Framer.Frame].
//
// For an uncompressed frame the result aliases src, which is consistent with
// the store handing out its own slices. Callers must not mutate it.
func Unframe(src []byte) ([]byte, error) {
	if len(src) == 0 {
		return nil, fmt.Errorf("codec: empty frame: %w", errCorruptFrame)
	}
	switch code := src[0]; code {
	case frameRaw:
		return src[1:], nil
	default:
		name, known := nameByCode[code]
		if !known {
			return nil, fmt.Errorf("codec: unknown frame code %d: %w", code, errCorruptFrame)
		}
		size, n := binary.Uvarint(src[1:])
		if n <= 0 {
			return nil, fmt.Errorf("codec: unreadable size in %s frame: %w", name, errCorruptFrame)
		}
		if size > uint64(maxDecompressedSize) {
			return nil, fmt.Errorf("codec: %s frame declares %d bytes, over the %d limit: %w",
				name, size, maxDecompressedSize, errCorruptFrame)
		}
		payload := src[1+n:]
		switch code {
		case frameLZ4:
			return DecompressSized(payload, int(size))
		default:
			// Unreachable while codeByName and this switch agree. If
			// they ever disagree, failing is the only safe answer.
			return nil, fmt.Errorf("codec: frame code %d has no decoder: %w", code, errCorruptFrame)
		}
	}
}

// maxDecompressedSize bounds what a frame may claim to decompress to, so that a
// corrupt or hostile header cannot make the process allocate arbitrarily.
const maxDecompressedSize = 1 << 30 // 1 GiB

// IsCorruptFrame reports whether err came from an unreadable frame.
func IsCorruptFrame(err error) bool { return errors.Is(err, errCorruptFrame) }
