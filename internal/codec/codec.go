package codec

import (
	"fmt"
	"sort"
	"sync"

	"github.com/arwenizEr/peercache/internal/core"
)

// Codec converts values to and from bytes.
//
// Implementations must be safe for concurrent use and must round-trip: for any
// v that the codec accepts, Unmarshal(Marshal(v)) reproduces v. A codec that
// silently drops fields is worse than one that errors, because the loss is
// discovered by whoever reads the data next, days later.
//
// FuzzCodecRoundTrip enforces that on every registered codec. One built-in
// breaks it: [NameJSON] replaces invalid UTF-8 in a string with U+FFFD and
// reports success, for reasons that live in encoding/json rather than here. The
// exception is documented on the codec, pinned by
// TestJSONCodecReplacesInvalidUTF8 and excluded by name from the fuzz target, so
// that it is a known hole rather than an unenforced sentence.
type Codec interface {
	// Name is the registry key. It appears in configuration, so it is part
	// of the public contract and must not change.
	Name() string

	// Marshal encodes v. The returned slice belongs to the caller.
	Marshal(v any) ([]byte, error)

	// Unmarshal decodes data into v, which must be a non-nil pointer.
	Unmarshal(data []byte, v any) error
}

// Compressor compresses and decompresses byte slices.
//
// Implementations must be safe for concurrent use. Decompress must fail rather
// than return partial output on corrupt input: a cache that hands back
// half a value is indistinguishable from one that hands back the wrong value.
type Compressor interface {
	// Name is the registry key.
	Name() string

	// Compress returns the compressed form of src.
	Compress(src []byte) ([]byte, error)

	// Decompress returns the original form of src.
	Decompress(src []byte) ([]byte, error)
}

var (
	registryMu  sync.RWMutex
	codecs      = make(map[string]Codec)
	compressors = make(map[string]Compressor)
)

// Register adds c to the codec registry.
//
// It returns an error rather than panicking on a duplicate name, because a
// duplicate is a programming error that should surface at startup with a
// message, not as a stack trace in a library the application did not write.
func Register(c Codec) error {
	if c == nil {
		return fmt.Errorf("codec: nil codec: %w", core.ErrInvalidOption)
	}
	name := c.Name()
	if name == "" {
		return fmt.Errorf("codec: codec with an empty name: %w", core.ErrInvalidOption)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := codecs[name]; dup {
		return fmt.Errorf("codec: %q is already registered: %w", name, core.ErrDuplicateRegistration)
	}
	codecs[name] = c
	return nil
}

// RegisterCompressor adds c to the compressor registry.
func RegisterCompressor(c Compressor) error {
	if c == nil {
		return fmt.Errorf("codec: nil compressor: %w", core.ErrInvalidOption)
	}
	name := c.Name()
	if name == "" {
		return fmt.Errorf("codec: compressor with an empty name: %w", core.ErrInvalidOption)
	}

	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := compressors[name]; dup {
		return fmt.Errorf("codec: compressor %q is already registered: %w", name, core.ErrDuplicateRegistration)
	}
	compressors[name] = c
	return nil
}

// Get returns the codec registered under name.
func Get(name string) (Codec, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := codecs[name]
	if !ok {
		return nil, fmt.Errorf("codec: %q is not registered (have %v): %w", name, namesLocked(), core.ErrInvalidOption)
	}
	return c, nil
}

// GetCompressor returns the compressor registered under name.
func GetCompressor(name string) (Compressor, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := compressors[name]
	if !ok {
		return nil, fmt.Errorf("codec: compressor %q is not registered (have %v): %w",
			name, compressorNamesLocked(), core.ErrInvalidOption)
	}
	return c, nil
}

// Names returns every registered codec name, sorted.
func Names() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return namesLocked()
}

// CompressorNames returns every registered compressor name, sorted.
func CompressorNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return compressorNamesLocked()
}

func namesLocked() []string {
	out := make([]string, 0, len(codecs))
	for name := range codecs {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func compressorNamesLocked() []string {
	out := make([]string, 0, len(compressors))
	for name := range compressors {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}
