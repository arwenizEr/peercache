package codec

import "encoding/json"

// NameJSON is the registry key for the JSON codec.
const NameJSON = "json"

// jsonCodec is the standard-library JSON codec.
//
// It is the default because it is the one every operator can read in a debug
// dump, not because it is the fastest. When size or speed matters, name
// [NameMsgPack] instead.
type jsonCodec struct{}

// Name implements [Codec].
func (jsonCodec) Name() string { return NameJSON }

// Marshal implements [Codec].
func (jsonCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal implements [Codec].
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

func init() {
	// Built-in codecs are installed directly rather than through Register.
	// Register returns an error, and there is nothing an init function may
	// do with an error except panic — which CLAUDE.md forbids in library
	// paths, and rightly: a library that aborts an application's startup
	// over its own internal bookkeeping is not a library.
	//
	// The name is a constant registered exactly once here, so there is no
	// duplicate to detect. External callers still go through Register and
	// still get ErrDuplicateRegistration if they collide with this name.
	// TestBuiltinsAreRegistered covers the case where an edit removes this.
	codecs[NameJSON] = jsonCodec{}
}
