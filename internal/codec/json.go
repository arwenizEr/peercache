package codec

import "encoding/json"

// NameJSON is the registry key for the JSON codec.
const NameJSON = "json"

// jsonCodec is the standard-library JSON codec.
//
// It is the default because it is the one every operator can read in a debug
// dump, not because it is the fastest. When size or speed matters, name
// [NameMsgPack] instead.
//
// It does not round-trip a string containing invalid UTF-8. encoding/json
// substitutes U+FFFD for each bad byte and returns no error, so the value read
// back is not the value written and nothing says so. There is no option to
// disable it and no way to detect it from the encoded bytes, which are always
// valid UTF-8 by then.
//
// A []byte field is unaffected — JSON carries it as base64 — and [NameMsgPack]
// is unaffected entirely. Name MsgPack for any value whose strings may hold
// arbitrary bytes. TestJSONCodecReplacesInvalidUTF8 pins the behaviour and
// question 41 in docs/decisions/open.md carries the decision not to work
// around it.
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
