package codec_test

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache/internal/codec"
	"github.com/arwenizEr/peercache/internal/core"
)

// TestBuiltinsAreRegistered guards the init functions. They install codecs by
// direct map assignment rather than through Register, so nothing else would
// notice if an edit removed one — a namespace naming that codec would simply
// fail at startup with a confusing message.
func TestBuiltinsAreRegistered(t *testing.T) {
	names := codec.Names()
	for _, want := range []string{codec.NameJSON, codec.NameMsgPack} {
		if !slices.Contains(names, want) {
			t.Errorf("codec %q is not registered; have %v", want, names)
		}
	}
	if got := codec.CompressorNames(); !slices.Contains(got, codec.NameLZ4) {
		t.Errorf("compressor %q is not registered; have %v", codec.NameLZ4, got)
	}
}

type profile struct {
	ID    string            `json:"id"`
	Score int               `json:"score"`
	Tags  []string          `json:"tags"`
	Meta  map[string]string `json:"meta"`
}

func TestCodecRoundTrip(t *testing.T) {
	values := map[string]profile{
		"populated": {
			ID:    "user:123",
			Score: 42,
			Tags:  []string{"a", "b"},
			Meta:  map[string]string{"region": "eu"},
		},
		"zero": {},
		"empty slice": {
			ID:   "user:456",
			Tags: []string{},
		},
	}

	for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
		c, err := codec.Get(name)
		if err != nil {
			t.Fatalf("Get(%q): %v", name, err)
		}
		if c.Name() != name {
			t.Fatalf("codec reports name %q, registered as %q", c.Name(), name)
		}
		for label, want := range values {
			t.Run(name+"/"+label, func(t *testing.T) {
				encoded, err := c.Marshal(want)
				if err != nil {
					t.Fatalf("Marshal: %v", err)
				}
				var got profile
				if err := c.Unmarshal(encoded, &got); err != nil {
					t.Fatalf("Unmarshal: %v", err)
				}
				if got.ID != want.ID || got.Score != want.Score {
					t.Fatalf("round trip = %+v, want %+v", got, want)
				}
				if len(got.Tags) != len(want.Tags) {
					t.Fatalf("tags = %v, want %v", got.Tags, want.Tags)
				}
				if len(got.Meta) != len(want.Meta) {
					t.Fatalf("meta = %v, want %v", got.Meta, want.Meta)
				}
			})
		}
	}
}

// TestCodecsAgreeOnFieldNames matters during a rolling upgrade: a value written
// by a pod using one codec must be readable by a pod using the same codec after
// a struct field is added. Both codecs are told to key on the json tag so that
// neither is positional.
func TestCodecsAgreeOnFieldNames(t *testing.T) {
	type v1 struct {
		ID string `json:"id"`
	}
	type v2 struct {
		ID    string `json:"id"`
		Added int    `json:"added"`
	}

	for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
		t.Run(name, func(t *testing.T) {
			c, err := codec.Get(name)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			encoded, err := c.Marshal(v1{ID: "x"})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var got v2
			if err := c.Unmarshal(encoded, &got); err != nil {
				t.Fatalf("Unmarshal into the wider struct: %v", err)
			}
			if got.ID != "x" || got.Added != 0 {
				t.Fatalf("decoded %+v, want {x 0}", got)
			}
		})
	}
}

func TestGetUnknownCodec(t *testing.T) {
	_, err := codec.Get("nope")
	if !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Get of an unknown codec = %v, want ErrInvalidOption", err)
	}
	// The message should name what is available; a bare "not found" makes
	// the operator go read the source.
	if !strings.Contains(err.Error(), codec.NameJSON) {
		t.Fatalf("error %q does not list the available codecs", err)
	}
}

type namedCodec struct{ name string }

func (c namedCodec) Name() string              { return c.name }
func (namedCodec) Marshal(any) ([]byte, error) { return nil, nil }
func (namedCodec) Unmarshal([]byte, any) error { return nil }

func TestRegisterRejectsBadInput(t *testing.T) {
	if err := codec.Register(nil); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Register(nil) = %v, want ErrInvalidOption", err)
	}
	if err := codec.Register(namedCodec{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Register with an empty name = %v, want ErrInvalidOption", err)
	}
	if err := codec.Register(namedCodec{name: codec.NameJSON}); !errors.Is(err, core.ErrDuplicateRegistration) {
		t.Fatalf("Register of a duplicate name = %v, want ErrDuplicateRegistration", err)
	}
	if err := codec.RegisterCompressor(nil); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("RegisterCompressor(nil) = %v, want ErrInvalidOption", err)
	}
}

func TestGetUnknownCompressor(t *testing.T) {
	if _, err := codec.GetCompressor("nope"); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("GetCompressor = %v, want ErrInvalidOption", err)
	}
}

// identityCompressor is a third-party compressor, used to exercise the
// registration path that ships with no in-tree caller.
type identityCompressor struct{ name string }

func (c identityCompressor) Name() string { return c.name }

func (identityCompressor) Compress(src []byte) ([]byte, error) {
	return append([]byte(nil), src...), nil
}

func (identityCompressor) Decompress(src []byte) ([]byte, error) {
	return append([]byte(nil), src...), nil
}

// registrations generates names unique within this process.
//
// Registration is deliberately global and permanent — it models startup, where
// every pod registers the same names once — so a test cannot undo it. Under
// `go test -count=2`, which is the gate CLAUDE.md specifies, a fixed name would
// collide with the previous run and the test would fail on its second pass.
// That is not a flaw in the registry; it is a flaw in a test that assumes a
// clean process.
var registrations atomic.Int64

func uniqueName(prefix string) string {
	return prefix + "-" + strconv.FormatInt(registrations.Add(1), 10)
}

// TestRegisterCompressorSucceeds covers the half of RegisterCompressor that had
// no coverage at all: the one that works.
//
// The registry exists so that a compressor can be added from outside this
// package, and nothing in the tree does that — LZ4 is installed directly by
// init. So the only path anyone would actually use was the untested one.
func TestRegisterCompressorSucceeds(t *testing.T) {
	name := uniqueName("identity-test")

	if err := codec.RegisterCompressor(identityCompressor{name: name}); err != nil {
		t.Fatalf("RegisterCompressor: %v", err)
	}
	if err := codec.RegisterCompressor(identityCompressor{name: name}); !errors.Is(err, core.ErrDuplicateRegistration) {
		t.Fatalf("duplicate = %v, want ErrDuplicateRegistration", err)
	}

	got, err := codec.GetCompressor(name)
	if err != nil {
		t.Fatalf("GetCompressor: %v", err)
	}
	if got.Name() != name {
		t.Fatalf("Name() = %q, want %q", got.Name(), name)
	}
	if !slices.Contains(codec.CompressorNames(), name) {
		t.Fatalf("CompressorNames() = %v, missing %q", codec.CompressorNames(), name)
	}

	// A compressor with no frame code cannot be used for framing. That is
	// deliberate: adding a code is a wire-format change.
	if _, err := codec.NewFramer(name, 64); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("NewFramer with an un-coded compressor = %v, want ErrInvalidOption", err)
	}
}

// TestRegisterSucceedsForANewCodec covers the same gap on the codec side.
func TestRegisterSucceedsForANewCodec(t *testing.T) {
	name := uniqueName("passthrough-test")

	if err := codec.Register(namedCodec{name: name}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := codec.Get(name)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Name() != name {
		t.Fatalf("Name() = %q, want %q", got.Name(), name)
	}
	if !slices.Contains(codec.Names(), name) {
		t.Fatalf("Names() = %v, missing %q", codec.Names(), name)
	}
}

// TestCodecErrorsAreReported covers the failure branches of the codecs
// themselves, which had no tests: a value that cannot be encoded, and input
// that cannot be decoded.
func TestCodecErrorsAreReported(t *testing.T) {
	for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
		c, err := codec.Get(name)
		if err != nil {
			t.Fatalf("Get(%q): %v", name, err)
		}

		t.Run(name+"/unencodable value", func(t *testing.T) {
			// A channel has no representation in either format.
			if _, err := c.Marshal(make(chan int)); err == nil {
				t.Fatal("Marshal of a channel succeeded")
			}
		})

		t.Run(name+"/undecodable input", func(t *testing.T) {
			var out profile
			if err := c.Unmarshal([]byte{0xc1, 0xff, 0xfe, 0xfd}, &out); err == nil {
				t.Fatal("Unmarshal of malformed input succeeded")
			}
		})

		t.Run(name+"/non-pointer target", func(t *testing.T) {
			encoded, err := c.Marshal(profile{ID: "x"})
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var out profile
			if err := c.Unmarshal(encoded, out); err == nil {
				t.Fatal("Unmarshal into a non-pointer succeeded")
			}
		})
	}
}

// TestJSONCodecReplacesInvalidUTF8 pins behaviour this package does not want and
// cannot change: encoding/json substitutes U+FFFD for every byte of an invalid
// UTF-8 sequence in a string, and reports no error doing it. A value written
// through the JSON codec and read back is therefore not always the value that
// went in.
//
// FuzzCodecRoundTrip found it on its first run, against a codec that had shipped
// since M1 with a round-trip guarantee in the interface doc above it.
//
// It is pinned rather than fixed because there is no fix inside this package.
// The substitution happens inside encoding/json, no option disables it, and
// detecting it would mean walking every value by reflection before every
// marshal. Question 41 in docs/decisions/open.md carries the decision; this test
// exists so that the day the behaviour changes — a Go release, or a wrapper
// added here — it is a failing assertion rather than a silent one.
//
// The two facts worth keeping separate: MsgPack carries the same bytes through
// unharmed, and a []byte field survives JSON too, because it is base64. Only
// Go strings are affected.
func TestJSONCodecReplacesInvalidUTF8(t *testing.T) {
	type value struct {
		S string `json:"s"`
		B []byte `json:"b"`
	}
	// 0xff is not a legal UTF-8 byte in any position.
	in := value{S: "\xff\xfe", B: []byte{0xff, 0xfe}}

	jsonCodec, err := codec.Get(codec.NameJSON)
	if err != nil {
		t.Fatalf("Get(%q): %v", codec.NameJSON, err)
	}
	encoded, err := jsonCodec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal reported an error; if encoding/json now rejects "+
			"invalid UTF-8 rather than substituting, question 41 is resolved "+
			"and this test should assert that instead: %v", err)
	}
	var got value
	if err := jsonCodec.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got.S == in.S {
		t.Fatal("the JSON codec now round-trips invalid UTF-8 in a string; " +
			"question 41 is resolved, and FuzzCodecRoundTrip's exclusion for " +
			"it should be removed")
	}
	if want := "��"; got.S != want {
		t.Errorf("string came back as %q, want the replacement characters %q; "+
			"the corruption is not the shape this test and question 41 describe",
			got.S, want)
	}
	if !slices.Equal(got.B, in.B) {
		t.Errorf("bytes came back as %v, want %v; the []byte field is base64 in "+
			"JSON and must be unaffected", got.B, in.B)
	}

	// The same value through MsgPack, which has no such rule.
	msgpackCodec, err := codec.Get(codec.NameMsgPack)
	if err != nil {
		t.Fatalf("Get(%q): %v", codec.NameMsgPack, err)
	}
	encoded, err = msgpackCodec.Marshal(in)
	if err != nil {
		t.Fatalf("MsgPack Marshal: %v", err)
	}
	var viaMsgPack value
	if err := msgpackCodec.Unmarshal(encoded, &viaMsgPack); err != nil {
		t.Fatalf("MsgPack Unmarshal: %v", err)
	}
	if viaMsgPack.S != in.S {
		t.Errorf("MsgPack changed the string too: %q -> %q; the difference "+
			"between the codecs is the reason question 41 recommends naming one",
			in.S, viaMsgPack.S)
	}
}
