package codec_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/peercache/peercache/internal/codec"
	"github.com/peercache/peercache/internal/core"
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
