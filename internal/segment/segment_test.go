package segment_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"hash/crc32"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/arwenizEr/peercache/internal/segment"
)

var update = flag.Bool("update", false, "rewrite testdata/golden.bin")

func sample() *segment.File {
	return &segment.File{
		Kind:    segment.KindSnapshot,
		Covered: []string{"orders/p007/seg-1-n0.bin", "orders/p007/seg-2-n1.bin"},
		Records: []segment.Record{
			{Namespace: "orders", Key: "o:1", Writer: "n0", Order: 1 << 20, ExpiresAt: 1_700_000_000_000_000_000, Value: []byte("pending")},
			{Namespace: "orders", Key: "o:2", Writer: "n1", Order: 2 << 20, Tombstone: true},
			{Namespace: "orders", Key: "", Writer: "", Order: 0, Value: []byte{}},
		},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, f := range []*segment.File{
		sample(),
		{Kind: segment.KindSegment, Records: sample().Records},
		{Kind: segment.KindSegment},
	} {
		got, err := segment.Decode(segment.Encode(f))
		if err != nil {
			t.Fatalf("Decode(Encode(%v)): %v", f.Kind, err)
		}
		normalise(f)
		normalise(got)
		if !reflect.DeepEqual(got, f) {
			t.Fatalf("round trip changed the file:\n got %+v\nwant %+v", got, f)
		}
	}
}

// normalise makes nil and empty slices compare equal; the format does not
// distinguish them and need not.
func normalise(f *segment.File) {
	if len(f.Covered) == 0 {
		f.Covered = nil
	}
	if len(f.Records) == 0 {
		f.Records = nil
	}
	for i := range f.Records {
		if len(f.Records[i].Value) == 0 {
			f.Records[i].Value = nil
		}
	}
}

// TestEveryBitFlipIsCorrupt: the checksum is what makes a damaged object an
// error rather than a wrong value restored into memory.
func TestEveryBitFlipIsCorrupt(t *testing.T) {
	data := segment.Encode(sample())
	for i := range data {
		for bit := 0; bit < 8; bit++ {
			bad := bytes.Clone(data)
			bad[i] ^= 1 << bit
			if _, err := segment.Decode(bad); !errors.Is(err, segment.ErrCorrupt) {
				t.Fatalf("flipping bit %d of byte %d: %v, want ErrCorrupt", bit, i, err)
			}
		}
	}
}

func TestEveryTruncationIsCorrupt(t *testing.T) {
	data := segment.Encode(sample())
	for n := 0; n < len(data); n++ {
		if _, err := segment.Decode(data[:n]); !errors.Is(err, segment.ErrCorrupt) {
			t.Fatalf("first %d of %d bytes: %v, want ErrCorrupt", n, len(data), err)
		}
	}
}

// reseal recomputes the checksum, so a test can reach the structural checks
// behind it with bytes a well-meaning writer could never have produced.
func reseal(body []byte) []byte {
	return binary.LittleEndian.AppendUint32(bytes.Clone(body),
		crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)))
}

// TestADeclaredCountIsNotBelieved: a count the remaining bytes cannot hold is
// refused before anything is allocated for it, checksum or no checksum.
func TestADeclaredCountIsNotBelieved(t *testing.T) {
	body := []byte("PCSG\x01\x01\x00") // segment, no covered names
	body = binary.AppendUvarint(body, 1<<40)
	forged := reseal(body)
	allocs := testing.AllocsPerRun(10, func() {
		if _, err := segment.Decode(forged); !errors.Is(err, segment.ErrCorrupt) {
			t.Fatalf("a record count of 2^40 in a few bytes: %v, want ErrCorrupt", err)
		}
	})
	// The race detector allocates on its own account — 15 of 200 runs
	// counted a fifth — so the bound is checked only without it. The
	// refusal above is checked either way.
	if allocs > 4 && !raceEnabled {
		t.Fatalf("%.0f allocations decoding a forged count; it must be refused, not sized", allocs)
	}
}

func TestStructuralDefectsAreCorrupt(t *testing.T) {
	seg := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: sample().Records[:1]})
	body := seg[:len(seg)-4]
	cases := map[string][]byte{
		"unknown version":      append([]byte("PCSG\x02"), body[5:]...),
		"unknown kind":         append([]byte("PCSG\x01\x09"), body[6:]...),
		"bad magic":            append([]byte("XXXX"), body[4:]...),
		"trailing bytes":       append(bytes.Clone(body), 0),
		"segment with covered": append([]byte("PCSG\x01\x01\x01\x01a"), body[7:]...),
	}
	// A tombstone carrying a value: flags byte set, value left in place.
	tomb := bytes.Clone(body)
	tomb[8] = 1 // the first record's flags, after magic, version, kind, two counts
	cases["tombstone with value"] = tomb
	// A flag bit nothing defines.
	odd := bytes.Clone(body)
	odd[8] = 0x80
	cases["unknown flag"] = odd

	for name, b := range cases {
		if _, err := segment.Decode(reseal(b)); !errors.Is(err, segment.ErrCorrupt) {
			t.Errorf("%s: %v, want ErrCorrupt", name, err)
		}
	}
}

// TestTheFormatIsPinned: bytes on storage must stay readable by every later
// release. Changing the encoder changes this file, and that is the moment to
// add a version rather than edit this one.
func TestTheFormatIsPinned(t *testing.T) {
	path := filepath.Join("testdata", "golden.bin")
	got := segment.Encode(sample())
	if *update {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the golden file: %v (run with -update once to create it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("the encoding of the golden sample changed; storage written by an earlier " +
			"release would no longer match. Add a format version instead.")
	}
	if _, err := segment.Decode(want); err != nil {
		t.Fatalf("the golden file no longer decodes: %v", err)
	}
}

func TestNewerBreaksTiesByWriter(t *testing.T) {
	a := segment.Record{Order: 5, Writer: "n1"}
	b := segment.Record{Order: 5, Writer: "n2"}
	c := segment.Record{Order: 6, Writer: "n0"}
	if !b.Newer(&a) || a.Newer(&b) {
		t.Fatal("equal orders must be broken by writer, the same way on every node")
	}
	if !c.Newer(&b) || b.Newer(&c) {
		t.Fatal("a later order must win whatever the writer")
	}
	if a.Newer(&a) {
		t.Fatal("a record is not newer than itself")
	}
}

// FuzzDecode: no input panics, and anything that decodes re-encodes to the
// same bytes — so a decoded file is exactly what was written.
func FuzzDecode(f *testing.F) {
	f.Add(segment.Encode(sample()))
	f.Add(segment.Encode(&segment.File{Kind: segment.KindSegment}))
	f.Fuzz(func(t *testing.T, data []byte) {
		file, err := segment.Decode(data)
		if err != nil {
			if !errors.Is(err, segment.ErrCorrupt) {
				t.Fatalf("Decode returned %v, which is not ErrCorrupt", err)
			}
			return
		}
		if again := segment.Encode(file); !bytes.Equal(again, data) {
			t.Fatalf("decoded and re-encoded to different bytes")
		}
	})
}
