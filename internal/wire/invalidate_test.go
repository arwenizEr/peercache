package wire_test

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/arwenizEr/peercache/internal/wire"
)

// TestABatchedInvalidationStartsAsTheOldOneDid: a release before Entries
// reads an invalidation as three fields and stops, so a batched request must
// begin with exactly the fields of an unbatched one holding its first hint,
// and an unbatched request must still be those three fields alone.
func TestABatchedInvalidationStartsAsTheOldOneDid(t *testing.T) {
	first := wire.InvalidateRequest{Namespace: "n", Keys: []string{"x"}, Stamp: wire.Stamp{Epoch: 2, Version: 3}}
	batched := first
	batched.Entries = []wire.InvalidateEntry{{Key: "y", Stamp: wire.Stamp{Epoch: 2, Version: 9}}}

	plain, err := wire.Encode(&first)
	if err != nil {
		t.Fatal(err)
	}
	if plain[0] != 0x93 {
		t.Fatalf("an unbatched request opens with %#x, want a three-field array (0x93)", plain[0])
	}
	both, err := wire.Encode(&batched)
	if err != nil {
		t.Fatal(err)
	}
	if both[0] != 0x94 {
		t.Fatalf("a batched request opens with %#x, want a four-field array (0x94)", both[0])
	}
	// Past the array header, the old three fields come first, unchanged.
	if !bytes.HasPrefix(both[1:], plain[1:]) {
		t.Fatalf("a batched request does not begin with the unbatched one's fields:\n%x\n%x", both, plain)
	}

	// And what the old reader did — three fields, the rest left unread —
	// finds the first hint.
	dec := msgpack.NewDecoder(bytes.NewReader(both))
	if _, err := dec.DecodeArrayLen(); err != nil {
		t.Fatal(err)
	}
	ns, err := dec.DecodeString()
	if err != nil || ns != "n" {
		t.Fatalf("namespace = %q, %v", ns, err)
	}
	var keys []string
	if err := dec.Decode(&keys); err != nil || len(keys) != 1 || keys[0] != "x" {
		t.Fatalf("keys = %v, %v", keys, err)
	}
	var stamp wire.Stamp
	if err := dec.Decode(&stamp); err != nil || stamp != first.Stamp {
		t.Fatalf("stamp = %+v, %v; want %+v", stamp, err, first.Stamp)
	}
}

// TestALaterInvalidationsExtraFieldsAreSkipped: fields past Entries are a
// later release's, and reading one must not fail.
func TestALaterInvalidationsExtraFieldsAreSkipped(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(5)
	_ = enc.EncodeString("n")
	_ = enc.Encode([]string{"x"})
	_ = enc.Encode(&wire.Stamp{Epoch: 1, Version: 1})
	_ = enc.EncodeArrayLen(1)
	_ = enc.EncodeArrayLen(3) // an entry with a field from later still
	_ = enc.EncodeString("y")
	_ = enc.Encode(&wire.Stamp{Epoch: 1, Version: 2})
	_ = enc.EncodeString("later")
	_ = enc.EncodeString("later")

	var got wire.InvalidateRequest
	if err := wire.Decode(buf.Bytes(), &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Entries) != 1 || got.Entries[0].Key != "y" || got.Entries[0].Stamp.Version != 2 {
		t.Fatalf("Entries = %+v", got.Entries)
	}
}
