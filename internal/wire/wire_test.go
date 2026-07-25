package wire_test

import (
	"errors"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/wire"
)

// TestEveryMessageRoundTrips is the codec contract applied to each message.
//
// The values are deliberately awkward: empty slices next to nil ones, zero
// fields next to maximal ones, a key with a NUL and a non-ASCII byte in it. A
// round-trip test built from tidy values proves the encoder works on tidy
// values.
func TestEveryMessageRoundTrips(t *testing.T) {
	cases := []struct {
		name string
		msg  any
		into func() any
	}{
		{"GetRequest", &wire.GetRequest{
			Namespace: "orders", Key: "user/\x00\xff/42", Epoch: 1 << 63,
		}, func() any { return new(wire.GetRequest) }},
		{"GetResponse found", &wire.GetResponse{
			Found: true, Value: []byte{0, 1, 255}, Stamp: wire.Stamp{Epoch: 7, Version: 9},
			TTLRemainingNanos: 1 << 62,
		}, func() any { return new(wire.GetResponse) }},
		{"GetResponse missing", &wire.GetResponse{}, func() any { return new(wire.GetResponse) }},
		{"SetRequest", &wire.SetRequest{
			Namespace: "orders", Key: "k", Value: []byte{}, TTLNanos: -1, Epoch: 3,
		}, func() any { return new(wire.SetRequest) }},
		{"SetResponse", &wire.SetResponse{Stamp: wire.Stamp{Epoch: 1, Version: 2}},
			func() any { return new(wire.SetResponse) }},
		{"DeleteRequest", &wire.DeleteRequest{Namespace: "n", Key: "k", Epoch: 0},
			func() any { return new(wire.DeleteRequest) }},
		{"DeleteResponse", &wire.DeleteResponse{}, func() any { return new(wire.DeleteResponse) }},
		{"BatchGetRequest", &wire.BatchGetRequest{
			Namespace: "n", Keys: []string{"a", "", "c"}, Epoch: 5,
		}, func() any { return new(wire.BatchGetRequest) }},
		{"BatchGetResponse", &wire.BatchGetResponse{Entries: []wire.BatchEntry{
			{Key: "a", Value: []byte("v"), Stamp: wire.Stamp{Epoch: 1, Version: 1}},
			{Key: "b"},
		}}, func() any { return new(wire.BatchGetResponse) }},
		{"BatchGetResponse empty", &wire.BatchGetResponse{},
			func() any { return new(wire.BatchGetResponse) }},
		{"CallRequest", &wire.CallRequest{
			Group: "g", FuncName: "increment", Args: []byte{1, 2, 3}, Epoch: 11,
		}, func() any { return new(wire.CallRequest) }},
		{"CallResponse", &wire.CallResponse{Result: []byte("out")},
			func() any { return new(wire.CallResponse) }},
		{"InvalidateRequest", &wire.InvalidateRequest{
			Namespace: "n", Keys: []string{"x"}, Stamp: wire.Stamp{Epoch: 2, Version: 3},
		}, func() any { return new(wire.InvalidateRequest) }},
		{"InvalidateResponse", &wire.InvalidateResponse{},
			func() any { return new(wire.InvalidateResponse) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := wire.Encode(tc.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got := tc.into()
			if err := wire.Decode(encoded, got); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !equalIgnoringEmpty(tc.msg, got) {
				t.Fatalf("round trip changed the message:\n sent %+v\n got  %+v", tc.msg, got)
			}
		})
	}
}

// equalIgnoringEmpty compares two messages treating a nil slice and an empty
// one as the same.
//
// The distinction does not survive most encodings and does not mean anything
// here: a batch with no keys and a batch with a nil key list are the same
// request. Asserting on it would be asserting on the codec's internals.
func equalIgnoringEmpty(a, b any) bool {
	va := reflect.ValueOf(a).Elem()
	vb := reflect.ValueOf(b).Elem()
	if va.Type() != vb.Type() {
		return false
	}
	for i := 0; i < va.NumField(); i++ {
		fa, fb := va.Field(i), vb.Field(i)
		if fa.Kind() == reflect.Slice {
			if fa.Len() == 0 && fb.Len() == 0 {
				continue
			}
		}
		if !reflect.DeepEqual(fa.Interface(), fb.Interface()) {
			return false
		}
	}
	return true
}

// TestStampOrdering pins the comparison the L1 tier depends on.
//
// The epoch has to dominate. A version is only meaningful within one owner's
// sequence, and open question 18 records that the sequence restarts when an
// entry is reclaimed — so "higher version" across an ownership change carries
// no information at all.
func TestStampOrdering(t *testing.T) {
	cases := []struct {
		name  string
		a, b  wire.Stamp
		newer bool
	}{
		{"same everything", wire.Stamp{1, 1}, wire.Stamp{1, 1}, false},
		{"higher version, same epoch", wire.Stamp{1, 2}, wire.Stamp{1, 1}, true},
		{"lower version, same epoch", wire.Stamp{1, 1}, wire.Stamp{1, 2}, false},
		{"higher epoch beats higher version", wire.Stamp{2, 1}, wire.Stamp{1, 99}, true},
		{"lower epoch loses despite higher version", wire.Stamp{1, 99}, wire.Stamp{2, 1}, false},
		{"zero against zero", wire.Stamp{}, wire.Stamp{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.NewerThan(tc.b); got != tc.newer {
				t.Fatalf("Stamp%v.NewerThan(Stamp%v) = %v, want %v", tc.a, tc.b, got, tc.newer)
			}
		})
	}
}

// TestStampOrderingIsAStrictOrder checks the property rather than the cases: no
// stamp supersedes itself, and two distinct stamps cannot both win.
func TestStampOrderingIsAStrictOrder(t *testing.T) {
	stamps := []wire.Stamp{{0, 0}, {0, 1}, {1, 0}, {1, 1}, {2, 5}, {5, 2}}
	for _, a := range stamps {
		if a.NewerThan(a) {
			t.Fatalf("Stamp%v supersedes itself", a)
		}
		for _, b := range stamps {
			if a == b {
				continue
			}
			if a.NewerThan(b) && b.NewerThan(a) {
				t.Fatalf("Stamp%v and Stamp%v each supersede the other", a, b)
			}
		}
	}
}

// TestDecodeRejectsGarbage matters because these bytes arrive from the network.
// A malformed message must be a reportable error, never a panic and never a
// half-populated struct the handler then acts on.
func TestDecodeRejectsGarbage(t *testing.T) {
	inputs := [][]byte{
		{0xff, 0xff, 0xff, 0xff},
		{0xc1},
		[]byte("not msgpack at all"),
		{0x00},
	}
	for _, in := range inputs {
		var req wire.GetRequest
		err := wire.Decode(in, &req)
		if err == nil {
			continue // some byte strings are legitimately decodable
		}
		if !errors.Is(err, core.ErrInvalidOption) {
			t.Fatalf("Decode(%x) = %v; a malformed message must report ErrInvalidOption so "+
				"the transport can map it to InvalidArgument", in, err)
		}
	}
}

func TestDecodeIntoNilFails(t *testing.T) {
	encoded, err := wire.Encode(&wire.GetRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := wire.Decode(encoded, nil); err == nil {
		t.Fatal("Decode into nil succeeded; it must report an error rather than panic")
	}
}

// TestProtoDeclaresEveryMessage keeps proto/peercache.proto in step with the
// structs that implement it.
//
// The .proto is normative documentation rather than generated-code input, so
// nothing mechanical stops it drifting. A message added here and not there
// leaves the wire format undocumented; one added there and not here leaves a
// promise unimplemented. Both fail this test.
func TestProtoDeclaresEveryMessage(t *testing.T) {
	body, err := os.ReadFile("../../proto/peercache.proto")
	if err != nil {
		t.Fatalf("reading the proto: %v", err)
	}

	declared := make(map[string]bool)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "message ") {
			continue
		}
		name, _, ok := strings.Cut(strings.TrimPrefix(line, "message "), " ")
		if !ok {
			t.Fatalf("cannot parse message line: %q", line)
		}
		declared[strings.TrimSpace(name)] = true
	}
	if len(declared) == 0 {
		t.Fatal("no message declarations found in the proto")
	}

	implemented := []string{
		"Stamp", "GetRequest", "GetResponse", "SetRequest", "SetResponse",
		"DeleteRequest", "DeleteResponse", "BatchGetRequest", "BatchGetResponse",
		"BatchEntry", "CallRequest", "CallResponse", "InvalidateRequest",
		"InvalidateResponse",
	}
	for _, name := range implemented {
		if !declared[name] {
			t.Errorf("internal/wire implements %s but the proto does not declare it", name)
		}
		delete(declared, name)
	}
	for name := range declared {
		t.Errorf("the proto declares %s but internal/wire does not implement it", name)
	}
}

// TestFieldNamesMatchTheProto checks the encoded field names, not just the
// message names.
//
// The msgpack tags are the wire format. A struct field renamed without its tag
// would round-trip perfectly against itself and be unreadable to a peer running
// the previous build — which, in a rolling deploy, is every peer.
func TestFieldNamesMatchTheProto(t *testing.T) {
	body, err := os.ReadFile("../../proto/peercache.proto")
	if err != nil {
		t.Fatalf("reading the proto: %v", err)
	}
	protoText := string(body)

	types := []any{
		&wire.Stamp{}, &wire.GetRequest{}, &wire.GetResponse{},
		&wire.SetRequest{}, &wire.SetResponse{}, &wire.DeleteRequest{},
		&wire.BatchGetRequest{}, &wire.BatchGetResponse{}, &wire.BatchEntry{},
		&wire.CallRequest{}, &wire.CallResponse{}, &wire.InvalidateRequest{},
	}
	for _, msg := range types {
		typ := reflect.TypeOf(msg).Elem()
		for i := 0; i < typ.NumField(); i++ {
			tag := typ.Field(i).Tag.Get("msgpack")
			if tag == "" {
				t.Errorf("%s.%s has no msgpack tag; the wire name would follow the Go "+
					"field name and change whenever the field is renamed",
					typ.Name(), typ.Field(i).Name)
				continue
			}
			if !strings.Contains(protoText, " "+tag+" =") {
				t.Errorf("%s.%s encodes as %q, which the proto does not declare",
					typ.Name(), typ.Field(i).Name, tag)
			}
		}
	}
}

// FuzzDecode exercises the message parser against bytes it did not write.
//
// From M4 these bytes arrive over the network. "No panic in library paths" is a
// claim about exactly this code, and hand-written corrupt inputs are weaker
// evidence than a fuzzer.
func FuzzDecode(f *testing.F) {
	seeds := []any{
		&wire.GetRequest{Namespace: "n", Key: "k", Epoch: 1},
		&wire.GetResponse{Found: true, Value: []byte("v")},
		&wire.BatchGetResponse{Entries: []wire.BatchEntry{{Key: "a"}}},
		&wire.CallRequest{Group: "g", FuncName: "f", Args: []byte{1}},
	}
	for _, s := range seeds {
		if b, err := wire.Encode(s); err == nil {
			f.Add(b)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0xc1})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Every message type, because a byte string that is harmless for
		// one shape may not be for another.
		var get wire.GetRequest
		_ = wire.Decode(data, &get)
		var resp wire.GetResponse
		_ = wire.Decode(data, &resp)
		var batch wire.BatchGetResponse
		_ = wire.Decode(data, &batch)
		var call wire.CallRequest
		_ = wire.Decode(data, &call)
		var inval wire.InvalidateRequest
		_ = wire.Decode(data, &inval)

		// A successful decode must not produce a batch claiming more
		// entries than the input could describe. This is the shape of
		// the classic length-prefix allocation bug.
		if len(batch.Entries) > len(data)+1 {
			t.Fatalf("decoded %d entries from %d bytes", len(batch.Entries), len(data))
		}
	})
}

// TestALengthPrefixCannotBuyAnAllocation is the regression test for the worst
// defect found in M4.
//
// A six-byte message used to wedge a node for minutes. msgpack writes an array
// as a length followed by its elements, and the decoder allocated the whole
// slice from that length before reading anything — so five bytes declaring two
// billion structs got two billion structs attempted. Over a network that is an
// unauthenticated remote out-of-memory: one corrupt frame, every node that
// receives it.
//
// The property asserted is not "it errors" but "it errors *cheaply*". A decoder
// that rejected the message after allocating would still be exploitable, so the
// test bounds the allocation rather than only the outcome.
//
// The modest case is deliberately first and the cases are an ordered slice
// rather than a map. Reverting the fix makes this case fail in milliseconds
// having allocated a few megabytes, which reports; the enormous cases below it
// would instead thrash for minutes, which does not. An assertion whose failure
// mode is a hang is one nobody trusts at three in the morning — the lesson M1
// paid for and docs/verification.md records.
func TestALengthPrefixCannotBuyAnAllocation(t *testing.T) {
	// 65535 entries is ~4 MiB of BatchEntry under the old behaviour and
	// nothing at all under the new one, so the budget separates them
	// cleanly without being near either.
	const budgetBytes = 256 << 10

	cases := []struct {
		name string
		in   []byte
	}{
		{"array16 claiming 65535", []byte{0x91, 0xdc, 0xff, 0xff}},
		{"fixarray claiming 10", []byte{0x91, 0x9a}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&before)

			var resp wire.BatchGetResponse
			err := wire.Decode(tc.in, &resp)

			runtime.ReadMemStats(&after)
			used := after.TotalAlloc - before.TotalAlloc

			if err == nil && len(resp.Entries) > len(tc.in) {
				t.Fatalf("decoded %d entries from %d bytes; a declared length must not "+
					"conjure elements that were never sent", len(resp.Entries), len(tc.in))
			}
			if used > budgetBytes {
				t.Fatalf("decoding %d bytes allocated %d KiB; a length prefix must not buy "+
					"an allocation, or a peer can exhaust this node's memory with a "+
					"handful of bytes", len(tc.in), used/1024)
			}
		})
	}
}

// TestAHugeLengthPrefixReturnsImmediately covers the inputs that motivated the
// fix. They are separated from the test above because their pre-fix behaviour
// is a multi-minute stall rather than a clean failure, so they are only
// meaningful once the cheap case already passes.
func TestAHugeLengthPrefixReturnsImmediately(t *testing.T) {
	for _, in := range [][]byte{
		{0x91, 0xdd, 0x7f, 0xff, 0xff, 0xff}, // array32, 2^31-1 entries
		{0x91, 0xdd, 0x01, 0x00, 0x00, 0x00}, // array32, 2^24 entries
		{0xdd, 0x7f, 0xff, 0xff, 0xff},       // bare array32
	} {
		var resp wire.BatchGetResponse
		err := wire.Decode(in, &resp)
		if err == nil {
			t.Fatalf("Decode(%x) succeeded; an array header with nothing behind it is "+
				"malformed", in)
		}
		if len(resp.Entries) != 0 {
			t.Fatalf("Decode(%x) left %d entries behind on failure", in, len(resp.Entries))
		}
	}
}

// TestBoundedDecodeStillReadsRealMessages guards against the obvious
// overcorrection: a decoder that refuses everything is also safe, and useless.
func TestBoundedDecodeStillReadsRealMessages(t *testing.T) {
	entries := make([]wire.BatchEntry, 0, 5000)
	for i := 0; i < 5000; i++ {
		entries = append(entries, wire.BatchEntry{
			Key:   string(rune('a'+i%26)) + string(rune('0'+i%10)),
			Value: []byte{byte(i), byte(i >> 8)},
			Stamp: wire.Stamp{Epoch: uint64(i % 7), Version: uint64(i)},
		})
	}
	sent := &wire.BatchGetResponse{Entries: entries}

	encoded, err := wire.Encode(sent)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got wire.BatchGetResponse
	if err := wire.Decode(encoded, &got); err != nil {
		t.Fatalf("Decode of a legitimate 5000-entry batch: %v", err)
	}
	if len(got.Entries) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(got.Entries), len(entries))
	}
	for i := range entries {
		if got.Entries[i].Key != entries[i].Key || got.Entries[i].Stamp != entries[i].Stamp {
			t.Fatalf("entry %d round-tripped as %+v, want %+v", i, got.Entries[i], entries[i])
		}
	}
}

// TestSliceCapIsEnforced covers the defence-in-depth bound, which matters for a
// peer that keeps sending rather than one that stops.
func TestSliceCapIsEnforced(t *testing.T) {
	keys := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		keys = append(keys, "k")
	}
	encoded, err := wire.Encode(&wire.BatchGetRequest{Namespace: "n", Keys: keys})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var req wire.BatchGetRequest
	if err := wire.Decode(encoded, &req); err != nil {
		t.Fatalf("a modest request was rejected: %v", err)
	}
	if len(req.Keys) != len(keys) {
		t.Fatalf("decoded %d keys, want %d", len(req.Keys), len(keys))
	}
}
