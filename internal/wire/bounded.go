package wire

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/arwenizEr/peercache/internal/core"
)

// MaxSliceElements caps how many elements a decoded message may contain.
//
// It is defence in depth rather than the actual protection — the decoders below
// never allocate ahead of the input, so a declared length costs nothing until
// the elements really arrive. The cap bounds how long a peer can keep a decoder
// busy with a stream that does keep arriving.
//
// A million entries in one batch is far beyond anything the router produces;
// the router's batching exists precisely to keep requests small.
const MaxSliceElements = 1 << 20

// Why these decoders exist
//
// A six-byte message used to hang a node for minutes. msgpack encodes an array
// as a length followed by elements, and vmihailenco/msgpack v5 pre-allocates
// the whole slice from that length before reading a single element — so
// `dd 7f ff ff ff` asks for two billion structs and gets them attempted.
//
// The library does have a limit for this, and it is unreachable. decode_slice.go
// computes `noLimit := d.flags&disableAllocLimitFlag != 1`, and since that flag
// is not bit zero the expression is true whatever the flag is set to. Its
// DisableAllocLimit option therefore cannot switch the limit on, and maps and
// byte strings are guarded while slices of structs are not.
//
// Rather than depend on that being fixed, these decoders append as they go. A
// declared length is treated as a hint about intent, never as an instruction to
// allocate: the cost of a malformed message becomes proportional to the bytes
// actually sent, which is the property that makes it harmless.
//
// Found by FuzzDecode in under two seconds, on the first run.

// decodeStrings reads a msgpack array of strings without trusting its length.
func decodeStrings(dec *msgpack.Decoder) ([]string, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}

	// Deliberately not make([]string, 0, n): n is attacker-controlled and
	// the whole point is to allocate for what arrives, not what is claimed.
	var out []string
	for i := 0; i < n; i++ {
		s, err := dec.DecodeString()
		if err != nil {
			return nil, fmt.Errorf("wire: element %d of %d: %w", i, n, err)
		}
		out = append(out, s)
	}
	return out, nil
}

// decodeEntries reads a msgpack array of [BatchEntry] under the same rule.
func decodeEntries(dec *msgpack.Decoder) ([]BatchEntry, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}

	var out []BatchEntry
	for i := 0; i < n; i++ {
		var e BatchEntry
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("wire: entry %d of %d: %w", i, n, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// The custom decoders below take over the whole message, so each one has to
// read every field itself. They are written as an array of fields rather than a
// map, which is what the paired encoders emit — matching shapes matter more
// than matching the library's default here, because these two halves are only
// ever used together.

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r BatchGetRequest) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(4); err != nil {
		return err
	}
	if err := enc.EncodeString(r.Namespace); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Keys)); err != nil {
		return err
	}
	for _, k := range r.Keys {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
	}
	if err := enc.EncodeUint(r.Epoch); err != nil {
		return err
	}
	return enc.EncodeBool(r.Load)
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
//
// Three fields is a request from a release before Load; fields past the fourth
// are from a later one and are skipped, so either side of a rolling upgrade
// can read the other.
func (r *BatchGetRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if n < 3 {
		return fmt.Errorf("wire: a batch get request of %d fields", n)
	}
	if r.Namespace, err = dec.DecodeString(); err != nil {
		return err
	}
	if r.Keys, err = decodeStrings(dec); err != nil {
		return err
	}
	if r.Epoch, err = dec.DecodeUint64(); err != nil {
		return err
	}
	if n >= 4 {
		if r.Load, err = dec.DecodeBool(); err != nil {
			return err
		}
	}
	for i := 4; i < n; i++ {
		if err := dec.Skip(); err != nil {
			return err
		}
	}
	return nil
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r BatchGetResponse) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Entries)); err != nil {
		return err
	}
	for i := range r.Entries {
		if err := enc.Encode(&r.Entries[i]); err != nil {
			return err
		}
	}
	if err := enc.EncodeArrayLen(len(r.Misrouted)); err != nil {
		return err
	}
	for _, k := range r.Misrouted {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
//
// One field is a response from a release before Misrouted; fields past the
// second are from a later one and are skipped.
func (r *BatchGetResponse) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if n < 1 {
		return fmt.Errorf("wire: a batch get response of %d fields", n)
	}
	if r.Entries, err = decodeEntries(dec); err != nil {
		return err
	}
	if n >= 2 {
		if r.Misrouted, err = decodeStrings(dec); err != nil {
			return err
		}
	}
	for i := 2; i < n; i++ {
		if err := dec.Skip(); err != nil {
			return err
		}
	}
	return nil
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
//
// Without Entries it is the three fields every release reads, byte for byte.
func (r InvalidateRequest) EncodeMsgpack(enc *msgpack.Encoder) error {
	fields := 3
	if len(r.Entries) > 0 {
		fields = 4
	}
	if err := enc.EncodeArrayLen(fields); err != nil {
		return err
	}
	if err := enc.EncodeString(r.Namespace); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Keys)); err != nil {
		return err
	}
	for _, k := range r.Keys {
		if err := enc.EncodeString(k); err != nil {
			return err
		}
	}
	if err := enc.Encode(&r.Stamp); err != nil {
		return err
	}
	if fields == 3 {
		return nil
	}
	if err := enc.EncodeArrayLen(len(r.Entries)); err != nil {
		return err
	}
	for i := range r.Entries {
		if err := enc.EncodeArrayLen(2); err != nil {
			return err
		}
		if err := enc.EncodeString(r.Entries[i].Key); err != nil {
			return err
		}
		if err := enc.Encode(&r.Entries[i].Stamp); err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
//
// Three fields are a request from a release before Entries, or one with none;
// fields past the fourth are from a later release and are skipped.
func (r *InvalidateRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return err
	}
	if n < 3 {
		return fmt.Errorf("wire: an invalidate request of %d fields", n)
	}
	if r.Namespace, err = dec.DecodeString(); err != nil {
		return err
	}
	if r.Keys, err = decodeStrings(dec); err != nil {
		return err
	}
	if err := dec.Decode(&r.Stamp); err != nil {
		return err
	}
	if n >= 4 {
		if r.Entries, err = decodeInvalidateEntries(dec); err != nil {
			return err
		}
	}
	for i := 4; i < n; i++ {
		if err := dec.Skip(); err != nil {
			return err
		}
	}
	return nil
}

// decodeInvalidateEntries reads a msgpack array of [key, stamp] pairs without
// trusting its length, on the same rule as everything else in this file.
func decodeInvalidateEntries(dec *msgpack.Decoder) ([]InvalidateEntry, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}
	var out []InvalidateEntry
	for i := 0; i < n; i++ {
		m, err := dec.DecodeArrayLen()
		if err != nil {
			return nil, err
		}
		if m < 2 {
			return nil, fmt.Errorf("wire: an invalidate entry of %d fields", m)
		}
		var e InvalidateEntry
		if e.Key, err = dec.DecodeString(); err != nil {
			return nil, err
		}
		if err := dec.Decode(&e.Stamp); err != nil {
			return nil, err
		}
		for j := 2; j < m; j++ {
			if err := dec.Skip(); err != nil {
				return nil, err
			}
		}
		out = append(out, e)
	}
	return out, nil
}

// decodeMessages reads a msgpack array of byte strings without trusting its
// length, on the same rule as everything else in this file.
func decodeMessages(dec *msgpack.Decoder) ([][]byte, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}
	var out [][]byte
	for i := 0; i < n; i++ {
		b, err := dec.DecodeBytes()
		if err != nil {
			return nil, fmt.Errorf("wire: message %d of %d: %w", i, n, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// decodeBatches reads a msgpack array of [GroupBatch] under the same rule.
func decodeBatches(dec *msgpack.Decoder) ([]GroupBatch, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}
	var out []GroupBatch
	for i := 0; i < n; i++ {
		var b GroupBatch
		if err := dec.Decode(&b); err != nil {
			return nil, fmt.Errorf("wire: batch %d of %d: %w", i, n, err)
		}
		out = append(out, b)
	}
	return out, nil
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (b GroupBatch) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := enc.EncodeString(b.Group); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(b.Messages)); err != nil {
		return err
	}
	for _, m := range b.Messages {
		if err := enc.EncodeBytes(m); err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (b *GroupBatch) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if b.Group, err = dec.DecodeString(); err != nil {
		return err
	}
	b.Messages, err = decodeMessages(dec)
	return err
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r SubmitRequest) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := enc.EncodeString(r.FuncName); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Batches)); err != nil {
		return err
	}
	for i := range r.Batches {
		if err := enc.Encode(&r.Batches[i]); err != nil {
			return err
		}
	}
	return enc.EncodeUint(r.Epoch)
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (r *SubmitRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if r.FuncName, err = dec.DecodeString(); err != nil {
		return err
	}
	if r.Batches, err = decodeBatches(dec); err != nil {
		return err
	}
	r.Epoch, err = dec.DecodeUint64()
	return err
}

// decodeFailures reads a submit response's failure list under the same rule.
func decodeFailures(dec *msgpack.Decoder) ([]GroupFailure, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}
	var out []GroupFailure
	for i := 0; i < n; i++ {
		var f GroupFailure
		if err := dec.Decode(&f); err != nil {
			return nil, fmt.Errorf("wire: failure %d of %d: %w", i, n, err)
		}
		out = append(out, f)
	}
	return out, nil
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r SubmitResponse) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(1); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Failures)); err != nil {
		return err
	}
	for i := range r.Failures {
		if err := enc.Encode(&r.Failures[i]); err != nil {
			return err
		}
	}
	return nil
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
//
// # Why this pair exists separately
//
// Every other variable-length message got a bounded decoder at once. This one
// did not, and the reason is the reason it is worth writing down: the first fuzz
// target covered five message types chosen as a representative sample, and
// SubmitResponse was not among the five. The defect the sample was written to
// catch was therefore still present, in the one shape nobody had pointed the
// fuzzer at.
//
// Six bytes reproduce it — 91 dd ff ff ff ff, an array of one containing an
// array32 declaring 4,294,967,295 elements. [GroupFailure] is 32 bytes, so the
// pre-allocation msgpack v5 performs from that declared length asks the
// allocator for 137 GB before reading a single element. Under the race detector
// the process aborts outright; without it, the machine swaps until something
// gives.
//
// A peer sends this. So can anything that can reach the port.
//
// Found by FuzzDecodeEveryMessage, which decodes each input into every one of the
// message types rather than a subset, precisely so that "no input from a peer
// may panic a node" is checked against the word "no".
func (r *SubmitResponse) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	r.Failures, err = decodeFailures(dec)
	return err
}

// decodeCatchUpEntries reads a catch-up page without trusting its length.
func decodeCatchUpEntries(dec *msgpack.Decoder) ([]CatchUpEntry, error) {
	n, err := dec.DecodeArrayLen()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	if n > MaxSliceElements {
		return nil, fmt.Errorf("wire: array of %d elements exceeds the %d cap: %w",
			n, MaxSliceElements, core.ErrInvalidOption)
	}
	var out []CatchUpEntry
	for i := 0; i < n; i++ {
		var e CatchUpEntry
		if err := dec.Decode(&e); err != nil {
			return nil, fmt.Errorf("wire: catch-up entry %d of %d: %w", i, n, err)
		}
		out = append(out, e)
	}
	return out, nil
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r CatchUpResponse) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := enc.EncodeArrayLen(len(r.Entries)); err != nil {
		return err
	}
	for i := range r.Entries {
		if err := enc.Encode(&r.Entries[i]); err != nil {
			return err
		}
	}
	return enc.EncodeBool(r.More)
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (r *CatchUpResponse) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if r.Entries, err = decodeCatchUpEntries(dec); err != nil {
		return err
	}
	r.More, err = dec.DecodeBool()
	return err
}

var (
	_ msgpack.CustomEncoder = CatchUpResponse{}
	_ msgpack.CustomDecoder = (*CatchUpResponse)(nil)
	_ msgpack.CustomEncoder = GroupBatch{}
	_ msgpack.CustomDecoder = (*GroupBatch)(nil)
	_ msgpack.CustomEncoder = SubmitRequest{}
	_ msgpack.CustomDecoder = (*SubmitRequest)(nil)
	_ msgpack.CustomEncoder = SubmitResponse{}
	_ msgpack.CustomDecoder = (*SubmitResponse)(nil)
	_ msgpack.CustomEncoder = BatchGetRequest{}
	_ msgpack.CustomDecoder = (*BatchGetRequest)(nil)
	_ msgpack.CustomEncoder = BatchGetResponse{}
	_ msgpack.CustomDecoder = (*BatchGetResponse)(nil)
	_ msgpack.CustomEncoder = InvalidateRequest{}
	_ msgpack.CustomDecoder = (*InvalidateRequest)(nil)
)
