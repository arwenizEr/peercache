package wire

import (
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/peercache/peercache/internal/core"
)

// MaxSliceElements caps how many elements a decoded message may contain.
//
// It is defence in depth rather than the actual protection — the decoders below
// never allocate ahead of the input, so a declared length costs nothing until
// the elements really arrive. The cap bounds how long a peer can keep a decoder
// busy with a stream that does keep arriving.
//
// A million entries in one batch is far beyond anything the router produces;
// SPEC.md §3.4's batching exists precisely to keep requests small.
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
	if err := enc.EncodeArrayLen(3); err != nil {
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
	return enc.EncodeUint(r.Epoch)
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (r *BatchGetRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if r.Namespace, err = dec.DecodeString(); err != nil {
		return err
	}
	if r.Keys, err = decodeStrings(dec); err != nil {
		return err
	}
	r.Epoch, err = dec.DecodeUint64()
	return err
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r BatchGetResponse) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(1); err != nil {
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
	return nil
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (r *BatchGetResponse) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	r.Entries, err = decodeEntries(dec)
	return err
}

// EncodeMsgpack implements [msgpack.CustomEncoder].
func (r InvalidateRequest) EncodeMsgpack(enc *msgpack.Encoder) error {
	if err := enc.EncodeArrayLen(3); err != nil {
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
	return enc.Encode(&r.Stamp)
}

// DecodeMsgpack implements [msgpack.CustomDecoder].
func (r *InvalidateRequest) DecodeMsgpack(dec *msgpack.Decoder) error {
	if _, err := dec.DecodeArrayLen(); err != nil {
		return err
	}
	var err error
	if r.Namespace, err = dec.DecodeString(); err != nil {
		return err
	}
	if r.Keys, err = decodeStrings(dec); err != nil {
		return err
	}
	return dec.Decode(&r.Stamp)
}

var (
	_ msgpack.CustomEncoder = BatchGetRequest{}
	_ msgpack.CustomDecoder = (*BatchGetRequest)(nil)
	_ msgpack.CustomEncoder = BatchGetResponse{}
	_ msgpack.CustomDecoder = (*BatchGetResponse)(nil)
	_ msgpack.CustomEncoder = InvalidateRequest{}
	_ msgpack.CustomDecoder = (*InvalidateRequest)(nil)
)
