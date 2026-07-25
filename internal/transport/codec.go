package transport

import (
	"fmt"

	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	"github.com/peercache/peercache/internal/core"
)

// codecName is the content subtype this transport negotiates.
//
// It is deliberately not "proto". gRPC selects a codec by the subtype in the
// Content-Type header, so naming ours distinctly means a peer speaking real
// protobuf and a peer speaking this one cannot silently half-understand each
// other: the mismatch surfaces as an unsupported-codec error at the first
// request rather than as a garbled body.
const codecName = "peercache.raw"

// rawCodec passes message bodies through untouched.
//
// The bodies are already encoded — internal/codec framed them, and the
// transport's job is to move bytes, not to interpret them. Registering a codec
// that does nothing is what lets gRPC carry an opaque payload while still
// providing the parts of gRPC actually wanted here: connection pooling,
// keepalive, mTLS, flow control and deadline propagation.
//
// See docs/decisions/0014-grpc-without-protobuf.md.
type rawCodec struct{}

// Marshal implements [encoding.CodecV2].
func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.(*rawMessage)
	if !ok {
		return nil, fmt.Errorf("transport: cannot marshal %T: %w", v, core.ErrInvalidOption)
	}
	// The payload is owned by the caller and must not be retained, so it is
	// copied into a pooled buffer that gRPC will release when the write
	// completes. Handing gRPC the caller's slice directly would alias a
	// buffer the caller is free to reuse the moment Invoke returns.
	if len(b.payload) == 0 {
		return nil, nil
	}
	buf := mem.Copy(b.payload, mem.DefaultBufferPool())
	return mem.BufferSlice{buf}, nil
}

// Unmarshal implements [encoding.CodecV2].
func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	b, ok := v.(*rawMessage)
	if !ok {
		return fmt.Errorf("transport: cannot unmarshal into %T: %w", v, core.ErrInvalidOption)
	}
	// Materialise rather than alias: gRPC recycles the buffers behind data
	// once this returns, and the handler is entitled to hold the bytes for
	// the duration of its call.
	b.payload = data.Materialize()
	return nil
}

// Name implements [encoding.CodecV2].
func (rawCodec) Name() string { return codecName }

// rawMessage is the single message type on the wire. Every method takes one and
// returns one; what the bytes mean is the caller's business.
type rawMessage struct {
	payload []byte
}

func init() {
	// Registering globally is how gRPC's codec lookup works — there is no
	// per-server registry. The name is unique to this project, so this
	// cannot collide with a codec another library registers.
	encoding.RegisterCodecV2(rawCodec{})
}
