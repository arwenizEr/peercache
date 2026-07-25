// Package codec encodes and decodes namespace values, and compresses them.
//
// # Registry
//
// A registry maps a codec name to an implementation, so a namespace names its
// codec in configuration rather than binding to one at compile time. JSON and
// MsgPack ship in v1. The registry hook exists so that Protobuf and CBOR can be
// added without touching callers, and they are deliberately not in v1.
//
// Registration happens at package init and is process-global, which is safe
// only because it is startup-only: every pod runs the same binary and therefore
// registers the same names. A codec registered after the first namespace is
// constructed is a bug the registry cannot detect, which is why [Register]
// refuses to replace an existing name.
//
// # Framing
//
// Compression is applied above a configurable size threshold, because
// compressing a forty-byte counter costs more than it saves. That means a
// stored value may or may not be compressed, and the reader has to be able to
// tell. [Frame] prepends a one-byte header for exactly that purpose; [Unframe]
// reverses it.
//
// The header is one byte and not a variable-length field on purpose: it is paid
// on every framed value, including the ones too small to compress.
//
// # What is not here
//
// Compression is off by default at the namespace level. A decompress on every
// read would put LZ4 on the hot path of a cache whose entire premise is
// sub-microsecond local reads. It is opt-in, per namespace, with a threshold.
package codec
