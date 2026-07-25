// Package codec encodes and decodes namespace values.
//
// A registry maps a codec name to an implementation, so a namespace names its
// codec in configuration rather than binding to one at compile time. JSON and
// MsgPack ship in v1; the registry hook exists so that Protobuf and CBOR can be
// added without touching this package's callers, and they are deliberately not
// in v1.
//
// Compression is applied above a configurable size threshold, because
// compressing a forty-byte counter costs more than it saves. LZ4 ships in v1;
// Snappy and Zstd are deferred behind the same interface.
//
// Milestone: M1. This package is empty until then.
package codec
