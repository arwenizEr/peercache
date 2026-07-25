// Package wire defines the messages peers exchange and how they are encoded.
//
// It sits between internal/transport, which moves opaque bytes, and the cache,
// which knows what the bytes mean. Keeping it separate means the message shapes
// can be round-tripped and fuzzed without a socket, and that adding a field is
// a change in one place rather than in a handler and a caller that must agree.
//
// The layout is documented in proto/peercache.proto. That file is normative
// documentation rather than the source of generated code; see
// docs/decisions/0014-grpc-without-protobuf.md. The structs here are the
// implementation of it, and TestFieldNamesMatchTheProto keeps the two in step.
package wire
