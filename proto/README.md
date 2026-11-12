# proto

gRPC service definitions for the peer transport.

The transport interface it implements is defined in
`internal/core/transport.go`; the gRPC implementation is the `transport`
package. Messages are encoded with msgpack rather than protobuf — see
`docs/decisions/0014-grpc-without-protobuf.md` — so this file documents the
service shape rather than generating code.

`internal/testkit` provides an in-process transport with fault injection that
satisfies the same interface, so most of the suite is testable without a
socket.
