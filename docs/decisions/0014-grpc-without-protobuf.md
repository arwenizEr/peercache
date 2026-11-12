# 0014 — gRPC as the transport, without protobuf as the codec

**Status:** accepted
**Milestone:** M4
**Deviates from:** docs/design.md §4, which says "gRPC service per `proto/`"

## Context

docs/design.md's M4 line reads "gRPC service per `proto/`", which ordinarily means a
`.proto` file, a protoc invocation, generated `.pb.go` in the tree, and
`google.golang.org/protobuf` as a direct dependency.

Two facts make that a worse trade here than it looks.

**Every pod runs the same binary.** CLAUDE.md non-negotiable 3 states this as an
assumption the whole design rests on — it is why functions are registered by
name rather than serialised. Protobuf's principal payoffs are cross-language
clients and safe schema evolution between mixed versions. This system has no
other language and, by its own rules, no mixed schema: a rolling upgrade
replaces every pod with the same build.

**A codec already exists and is better tested than a new one would be.**
`internal/codec` was built in M1: a registry, msgpack, LZ4 framing, and two fuzz
targets that have run millions of executions. Introducing protobuf would mean
two serialisation stacks in one binary, one of them exercised only on the peer
path.

Against that, the cost of codegen is not zero: protoc is a build-time
dependency that CI must install and every contributor must have, or generated
code must be committed and kept in step by convention.

## Decision

Use gRPC as the transport and not as the codec.

- `proto/peercache.proto` is committed and is **normative documentation**. It
  describes the service, the method names and the message layout in one
  reviewable place. It is not compiled and there is no `.pb.go` in the tree.
- The gRPC service is registered from a hand-written `grpc.ServiceDesc`. Every
  method has the same shape — one opaque request, one opaque response — so all
  of them share a single handler that recovers the method name from the stream.
  This is what protoc would emit for seven identical unary methods, minus seven
  copies of the same function.
- A passthrough `encoding.CodecV2` registered under the content subtype
  `peercache.raw` carries the bodies. Message bodies are encoded by
  `internal/wire`, which uses the M1 codec.

What this keeps from gRPC is everything that made it the right choice: the
connection pool, keepalive, mTLS, HTTP/2 flow control, and deadline propagation
via `grpc-timeout`. What it declines is protobuf's wire format.

## Consequences

**The `.proto` can drift from the code, so two tests stop it.**
`TestProtoMatchesMethodConstants` asserts that the rpc entries and the method
constants name the same set; `TestProtoDeclaresEveryMessage` and
`TestFieldNamesMatchTheProto` assert the same for messages and for the encoded
field names. Adding an rpc to one without the other fails a test rather than
producing a peer that answers "unknown method" in production. This is weaker
than codegen, where drift is impossible, and it is the main thing given up.

**The content subtype is deliberately not "proto".** gRPC selects a codec by the
subtype in the Content-Type header. Naming ours distinctly means a peer speaking
real protobuf and a peer speaking this one cannot half-understand each other:
the mismatch surfaces as an unsupported-codec error on the first request rather
than as a garbled body.

**No cross-language client is possible without writing one.** A consumer in
another language would have to implement the msgpack layout in the `.proto`
rather than run protoc against it. If that ever becomes a requirement, the
change is contained: swap the codec and generate the messages, leaving the
service, the method names and every Go-level interface as they are.

**One dependency avoided, one already present.** `google.golang.org/protobuf`
arrives transitively with grpc-go regardless; this decision keeps it out of the
*direct* dependency set and keeps protoc out of the build entirely. Per
CLAUDE.md's dependency budget, grpc-go itself is the addition, and it is one the
spec names.

## Alternatives rejected

**Full protobuf with committed generated code.** The conventional answer, and
the one to adopt if a second language ever appears. Rejected for M4 because it
buys schema evolution this system has ruled out, adds a second codec beside a
fuzzed one, and puts a generator between the source and the binary for no
benefit that could be named.

**Protobuf with protoc run in CI.** Same benefits, plus a build that fails on a
machine without protoc. Rejected for the same reasons and one more.

**No `.proto` at all, Go structs as the only description.** Honest, and slightly
simpler. Rejected because the wire format is the one part of this system another
team might have to read without reading the Go, and a single file describing it
is worth keeping even when nothing compiles it.
