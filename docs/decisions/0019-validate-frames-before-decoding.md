# 0019 — Validate frames before decoding them

**Status:** accepted
**Milestone:** M8
**Adds:** nothing — `internal/wire/validate.go`, no new dependency

## Context

CLAUDE.md forbids `panic` in library paths, and docs/design.md's M8 hardening asks that
no input from a peer may take a node down. `internal/wire` is the only code here
that interprets bytes it did not write, and from M4 those bytes arrive over the
network from anything that can reach the port.

M4 already found and fixed one instance of the hazard. msgpack encodes a
collection as a length followed by elements, and `vmihailenco/msgpack` v5
pre-allocates the whole slice from that length before reading a single element,
so five bytes could ask for two billion structs. `internal/wire/bounded.go` gave
the collection types custom decoders that append as they go, and `FuzzDecode`
covered five message types as a representative sample.

Two things were wrong with that, and M8 found both.

**The sample was not representative.** `SubmitResponse` was not among the five,
and it was the one message carrying a slice of structs with no custom decoder.
Six bytes — `91 dd ff ff ff ff` — asked for 4,294,967,295 `GroupFailure` values,
137 GB, and got the allocation attempted. The defect M4 was written to eliminate
survived M4 in the single place nobody pointed the fuzzer at.

**The library's guard has a second hole, and bounded decoders cannot close it.**
`Decoder.readN` respects an allocation limit and caps growth at 1 MiB per step.
The byte-string paths — `Decoder.bytes` and `Decoder.bytesPtr`, which every
`[]byte` struct field goes through — do not call that method. They call the
package-level `readN` directly, whose first statement is

```go
b = append(b, make([]byte, n-len(b))...)
```

with `n` taken from the wire. `c6 ff ff ff ff` declares a 4 GiB byte string and
gets a 4 GiB allocation before a byte of payload is read. Every message in this
package with a `[]byte` field is reachable that way: values, call arguments, call
results, replicated payloads, catch-up entries — ten of the twenty-one.

Under `-race` the process aborts outright: ThreadSanitizer fails to map the
shadow memory and the test binary dies with an allocator error rather than a Go
panic. Without it, the machine swaps.

## Decision

Validate the frame's structure once, in `wire.Decode`, before the decoder sees
it.

`validate` walks the msgpack value and checks one thing: that every declared
length is no larger than the bytes remaining behind it. A byte string claiming
4 GiB inside a 6-byte frame is rejected; so is an array claiming four billion
elements, because every element costs at least one byte to encode. After it
returns nil, a decoder may allocate from a declared length as freely as it likes
— the declaration is bounded by the frame, and the frame is bounded by the
transport.

It deliberately does not check that the message *means* anything. Types, field
names and required fields are the decoder's business. This answers one question:
can this message's claims about its own size be true?

### Why not a bounded decoder per message

That was the alternative and it works. It needs eight more encoder/decoder pairs
kept in exact agreement by hand, and a new pair every time a message gains a
byte-string field — a rule enforced by remembering it. The failure mode is
exactly the one that produced this note: a hardening applied to the places
someone thought of, which reads in the commit log like a complete one.

Validating the frame covers every existing message and every future one, in a
place that cannot be forgotten because every decode goes through it.

### Why the collection decoders stay

`bounded.go` is now redundant for allocation safety — the validator rejects those
frames first. It stays because it bounds something else: `MaxSliceElements` caps
how long a peer can keep a decoder busy with a stream that *does* keep arriving,
which is a live connection rather than a single frame, and the validator says
nothing about that. Defence in depth, with the two layers doing different jobs.

`SubmitResponse` got its bounded pair anyway, so that all seven
collection-bearing messages are handled the same way. A single exception, however
well reasoned, is how the original gap happened.

### Depth

The walk is recursive, so a frame nesting as deep as its byte count allows would
put a peer in charge of this node's stack — a panic no `recover` could usefully
catch. `MaxNestingDepth` is 16. The protocol's deepest real shape is a
`SubmitRequest` at five levels — field array, batches array, one batch's field
array, its messages array, a byte string — measured rather than counted by eye,
which is how the "four" first written here was wrong. `TestValidateRejectsUnboundedNesting`
asserts both halves: 4096 levels are refused, and the deepest real message is not.

## Consequences

**Cost.** One linear pass, no allocation: 25 ns for a `GetRequest`, 51 ns for a
256-byte `GetResponse`, 4.1 µs for a hundred-entry `BatchGetResponse` — about 9%
of what decoding the same message costs, and three orders of magnitude below the
175 µs remote round trip it defends. `BenchmarkValidate` measures it and
`docs/benchmarks.md` records it. The type-byte dispatch checks the four
inline-length families first, by high bits, because between them they cover
almost every byte this protocol emits.

**A rejected frame is `core.ErrInvalidOption`**, the same sentinel a malformed
message already produced, which the transport maps to `InvalidArgument`. No
caller learns a new error.

**The fuzz target is now exhaustive rather than sampled.**
`FuzzDecodeEveryMessage` decodes each input into all twenty-one message types and
asserts no collection decoded more elements than the input had bytes.
`TestNoMessageAllocatesFromADeclaredLength` does the same over a fixed table of
hostile shapes with an allocation budget, so the property is checked in CI and
not only during a fuzz run.

**Both fixes were confirmed against the unfixed code**, which is the rule
question 20 records the cost of learning: the exhaustive test was run before the
fix and failed on `CallResponse` at 4 GiB and on four other messages at 1 MiB,
and the probe that found `SubmitResponse` aborted the race build. Neither test
passes on the code it was written against.
