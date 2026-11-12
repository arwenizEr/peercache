# 0008 — Compression is opt-in, per namespace, above a threshold

**Status:** accepted, M1
**Affects:** `internal/codec`, `Namespace`

## The gap in the spec

docs/design.md section 4 (M1) requires "Compression: LZ4, applied above a configurable
size threshold". Section 3.1 defines the namespace options and contains no
threshold, and no compression option at all.

Something had to be invented. This records what, and why it defaults to off.

## The decision

```go
func WithCompression(name string, threshold int) NamespaceOption
```

Unset by default. A namespace without it stores values verbatim with no header
at all. A namespace with it frames every value with a one-byte header and
compresses those at or above the threshold.

## Why off by default

Measured on Machine A, reading a 4 KiB compressible value:

| | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| plain | 72.2 | 0 | 0 |
| lz4 | 5023 | 4864 | 1 |

Seventy times slower, and it allocates.

That is not an argument against compression — it is an argument against
compression being a default in a library whose stated premise is sub-microsecond
local reads, and whose M1 acceptance criterion is a p99 under one microsecond.
Turning it on globally would have made the acceptance number a measurement of
LZ4 rather than of the cache. Compression is a good trade for large, cold,
compressible values and a bad one for a forty-byte counter, and only the
application knows which it has.

The threshold exists for the same reason within a namespace: most cached values
are far too small to benefit, and paying a compress and a decompress to save
nothing is a pure loss.

## Framing

A stored value may or may not be compressed, so the reader has to be able to
tell. A one-byte header records which, followed by a uvarint original length
when compressed.

Framing is all-or-nothing per namespace. Mixing framed and unframed values in
one namespace would mean guessing at read time whether a leading zero byte is a
header or data, and a cache that guesses eventually guesses wrong. So a
namespace without compression pays no header byte and no branch, and one with
compression frames everything including values too small to compress.

Two further rules, both about not making things worse:

- A payload that does not shrink is stored raw. Paying to make a value bigger
  and then paying again to decompress it is a pure loss, and incompressible
  payloads are common.
- A frame declaring an implausible decompressed size is rejected rather than
  allocated, so a corrupt header cannot make the process allocate arbitrarily.

LZ4 block format is used rather than frame format: no per-stream header, no
checksum, no magic bytes. The framing here already records everything the frame
format's header would, so it would be redundant overhead on every value.

## Interaction with the codec registry

`WithCodec` and `WithCompression` are independent. The codec registry (JSON,
MsgPack) is built and tested in M1 but has nothing to encode yet, because
docs/design.md section 3.1 defines a `[]byte` API and no typed one — see the M1 plan.
`WithCodec` is validated at construction so an unregistered name fails at
startup, which is the point of naming a codec in configuration.

## Reversal

Entirely additive. Removing the option, or changing the default, only affects
namespaces that opted in.
