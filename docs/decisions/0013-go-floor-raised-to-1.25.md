# 0013 — The Go floor moves from 1.23 to 1.25

**Status:** accepted, M3 — **deviates from docs/design.md**
**Affects:** `go.mod`, and every consumer of this library

## The deviation

docs/design.md's engineering rules open with "Go 1.23+". This milestone raises the
floor to **1.25**. That is a real change to the contract with anyone embedding
the library, so it is recorded rather than absorbed silently.

## Why it is unavoidable

docs/design.md section 4 (M3) requires "Gossip membership via memberlist".
`hashicorp/memberlist` pulls in `miekg/dns`, `golang.org/x/net`,
`golang.org/x/sys` and `golang.org/x/sync`, and those have collectively moved
past 1.23:

| Module | Requires |
|---|---|
| `golang.org/x/net` | 1.25 |
| `golang.org/x/sys` | 1.25 |
| `miekg/dns` | 1.24 |
| `golang.org/x/sync` | 1.24 |
| `hashicorp/go-msgpack/v2` | 1.24 |

This is not a matter of picking an older memberlist. Both v0.5.0 (whose own
directive is `go 1.12`) and v0.5.1 resolve to the same floor, because the
constraint comes from the graph beneath them rather than from memberlist itself.

## The alternative, and why it was rejected

The floor *could* be held at 1.23 by pinning `x/net`, `x/sys`, `x/sync`,
`miekg/dns` and `go-msgpack` to older releases.

That was rejected. `x/net` and `x/sys` are exactly the modules that receive
security patches, and pinning them backwards to satisfy a compiler-version
promise inverts the priorities: it trades a real, recurring exposure for a
cosmetic constraint. It also creates a standing maintenance burden — five pins
that fight every future `go get` and that someone will eventually bump without
noticing what they were for.

A version floor is a cost to users; an unpatched networking stack is a cost to
users *and* a hazard. The floor is the cheaper of the two.

## What this makes moot, and what it does not

Decision 0006 rejected `golang.org/x/sync/singleflight` partly because its
releases required Go 1.25. Memberlist now drags `x/sync` into the graph anyway,
so **that specific argument no longer holds**.

The other two reasons stand unchanged and are the ones that mattered:

- it re-panics in every waiter when the shared call panics, which is
  unacceptable for user-supplied loader code;
- it offers no way to run the loader under a context detached from whichever
  caller arrived first.

So the hand-rolled implementation stays, on its merits rather than on a version
technicality. Decision 0006 has been annotated rather than rewritten, because
the reasoning at the time was sound on the information available.

## What would reverse this

Nothing likely. Floors move in one direction, and 1.25 is current. If a future
milestone finds a gossip implementation with a leaner graph, revisit — but
memberlist is what docs/design.md names.
