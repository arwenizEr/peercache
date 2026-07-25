# proto

gRPC service definitions for the peer transport.

Empty until **M4**, which is the milestone that introduces networking. The
transport interface it implements is already defined — see
`internal/core/transport.go` — and its shape is recorded as an open question in
`docs/decisions/open.md`, because SPEC.md section 3.6 names `PeerClient` and
`Handler` without defining them.

Until then, `internal/testkit` provides an in-process transport with fault
injection that satisfies the same interface, so milestones M1 through M3 are
testable without a socket.
