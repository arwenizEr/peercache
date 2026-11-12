// Package transport carries requests between peers.
//
// It implements [core.Transport] over gRPC: a connection pool with keepalive,
// optional mTLS, and deadline propagation from the calling context into the
// remote handler and onward into the group turn.
//
// The single hard requirement is that no call may outlive its context. A hung
// request is worse than a failed one: it consumes the caller's deadline budget
// silently, leaves nothing useful in metrics, and turns one dead peer into a
// pool of blocked goroutines.
//
// The test harness supplies an in-process transport with fault injection; see
// internal/testkit.
package transport
