package core

import (
	"context"
	"net"
)

// Transport is the peer-to-peer message fabric.
//
// SPEC.md §3.6 names this interface and its Dial/Serve/Close shape but never
// defines PeerClient or Handler. The definitions below are this project's
// choice; they are recorded as an open question in docs/decisions/open.md and
// may change in M4 when the gRPC implementation lands.
//
// The shape is deliberately byte-oriented rather than one Go method per RPC.
// gRPC can implement it, an in-process fake can implement it in a few hundred
// lines, and adding an RPC in a later milestone does not widen this interface.
type Transport interface {
	// Dial returns a client for node. Implementations are expected to pool
	// connections, so repeated Dials for the same node are cheap and the
	// returned clients may share a connection.
	Dial(node Node) (PeerClient, error)

	// Serve begins accepting requests and dispatching them to h. It blocks
	// until the transport is closed or the listener fails.
	//
	// The [net.Listener] argument comes from SPEC.md §3.6 and suits the
	// gRPC implementation. An in-process transport has nothing to listen
	// on and accepts a nil listener.
	Serve(l net.Listener, h Handler) error

	// Close shuts down the server side and every pooled client. In-flight
	// requests fail rather than hang.
	Close() error
}

// PeerClient is a handle to one remote node.
//
// Invoke carries the caller's deadline to the remote handler. Every
// implementation must guarantee that Invoke returns when ctx is done, whatever
// the state of the network — a hang is a worse failure than an error, because
// it consumes the caller's deadline budget silently.
type PeerClient interface {
	Invoke(ctx context.Context, method string, req []byte) ([]byte, error)
	Close() error
}

// Handler is the server side of the transport: it executes a request that
// arrived from a peer.
//
// from identifies the caller. Implementations must not retain req after
// returning; the transport may reuse the buffer.
type Handler interface {
	Handle(ctx context.Context, from Node, method string, req []byte) ([]byte, error)
}

// HandlerFunc adapts a function to [Handler].
type HandlerFunc func(ctx context.Context, from Node, method string, req []byte) ([]byte, error)

// Handle implements [Handler].
func (f HandlerFunc) Handle(ctx context.Context, from Node, method string, req []byte) ([]byte, error) {
	return f(ctx, from, method, req)
}
