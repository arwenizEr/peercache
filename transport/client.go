package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/arwenizEr/peercache/internal/core"
)

// DefaultDialTimeout bounds establishing a connection to a peer.
const DefaultDialTimeout = 2 * time.Second

// DefaultMaxMessageBytes caps a single request or response at 16 MiB.
//
// It is a guard against a malformed length prefix turning into an allocation,
// not a considered capacity limit. A batch large enough to approach it should
// have been split by the caller; the router exists to do that.
const DefaultMaxMessageBytes = 16 << 20

// Dial implements [core.Transport].
//
// Connections are pooled per node ID. gRPC reconnects underneath a
// [grpc.ClientConn], so a peer that restarts is recovered without redialing —
// which is why the pool is keyed by identity rather than by address, and why
// Dial does not itself wait for the connection to come up.
func (t *Transport) Dial(node core.Node) (core.PeerClient, error) {
	if node.IsZero() {
		return nil, fmt.Errorf("transport: cannot dial the zero node: %w", core.ErrInvalidOption)
	}
	if node.Addr == "" {
		return nil, fmt.Errorf("transport: node %s has no address: %w", node.ID, core.ErrInvalidOption)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, fmt.Errorf("transport: closed: %w", core.ErrClosed)
	}
	if c, ok := t.clients[node.ID]; ok && c.addr == node.Addr {
		t.mu.Unlock()
		return c, nil
	}
	// A node under a known ID at a new address has moved. Replace the
	// entry rather than reuse it: the old ClientConn would keep trying to
	// reconnect to an address nobody is listening on.
	stale := t.clients[node.ID]
	t.mu.Unlock()

	if stale != nil {
		_ = stale.Close()
	}

	conn, err := t.connect(node)
	if err != nil {
		return nil, err
	}
	c := &client{transport: t, node: node.Clone(), addr: node.Addr, conn: conn}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.Close()
		return nil, fmt.Errorf("transport: closed: %w", core.ErrClosed)
	}
	// Another goroutine may have dialled the same node while this one was
	// connecting. Keep whichever landed first so that the pool holds one
	// connection per peer, and discard this one.
	if existing, ok := t.clients[node.ID]; ok && existing.addr == node.Addr {
		t.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	t.clients[node.ID] = c
	t.mu.Unlock()
	return c, nil
}

// connect builds the ClientConn. It does not block on the connection becoming
// ready: gRPC dials lazily and the first Invoke carries a deadline, so waiting
// here would only move the failure earlier without making it more informative.
func (t *Transport) connect(node core.Node) (*grpc.ClientConn, error) {
	creds := insecure.NewCredentials()
	if t.opts.TLS != nil {
		cfg, err := t.opts.TLS.clientConfig(node)
		if err != nil {
			return nil, fmt.Errorf("transport: client TLS for %s: %w", node.ID, err)
		}
		creds = credentials.NewTLS(cfg)
	}

	conn, err := grpc.NewClient(node.Addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(
			grpc.CallContentSubtype(codecName),
			grpc.MaxCallRecvMsgSize(t.opts.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(t.opts.MaxMessageBytes),
		),
		grpc.WithKeepaliveParams(t.opts.Keepalive.clientParams()),
	)
	if err != nil {
		return nil, fmt.Errorf("transport: dialling %s at %s: %w",
			node.ID, node.Addr, errors.Join(err, core.ErrNodeUnreachable))
	}
	return conn, nil
}

// client is one pooled connection to one peer.
type client struct {
	transport *Transport
	node      core.Node
	addr      string
	conn      *grpc.ClientConn

	closeOnce sync.Once
}

// Invoke implements [core.PeerClient].
//
// It returns when ctx is done, whatever the network is doing. gRPC enforces
// this itself — the call is bound to the context — so the guarantee is not
// reimplemented here, only relied upon and asserted by
// TestInvokeReturnsWhenTheContextExpires.
func (c *client) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Identify the caller by node ID. The peer address gRPC can see names a
	// connection, not a cluster member.
	ctx = metadata.AppendToOutgoingContext(ctx, metadataFromNode, c.transport.self.ID)

	in := &rawMessage{payload: req}
	out := &rawMessage{}
	if err := c.conn.Invoke(ctx, method, in, out); err != nil {
		return nil, fromStatus(err, c.node)
	}
	return out.payload, nil
}

// Close implements [core.PeerClient]. It is idempotent.
//
// Closing a pooled client removes it from the pool, so a later Dial for the
// same node builds a fresh connection rather than handing out a closed one.
func (c *client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.transport.mu.Lock()
		if pooled, ok := c.transport.clients[c.node.ID]; ok && pooled == c {
			delete(c.transport.clients, c.node.ID)
		}
		c.transport.mu.Unlock()
		err = c.conn.Close()
	})
	return err
}

// toStatus maps a handler error onto a gRPC status.
//
// The sentinel is carried in the message rather than only in the code, because
// codes are a small fixed set and the sentinels are the library's contract.
// fromStatus reverses it. A sentinel that survives the round trip is what lets
// a caller write errors.Is(err, ErrNotOwner) against a remote node.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	// Unknown, not Internal, for an error this mapping does not recognise:
	// it is the handler's own failure — a loader whose backend is down — and
	// the peer did answer. gRPC raises Internal itself for a broken stream,
	// which fromStatus reads as unreachable; sharing that code made every
	// application error look like a network fault.
	code := codes.Unknown
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, core.ErrNotFound):
		code = codes.NotFound
	case errors.Is(err, core.ErrNotOwner):
		code = codes.FailedPrecondition
	case errors.Is(err, core.ErrStaleEpoch):
		code = codes.Aborted
	case errors.Is(err, core.ErrActivationPending):
		code = codes.Unavailable
	case errors.Is(err, core.ErrGroupOverloaded):
		code = codes.ResourceExhausted
	case errors.Is(err, core.ErrFuncNotRegistered):
		code = codes.Unimplemented
	case errors.Is(err, core.ErrInvalidOption):
		code = codes.InvalidArgument
	case errors.Is(err, core.ErrClosed):
		code = codes.Unavailable
	case errors.Is(err, core.ErrCodec):
		// Like ErrUnderReplicated: no gRPC code means it, so Internal plus
		// the sentinel's text, recovered by fromStatus.
		code = codes.Internal
	case errors.Is(err, core.ErrNotAnInteger):
		// The peer answered and refused; no gRPC code says why. Internal
		// plus the sentinel's text, recovered by fromStatus.
		code = codes.Internal
	case errors.Is(err, core.ErrUnderReplicated):
		// No gRPC code means "written, but not to enough nodes", and
		// borrowing one would mislead anything that reads codes alone.
		// Internal plus the sentinel's text, recovered by fromStatus.
		code = codes.Internal
	}
	return status.Error(code, err.Error())
}

// fromStatus turns a gRPC status back into a library error.
//
// Transport-level failures — the peer is down, the connection broke — become
// [core.ErrNodeUnreachable]. Handler failures are reconstructed from the code
// so that errors.Is works across the hop.
func fromStatus(err error, node core.Node) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	var sentinel error
	switch st.Code() {
	case codes.DeadlineExceeded:
		return fmt.Errorf("transport: %s: %s: %w", node.ID, st.Message(), context.DeadlineExceeded)
	case codes.Canceled:
		return fmt.Errorf("transport: %s: %s: %w", node.ID, st.Message(), context.Canceled)
	case codes.NotFound:
		sentinel = core.ErrNotFound
	case codes.FailedPrecondition:
		sentinel = core.ErrNotOwner
	case codes.Aborted:
		sentinel = core.ErrStaleEpoch
	case codes.ResourceExhausted:
		sentinel = core.ErrGroupOverloaded
	case codes.Unimplemented:
		sentinel = core.ErrFuncNotRegistered
	case codes.InvalidArgument:
		sentinel = core.ErrInvalidOption
	case codes.Unavailable:
		// Ambiguous on purpose, and resolved towards the transport. gRPC
		// raises Unavailable for a broken connection, and the handler
		// raises it for a deferred activation. A caller told
		// "unreachable" retries elsewhere; a caller told "pending"
		// retries here. Guessing "pending" for a genuinely dead peer
		// would pin a caller to a node that is never coming back, so
		// the message decides and the default is unreachable.
		if containsSentinel(st.Message(), core.ErrActivationPending.Error()) {
			sentinel = core.ErrActivationPending
		} else {
			sentinel = core.ErrNodeUnreachable
		}
	case codes.Unknown:
		// The handler failed for a reason of its own and said so. No
		// sentinel: there is nothing to match on but the message, which
		// is kept whole. Not unreachable — the peer answered.
		return fmt.Errorf("transport: %s: %s", node.ID, st.Message())
	case codes.Internal:
		// gRPC raises Internal itself for a broken stream, so it stays
		// unreachable unless the message names the sentinel.
		if containsSentinel(st.Message(), core.ErrUnderReplicated.Error()) {
			sentinel = core.ErrUnderReplicated
		} else if containsSentinel(st.Message(), core.ErrCodec.Error()) {
			sentinel = core.ErrCodec
		} else if containsSentinel(st.Message(), core.ErrNotAnInteger.Error()) {
			sentinel = core.ErrNotAnInteger
		} else {
			sentinel = core.ErrNodeUnreachable
		}
	default:
		sentinel = core.ErrNodeUnreachable
	}
	return fmt.Errorf("transport: %s: %s: %w", node.ID, st.Message(), sentinel)
}

// containsSentinel reports whether msg carries the text of a sentinel error.
func containsSentinel(msg, sentinel string) bool {
	return len(sentinel) > 0 && len(msg) >= len(sentinel) && indexOf(msg, sentinel) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
