package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/arwenizEr/peercache/internal/core"
)

// metadataFromNode is the header carrying the caller's node ID.
//
// gRPC's peer address identifies a connection, not a cluster member: behind a
// pooled dial the address is the pod IP, which is exactly the thing that
// changes when a pod restarts under a new identity. The handler is told who is
// calling by name, because ownership is derived from the name.
const metadataFromNode = "peercache-from-node"

// metadataTracePrefix prefixes the headers carrying a request's trace
// context, one per entry of the carrier the caller's tracer filled. See
// docs/decisions/0038-trace-propagation.md.
const metadataTracePrefix = "peercache-trace-"

// serviceDesc is the gRPC service, written out rather than generated.
//
// Every method has the same shape — one opaque request in, one opaque response
// out — so they share a single handler that recovers the method name from the
// stream. This is what protoc would emit for six identical unary methods,
// minus six copies of the same function.
func serviceDesc(handlerFor func(string) grpc.MethodHandler) *grpc.ServiceDesc {
	desc := &grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*any)(nil),
		Methods:     make([]grpc.MethodDesc, 0, len(methods)),
		Metadata:    "proto/peercache.proto",
	}
	for _, full := range methods {
		desc.Methods = append(desc.Methods, grpc.MethodDesc{
			MethodName: shortName(full),
			Handler:    handlerFor(full),
		})
	}
	return desc
}

// shortName turns "/peercache.v1.Peer/Get" into "Get", which is the form
// [grpc.ServiceDesc] wants.
func shortName(full string) string {
	for i := len(full) - 1; i >= 0; i-- {
		if full[i] == '/' {
			return full[i+1:]
		}
	}
	return full
}

// Options configures a [Transport].
type Options struct {
	// Self is this node. Its ID is sent with every outbound request so the
	// receiver knows who is calling. Required.
	Self core.Node
	// Logger receives diagnostics. Optional.
	Logger core.Logger
	// TLS, if set, is used for both the server and every dialled client.
	// Nil means plaintext, which is the right default inside a single
	// Kubernetes cluster with a service mesh or a private pod network.
	TLS *TLSConfig
	// DialTimeout bounds establishing a connection. Zero takes
	// DefaultDialTimeout.
	DialTimeout time.Duration
	// Keepalive overrides the keepalive profile. Zero values take the
	// defaults, which are tuned for a pod network rather than the
	// internet.
	Keepalive KeepaliveConfig
	// MaxMessageBytes caps both send and receive. Zero takes
	// DefaultMaxMessageBytes.
	MaxMessageBytes int
}

// Transport implements [core.Transport] over gRPC.
type Transport struct {
	self   core.Node
	logger core.Logger
	opts   Options

	server *grpc.Server

	mu      sync.Mutex
	clients map[string]*client // node ID -> pooled client
	closed  bool

	// ready is closed once Serve is about to accept connections.
	//
	// It exists because a node must not announce itself to gossip before it
	// can answer: peers derive ownership from membership, so a pod that is
	// a member but not yet serving is a pod that owns keys it will refuse
	// to serve. Callers wait on Ready between starting the transport and
	// joining the cluster.
	//
	// It is also what makes Serve and Close orderable from outside. Without
	// it, `go tr.Serve(...)` followed by tr.Close() is a race on which
	// goroutine reaches the mutex first, and Serve returning ErrClosed for
	// a transport the caller genuinely asked to serve is indistinguishable
	// from the same error for one closed an hour ago.
	readyOnce sync.Once
	ready     chan struct{}
}

// Ready is closed when this transport is accepting connections.
//
// It is never closed if Serve is never called, and a transport closed before
// Serve reaches the accept loop leaves it open — so a caller waiting on Ready
// must also watch for its own shutdown.
func (t *Transport) Ready() <-chan struct{} { return t.ready }

// New returns a gRPC transport.
func New(opts Options) (*Transport, error) {
	if opts.Self.IsZero() {
		return nil, fmt.Errorf("transport: Self is required: %w", core.ErrInvalidOption)
	}
	if opts.Logger == nil {
		opts.Logger = core.NopLogger{}
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = DefaultDialTimeout
	}
	if opts.MaxMessageBytes == 0 {
		opts.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if opts.MaxMessageBytes < 0 {
		return nil, fmt.Errorf("transport: MaxMessageBytes %d must be positive: %w",
			opts.MaxMessageBytes, core.ErrInvalidOption)
	}
	opts.Keepalive = opts.Keepalive.withDefaults()

	return &Transport{
		self:    opts.Self.Clone(),
		logger:  opts.Logger,
		opts:    opts,
		clients: make(map[string]*client),
		ready:   make(chan struct{}),
	}, nil
}

// Serve implements [core.Transport]. It blocks until [Transport.Close].
func (t *Transport) Serve(l net.Listener, h core.Handler) error {
	if h == nil {
		return fmt.Errorf("transport: nil handler: %w", core.ErrInvalidOption)
	}
	if l == nil {
		return fmt.Errorf("transport: nil listener: %w", core.ErrInvalidOption)
	}

	srvOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(t.opts.MaxMessageBytes),
		grpc.MaxSendMsgSize(t.opts.MaxMessageBytes),
		grpc.KeepaliveParams(t.opts.Keepalive.serverParams()),
		grpc.KeepaliveEnforcementPolicy(t.opts.Keepalive.enforcement()),
	}
	if t.opts.TLS != nil {
		cfg, err := t.opts.TLS.serverConfig()
		if err != nil {
			return fmt.Errorf("transport: server TLS: %w", err)
		}
		srvOpts = append(srvOpts, grpc.Creds(credentials.NewTLS(cfg)))
	}

	srv := grpc.NewServer(srvOpts...)
	srv.RegisterService(serviceDesc(func(method string) grpc.MethodHandler {
		return func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			return dispatch(ctx, h, method, dec)
		}
	}), nil)

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		srv.Stop()
		return fmt.Errorf("transport: closed: %w", core.ErrClosed)
	}
	t.server = srv
	t.mu.Unlock()

	t.readyOnce.Do(func() { close(t.ready) })

	err := srv.Serve(l)
	// GracefulStop and Stop both make Serve return ErrServerStopped. That
	// is the normal shutdown path, not a failure to report.
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// dispatch runs one request against the [core.Handler].
//
// The context arrives with gRPC's deadline already applied, which is how the
// caller's budget reaches the group turn: grpc-timeout on the wire becomes a
// context deadline here, and the handler passes the same context onward. No
// deadline is invented and none is extended.
func dispatch(ctx context.Context, h core.Handler, method string, dec func(any) error) (any, error) {
	var in rawMessage
	if err := dec(&in); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "decoding %s: %v", method, err)
	}

	from := core.Node{ID: callerID(ctx)}
	if carrier := traceCarrier(ctx); carrier != nil {
		ctx = core.WithCarrier(ctx, carrier)
	}
	out, err := h.Handle(ctx, from, method, in.payload)
	if err != nil {
		return nil, toStatus(err)
	}
	return &rawMessage{payload: out}, nil
}

// traceCarrier reads the trace headers of a request back into a carrier, or
// nil when there are none.
func traceCarrier(ctx context.Context) map[string]string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil
	}
	var carrier map[string]string
	for k, v := range md {
		if len(v) == 0 || !strings.HasPrefix(k, metadataTracePrefix) {
			continue
		}
		if carrier == nil {
			carrier = make(map[string]string, 2)
		}
		carrier[strings.TrimPrefix(k, metadataTracePrefix)] = v[0]
	}
	return carrier
}

// callerID reads the calling node's ID from the request metadata. An empty
// result means the caller did not identify itself, which the handler is free to
// treat as it likes; this layer does not invent an identity.
func callerID(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if v := md.Get(metadataFromNode); len(v) > 0 {
		return v[0]
	}
	return ""
}

// Close implements [core.Transport]. It is idempotent.
//
// In-flight requests fail rather than hang: Stop, not GracefulStop. A graceful
// drain belongs to the cache's Drain, which transfers ownership first and
// therefore has somewhere for the work to go. Draining here would only make
// Close slow.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	srv := t.server
	clients := make([]*client, 0, len(t.clients))
	for _, c := range t.clients {
		clients = append(clients, c)
	}
	t.clients = make(map[string]*client)
	t.mu.Unlock()

	var errs error
	for _, c := range clients {
		if err := c.Close(); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	if srv != nil {
		srv.Stop()
	}
	return errs
}
