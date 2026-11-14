package core

import "context"

// Tracer starts spans around the cache's hops and loads and carries their
// context to peers. The core knows no tracing system; an implementation
// adapts one, as the otel module does for OpenTelemetry. See
// docs/decisions/0038-trace-propagation.md.
type Tracer interface {
	// Start begins a span named name, a child of whatever span ctx
	// carries, and returns a context carrying it.
	Start(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
	// Inject writes the span context ctx carries into carrier, to be sent
	// to a peer with a request.
	Inject(ctx context.Context, carrier map[string]string)
	// Extract returns ctx carrying the span context a peer sent in
	// carrier, so that spans started from it are its children.
	Extract(ctx context.Context, carrier map[string]string) context.Context
}

// Span is one traced operation.
type Span interface {
	// End finishes the span, recording err if it is not nil.
	End(err error)
}

// Attr is one span attribute. Keys and values of the cache are never
// attributes.
type Attr struct {
	Key   string
	Value string
}

// NopTracer traces nothing. It is the default.
type NopTracer struct{}

// Start implements [Tracer].
func (NopTracer) Start(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	return ctx, nopSpan{}
}

// Inject implements [Tracer].
func (NopTracer) Inject(context.Context, map[string]string) {}

// Extract implements [Tracer].
func (NopTracer) Extract(ctx context.Context, _ map[string]string) context.Context { return ctx }

type nopSpan struct{}

func (nopSpan) End(error) {}

type carrierKey struct{}

// WithCarrier returns ctx carrying the trace headers to send with, or that
// arrived with, one peer request. Transports read it on the way out and set
// it on the way in; nothing else does.
func WithCarrier(ctx context.Context, carrier map[string]string) context.Context {
	return context.WithValue(ctx, carrierKey{}, carrier)
}

// CarrierFrom returns the trace headers ctx carries, or nil.
func CarrierFrom(ctx context.Context) map[string]string {
	c, _ := ctx.Value(carrierKey{}).(map[string]string)
	return c
}
