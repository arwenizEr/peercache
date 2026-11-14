package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/arwenizEr/peercache"
)

// TracerName is the instrumentation name the adapter's tracer is created
// under.
const TracerName = "github.com/arwenizEr/peercache"

// Tracer adapts an OpenTelemetry tracer provider and propagator to
// peercache.Tracer, for peercache.WithTracer:
//
//	cache, err := peercache.New(..., peercache.WithTracer(
//	    pcotel.Tracer(otel.GetTracerProvider(), otel.GetTextMapPropagator())))
//
// Peer requests become client spans on the caller and server spans on the
// owner, loads internal spans, all in the caller's trace; the propagator (W3C
// trace context, usually) decides what crosses the hop. See
// docs/decisions/0038-trace-propagation.md.
func Tracer(provider trace.TracerProvider, propagator propagation.TextMapPropagator) peercache.Tracer {
	return &tracer{t: provider.Tracer(TracerName), p: propagator}
}

type tracer struct {
	t trace.Tracer
	p propagation.TextMapPropagator
}

func (t *tracer) Start(ctx context.Context, name string, attrs ...peercache.Attr) (context.Context, peercache.Span) {
	kv := make([]attribute.KeyValue, len(attrs))
	for i, a := range attrs {
		kv[i] = attribute.String(a.Key, a.Value)
	}
	ctx, s := t.t.Start(ctx, name, trace.WithAttributes(kv...), trace.WithSpanKind(kindOf(name)))
	return ctx, span{s}
}

func (t *tracer) Inject(ctx context.Context, carrier map[string]string) {
	t.p.Inject(ctx, propagation.MapCarrier(carrier))
}

func (t *tracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	return t.p.Extract(ctx, propagation.MapCarrier(carrier))
}

// kindOf maps the cache's span names to OpenTelemetry's kinds: a peer request
// is a client span, handling one a server span, a load an internal one.
func kindOf(name string) trace.SpanKind {
	switch {
	case len(name) > 14 && name[:14] == "peercache.rpc ":
		return trace.SpanKindClient
	case len(name) > 16 && name[:16] == "peercache.serve ":
		return trace.SpanKindServer
	}
	return trace.SpanKindInternal
}

type span struct{ s trace.Span }

func (s span) End(err error) {
	if err != nil {
		s.s.RecordError(err)
		s.s.SetStatus(codes.Error, err.Error())
	}
	s.s.End()
}
