package otel_test

import (
	"context"
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/arwenizEr/peercache"
	pcotel "github.com/arwenizEr/peercache/otel"
	"github.com/arwenizEr/peercache/peercachetest"
)

// TestARemoteGetIsOneOpenTelemetryTrace: with the adapter and W3C trace
// context, a Get served by another node is a client span on the caller and a
// server span on the owner, in the caller's trace, the server span the client
// span's child.
func TestARemoteGetIsOneOpenTelemetryTrace(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	tracer := pcotel.Tracer(tp, propagation.TraceContext{})
	c := peercachetest.New(t, 2, peercachetest.WithCacheOptions(peercache.WithTracer(tracer)))
	for i := 0; i < c.Size(); i++ {
		c.Cache(i).Namespace("users", peercache.WithL1TTL(-1))
	}
	var key string
	for i := 0; ; i++ {
		if key = fmt.Sprintf("u:%d", i); c.Owner("users", key) == 1 {
			break
		}
	}
	ctx := context.Background()
	if err := c.Cache(1).Namespace("users").Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	appCtx, app := tp.Tracer("app").Start(ctx, "app.handler")
	if _, err := c.Cache(0).Namespace("users").Get(appCtx, key); err != nil {
		t.Fatalf("Get: %v", err)
	}
	app.End()

	var client, server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		switch s.SpanKind() {
		case trace.SpanKindClient:
			client = s
		case trace.SpanKindServer:
			server = s
		}
	}
	if client == nil || server == nil {
		t.Fatalf("want a client and a server span, got %d spans", len(rec.Ended()))
	}
	traceID := app.SpanContext().TraceID()
	if client.SpanContext().TraceID() != traceID || server.SpanContext().TraceID() != traceID {
		t.Fatal("the cache's spans are not in the application's trace")
	}
	if client.Parent().SpanID() != app.SpanContext().SpanID() {
		t.Fatal("the client span is not the application span's child")
	}
	if server.Parent().SpanID() != client.SpanContext().SpanID() {
		t.Fatal("the owner's server span is not the caller's client span's child: the trace context did not cross the hop")
	}
}
