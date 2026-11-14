package transport_test

import (
	"context"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/transport"
)

// TestTraceHeadersCrossTheHop: the trace context the cache attached to a
// request arrives in the handler's context on the other node, entry for
// entry, over real gRPC (docs/decisions/0038-trace-propagation.md).
func TestTraceHeadersCrossTheHop(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)
	got := make(chan map[string]string, 1)
	b.handler.respond = func(ctx context.Context, _ string, req []byte) ([]byte, error) {
		got <- core.CarrierFrom(ctx)
		return req, nil
	}
	sent := map[string]string{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "tracestate": "vendor=1"}
	ctx := core.WithCarrier(context.Background(), sent)
	if _, err := dial(t, a, b).Invoke(ctx, transport.MethodCall, []byte("x")); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	carrier := <-got
	for k, v := range sent {
		if carrier[k] != v {
			t.Fatalf("header %q arrived as %q, want %q (carrier %v)", k, carrier[k], v, carrier)
		}
	}
	if len(carrier) != len(sent) {
		t.Fatalf("carrier %v has entries that were never sent", carrier)
	}
}

// TestNoTraceHeadersNoCarrier: a request without trace context reaches the
// handler with none, rather than an empty one.
func TestNoTraceHeadersNoCarrier(t *testing.T) {
	a := newPeer(t, "node-a", nil)
	b := newPeer(t, "node-b", nil)
	got := make(chan map[string]string, 1)
	b.handler.respond = func(ctx context.Context, _ string, req []byte) ([]byte, error) {
		got <- core.CarrierFrom(ctx)
		return req, nil
	}
	if _, err := dial(t, a, b).Invoke(context.Background(), transport.MethodCall, []byte("x")); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if c := <-got; c != nil {
		t.Fatalf("a request without trace context arrived with carrier %v", c)
	}
}
