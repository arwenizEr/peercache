package transport_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/transport"
)

// These measure the transport alone: an echo handler, no store, no ownership,
// no group runtime. What is left is the cost of a gRPC round trip over loopback
// plus this package's codec — which is the floor under every remote operation
// the cache performs, and the number to subtract when reading the end-to-end
// figures in docs/benchmarks.md.

// benchPeer starts a transport with an echo handler and returns it.
func benchPeer(b *testing.B, id string, tlsCfg *transport.TLSConfig) (*transport.Transport, core.Node) {
	b.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	node := core.Node{ID: id, Addr: l.Addr().String()}

	tr, err := transport.New(transport.Options{Self: node, TLS: tlsCfg})
	if err != nil {
		b.Fatalf("transport.New: %v", err)
	}
	go func() { _ = tr.Serve(l, core.HandlerFunc(echo)) }()
	select {
	case <-tr.Ready():
	case <-time.After(30 * time.Second):
		b.Fatal("transport was not ready")
	}
	b.Cleanup(func() { _ = tr.Close() })
	return tr, node
}

func echo(_ context.Context, _ core.Node, _ string, req []byte) ([]byte, error) {
	out := make([]byte, len(req))
	copy(out, req)
	return out, nil
}

// BenchmarkInvoke is the single-request round trip, which sets the floor on
// remote Get latency.
func BenchmarkInvoke(b *testing.B) {
	for _, size := range []int{0, 64, 1024, 16384} {
		b.Run(fmt.Sprintf("payload=%dB", size), func(b *testing.B) {
			a, _ := benchPeer(b, "bench-a", nil)
			_, bnode := benchPeer(b, "bench-b", nil)

			client, err := a.Dial(bnode)
			if err != nil {
				b.Fatalf("Dial: %v", err)
			}
			payload := make([]byte, size)
			ctx := context.Background()

			// Warm the connection: the first request pays for a TCP
			// handshake and an HTTP/2 preface, which is not what this
			// is measuring.
			if _, err := client.Invoke(ctx, transport.MethodGet, payload); err != nil {
				b.Fatalf("warm-up: %v", err)
			}

			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := client.Invoke(ctx, transport.MethodGet, payload); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkInvokeParallel is the number that matters for throughput: many
// callers sharing one pooled connection, which is how the cache uses it.
func BenchmarkInvokeParallel(b *testing.B) {
	a, _ := benchPeer(b, "bench-a", nil)
	_, bnode := benchPeer(b, "bench-b", nil)

	client, err := a.Dial(bnode)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	payload := make([]byte, 256)
	ctx := context.Background()
	if _, err := client.Invoke(ctx, transport.MethodGet, payload); err != nil {
		b.Fatalf("warm-up: %v", err)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := client.Invoke(ctx, transport.MethodGet, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkDialPooled measures the pool lookup, which is on the path of every
// remote operation because the data path dials per request rather than caching
// a client itself.
func BenchmarkDialPooled(b *testing.B) {
	a, _ := benchPeer(b, "bench-a", nil)
	_, bnode := benchPeer(b, "bench-b", nil)

	if _, err := a.Dial(bnode); err != nil {
		b.Fatalf("Dial: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := a.Dial(bnode); err != nil {
			b.Fatal(err)
		}
	}
}
