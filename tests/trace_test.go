package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// recordingTracer records spans with their parents. A span's context carries
// its ID; Inject writes it as "span", and Extract makes it the parent of what
// starts next — the shape every real propagator has.
type recordingTracer struct {
	mu    sync.Mutex
	spans []*recSpan
}

type recSpan struct {
	id, parent, name string
	attrs            []peercache.Attr
	ended            bool
	err              error
}

type spanKey struct{}

func (r *recordingTracer) Start(ctx context.Context, name string, attrs ...peercache.Attr) (context.Context, peercache.Span) {
	r.mu.Lock()
	defer r.mu.Unlock()
	parent, _ := ctx.Value(spanKey{}).(string)
	s := &recSpan{id: fmt.Sprintf("s%d", len(r.spans)+1), parent: parent, name: name, attrs: attrs}
	r.spans = append(r.spans, s)
	return context.WithValue(ctx, spanKey{}, s.id), s
}

func (r *recordingTracer) Inject(ctx context.Context, carrier map[string]string) {
	if id, ok := ctx.Value(spanKey{}).(string); ok {
		carrier["span"] = id
	}
}

func (r *recordingTracer) Extract(ctx context.Context, carrier map[string]string) context.Context {
	if id, ok := carrier["span"]; ok {
		return context.WithValue(ctx, spanKey{}, id)
	}
	return ctx
}

func (s *recSpan) End(err error) { s.ended, s.err = true, err }

func (r *recordingTracer) named(prefix string) []*recSpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*recSpan
	for _, s := range r.spans {
		if strings.HasPrefix(s.name, prefix) {
			out = append(out, s)
		}
	}
	return out
}

// TestARemoteReadIsOneTraceAcrossTwoNodes: a Get served by another node
// produces a client span on the caller, child of the application's span, and a
// serve span on the owner, child of the client span — the hop visible in the
// application's trace. No span names the key.
func TestARemoteReadIsOneTraceAcrossTwoNodes(t *testing.T) {
	rec := &recordingTracer{}
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1, withCacheOptions(peercache.WithTracer(rec)))
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("users", peercache.WithL1TTL(-1))
	}
	via := f.ids[0]
	key := traceRemoteKey(t, f, via)
	if err := f.caches[f.ownerOf(t, key)].Namespace("users").Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before := len(rec.named(""))

	appCtx, app := rec.Start(ctx, "app.handler")
	if _, err := f.caches[via].Namespace("users").Get(appCtx, key); err != nil {
		t.Fatalf("Get: %v", err)
	}
	app.End(nil)

	var rpc, serve *recSpan
	for _, s := range rec.named("")[before:] {
		switch s.name {
		case "peercache.rpc " + transport.MethodGet:
			rpc = s
		case "peercache.serve " + transport.MethodGet:
			serve = s
		}
	}
	if rpc == nil || serve == nil {
		t.Fatalf("spans after the Get: %v; want an rpc and a serve span for Get", spanNames(rec.named("")[before:]))
	}
	appID := rec.named("app.handler")[0].id
	if rpc.parent != appID {
		t.Fatalf("the rpc span's parent is %q, want the application's span %q", rpc.parent, appID)
	}
	if serve.parent != rpc.id {
		t.Fatalf("the owner's serve span's parent is %q, want the caller's rpc span %q", serve.parent, rpc.id)
	}
	if !rpc.ended || !serve.ended {
		t.Fatal("a span was never ended")
	}
	for _, s := range rec.named("peercache.") {
		for _, a := range s.attrs {
			if strings.Contains(a.Value, key) {
				t.Fatalf("span %s carries the key in %s=%q", s.name, a.Key, a.Value)
			}
		}
	}
}

// TestAForwardedLoadIsTracedOnTheOwner: GetOrLoad through a non-owner runs the
// loader on the owner under a load span, child of the owner's serve span.
func TestAForwardedLoadIsTracedOnTheOwner(t *testing.T) {
	rec := &recordingTracer{}
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1, withCacheOptions(peercache.WithTracer(rec)))
	for _, id := range f.ids {
		f.caches[id].Namespace("profiles", peercache.WithL1TTL(-1), peercache.WithLoader(
			func(context.Context, string) ([]byte, time.Duration, error) { return []byte("p"), 0, nil }))
	}
	via := f.ids[0]
	key := traceRemoteKey(t, f, via)
	if _, err := f.caches[via].Namespace("profiles").GetOrLoad(context.Background(), key); err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	loads := rec.named("peercache.load")
	if len(loads) != 1 {
		t.Fatalf("%d load spans, want 1", len(loads))
	}
	var serve *recSpan
	for _, s := range rec.named("peercache.serve") {
		if s.name == "peercache.serve "+transport.MethodLoad {
			serve = s
		}
	}
	if serve == nil || loads[0].parent != serve.id {
		t.Fatalf("the load span's parent is %q; want the owner's serve span for Load (%v)", loads[0].parent, spanNames(rec.named("")))
	}
}

func spanNames(spans []*recSpan) []string {
	var out []string
	for _, s := range spans {
		out = append(out, s.name)
	}
	return out
}

// traceRemoteKey returns a key node via does not own.
func traceRemoteKey(t *testing.T, f *fabric, via string) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		if k := fmt.Sprintf("u:%d", i); f.ownerOf(t, k) != via {
			return k
		}
	}
	t.Fatal("no key owned elsewhere")
	return ""
}
