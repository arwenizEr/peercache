package actor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/actor"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
)

func benchRuntime(b *testing.B, opts ...func(*actor.Options)) *actor.Runtime {
	b.Helper()
	o := actor.Options{Clock: clock.Real(), IdleTimeout: time.Hour, MailboxSize: 1024}
	for _, opt := range opts {
		opt(&o)
	}
	rt, err := actor.New(o)
	if err != nil {
		b.Fatalf("actor.New: %v", err)
	}
	b.Cleanup(func() {
		if err := rt.Close(context.Background()); err != nil {
			b.Errorf("Close: %v", err)
		}
	})
	return rt
}

func noopFunc(context.Context, core.GroupState, []byte) ([]byte, error) { return nil, nil }

// BenchmarkCallHot measures a turn on an already-activated group: the mailbox
// round trip and nothing else. This is the floor for every group operation.
func BenchmarkCallHot(b *testing.B) {
	rt := benchRuntime(b)
	if err := rt.Register("noop", noopFunc); err != nil {
		b.Fatalf("Register: %v", err)
	}
	ctx := context.Background()
	if _, err := rt.Call(ctx, "g", "noop", nil); err != nil {
		b.Fatalf("warm-up: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rt.Call(ctx, "g", "noop", nil); err != nil {
			b.Fatalf("Call: %v", err)
		}
	}
}

// BenchmarkCallHotParallel is the same against many callers. Throughput is
// bounded by the single turn goroutine by design: a group is a serialization
// point, and this measures what that costs.
func BenchmarkCallHotParallel(b *testing.B) {
	rt := benchRuntime(b)
	if err := rt.Register("noop", noopFunc); err != nil {
		b.Fatalf("Register: %v", err)
	}
	ctx := context.Background()
	if _, err := rt.Call(ctx, "g", "noop", nil); err != nil {
		b.Fatalf("warm-up: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Rejections are expected here — the mailbox is finite and
			// this deliberately oversubscribes it. Both outcomes are
			// legitimate results of the same call.
			_, _ = rt.Call(ctx, "g", "noop", nil)
		}
	})
}

// BenchmarkCallAcrossGroups spreads load over many groups, which is the shape
// a real workload has: parallelism comes from having many groups, never from
// one.
func BenchmarkCallAcrossGroups(b *testing.B) {
	const groups = 1024

	rt := benchRuntime(b, func(o *actor.Options) { o.MaxActive = -1 })
	if err := rt.Register("noop", noopFunc); err != nil {
		b.Fatalf("Register: %v", err)
	}
	ctx := context.Background()
	ids := make([]string, groups)
	for i := range ids {
		ids[i] = fmt.Sprintf("g%d", i)
		if _, err := rt.Call(ctx, ids[i], "noop", nil); err != nil {
			b.Fatalf("warm-up: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = rt.Call(ctx, ids[i%groups], "noop", nil)
			i++
		}
	})
}

// BenchmarkActivation measures the cold path: a group with no activation
// receiving its first call. This is what an eviction costs the next caller.
func BenchmarkActivation(b *testing.B) {
	rt := benchRuntime(b, func(o *actor.Options) { o.MaxActive = -1 })
	if err := rt.Register("noop", noopFunc); err != nil {
		b.Fatalf("Register: %v", err)
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		id := fmt.Sprintf("g%d", i)
		b.StartTimer()

		if _, err := rt.Call(ctx, id, "noop", nil); err != nil {
			b.Fatalf("Call: %v", err)
		}

		b.StopTimer()
		rt.Deactivate(id)
		b.StartTimer()
	}
}

// BenchmarkStateAccess measures the group-state operations themselves, which
// are plain map access with a generation check and no lock at all.
func BenchmarkStateAccess(b *testing.B) {
	rt := benchRuntime(b)
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		fn   core.GroupFunc
	}{
		{"Get", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
			s.Get("k")
			return nil, nil
		}},
		{"Set", func(_ context.Context, s core.GroupState, _ []byte) ([]byte, error) {
			s.Set("k", []byte("value"))
			return nil, nil
		}},
	} {
		if err := rt.Register(tc.name, tc.fn); err != nil {
			b.Fatalf("Register: %v", err)
		}
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rt.Call(ctx, "g", tc.name, nil); err != nil {
					b.Fatalf("Call: %v", err)
				}
			}
		})
	}
}
