package ownership

import (
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
)

// benchNodes builds n members with stable IDs, so that two runs of the same
// size score identically and a regression is a real change rather than a
// different random cluster.
func benchNodes(n int) []core.Node {
	out := make([]core.Node, n)
	for i := range out {
		out[i] = core.Node{ID: fmt.Sprintf("node-%03d", i), Addr: fmt.Sprintf("10.0.0.%d:7946", i)}
	}
	return out
}

// benchKeys builds keys outside the timed loop. Formatting a key inside the
// loop would measure fmt, which is roughly the cost of the lookup itself.
func benchKeys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("tenant-%d/user-%d", i%97, i)
	}
	return out
}

// BenchmarkOwner is the number that matters. Owner runs on every activation
// and on every remote call, so its cost is paid per request rather
// than per membership change.
//
// It is benchmarked across cluster sizes because rendezvous hashing is O(n) in
// members by construction: the score is a maximum over the whole view. The
// point of the sweep is to show where that linearity starts to hurt.
func BenchmarkOwner(b *testing.B) {
	for _, size := range []int{3, 7, 16, 64, 256} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			t := NewTable()
			t.Update(benchNodes(size), 1)
			keys := benchKeys(1024)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := t.Owner(keys[i%len(keys)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkOwnerParallel checks that the atomic snapshot read really is
// lock-free under contention. A read lock here would show up as a per-op cost
// that climbs with GOMAXPROCS; a pointer load should not.
func BenchmarkOwnerParallel(b *testing.B) {
	t := NewTable()
	t.Update(benchNodes(7), 1)
	keys := benchKeys(1024)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, _, err := t.Owner(keys[i%len(keys)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

// BenchmarkOwnerDuringUpdate measures lookups while membership churns
// underneath them. This is the shape of a rolling deploy, and it is the case
// where a lock-based table would serialise every reader behind the writer.
func BenchmarkOwnerDuringUpdate(b *testing.B) {
	t := NewTable()
	nodes := benchNodes(7)
	t.Update(nodes, 1)
	keys := benchKeys(1024)

	done := make(chan struct{})
	go func() {
		for epoch := core.Epoch(2); ; epoch++ {
			select {
			case <-done:
				return
			default:
			}
			t.Update(nodes, epoch)
		}
	}()
	b.Cleanup(func() { close(done) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := t.Owner(keys[i%len(keys)]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkReplicas is the replication path. It sorts the whole view per call,
// so it is deliberately not on the ownership hot path and is measured here to record how much
// more expensive it is than taking the head.
func BenchmarkReplicas(b *testing.B) {
	for _, size := range []int{7, 64} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			t := NewTable()
			t.Update(benchNodes(size), 1)
			keys := benchKeys(1024)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := t.Replicas(keys[i%len(keys)], 3); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUpdate is the cost of a membership change. It is paid once per
// gossip event rather than per request, so a hash and a sort per node is fine;
// the number is recorded so that a future change that makes it quadratic is
// visible.
func BenchmarkUpdate(b *testing.B) {
	for _, size := range []int{7, 256} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			t := NewTable()
			nodes := benchNodes(size)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				t.Update(nodes, core.Epoch(i))
			}
		})
	}
}

// BenchmarkCanActivate measures the full guard check: ownership, then the
// deferral window. The window is closed here, which is the steady state — the
// open-window path additionally scores the key against the settled view and is
// measured separately.
func BenchmarkCanActivate(b *testing.B) {
	self := core.Node{ID: "node-003"}
	t := NewTable()
	t.Update(benchNodes(7), 1)
	g, err := NewGuard(GuardOptions{Self: self, Table: t, Clock: clock.NewFake(time.Unix(0, 0))})
	if err != nil {
		b.Fatal(err)
	}
	keys := benchKeys(1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Errors are expected: this node owns roughly one key in seven.
		// Discarding them is the point — the branch is measured, not the
		// outcome.
		_ = g.CanActivate(keys[i%len(keys)])
	}
}
