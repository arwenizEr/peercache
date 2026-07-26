package rebalance_test

import (
	"fmt"
	"testing"

	"github.com/peercache/peercache/internal/rebalance"
)

// Planning runs once per membership change, over the keys a node holds. It is
// allowed to be expensive relative to a request — it is not on any request
// path — but it bounds how quickly a large node can react to a failure, so the
// number is worth knowing before a cluster grows into it.

func BenchmarkComputeRemoval(b *testing.B) {
	for _, keys := range []int{1_000, 100_000} {
		b.Run(fmt.Sprintf("keys=%d", keys), func(b *testing.B) {
			before := tableOf(9)
			after := tableWithout(9, "n4")
			keyList := keysN(keys)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rebalance.Compute(before, after, keyList, 3); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkComputeByClusterSize shows the cost scaling with membership.
//
// Each key is ranked against every member twice — once per view — so planning
// is O(keys × members × rf). At the cluster sizes SPEC.md targets that is fine;
// this is here so the point at which it stops being fine is measured rather
// than discovered.
func BenchmarkComputeByClusterSize(b *testing.B) {
	const keys = 10_000
	keyList := keysN(keys)

	for _, size := range []int{3, 9, 32} {
		b.Run(fmt.Sprintf("nodes=%d", size), func(b *testing.B) {
			before := tableOf(size)
			after := tableWithout(size, "n1")

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := rebalance.Compute(before, after, keyList, 3); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkComputeNoChange is the common case: gossip re-delivers a view that
// has not changed. It must be cheap, because a cluster with any churn does this
// far more often than it does real work.
func BenchmarkComputeNoChange(b *testing.B) {
	const keys = 10_000
	view := tableOf(9)
	keyList := keysN(keys)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plan, err := rebalance.Compute(view, tableOf(9), keyList, 3)
		if err != nil {
			b.Fatal(err)
		}
		if plan.Total() != 0 {
			b.Fatalf("unchanged view produced %d moves", plan.Total())
		}
	}
}

func BenchmarkExpectedMoveFraction(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if f := rebalance.ExpectedMoveFraction(9, 8, 3); f <= 0 {
			b.Fatal("bad fraction")
		}
	}
}
