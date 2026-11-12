package testkit_test

import (
	"context"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/testkit"
)

// BenchmarkClusterPartitionHeal measures the M0 acceptance scenario end to end.
// It is the number that decides whether later milestones can afford to test
// their failure paths, so it is tracked rather than merely asserted once.
func BenchmarkClusterPartitionHeal(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := testkit.NewCluster(b, testkit.WithNodes(5))
		c.Net.Partition([]string{"n0", "n1", "n2"}, []string{"n3", "n4"})
		c.Advance(time.Hour)
		c.Net.Heal()
		c.Advance(time.Hour)
		// Explicit, because b.Cleanup would otherwise keep every
		// iteration's cluster alive until the benchmark ended.
		c.Close()
	}
}

// BenchmarkClusterStartup isolates the cost of building and converging a
// cluster, which is the fixed overhead every test in the repository pays.
func BenchmarkClusterStartup(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := testkit.NewCluster(b, testkit.WithNodes(5))
		c.Close()
	}
}

// BenchmarkNetworkRoundTrip measures one in-process request/response with no
// faults injected. It bounds how much of a later milestone's benchmark is
// harness overhead rather than the code under test.
func BenchmarkNetworkRoundTrip(b *testing.B) {
	c := testkit.NewCluster(b, testkit.WithNodes(2))
	defer c.Close()

	client, err := c.Node("n0").Transport.Dial(c.Node("n1").Info)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	payload := []byte("benchmark")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Invoke(ctx, "ping", payload); err != nil {
			b.Fatalf("Invoke: %v", err)
		}
	}
}

// BenchmarkMembershipRefresh measures the cost of re-evaluating every peer,
// which happens on every reachability change and is therefore on the path of
// every fault a test injects.
func BenchmarkMembershipRefresh(b *testing.B) {
	c := testkit.NewCluster(b, testkit.WithNodes(9))
	defer c.Close()

	m := c.Node("n0").Membership
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Refresh()
	}
}
