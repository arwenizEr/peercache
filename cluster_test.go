package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/membership"
	"github.com/peercache/peercache/internal/testkit"
)

// These are the only tests in the repository that run two real cache instances
// against real gossip.
//
// Everything else about M3 is tested deterministically: the hashing in
// internal/ownership/table_test.go, the deferral window in guard_test.go
// against a fake clock, the gossip adapter in internal/membership. Each of
// those tests one seam. None of them proves the seams are connected — that
// peercache.New actually builds a guard, actually points it at the membership
// authority, and that the answer actually reaches Group.Call.
//
// That gap is worth two real-time tests. They use clock.Real deliberately: the
// activation delay has to elapse on the same clock memberlist converges on, and
// a fake clock here would test a configuration that cannot exist in production.
// They poll rather than sleep, and every wait is bounded so a failure reports
// instead of hanging.

// The activation delay is chosen per test, because the two tests want opposite
// things from it and a single value cannot serve both.
//
// The first run of the deferral test used 100ms for both and saw no deferral at
// all. That was the test's fault, not the guard's: the window opens when a node
// learns of its new peer, and waiting for *both* nodes to report two members
// takes longer over loopback gossip than 100ms, so the window had already
// closed before the first probe. A window has to outlast detection to be
// observable, which is also the reason DefaultActivationDelay is seconds rather
// than milliseconds.
const (
	// shortDelay effectively disables deferral, so the ownership test
	// measures ownership and nothing else.
	shortDelay = time.Millisecond
	// deferDelay is long enough to still be open once both nodes have
	// converged and been observed to converge.
	deferDelay = 3 * time.Second
)

// clusterBudget bounds every wait. Gossip over loopback converges in
// milliseconds; anything approaching this is a failure, not slowness.
const clusterBudget = 30 * time.Second

// clusterNode is one node's gossip authority and cache together, because
// neither is useful without the other and both need closing.
type clusterNode struct {
	id     string
	gossip *membership.Gossip
	cache  peercache.Cache
}

func newClusterNode(t *testing.T, id string, activationDelay time.Duration) *clusterNode {
	t.Helper()

	// Retry a bind the OS refuses after handing out the port. These two
	// tests bind real sockets, and on Windows the ephemeral range overlaps a
	// reserved one — see testkit.IsPortRefused. internal/membership got this
	// fix in M6; this call site did not, and failed under -count=2 in M7's
	// gate. The helper is shared now so there is one place to fix.
	g, err := testkit.RetryBind(func() (*membership.Gossip, error) {
		return membership.New(membership.Options{
			Self:   core.Node{ID: id},
			Config: testkit.LocalGossipConfig(id),
		})
	})
	if err != nil {
		t.Fatalf("membership.New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Errorf("gossip Close(%s): %v", id, err)
		}
	})

	c, err := peercache.New(
		peercache.WithNodeID(id),
		peercache.WithMembership(g),
		peercache.WithClock(clock.Real()),
		peercache.WithActivationDelay(activationDelay),
	)
	if err != nil {
		t.Fatalf("peercache.New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), clusterBudget)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Errorf("cache Close(%s): %v", id, err)
		}
	})

	return &clusterNode{id: id, gossip: g, cache: c}
}

// awaitCluster polls until cond holds or the budget expires. It yields rather
// than sleeping: CLAUDE.md forbids time.Sleep in tests, and the cost of a busy
// wait is acceptable for a handful of integration tests.
func awaitCluster(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(clusterBudget)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", clusterBudget, what)
		}
		runtime.Gosched()
	}
}

// TestTwoNodesAgreeOnOwnership is the end-to-end M3 claim: two independently
// built caches, joined only by gossip, partition the keyspace between them with
// no key claimed twice and no key claimed by neither.
//
// The internal tests prove the hash function partitions a keyspace. This proves
// the library uses it.
func TestTwoNodesAgreeOnOwnership(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and waits on real gossip")
	}

	a := newClusterNode(t, "node-a", shortDelay)
	b := newClusterNode(t, "node-b", shortDelay)

	if err := b.gossip.Join([]string{a.gossip.LocalAddr()}); err != nil {
		t.Fatalf("join: %v", err)
	}
	awaitCluster(t, "both nodes to see a two-node cluster", func() bool {
		return a.cache.Stats().Members == 2 && b.cache.Stats().Members == 2
	})

	// The window opens on the membership change that just happened, so wait
	// it out before asking who owns what. Deferral is tested on its own
	// below; here it is noise.
	awaitCluster(t, "the activation window to close", func() bool {
		return acceptsAny(t, a) && acceptsAny(t, b)
	})

	const keys = 200
	var byA, byB, byNeither, byBoth int
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("group-%d", i)
		okA := owns(t, a, key)
		okB := owns(t, b, key)
		switch {
		case okA && okB:
			byBoth++
		case okA:
			byA++
		case okB:
			byB++
		default:
			byNeither++
		}
	}

	if byBoth != 0 {
		t.Errorf("%d of %d groups were claimed by both nodes; a group must have "+
			"exactly one owner or two nodes will mutate the same state", byBoth, keys)
	}
	if byNeither != 0 {
		t.Errorf("%d of %d groups were claimed by neither node; every key in a "+
			"healthy cluster must be owned by somebody", byNeither, keys)
	}
	// Rendezvous hashing over two nodes should split roughly evenly. This is
	// a loose bound on purpose: 200 keys is a small sample, and the tight
	// uniformity assertion lives in internal/ownership over 100k keys. What
	// it catches here is a wiring bug that makes one node own everything,
	// which would otherwise pass both assertions above.
	if byA == 0 || byB == 0 {
		t.Fatalf("the split was %d/%d; one node owns the entire keyspace, which "+
			"means ownership is not reaching the guard", byA, byB)
	}
	t.Logf("split: node-a %d, node-b %d of %d", byA, byB, keys)
}

// TestJoiningNodeDefersNewlyAcquiredGroups proves ErrActivationPending reaches
// a caller through the public API.
//
// The guard's own test drives a fake clock and asserts the error directly. This
// asserts the thing a user experiences: a group that moved to a node that just
// joined is briefly unavailable there, with a retryable error, rather than
// activated in two places at once.
func TestJoiningNodeDefersNewlyAcquiredGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and waits on real gossip")
	}

	a := newClusterNode(t, "node-a", deferDelay)
	b := newClusterNode(t, "node-b", deferDelay)

	if err := b.gossip.Join([]string{a.gossip.LocalAddr()}); err != nil {
		t.Fatalf("join: %v", err)
	}
	awaitCluster(t, "both nodes to see a two-node cluster", func() bool {
		return a.cache.Stats().Members == 2 && b.cache.Stats().Members == 2
	})

	// Probe hard during the window. Each node has just acquired roughly half
	// the keyspace from the other's point of view, so pending answers should
	// be plentiful — but the window is short and this is real time, so the
	// assertion is on the counter, which cannot be missed by sampling.
	deadline := time.Now().Add(clusterBudget)
	for i := 0; ; i++ {
		key := fmt.Sprintf("deferred-%d", i)
		_, _ = a.cache.Group(key).Keys(context.Background())
		_, _ = b.cache.Group(key).Keys(context.Background())

		if a.cache.Stats().DeferredActivations > 0 || b.cache.Stats().DeferredActivations > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no activation was deferred in %v after a join; a node that "+
				"has just acquired a group must wait for the previous owner to "+
				"notice before activating it", clusterBudget)
		}
		runtime.Gosched()
	}

	// And it must end. A deferral that never lifts is an outage, so the same
	// keys have to become servable once the window closes.
	awaitCluster(t, "both nodes to resume activating after the window", func() bool {
		return acceptsAny(t, a) && acceptsAny(t, b)
	})
}

// owns reports whether n will activate key, distinguishing "not mine" from
// "not yet". A pending answer is retried until the window closes, because the
// question asked here is about ownership, not timing.
func owns(t *testing.T, n *clusterNode, key string) bool {
	t.Helper()
	deadline := time.Now().Add(clusterBudget)
	for {
		_, err := n.cache.Group(key).Keys(context.Background())
		switch {
		case err == nil:
			return true
		case errors.Is(err, peercache.ErrNotOwner):
			return false
		case errors.Is(err, peercache.ErrActivationPending):
			if time.Now().After(deadline) {
				t.Fatalf("%s deferred %q for %v; the activation window must close",
					n.id, key, clusterBudget)
			}
			runtime.Gosched()
		default:
			t.Fatalf("%s Keys(%q): unexpected error: %v", n.id, key, err)
		}
	}
}

// acceptsAny reports whether n will activate at least one of a few probe keys
// without deferring. It is the cheapest available evidence that the node's
// activation window has closed.
func acceptsAny(t *testing.T, n *clusterNode) bool {
	t.Helper()
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("probe-%s-%d", n.id, i)
		_, err := n.cache.Group(key).Keys(context.Background())
		if err == nil {
			return true
		}
		if errors.Is(err, peercache.ErrNotOwner) || errors.Is(err, peercache.ErrActivationPending) {
			continue
		}
		t.Fatalf("%s Keys(%q): unexpected error: %v", n.id, key, err)
	}
	return false
}
