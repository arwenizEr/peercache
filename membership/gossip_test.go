package membership_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/membership"
)

// These are integration tests over loopback, and they are honestly real-time:
// gossip converges on memberlist's own timers, which no injected clock reaches.
//
// They poll rather than sleep, so they satisfy the letter of CLAUDE.md's rule,
// but the spirit of that rule — that a test never waits on wall time — cannot be
// met for a third-party network protocol. The logic that actually depends on
// membership (ownership, the activation guard) is tested deterministically
// against internal/testkit instead. See internal/membership/doc.go.

// localConfig returns a memberlist profile tuned for a loopback test: fast
// probes, short timeouts, no compression.
// It delegates the port choice to testkit rather than asking for port 0,
// because on Windows port 0 lands inside a reserved block often enough to fail
// a run and cannot retry its way out — see testkit.FreeLoopbackPort. This
// function kept its own copy of that logic through M6 and M7 and failed both
// times; there is now one implementation.
func localConfig(name string) *memberlist.Config {
	return testkit.LocalGossipConfig(name)
}

// newGossipRetrying starts a gossip node, retrying if the OS hands out a port
// it will not then let us bind.
//
// Windows reserves large contiguous ephemeral ranges for Hyper-V and friends —
// 50510-51109 on the machine this was written on — and memberlist's port-0
// selection lands in them often enough to fail a run. The bind fails with
// "access permissions", which reads like a defect and is not one: the port was
// allocated and then refused.
//
// Retrying is what a real deployment does too, so this is not test-only
// sleight of hand. It is bounded, and the last error is reported so a genuine
// bind problem still fails loudly rather than looping.
//
// See docs/decisions/open.md question 27.
func newGossipRetrying(t *testing.T, id string) (*membership.Gossip, error) {
	return newGossipAt(t, core.Node{ID: id})
}

// newGossipAt is newGossipRetrying for a node with an explicit address.
func newGossipAt(t *testing.T, self core.Node) (*membership.Gossip, error) {
	t.Helper()
	return testkit.RetryBind(func() (*membership.Gossip, error) {
		return membership.New(membership.Options{
			Self:   self,
			Config: localConfig(self.ID),
		})
	})
}

func newGossip(t *testing.T, id string) *membership.Gossip {
	t.Helper()
	g, err := newGossipRetrying(t, id)
	if err != nil {
		t.Fatalf("membership.New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Errorf("Close(%s): %v", id, err)
		}
	})
	return g
}

// awaitReal polls until cond holds or the budget expires. It yields rather than
// sleeping, which burns CPU — acceptable for a handful of integration tests and
// bounded so a failure reports rather than hangs.
func awaitReal(t *testing.T, what string, budget time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", budget, what)
		}
		runtime.Gosched()
	}
}

func TestGossipFormsACluster(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and waits on real gossip")
	}

	const nodes = 3
	cluster := make([]*membership.Gossip, nodes)
	for i := range cluster {
		cluster[i] = newGossip(t, fmt.Sprintf("n%d", i))
	}

	seed := cluster[0].LocalAddr()
	for _, g := range cluster[1:] {
		if err := g.Join([]string{seed}); err != nil {
			t.Fatalf("Join: %v", err)
		}
	}

	for i, g := range cluster {
		g := g
		awaitReal(t, fmt.Sprintf("n%d to see the whole cluster", i), 10*time.Second, func() bool {
			members, _ := g.Members()
			return len(members) == nodes
		})
	}

	// Members are sorted, so two nodes can compare views without sorting.
	members, epoch := cluster[0].Members()
	for i := 1; i < len(members); i++ {
		if members[i-1].ID >= members[i].ID {
			t.Fatalf("Members is not sorted: %v", members)
		}
	}
	if epoch == 0 {
		t.Fatal("epoch did not advance as nodes joined")
	}
}

func TestGossipEmptySeedFormsAClusterOfOne(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	g := newGossip(t, "solo")
	if err := g.Join(nil); err != nil {
		t.Fatalf("Join(nil): %v", err)
	}
	members, _ := g.Members()
	if len(members) != 1 || members[0].ID != "solo" {
		t.Fatalf("Members = %v, want just the node itself", members)
	}
	if got := g.Self().ID; got != "solo" {
		t.Fatalf("Self() = %q", got)
	}
}

// TestGossipReportsDepartures covers the path a rolling upgrade takes: a node
// leaves gracefully and its peers notice without waiting out the failure
// detector.
func TestGossipReportsDepartures(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	a := newGossip(t, "a")
	b := newGossip(t, "b")
	if err := b.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	awaitReal(t, "both nodes to see each other", 10*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 2
	})

	changes := a.Watch()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Leave(ctx); err != nil {
		t.Fatalf("Leave: %v", err)
	}

	awaitReal(t, "a to notice b leaving", 10*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 1
	})

	// A departure must have been published, not just the member list
	// updated. Metadata updates may be interleaved, so look for the one
	// that matters rather than assuming it is first.
	sawDeparture := false
	for len(changes) > 0 {
		change := <-changes
		if change.Node.ID == "b" && change.Kind == core.ChangeFail {
			sawDeparture = true
			if change.Epoch == 0 {
				t.Fatal("the departure carried no epoch")
			}
		}
	}
	if !sawDeparture {
		t.Fatal("no departure was published for b")
	}
}

// TestEpochDoesNotRunAway guards against gossip amplification.
//
// Republishing this node's metadata appears to every peer as a NodeUpdate. If
// an update advanced the epoch, each republish would provoke a republish
// everywhere else, and that everywhere else would provoke another here — an
// unbounded loop with the epoch climbing as fast as the network allows. An
// earlier version of consume did this and reached epoch 222 in a two-node test
// lasting a fraction of a second.
//
// Two nodes joining is a small, bounded number of transitions. Anything beyond
// a handful means the feedback loop is back.
func TestEpochDoesNotRunAway(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	a := newGossip(t, "a")
	b := newGossip(t, "b")
	if err := b.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	awaitReal(t, "the cluster to form", 10*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 2
	})

	// Let any amplification run for a while. This deliberately spends real
	// time: a loop needs wall-clock room to show itself.
	settle := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(settle) {
		runtime.Gosched()
	}

	_, epoch := a.Members()
	t.Logf("epoch after two nodes joined and settling: %d", epoch)

	const ceiling = 10
	if epoch > ceiling {
		t.Fatalf("epoch reached %d after a two-node join; metadata updates are feeding back "+
			"into membership transitions", epoch)
	}
}

// TestPeerEpochIsVisible checks that the epoch travels in node metadata, which
// is how one node sees that another's view is behind without a round trip.
func TestPeerEpochIsVisible(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	a := newGossip(t, "a")
	b := newGossip(t, "b")
	if err := b.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("Join: %v", err)
	}

	awaitReal(t, "the epoch of a peer to become visible", 10*time.Second, func() bool {
		_, ok := a.PeerEpoch("b")
		return ok
	})

	epoch, ok := a.PeerEpoch("b")
	if !ok {
		t.Fatal("PeerEpoch reported nothing for a live peer")
	}
	t.Logf("a sees b at epoch %d", epoch)

	if _, ok := a.PeerEpoch("nonexistent"); ok {
		t.Fatal("PeerEpoch reported an epoch for an unknown node")
	}
}

func TestGossipValidatesOptions(t *testing.T) {
	if _, err := membership.New(membership.Options{}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("New without a node ID = %v, want ErrInvalidOption", err)
	}
	if _, err := membership.New(membership.Options{
		Self:     core.Node{ID: "n0"},
		BindAddr: "not-a-host-port",
	}); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("New with a bad bind address = %v, want ErrInvalidOption", err)
	}
}

func TestGossipCloseIsIdempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	g, err := newGossipRetrying(t, "solo")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	changes := g.Watch()

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, open := <-changes; open {
		t.Fatal("the watch channel delivered after Close instead of closing")
	}
	// Watching a closed authority yields a closed channel rather than one
	// that never fires, so a watcher started during shutdown terminates.
	if _, open := <-g.Watch(); open {
		t.Fatal("Watch after Close returned an open channel")
	}
}

// TestCloseLeavesNoGoroutines is the test that should have existed from the
// start of M3.
//
// memberlist never closes the channel it delivers node events on, not even from
// Shutdown, so the goroutine that ranged over it waited forever: one leaked
// goroutine per node, for the life of the process. Nothing caught it, because
// every membership test constructs a node, asserts something and exits, and a
// goroutine that leaks after the assertion is invisible to the assertion.
//
// What found it was a two-node end-to-end test in the root package, running in
// the same binary as the M1 p99 acceptance test. Under -count=2 the leaked
// goroutines from the first iteration pushed the second iteration's p99 from
// 0.5µs to 1.5µs and broke a budget that had nothing to do with membership.
func TestCloseLeavesNoGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets")
	}

	// Settle first: earlier tests in this package may still be tearing down,
	// and a baseline taken mid-teardown would make this test pass for the
	// wrong reason.
	settle(t)
	before := runtime.NumGoroutine()

	const nodes = 4
	gossips := make([]*membership.Gossip, 0, nodes)
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("leak-%d", i)
		g, err := newGossipRetrying(t, id)
		if err != nil {
			t.Fatalf("membership.New(%s): %v", id, err)
		}
		gossips = append(gossips, g)
	}
	for _, g := range gossips {
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	// Close waits for its own goroutine, so the count must come back on its
	// own. It is still polled rather than read immediately, because
	// memberlist's internal goroutines are not ours and unwind on their own
	// schedule; what is asserted is that the count returns, not that it
	// returns instantly.
	awaitReal(t, "the goroutine count to return to baseline", 30*time.Second, func() bool {
		return runtime.NumGoroutine() <= before
	})
}

// settle yields until the goroutine count stops changing, so that a baseline is
// taken at rest rather than mid-teardown.
func settle(t *testing.T) {
	t.Helper()
	stable, last := 0, -1
	for stable < 1000 {
		if n := runtime.NumGoroutine(); n == last {
			stable++
		} else {
			stable, last = 0, n
		}
		runtime.Gosched()
	}
}

// TestGossipAdvertisesTheRPCAddress pins the distinction that broke the first
// real-network run: where a node gossips is not where it serves data.
//
// memberlist binds its own port for SWIM; the peer transport binds another for
// gRPC. The membership view feeds the data path's Dial, so if it reports the
// gossip address the whole cluster dials memberlist expecting gRPC. The symptom
// was "error reading server preface: EOF" — a protocol talking to the wrong
// listener — and nothing in the deterministic tests could have produced it,
// because the in-process fabric has only one address per node.
func TestGossipAdvertisesTheRPCAddress(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: binds loopback sockets and waits on real gossip")
	}

	// Two nodes whose RPC addresses are deliberately nothing like the ports
	// memberlist will choose.
	a, err := newGossipAt(t, core.Node{ID: "node-a", Addr: "10.0.0.1:9999"})
	if err != nil {
		t.Fatalf("membership.New(node-a): %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	b, err := newGossipAt(t, core.Node{ID: "node-b", Addr: "10.0.0.2:8888"})
	if err != nil {
		t.Fatalf("membership.New(node-b): %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if err := b.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("join: %v", err)
	}

	awaitReal(t, "node-a to learn node-b's advertised address", 30*time.Second, func() bool {
		members, _ := a.Members()
		for _, m := range members {
			if m.ID == "node-b" {
				return m.Addr == "10.0.0.2:8888"
			}
		}
		return false
	})

	members, _ := a.Members()
	for _, m := range members {
		if m.ID != "node-b" {
			continue
		}
		if m.Addr == a.LocalAddr() || m.Addr == b.LocalAddr() {
			t.Fatalf("node-b is advertised at %q, which is a gossip address; the data path "+
				"would dial memberlist expecting gRPC", m.Addr)
		}
	}

	// And this node's own entry keeps the address it was given, rather than
	// being overwritten with the gossip address at construction.
	if self := a.Self(); self.Addr != "10.0.0.1:9999" {
		t.Fatalf("Self().Addr = %q, want the configured RPC address", self.Addr)
	}
}
