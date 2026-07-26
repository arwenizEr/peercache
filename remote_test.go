package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/testkit"
	"github.com/peercache/peercache/internal/transport"
)

// These tests run a real multi-node cache over the in-process fabric from
// internal/testkit: a fake network and a fake clock, both fully deterministic.
//
// That is the point. cluster_test.go proves the gRPC wiring is connected, and
// it has to wait on real gossip to do it. These prove the *semantics* of the
// remote data path — that a read reaches the owner, that a non-owned copy is
// cached and bounded, that a misrouted request is re-resolved rather than
// failed — with no sockets, no sleeps and no timing assumptions at all.

// fabric is a set of caches sharing one fake network, clock and registry.
type fabric struct {
	clk    *clock.Fake
	net    *testkit.Network
	nodes  map[string]core.Node
	caches map[string]peercache.Cache
	ids    []string

	// counters is populated only by newFabricCounting.
	counters map[string]*countingTransport
}

// newFabric builds n caches wired to each other through the fake fabric.
//
// The activation delay is negative — deferral off — because these tests are
// about routing rather than about the M3 handover window, which has its own
// tests. Leaving it on would make every assertion here wait out a window that
// is not what is being examined.
func newFabric(t *testing.T, n int) *fabric {
	t.Helper()
	return newFabricWith(t, n, false)
}

// newFabricCounting is newFabric with every node's transport instrumented.
func newFabricCounting(t *testing.T, n int) *fabric {
	t.Helper()
	return newFabricWith(t, n, true)
}

func newFabricWith(t *testing.T, n int, counting bool) *fabric {
	t.Helper()

	seed := testkit.Seed(t)
	clk := clock.NewFake(clock.Epoch)
	clk.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	f := &fabric{
		clk:      clk,
		net:      testkit.NewNetwork(clk, seed),
		nodes:    make(map[string]core.Node, n),
		caches:   make(map[string]peercache.Cache, n),
		counters: make(map[string]*countingTransport, n),
	}
	reg := testkit.NewRegistry()

	memberships := make(map[string]*testkit.Membership, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("n%d", i)
		info := core.Node{ID: id, Addr: id + ":7946"}
		f.nodes[id] = info
		f.ids = append(f.ids, id)
		memberships[id] = testkit.NewMembership(info, f.net, clk, reg, testkit.MembershipConfig{})
	}
	for _, id := range f.ids {
		var tr core.Transport = f.net.Transport(f.nodes[id])
		if counting {
			ct := &countingTransport{Transport: tr, counts: map[string]int{}}
			f.counters[id] = ct
			tr = ct
		}
		c, err := peercache.New(
			peercache.WithNodeID(id),
			peercache.WithClock(clk),
			peercache.WithMembership(memberships[id]),
			peercache.WithTransport(tr),
			peercache.WithActivationDelay(-1),
		)
		if err != nil {
			t.Fatalf("peercache.New(%s): %v", id, err)
		}
		f.caches[id] = c
	}

	// Converge only once every cache is serving. A node appears in the fake
	// network's endpoint list when its handler registers, which happens
	// inside peercache.New — so advancing before they all exist would admit
	// a partial view and arm timers for intermediate ones.
	clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)

	t.Cleanup(func() {
		for _, id := range f.ids {
			if err := f.caches[id].Close(context.Background()); err != nil {
				t.Errorf("Close(%s): %v", id, err)
			}
		}
		f.net.Close()
	})
	return f
}

// converge waits until every cache's ownership table holds the full membership.
//
// Advancing the clock only *delivers* the membership change: the guard watches
// the authority on its own goroutine and applies the new view afterwards. So
// this spins rather than assuming, yielding instead of sleeping, bounded so a
// failure reports rather than hangs. There is no wall-clock time involved — the
// fake clock has already been advanced and does not move again.
func (f *fabric) converge(t *testing.T) {
	t.Helper()
	const maxYields = 5_000_000
	for i := 0; ; i++ {
		settled := true
		for _, id := range f.ids {
			if f.caches[id].Stats().Members != len(f.ids) {
				settled = false
				break
			}
		}
		if settled {
			return
		}
		if i > maxYields {
			var seen []string
			for _, id := range f.ids {
				seen = append(seen, fmt.Sprintf("%s=%d", id, f.caches[id].Stats().Members))
			}
			t.Fatalf("membership did not converge after %d yields: %v (want %d each)",
				maxYields, seen, len(f.ids))
		}
		runtime.Gosched()
	}
}

// ownerOf returns the node ID that owns key, according to every node — and
// fails the test if they disagree, since that would make any later assertion
// meaningless.
func (f *fabric) ownerOf(t *testing.T, key string) string {
	t.Helper()
	var agreed string
	for _, id := range f.ids {
		stats := f.caches[id].Stats()
		if stats.Members != len(f.ids) {
			t.Fatalf("node %s sees %d members, want %d; the fabric has not converged",
				id, stats.Members, len(f.ids))
		}
	}
	// Ownership is derived identically everywhere, so asking one node is
	// enough once membership agrees. Finding it by elimination keeps this
	// test independent of the hashing implementation.
	for _, id := range f.ids {
		if err := f.probeOwns(id, key); err == nil {
			if agreed != "" {
				t.Fatalf("both %s and %s claim to own %q", agreed, id, key)
			}
			agreed = id
		}
	}
	if agreed == "" {
		t.Fatalf("no node claims to own %q", key)
	}
	return agreed
}

// probeOwns reports whether id owns key, by asking it to run a group operation
// that only its owner can perform.
func (f *fabric) probeOwns(id, key string) error {
	_, err := f.caches[id].Group(key).Keys(context.Background())
	return err
}

// TestAReadReachesTheOwner is the core M4 claim: a key written on its owner is
// readable from a node that does not own it.
func TestAReadReachesTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "orders/42"
	owner := f.ownerOf(t, key)

	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("value"), 0); err != nil {
		t.Fatalf("Set on the owner: %v", err)
	}

	for _, id := range f.ids {
		got, err := f.caches[id].Namespace("data").Get(ctx, key)
		if err != nil {
			t.Fatalf("Get from %s: %v; every node must be able to read a key whatever "+
				"node owns it", id, err)
		}
		if string(got) != "value" {
			t.Fatalf("Get from %s = %q, want %q", id, got, "value")
		}
	}
}

// TestAWriteReachesTheOwner is the same claim for the write path: a value
// written from a non-owner ends up on the owner, not on the writer.
func TestAWriteReachesTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "orders/99"
	owner := f.ownerOf(t, key)

	var writer string
	for _, id := range f.ids {
		if id != owner {
			writer = id
			break
		}
	}

	if err := f.caches[writer].Namespace("data").Set(ctx, key, []byte("written remotely"), 0); err != nil {
		t.Fatalf("Set from a non-owner: %v", err)
	}

	got, err := f.caches[owner].Namespace("data").Get(ctx, key)
	if err != nil {
		t.Fatalf("the owner cannot read a value written through it: %v", err)
	}
	if string(got) != "written remotely" {
		t.Fatalf("the owner holds %q, want %q", got, "written remotely")
	}

	// And the writer is not holding it in its own owned store: a write that
	// stayed local would look identical from the writer and be invisible to
	// everyone else.
	if f.caches[writer].Stats().OwnedKeys != 0 {
		t.Fatalf("the writer owns %d keys after writing a key it does not own; the write "+
			"stayed local instead of being routed",
			f.caches[writer].Stats().OwnedKeys)
	}
}

// TestADeleteReachesTheOwner completes the trio.
func TestADeleteReachesTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "orders/7"
	owner := f.ownerOf(t, key)
	var other string
	for _, id := range f.ids {
		if id != owner {
			other = id
			break
		}
	}

	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("doomed"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := f.caches[other].Namespace("data").Delete(ctx, key); err != nil {
		t.Fatalf("Delete from a non-owner: %v", err)
	}
	if _, err := f.caches[owner].Namespace("data").Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("the owner still has the key after a remote delete: %v", err)
	}
}

// TestAMissIsAMissEverywhere checks that an absent key reports ErrNotFound
// rather than a transport error, from a node that has to ask for it.
func TestAMissIsAMissEverywhere(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "never/written"
	owner := f.ownerOf(t, key)
	var other string
	for _, id := range f.ids {
		if id != owner {
			other = id
			break
		}
	}

	_, err := f.caches[other].Namespace("data").Get(ctx, key)
	if !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a remote miss reported %v, want ErrNotFound; a caller cannot distinguish "+
			"a missing key from a broken peer otherwise", err)
	}
}

// TestARemoteCallRunsOnTheOwner is the primary API crossing the network:
// computation moves to the data.
func TestARemoteCallRunsOnTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	// Every node registers the same function, because every pod runs the
	// same binary (CLAUDE.md non-negotiable 3).
	for _, id := range f.ids {
		if err := f.caches[id].RegisterGroupFunc("append", func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
			existing, _ := s.Get("log")
			updated := append(append([]byte{}, existing...), arg...)
			s.Set("log", updated)
			return updated, nil
		}); err != nil {
			t.Fatalf("RegisterGroupFunc on %s: %v", id, err)
		}
	}

	const group = "session/abc"
	owner := f.ownerOf(t, group)
	var caller string
	for _, id := range f.ids {
		if id != owner {
			caller = id
			break
		}
	}

	out, err := f.caches[caller].Group(group).Call(ctx, "append", []byte("x"))
	if err != nil {
		t.Fatalf("Call from a non-owner: %v", err)
	}
	if string(out) != "x" {
		t.Fatalf("Call returned %q, want %q", out, "x")
	}

	// A second call from a *different* node must see the first call's
	// effect, which is only true if both ran on the owner's single
	// activation rather than on local copies.
	out, err = f.caches[owner].Group(group).Call(ctx, "append", []byte("y"))
	if err != nil {
		t.Fatalf("Call on the owner: %v", err)
	}
	if string(out) != "xy" {
		t.Fatalf("Call returned %q, want %q; the two calls did not share one activation",
			out, "xy")
	}
}

// TestANonOwnedReadIsCachedAndBounded is the L1 tier doing its job, and the
// bound that makes it safe.
//
// The first read costs a round trip; the second is served locally. That is the
// optimisation. The assertion that matters is the second one: the copy stops
// being served once the namespace's L1 TTL has passed, whether or not any
// invalidation ever arrived.
func TestANonOwnedReadIsCachedAndBounded(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "cached/1"
	owner := f.ownerOf(t, key)
	var reader string
	for _, id := range f.ids {
		if id != owner {
			reader = id
			break
		}
	}

	ns := f.caches[reader].Namespace("data", peercache.WithL1TTL(30*time.Second))
	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("first"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got, err := ns.Get(ctx, key); err != nil || string(got) != "first" {
		t.Fatalf("first read = %q, %v", got, err)
	}
	if got := f.caches[reader].Stats().L1Entries; got != 1 {
		t.Fatalf("the reader holds %d non-owned entries after a remote read, want 1", got)
	}

	// Change the value on the owner without letting the reader hear about
	// it. The reader is entitled to serve its cached copy until the TTL
	// expires — that is the documented staleness bound, not a bug.
	f.net.Isolate(reader)
	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("second"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := ns.Get(ctx, key); err != nil || string(got) != "first" {
		t.Fatalf("read during the TTL = %q, %v; want the cached copy", got, err)
	}

	// Past the bound, the copy must not be served, even though no
	// invalidation was ever delivered. This is the property CLAUDE.md
	// non-negotiable 6 requires: correctness does not depend on a message
	// arriving.
	// Past the TTL. Advancing also lets the other nodes notice the isolated
	// peer, so the view shrinks and has to be rebuilt before the read means
	// anything: a reader that believes it is alone believes it owns the key,
	// and would answer from its own empty store rather than from the owner.
	f.clk.Advance(31 * time.Second)

	// Rejoin, not Heal: the harness treats a partition and an isolated node
	// as different events on purpose, and Heal only undoes the former.
	f.net.Rejoin(reader)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)

	got, err := ns.Get(ctx, key)
	if err != nil {
		t.Fatalf("read after the TTL: %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("read after the TTL = %q, want %q; the non-owned copy outlived its bound, "+
			"which means staleness is unbounded when invalidation is lost", got, "second")
	}
}

// TestInvalidationIsOnlyAnOptimisation removes the message entirely and checks
// that nothing depends on it.
//
// The network drops every invalidation. The cluster must still converge on the
// new value once the TTL passes; if this test can be made to fail by dropping a
// message, the invalidation broadcast has become load-bearing and CLAUDE.md
// non-negotiable 6 is violated.
func TestInvalidationIsOnlyAnOptimisation(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	const key = "hint/1"
	owner := f.ownerOf(t, key)
	var reader string
	for _, id := range f.ids {
		if id != owner {
			reader = id
			break
		}
	}

	ns := f.caches[reader].Namespace("data", peercache.WithL1TTL(10*time.Second))
	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := ns.Get(ctx, key); err != nil {
		t.Fatalf("priming read: %v", err)
	}

	// Drop everything from the owner to the reader, which includes the
	// invalidation the next write will try to send.
	f.net.SetDropRate(owner, reader, 1.0)
	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("v2"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	f.net.SetDropRate(owner, reader, 0)

	f.clk.Advance(11 * time.Second)
	got, err := ns.Get(ctx, key)
	if err != nil {
		t.Fatalf("read after the TTL: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("read = %q, want %q; with invalidation dropped the TTL is the only thing "+
			"bounding staleness, and it did not", got, "v2")
	}
}

// TestAWriteDoesNotWaitOnAnUnreachablePeer is the regression test for the
// nastiest defect in M4's data path.
//
// Invalidation used to be broadcast inline, on the writing caller's context.
// On this fabric a dropped message looks like silence rather than an error
// (docs/decisions/0004), so a peer that had gone away did not fail the send —
// it never replied. A caller passing context.Background(), which is the
// ordinary thing to do, therefore blocked in Set forever.
//
// That inverted the whole point of a best-effort hint: the write path came to
// depend, without limit, on the health of every peer. The queue in
// invalidate.go makes it structural — the writer's involvement ends at a
// channel send.
//
// The wait is bounded by yields rather than by a timer, so a failure reports
// instead of hanging. That distinction is the lesson M1 paid for.
func TestAWriteDoesNotWaitOnAnUnreachablePeer(t *testing.T) {
	f := newFabric(t, 3)

	const key = "write/1"
	owner := f.ownerOf(t, key)

	// Every message out of the owner vanishes into silence. Nothing is
	// refused; replies simply never come.
	for _, id := range f.ids {
		if id != owner {
			f.net.SetDropRate(owner, id, 1.0)
		}
	}

	done := make(chan error, 1)
	go func() {
		// Background: no deadline at all. If anything on this path waits
		// for a peer, it waits forever.
		done <- f.caches[owner].Namespace("data").Set(context.Background(), key, []byte("v"), 0)
	}()

	const maxYields = 5_000_000
	for i := 0; ; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Set returned %v; a peer being unreachable must not fail a "+
					"write to a key this node owns", err)
			}
			return
		default:
		}
		if i > maxYields {
			t.Fatalf("Set to a locally owned key had not returned after %d yields while "+
				"peers were unreachable; the write path is waiting on an invalidation "+
				"hint, which makes a best-effort optimisation a hard dependency of "+
				"every write", maxYields)
		}
		runtime.Gosched()
	}
}

// TestInvalidationBacklogIsDroppedNotQueued checks the other half of the
// bound: a writer that outruns delivery must not accumulate work without limit.
//
// Hints are worth less the older they get — by the time a stale one is
// delivered, the TTL it was racing has usually expired — so dropping them is
// the correct behaviour rather than a compromise.
//
// Every key written here is one this node owns. That matters: a *remote* write
// to a silent peer blocks forever under context.Background(), and correctly so,
// because a remote write cannot succeed without the peer. Only the hint is
// best-effort, so only the hint is what this test may examine.
func TestInvalidationBacklogIsDroppedNotQueued(t *testing.T) {
	f := newFabric(t, 3)

	owner := f.ids[0]
	// Collect keys this node owns, so the writes below are all local and
	// the only peer interaction left is the invalidation hint.
	var mine []string
	for i := 0; len(mine) < 2000 && i < 20000; i++ {
		k := fmt.Sprintf("flood/%d", i)
		if f.probeOwns(owner, k) == nil {
			mine = append(mine, k)
		}
	}
	if len(mine) < 500 {
		t.Fatalf("only %d of 20000 keys resolved to %s; the fabric is not distributing keys",
			len(mine), owner)
	}

	// Every hint out of the owner vanishes into silence.
	for _, id := range f.ids {
		if id != owner {
			f.net.SetDropRate(owner, id, 1.0)
		}
	}

	ns := f.caches[owner].Namespace("data")
	done := make(chan error, 1)
	go func() {
		for _, k := range mine {
			if err := ns.Set(context.Background(), k, []byte("v"), 0); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	const maxYields = 20_000_000
	for i := 0; ; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Set: %v", err)
			}
			return
		default:
		}
		if i > maxYields {
			t.Fatalf("%d writes to locally owned keys had not completed after %d yields "+
				"with every peer silent; the invalidation queue is applying "+
				"backpressure to writes instead of dropping hints",
				len(mine), maxYields)
		}
		runtime.Gosched()
	}
}

// countingTransport wraps a transport and counts Invoke calls per method.
//
// It exists for one assertion — that a router flush costs one request per
// owner — and that assertion cannot be made any other way. "It was fast" is not
// evidence of batching; counting the requests is.
type countingTransport struct {
	core.Transport
	mu     sync.Mutex
	counts map[string]int
}

func (t *countingTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &countingClient{PeerClient: inner, parent: t}, nil
}

// Ready forwards the readiness signal from the wrapped transport.
//
// peercache.New only waits for a transport that advertises Ready, and an
// embedded interface does not carry the concrete type's extra methods. Without
// this the wrapper silently opts every node out of waiting, and the cluster
// never converges — which is exactly how this was found.
func (t *countingTransport) Ready() <-chan struct{} {
	if r, ok := t.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

func (t *countingTransport) count(method string) {
	t.mu.Lock()
	t.counts[method]++
	t.mu.Unlock()
}

func (t *countingTransport) get(method string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.counts[method]
}

type countingClient struct {
	core.PeerClient
	parent *countingTransport
}

func (c *countingClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	c.parent.count(method)
	return c.PeerClient.Invoke(ctx, method, req)
}

// TestRouterIssuesOneRequestPerOwner is the M4 acceptance criterion from
// SPEC.md §4: "a router submitting 10k messages across 3 owners issues at most
// 3 RPCs per flush window".
//
// The count is taken from the transport rather than inferred, and the messages
// are spread across many groups on purpose — the grouping that matters is by
// *owner*, not by group, so a router that batched per group would pass a
// weaker test and fail this one.
func TestRouterIssuesOneRequestPerOwner(t *testing.T) {
	f := newFabricCounting(t, 3)
	ctx := context.Background()

	for _, id := range f.ids {
		if err := f.caches[id].RegisterGroupFunc("apply",
			func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
				msgs, err := peercache.Batch[string](arg)
				if err != nil {
					return nil, err
				}
				n, _ := s.Get("count")
				s.Set("count", append(n, byte(len(msgs))))
				return nil, nil
			}); err != nil {
			t.Fatalf("RegisterGroupFunc on %s: %v", id, err)
		}
	}

	const messages = 10_000
	msgs := make([]string, 0, messages)
	for i := 0; i < messages; i++ {
		// 500 distinct groups, so the owner-level grouping has real work
		// to do rather than collapsing trivially.
		msgs = append(msgs, fmt.Sprintf("group-%d", i%500))
	}

	sender := f.ids[0]
	router := peercache.NewRouter(f.caches[sender], func(s string) string { return s }, "apply")
	if err := router.Submit(ctx, msgs...); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	got := f.counters[sender].get(transport.MethodSubmit)
	// At most one request per *remote* owner. The sender applies its own
	// share in-process, so with three nodes the ceiling is two.
	const ceiling = 2
	if got > ceiling {
		t.Fatalf("submitting %d messages across %d owners issued %d requests, want at most "+
			"%d; the router is batching per group or per message rather than per owner",
			messages, len(f.ids), got, ceiling)
	}
	if got == 0 {
		t.Fatalf("submitting %d messages issued no remote requests at all; the sender "+
			"cannot own every one of 500 groups, so the test is not exercising routing",
			messages)
	}
	t.Logf("%d messages across 500 groups and %d owners: %d remote requests",
		messages, len(f.ids), got)
}

// TestTheLoaderRunsOnlyOnTheOwner is why loaders are registered by name rather
// than passed per call.
//
// Every node in the cluster asks for the same cold key. If each ran its own
// loader, a cold key would send one request per pod to whatever sits behind it
// — the stampede single-flighting prevents within a process, reintroduced
// across the cluster. The loader must run once, on the owner.
func TestTheLoaderRunsOnlyOnTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()

	var mu sync.Mutex
	loadsBy := map[string]int{}

	// Every pod runs the same binary, so every pod registers the same
	// loader. Which one actually runs is the question under test.
	nss := map[string]peercache.Namespace{}
	for _, id := range f.ids {
		nss[id] = f.caches[id].Namespace("loaded", peercache.WithLoader(
			func(_ context.Context, key string) ([]byte, time.Duration, error) {
				mu.Lock()
				loadsBy[id]++
				mu.Unlock()
				return []byte("reconstructed"), 0, nil
			}))
	}

	const key = "cold/1"
	owner := f.ownerOf(t, key)

	for _, id := range f.ids {
		got, err := nss[id].GetOrLoad(ctx, key)
		if err != nil {
			t.Fatalf("GetOrLoad from %s: %v", id, err)
		}
		if string(got) != "reconstructed" {
			t.Fatalf("GetOrLoad from %s = %q", id, got)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range loadsBy {
		total += n
	}
	if loadsBy[owner] == 0 {
		t.Fatalf("the loader never ran on the owner %s; loads were %v", owner, loadsBy)
	}
	if total != loadsBy[owner] {
		t.Fatalf("the loader ran on nodes other than the owner: %v; a cold key must cost "+
			"one load for the cluster, not one per pod", loadsBy)
	}
	if loadsBy[owner] != 1 {
		t.Fatalf("the loader ran %d times on the owner for one key, want 1; the owner's "+
			"single flight is not collapsing the cluster's misses", loadsBy[owner])
	}
}
