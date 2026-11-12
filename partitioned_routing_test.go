package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// partitionedKey finds, in a three-node fabric, a key whose group is owned by
// one node while the key's own string would hash to a different one, and a
// third node to call from. It is the arrangement in which checking ownership by
// the raw key instead of the group key gives the wrong answer.
func partitionedKey(t *testing.T, f *fabric) (key, groupOwner, caller string) {
	t.Helper()
	for i := 0; i < 500; i++ {
		group := fmt.Sprintf("cust%d", i)
		k := group + ":order1"
		g, raw := f.ownerOf(t, group), f.ownerOf(t, k)
		if g == raw {
			continue
		}
		for _, id := range f.ids {
			if id != g {
				return k, g, id
			}
		}
	}
	t.Fatal("no key among 500 whose group owner differs from its raw-key owner")
	return "", "", ""
}

// TestAPartitionedNamespaceServesRemoteCallers: a namespace with a partitioner
// routes by group key, so the owner it reaches is the group's owner — and that
// owner must accept the request. Found by code review: the peer handler checked
// ownership of the raw key, which the partitioner had deliberately not used, so
// the group's owner refused every remote request whose key hashed elsewhere.
func TestAPartitionedNamespaceServesRemoteCallers(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("partitioned",
			peercache.WithPartitioner(peercache.PartitionByPrefix(":")),
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	key, owner, caller := partitionedKey(t, f)
	t.Logf("%s: group owned by %s, calling from %s", key, owner, caller)

	if err := openNS(caller).Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("remote Set of %s from %s: %v", key, caller, err)
	}
	if got, err := openNS(caller).Get(ctx, key); err != nil || string(got) != "v" {
		t.Fatalf("remote Get of %s from %s = %q, %v", key, caller, got, err)
	}
	if ok, err := openNS(caller).Exists(ctx, key); err != nil || !ok {
		t.Fatalf("remote Exists of %s from %s = %v, %v", key, caller, ok, err)
	}
	batch, err := openNS(caller).BatchGet(ctx, []string{key})
	if err != nil || string(batch[key]) != "v" {
		t.Fatalf("remote BatchGet of %s from %s = %v, %v", key, caller, batch, err)
	}
	if err := openNS(caller).Delete(ctx, key); err != nil {
		t.Fatalf("remote Delete of %s from %s: %v", key, caller, err)
	}
	if ok, _ := openNS(owner).Exists(ctx, key); ok {
		t.Fatalf("the remote Delete of %s did not reach the group's owner %s", key, owner)
	}
}

// TestBatchSetRoutesEachKeyToItsOwner: found by code review. BatchSet wrote
// every pair into the calling node's own store, with no routing, replication
// or invalidation, so on a cluster a key owned elsewhere landed where no read
// would ever look for it.
func TestBatchSetRoutesEachKeyToItsOwner(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("batchset",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication(),
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	kv := map[string][]byte{}
	for i := 0; i < 30; i++ {
		kv[fmt.Sprintf("batchset/%d", i)] = []byte(fmt.Sprintf("v%d", i))
	}
	if err := openNS("n0").BatchSet(ctx, kv, 0); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	for _, via := range f.ids {
		for key, want := range kv {
			got, err := openNS(via).Get(ctx, key)
			if err != nil || string(got) != string(want) {
				t.Fatalf("Get %s via %s after a BatchSet through n0 = %q, %v", key, via, got, err)
			}
		}
	}
	// And they were replicated: no node holds them all as its own.
	if st := f.caches["n0"].Stats(); st.OwnedKeys == int64(len(kv)) {
		t.Fatalf("n0 owns all %d keys of its BatchSet; they were not routed", st.OwnedKeys)
	}
	var replicas int64
	for _, id := range f.ids {
		replicas += f.caches[id].Stats().ReplicaKeys
	}
	if replicas < int64(len(kv)) {
		t.Fatalf("%d replica copies for %d keys at RF=2; BatchSet did not replicate", replicas, len(kv))
	}
}

// TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash: found by code
// review. A namespace whose options fail validation is still registered, with
// no store; a peer request for it reached the nil store and panicked on the
// handler's goroutine, which takes the whole process down.
func TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	f.caches["n1"].Namespace("misconfigured", peercache.WithCodec("no-such-codec"))
	good := f.caches["n0"].Namespace("misconfigured", peercache.WithL1TTL(-1))
	f.settle(t)

	key := ""
	for i := 0; i < 200 && key == ""; i++ {
		if k := fmt.Sprintf("m/%d", i); f.ownerOf(t, k) == "n1" {
			key = k
		}
	}
	if key == "" {
		t.Fatal("no key owned by n1")
	}
	for name, op := range map[string]func() error{
		"Set": func() error { return good.Set(ctx, key, []byte("v"), 0) },
		"Get": func() error { _, err := good.Get(ctx, key); return err },
		"BatchGet": func() error {
			_, err := good.BatchGet(ctx, []string{key})
			return err
		},
		"Delete": func() error { return good.Delete(ctx, key) },
	} {
		if err := op(); err == nil {
			t.Fatalf("%s against a key whose owner has the namespace misconfigured returned nil", name)
		}
	}
}

// TestCompressedValuesSurviveEveryRemotePath: found by code review.
// Compression had been tested on one node only. Across nodes the owner's
// handler already unframed a value and the caller unframed it again, so a
// remote read failed as a corrupt frame — or, for a value whose first byte
// happened to be a valid frame code, silently lost bytes — and a hand-off
// stored an already framed value framed a second time.
func TestCompressedValuesSurviveEveryRemotePath(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	loader := peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
		return []byte("loaded " + key), 0, nil
	})
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("compressed",
			peercache.WithCompression("lz4", 0), // every value framed
			peercache.WithL1TTL(-1), loader)
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	via := f.ids[0]
	remote := func(prefix string) string {
		for i := 0; i < 200; i++ {
			if k := fmt.Sprintf("%s/%d", prefix, i); f.ownerOf(t, k) != via {
				return k
			}
		}
		t.Fatalf("no remote key for %s", prefix)
		return ""
	}

	// Values whose first byte is 'h', 0x00 and 0x01 — an unknown frame code,
	// and two that look like frame codes.
	for i, val := range [][]byte{[]byte("hello"), {0x00, 'a', 'b'}, {0x01, 'x'}} {
		key := remote(fmt.Sprintf("c%d", i))
		if err := openNS(via).Set(ctx, key, val, 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
		got, err := openNS(via).Get(ctx, key)
		if err != nil || string(got) != string(val) {
			t.Fatalf("remote Get %s = %q, %v; want %q", key, got, err, val)
		}
		batch, err := openNS(via).BatchGet(ctx, []string{key})
		if err != nil || string(batch[key]) != string(val) {
			t.Fatalf("remote BatchGet %s = %q, %v; want %q", key, batch[key], err, val)
		}
	}

	lkey := remote("load")
	got, err := openNS(via).GetOrLoad(ctx, lkey)
	if err != nil || string(got) != "loaded "+lkey {
		t.Fatalf("remote GetOrLoad %s = %q, %v", lkey, got, err)
	}
}

// TestACompressedHandOffIsNotFramedTwice: a draining node sends each key in its
// stored, framed form, and the receiver adopting it framed it a second time, so
// a later read unframed once and returned the inner frame instead of the value.
func TestACompressedHandOffIsNotFramedTwice(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("handoff-compressed",
			peercache.WithCompression("lz4", 0), peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	want := map[string]string{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("hc/%d", i)
		want[key] = fmt.Sprintf("value-%d-%s", i, "padding so lz4 has something to do")
		if err := openNS(f.ids[i%3]).Set(ctx, key, []byte(want[key]), 0); err != nil {
			t.Fatal(err)
		}
	}
	leaver := f.ids[2]
	if err := f.caches[leaver].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if f.caches[leaver].Stats().KeysHandedOff == 0 {
		t.Fatal("the drain handed nothing off")
	}
	f.hardKill(t, leaver)
	f.clk.Advance(2 * time.Second)
	f.settle(t)

	for key, w := range want {
		got, err := readWithRetryFrom(t, f, openNS, f.ids[0], key)
		if err != nil || got != w {
			t.Fatalf("after the drain, %s = %q, %v; want %q", key, got, err, w)
		}
	}
}

// TestALoadDoesNotOverwriteAWriteThatRacedIt: found by code review. A load
// that started before a Set and finished after it stored its older value over
// the newer one, unconditionally, and the newer write was gone until the key's
// TTL — forever with none.
func TestALoadDoesNotOverwriteAWriteThatRacedIt(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c, _ := newCache(t)
	ns := c.Namespace("race", peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) {
		close(entered)
		<-release // the database answered with what it had before the write
		return []byte("v1-from-the-database"), 0, nil
	}))
	ctx := context.Background()

	done := make(chan []byte, 1)
	go func() {
		v, _ := ns.GetOrLoad(ctx, "k")
		done <- v
	}()
	<-entered
	if err := ns.Set(ctx, "k", []byte("v2-written-after"), 0); err != nil {
		t.Fatal(err)
	}
	close(release)
	loaded := <-done

	got, err := ns.Get(ctx, "k")
	if err != nil || string(got) != "v2-written-after" {
		t.Fatalf("after a load raced a Set, the key holds %q (%v); the newer write was overwritten", got, err)
	}
	if string(loaded) != "v2-written-after" {
		t.Fatalf("GetOrLoad returned %q while the key held the newer value", loaded)
	}
}

// TestCloseReleasesTheL1Tier: found by code review. Close closed each
// namespace's owned store and not its L1 tier, whose cache library runs a
// maintenance goroutine — one leaked per namespace per closed cache. Only a
// cache with membership has an L1, so this builds one by hand.
func TestCloseReleasesTheL1Tier(t *testing.T) {
	before := goleak.IgnoreCurrent()

	clk := clock.NewFake(clock.Epoch)
	net := testkit.NewNetwork(clk, 1)
	info := core.Node{ID: "n0", Addr: "n0:7946"}
	m := testkit.NewMembership(info, net, clk, testkit.NewRegistry(), testkit.MembershipConfig{})
	c, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithClock(clk),
		peercache.WithMembership(m), peercache.WithTransport(net.Transport(info)))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		c.Namespace(fmt.Sprintf("l1-%d", i)) // each with an L1 tier
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.Close()
	_ = net.Close()

	goleak.VerifyNone(t, before)
}

// refusingTransport fails to serve, the way a transport whose port is taken
// does, and never reports ready.
type refusingTransport struct{ core.Transport }

func (refusingTransport) Serve(net.Listener, core.Handler) error {
	return errors.New("address already in use")
}
func (refusingTransport) Ready() <-chan struct{} { return make(chan struct{}) }

// TestAFailedNewLeavesNothingRunning: found by code review. New's failure
// paths closed the background workers but not join probation's relay
// goroutine or the sweep ticker, and the transport-refused path left the
// group runtime open. A process that retries New on a busy port leaked on
// every attempt.
func TestAFailedNewLeavesNothingRunning(t *testing.T) {
	before := goleak.IgnoreCurrent()

	clk := clock.NewFake(clock.Epoch)
	network := testkit.NewNetwork(clk, 1)
	info := core.Node{ID: "n0", Addr: "n0:7946"}
	m := testkit.NewMembership(info, network, clk, testkit.NewRegistry(), testkit.MembershipConfig{})
	_, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithClock(clk),
		peercache.WithMembership(m), peercache.WithJoinProbation(time.Minute),
		peercache.WithTransport(refusingTransport{network.Transport(info)}))
	if err == nil {
		t.Fatal("New succeeded with a transport that cannot serve")
	}
	if pending := clk.Pending(); pending != 0 {
		t.Errorf("a failed New left %d timer(s) armed on the clock", pending)
	}
	m.Close()
	_ = network.Close()

	goleak.VerifyNone(t, before)
}
