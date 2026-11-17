package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// replicaCopiesFor asks holder, as asker, for its replica copies of the keys
// asker owns — the catch-up a node restarted faster than failure detection
// sends — and returns each key's writer as the copy names it.
func replicaCopiesFor(t *testing.T, f *fabric, asker, holder, ns string, rf int) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, e := range catchUpPage(t, f, asker, holder, ns, rf, true) {
		out[e.Key] = e.From
	}
	return out
}

// catchUpPage sends holder one catch-up request as asker, for what holder
// owns or, with replicas, for its replica copies of what asker owns, and
// returns the entries of the first page.
func catchUpPage(t *testing.T, f *fabric, asker, holder, ns string, rf int, replicas bool) []wire.CatchUpEntry {
	t.Helper()
	client, err := f.net.Transport(core.Node{ID: asker, Addr: asker + ":7946"}).
		Dial(core.Node{ID: holder, Addr: holder + ":7946"})
	if err != nil {
		t.Fatal(err)
	}
	req, err := wire.Encode(&wire.CatchUpRequest{
		Namespace: ns, Requester: asker, ReplicationFactor: rf, Limit: 1000, Replicas: replicas,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := client.Invoke(context.Background(), transport.MethodCatchUp, req)
	if err != nil {
		t.Fatalf("catch-up from %s: %v", holder, err)
	}
	var resp wire.CatchUpResponse
	if err := wire.Decode(raw, &resp); err != nil {
		t.Fatal(err)
	}
	return resp.Entries
}

// TestARelayedCatchUpCopyNamesItsWriter: a replica that sends its copies of
// another node's keys in a catch-up relays them, and each must still name the
// node that wrote it. The asker files a copy under that name; filed under the
// relay instead, the copy reads as another writer's, which is what fork
// detection compares and what eviction asks when its writer returns.
//
// Found by the mutation audit: dropping From from the relayed entries left
// every cluster-level test passing, because none of them read it.
func TestARelayedCatchUpCopyNamesItsWriter(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("writers",
			peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)

	var keys []string
	for i := 0; len(keys) < 5; i++ {
		if k := fmt.Sprintf("w/%d", i); f.ownerOf(t, k) == "n0" {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		if err := open("n0").Set(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}

	got := replicaCopiesFor(t, f, "n0", "n1", "writers", 3)
	for _, k := range keys {
		writer, ok := got[k]
		if !ok {
			t.Fatalf("n1 relayed no copy of %s; it is a replica at RF=3", k)
		}
		if writer != "n0" {
			t.Fatalf("n1 relayed %s as written by %q, want n0, its writer", k, writer)
		}
	}
}

// TestADemotedCopyNamesNoWriterItDidNotWrite: a node that adopted a key under
// one view and loses it under the next files its copy naming no writer, not
// itself. It never wrote the key.
//
// Demotion named the demoting node, and the owned store has no record of who
// wrote a key, so every adopted or promoted key that later moved away claimed
// a write that never happened. TestMajoritySideWritesSurviveAHeal caught it in
// about one run in twenty: a partitioned node adopted hand-offs during the
// heal, under a view still missing nodes, then demoted them as its own.
func TestADemotedCopyNamesNoWriterItDidNotWrite(t *testing.T) {
	f := newFabric(t, 4)
	const name = "demoted"
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace(name,
			peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)
	// Reconciliation visits the namespaces a node holds replicas of, so every
	// node is given some: at RF=3 of four, each holds most of these.
	for i := 0; i < 20; i++ {
		if err := open("n0").Set(context.Background(), fmt.Sprintf("other/%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// A key n3 owns and x ranks second for: with n3 gone, x owns it.
	var key, x string
	for i := 0; key == ""; i++ {
		k := fmt.Sprintf("d/%d", i)
		p, err := f.caches["n0"].DescribeKey(name, k)
		if err != nil {
			t.Fatal(err)
		}
		if p.Holders[0] == "n3" {
			key, x = k, p.Holders[1]
		}
	}
	var writer string
	for _, id := range f.ids[:3] {
		if id != x {
			writer = id
			break
		}
	}

	f.hardKill(t, "n3")
	f.clk.Advance(4 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 3)
	f.settle(t)

	adopted := f.caches[x].Stats().KeysAdopted
	sendCopy(t, f, writer, x, name, key, "written elsewhere")
	if f.caches[x].Stats().KeysAdopted == adopted {
		t.Fatalf("%s did not adopt %s from %s; the test needs it in the owned store", x, key, writer)
	}

	f.revive(t, "n3")
	open("n3")
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	f.settle(t)

	var found bool
	for _, e := range catchUpPage(t, f, "n3", x, name, 3, true) {
		if e.Key != key {
			continue
		}
		found = true
		if e.From == x {
			t.Fatalf("%s holds %s as written by itself; %s wrote it, and %s only adopted it", x, key, writer, x)
		}
	}
	if !found {
		t.Fatalf("%s holds no replica of %s after losing it to n3; it was not demoted", x, key)
	}
}
