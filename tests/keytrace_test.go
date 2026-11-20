package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// kinds lists a history's event kinds, for failure messages and checks.
func kinds(h []peercache.KeyEvent) []string {
	out := make([]string, len(h))
	for i, e := range h {
		out[i] = e.Kind
	}
	return out
}

func lastOf(h []peercache.KeyEvent, kind string) (peercache.KeyEvent, bool) {
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].Kind == kind {
			return h[i], true
		}
	}
	return peercache.KeyEvent{}, false
}

// TestATracedKeyRecordsItsWritesAndAnUntracedOneNothing is decision 0043's
// first criterion: the trace names the keys it records, and records each
// write with its order, writer and a hash that tells values apart.
func TestATracedKeyRecordsItsWritesAndAnUntracedOneNothing(t *testing.T) {
	c, _ := newCache(t, peercache.WithNodeID("solo"), peercache.WithKeyTrace("stock", "item-"))
	ns := c.Namespace("stock")
	ctx := context.Background()
	for _, v := range []string{"a", "b", "b"} {
		if err := ns.Set(ctx, "item-1", []byte(v), 0); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ns.Increment(ctx, "item-2", 3, 0); err != nil {
		t.Fatal(err)
	}
	_ = ns.Set(ctx, "other", []byte("x"), 0)
	_ = ns.Delete(ctx, "item-1")

	h := c.KeyHistory("stock", "item-1")
	if got := kinds(h); fmt.Sprint(got) != "[write write write delete]" {
		t.Fatalf("item-1's history is %v, want three writes and a delete", got)
	}
	if h[0].Hash == h[1].Hash || h[1].Hash != h[2].Hash || h[3].Hash != "" {
		t.Fatalf("hashes %q %q %q %q: two values must differ, one value must match, a delete has none",
			h[0].Hash, h[1].Hash, h[2].Hash, h[3].Hash)
	}
	for i, e := range h {
		if e.Writer != "solo" || e.Order == 0 || (i > 0 && e.Order <= h[i-1].Order) {
			t.Fatalf("event %d %+v: want writer solo and an order above the one before", i, e)
		}
	}
	if h := c.KeyHistory("stock", "item-2"); len(h) != 1 || h[0].Kind != "write" {
		t.Fatalf("an Increment is a write: history %v", kinds(h))
	}
	if h := c.KeyHistory("stock", "other"); h != nil {
		t.Fatalf("an untraced key has a history: %v", kinds(h))
	}

	// The debug handler serves the same, naming the node.
	rec := httptest.NewRecorder()
	c.DebugHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/keys?namespace=stock&key=item-1", nil))
	var body struct {
		Node   string               `json:"node"`
		Events []peercache.KeyEvent `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Node != "solo" || len(body.Events) != 4 {
		t.Fatalf("/keys answered %d %s (%v)", rec.Code, rec.Body.String(), err)
	}
}

// TestAKeysHistoryFollowsItAcrossNodes: the replica records the copy under the
// writer's order and hash, and the replica that takes over records the
// promotion — the sequence a kind incident has to be read from.
func TestAKeysHistoryFollowsItAcrossNodes(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithKeyTrace("traced", "")))
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("traced", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)
	const key = "k"
	owner := f.ownerOf(t, key)
	if err := open(owner).Set(context.Background(), key, []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	w, ok := lastOf(f.caches[owner].KeyHistory("traced", key), "write")
	if !ok {
		t.Fatalf("the owner recorded no write: %v", kinds(f.caches[owner].KeyHistory("traced", key)))
	}
	var replica string
	for _, id := range f.ids {
		if id == owner {
			continue
		}
		if r, ok := lastOf(f.caches[id].KeyHistory("traced", key), "replica-accepted"); ok {
			replica = id
			if r.Order != w.Order || r.Hash != w.Hash || r.Writer != owner {
				t.Fatalf("%s recorded the copy as %+v; the write was %+v by %s", id, r, w, owner)
			}
		}
	}
	if replica == "" {
		t.Fatal("no node recorded the replica copy")
	}

	killAndSettle(t, f, owner)
	p, ok := lastOf(f.caches[replica].KeyHistory("traced", key), "promote")
	if !ok || p.Order != w.Order || p.Hash != w.Hash {
		t.Fatalf("%s took over without recording a promotion of the written value: %v",
			replica, kinds(f.caches[replica].KeyHistory("traced", key)))
	}
}

// TestAShedAndItsReadBackAreTraced: the two events open questions 61 and 62
// were about, in the order they happened.
func TestAShedAndItsReadBackAreTraced(t *testing.T) {
	c, fc := newCache(t, peercache.WithMemoryBudget(128<<10), peercache.WithKeyTrace("stock", "item-"))
	stock := c.Namespace("stock", peercache.WithPersistence(persist.Memory()))
	bulk := c.Namespace("bulk")
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	for i := 0; i < 4000; i++ {
		_ = bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0)
	}
	found := false
	for i := 0; i < 20 && !found; i++ {
		key := fmt.Sprintf("item-%d", i)
		if _, ok := lastOf(c.KeyHistory("stock", key), "shed"); !ok {
			continue
		}
		if _, err := stock.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
		h := c.KeyHistory("stock", key)
		rb, ok := lastOf(h, "read-back")
		wr, _ := lastOf(h, "write")
		if !ok || rb.Order != wr.Order || rb.Hash != wr.Hash {
			t.Fatalf("%s was shed and read back; history %v, read-back %+v against write %+v", key, kinds(h), rb, wr)
		}
		found = true
	}
	if !found {
		t.Fatal("no traced key was shed")
	}
}

// TestARestoreAndACatchUpAreTraced: the two ways a key comes back to a node
// that lost its memory, each named as such.
func TestARestoreAndACatchUpAreTraced(t *testing.T) {
	t.Run("restore", func(t *testing.T) {
		store := persist.Memory()
		ctx := context.Background()
		first, fc := newCache(t, peercache.WithKeyTrace("durable", ""))
		if err := first.Namespace("durable", peercache.WithPersistence(store)).Set(ctx, "k", []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		flushed(t, first, fc, 1)
		w, _ := lastOf(first.KeyHistory("durable", "k"), "write")

		second, _ := newCache(t, peercache.WithKeyTrace("durable", ""))
		if _, err := second.Namespace("durable", peercache.WithPersistence(store)).Get(ctx, "k"); err != nil {
			t.Fatal(err)
		}
		r, ok := lastOf(second.KeyHistory("durable", "k"), "restore")
		if !ok || r.Order != w.Order || r.Hash != w.Hash {
			t.Fatalf("a restored key's history is %v; want a restore of the write %+v",
				kinds(second.KeyHistory("durable", "k")), w)
		}
	})
	t.Run("catch-up", func(t *testing.T) {
		f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
			withCacheOptions(peercache.WithKeyTrace("traced", "")))
		open := func(id string) peercache.Namespace {
			return f.caches[id].Namespace("traced", peercache.WithReplicationFactor(3))
		}
		for _, id := range f.ids {
			open(id)
		}
		f.settle(t)
		const key = "k"
		owner := f.ownerOf(t, key)
		var victim string
		for _, id := range f.ids {
			if id != owner {
				victim = id
				break
			}
		}
		killAndSettle(t, f, victim)
		if err := open(owner).Set(context.Background(), key, []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
		f.revive(t, victim)
		open(victim)
		f.clk.Advance(2 * testkit.DefaultJoinDelay)
		f.convergeTo(t, len(f.ids))
		f.settle(t)
		if err := f.caches[victim].CatchUp(context.Background()); err != nil {
			t.Fatal(err)
		}
		h := f.caches[victim].KeyHistory("traced", key)
		found := false
		for _, e := range h {
			if e.Kind == "catch-up" {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s caught up on %s without a catch-up event: %+v", victim, key, h)
		}
	})
}

// TestAReplicaCopyRemovedByADeleteIsTraced: a replica copy that leaves because
// its writer deleted the key says so. Untraced, a copy that vanished from a
// replica left no event at all, and a durable-simulation loss of a carts key
// (open question 69) could not be told from a drop with no cause.
func TestAReplicaCopyRemovedByADeleteIsTraced(t *testing.T) {
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithKeyTrace("traced", "")))
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("traced", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	f.settle(t)
	const key = "k"
	owner := f.ownerOf(t, key)
	ctx := context.Background()
	if err := open(owner).Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	if err := open(owner).Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	deleted := 0
	for _, id := range f.ids {
		if id == owner {
			continue
		}
		if e, ok := lastOf(f.caches[id].KeyHistory("traced", key), "replica-deleted"); ok {
			deleted++
			if e.Writer != owner {
				t.Errorf("%s recorded the deletion as by %q, want %s", id, e.Writer, owner)
			}
		}
	}
	if deleted != len(f.ids)-1 {
		t.Fatalf("%d replicas recorded the deletion that removed their copy, want %d", deleted, len(f.ids)-1)
	}
}
