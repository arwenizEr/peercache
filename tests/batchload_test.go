package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// batchRecorder is a batch loader per node that records each call's keys.
type batchRecorder struct {
	mu    sync.Mutex
	calls map[string][][]string // node -> the keys of each call
	fail  map[string]error      // node -> the error its loader returns
	omit  func(key string) bool // keys that do not exist
}

func newBatchRecorder() *batchRecorder {
	return &batchRecorder{calls: map[string][][]string{}, fail: map[string]error{}}
}

func (r *batchRecorder) loaderFor(node string) peercache.BatchLoader {
	return func(_ context.Context, keys []string) (map[string][]byte, time.Duration, error) {
		r.mu.Lock()
		r.calls[node] = append(r.calls[node], append([]string(nil), keys...))
		err := r.fail[node]
		r.mu.Unlock()
		if err != nil {
			return nil, 0, err
		}
		out := make(map[string][]byte, len(keys))
		for _, k := range keys {
			if r.omit == nil || !r.omit(k) {
				out[k] = []byte("loaded:" + k)
			}
		}
		return out, 0, nil
	}
}

func (r *batchRecorder) snapshot() map[string][][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][][]string, len(r.calls))
	for k, v := range r.calls {
		out[k] = append([][]string(nil), v...)
	}
	return out
}

func openBatch(f *fabric, r *batchRecorder) {
	for _, id := range f.ids {
		f.caches[id].Namespace("rows", peercache.WithBatchLoader(r.loaderFor(id)), peercache.WithL1TTL(-1))
	}
}

func batchKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("row/%02d", i)
	}
	return keys
}

// TestABatchLoadCostsOneLoaderCallPerOwner is the reason the method exists:
// fifty keys missing across three owners cost each owner one batch loader call
// with exactly the keys it owns — not fifty loads — and a second read loads
// nothing.
func TestABatchLoadCostsOneLoaderCallPerOwner(t *testing.T) {
	f := newFabric(t, 3)
	r := newBatchRecorder()
	openBatch(f, r)
	f.settle(t)
	keys := batchKeys(50)

	got, err := f.caches[f.ids[0]].Namespace("rows").BatchGetOrLoad(context.Background(), keys)
	if err != nil {
		t.Fatalf("BatchGetOrLoad: %v", err)
	}
	if len(got) != len(keys) {
		t.Fatalf("BatchGetOrLoad returned %d of %d keys", len(got), len(keys))
	}
	for _, k := range keys {
		if string(got[k]) != "loaded:"+k {
			t.Fatalf("%s = %q", k, got[k])
		}
	}

	owned := map[string][]string{}
	for _, k := range keys {
		o := f.ownerOf(t, k)
		owned[o] = append(owned[o], k)
	}
	calls := r.snapshot()
	for _, id := range f.ids {
		if len(owned[id]) == 0 {
			if len(calls[id]) != 0 {
				t.Fatalf("%s owns none of the keys but its loader ran %d times", id, len(calls[id]))
			}
			continue
		}
		if len(calls[id]) != 1 {
			t.Fatalf("%s owns %d of the keys and its batch loader ran %d times, want 1", id, len(owned[id]), len(calls[id]))
		}
		gotKeys := append([]string(nil), calls[id][0]...)
		sort.Strings(gotKeys)
		sort.Strings(owned[id])
		if strings.Join(gotKeys, ",") != strings.Join(owned[id], ",") {
			t.Fatalf("%s's loader was asked for %v, it owns %v", id, gotKeys, owned[id])
		}
	}

	if _, err := f.caches[f.ids[1]].Namespace("rows").BatchGetOrLoad(context.Background(), keys); err != nil {
		t.Fatalf("second BatchGetOrLoad: %v", err)
	}
	for id, c := range r.snapshot() {
		if len(c) != len(calls[id]) {
			t.Fatalf("%s's loader ran again for keys it had stored", id)
		}
	}
}

// TestKeysTheBatchLoaderOmitsAreAbsent: a key the loader leaves out of its map
// does not exist; it is left out of the result and nothing is stored for it.
func TestKeysTheBatchLoaderOmitsAreAbsent(t *testing.T) {
	f := newFabric(t, 3)
	r := newBatchRecorder()
	r.omit = func(k string) bool { return strings.HasSuffix(k, "7") }
	openBatch(f, r)
	f.settle(t)
	ns := f.caches[f.ids[0]].Namespace("rows")

	got, err := ns.BatchGetOrLoad(context.Background(), batchKeys(30))
	if err != nil {
		t.Fatalf("BatchGetOrLoad: %v", err)
	}
	for _, k := range batchKeys(30) {
		_, ok := got[k]
		if strings.HasSuffix(k, "7") {
			if ok {
				t.Fatalf("%s, which the loader omitted, is in the result", k)
			}
			if _, err := ns.Get(context.Background(), k); !errors.Is(err, peercache.ErrNotFound) {
				t.Fatalf("Get(%s) = %v; an omitted key was stored", k, err)
			}
		} else if !ok {
			t.Fatalf("%s is missing from the result", k)
		}
	}
}

// TestAWriteDuringTheBatchLoadWins: a Set that lands while the batch loader is
// out is newer than what the loader read, and is what the caller gets and what
// stays stored.
func TestAWriteDuringTheBatchLoadWins(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()
	var ns peercache.Namespace
	ns = c.Namespace("rows", peercache.WithBatchLoader(
		func(_ context.Context, keys []string) (map[string][]byte, time.Duration, error) {
			if err := ns.Set(ctx, "a", []byte("written"), 0); err != nil {
				t.Errorf("Set during the load: %v", err)
			}
			return map[string][]byte{"a": []byte("stale"), "b": []byte("loaded")}, 0, nil
		}))
	got, err := ns.BatchGetOrLoad(ctx, []string{"a", "b"})
	if err != nil {
		t.Fatalf("BatchGetOrLoad: %v", err)
	}
	if string(got["a"]) != "written" || string(got["b"]) != "loaded" {
		t.Fatalf("BatchGetOrLoad = %q; the write that landed during the load must win", got)
	}
	if v, _ := ns.Get(ctx, "a"); string(v) != "written" {
		t.Fatalf("stored a = %q; the load overwrote a newer write", v)
	}
}

// TestBatchGetOrLoadUsesThePerKeyLoaderWithoutABatchOne: the round trips are
// still saved, each missing key loaded once on its own owner; with no loader at
// all, ErrNoLoader.
func TestBatchGetOrLoadUsesThePerKeyLoaderWithoutABatchOne(t *testing.T) {
	f := newFabric(t, 3)
	var mu sync.Mutex
	ranOn := map[string][]string{}
	for _, id := range f.ids {
		id := id
		f.caches[id].Namespace("rows", peercache.WithL1TTL(-1), peercache.WithLoader(
			func(_ context.Context, key string) ([]byte, time.Duration, error) {
				mu.Lock()
				ranOn[key] = append(ranOn[key], id)
				mu.Unlock()
				return []byte("one:" + key), 0, nil
			}))
		f.caches[id].Namespace("bare")
	}
	f.settle(t)
	keys := batchKeys(20)
	got, err := f.caches[f.ids[0]].Namespace("rows").BatchGetOrLoad(context.Background(), keys)
	if err != nil || len(got) != len(keys) {
		t.Fatalf("BatchGetOrLoad = %d keys, %v", len(got), err)
	}
	for _, k := range keys {
		if len(ranOn[k]) != 1 || ranOn[k][0] != f.ownerOf(t, k) {
			t.Fatalf("%s was loaded on %v, want once on its owner %s", k, ranOn[k], f.ownerOf(t, k))
		}
	}
	if _, err := f.caches[f.ids[0]].Namespace("bare").BatchGetOrLoad(context.Background(), keys); !errors.Is(err, peercache.ErrNoLoader) {
		t.Fatalf("BatchGetOrLoad without a loader = %v, want ErrNoLoader", err)
	}
}

// TestAFailingOwnersShareIsMissing: one owner's loader fails; its keys are
// missing and its error is returned, beside every other owner's keys.
func TestAFailingOwnersShareIsMissing(t *testing.T) {
	f := newFabric(t, 3)
	r := newBatchRecorder()
	openBatch(f, r)
	f.settle(t)
	keys := batchKeys(50)
	broken := f.ownerOf(t, keys[0])
	r.fail[broken] = errors.New("the orders database is down")

	got, err := f.caches[f.ids[0]].Namespace("rows").BatchGetOrLoad(context.Background(), keys)
	if err == nil || !strings.Contains(err.Error(), "the orders database is down") {
		t.Fatalf("BatchGetOrLoad error = %v, want the failing owner's", err)
	}
	if errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a failed load came back as ErrNotFound: %v", err)
	}
	for _, k := range keys {
		_, ok := got[k]
		if f.ownerOf(t, k) == broken && ok {
			t.Fatalf("%s, owned by the failing node, is in the result", k)
		}
		if f.ownerOf(t, k) != broken && !ok {
			t.Fatalf("%s, owned by a healthy node, is missing", k)
		}
	}
}

// TestAMisroutedLoadNamesTheKeys: a load request reaching a node that does not
// own some of its keys — ownership moved in flight — names them rather than
// loading them, or leaving them out as if they did not exist.
func TestAMisroutedLoadNamesTheKeys(t *testing.T) {
	f := newFabric(t, 3)
	r := newBatchRecorder()
	openBatch(f, r)
	f.settle(t)

	target := f.ids[1]
	var mine, theirs []string
	for _, k := range batchKeys(40) {
		if f.ownerOf(t, k) == target {
			mine = append(mine, k)
		} else {
			theirs = append(theirs, k)
		}
	}
	req, err := wire.Encode(&wire.BatchGetRequest{Namespace: "rows", Keys: append(append([]string(nil), mine...), theirs...), Load: true})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	client, err := f.net.Transport(f.nodes[f.ids[0]]).Dial(f.nodes[target])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	raw, err := client.Invoke(context.Background(), transport.MethodBatchGet, req)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	var resp wire.BatchGetResponse
	if err := wire.Decode(raw, &resp); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	sort.Strings(resp.Misrouted)
	sort.Strings(theirs)
	if strings.Join(resp.Misrouted, ",") != strings.Join(theirs, ",") {
		t.Fatalf("misrouted = %v, want the keys %s does not own: %v", resp.Misrouted, target, theirs)
	}
	if len(resp.Entries) != len(mine) {
		t.Fatalf("%d entries, want the %d keys %s owns", len(resp.Entries), len(mine), target)
	}
	for _, c := range r.snapshot()[target] {
		for _, k := range c {
			if f.ownerOf(t, k) != target {
				t.Fatalf("%s loaded %s, which it does not own", target, k)
			}
		}
	}
}

// TestABatchLoadRemembersAndSkipsAbsences: in a namespace with negative
// caching, keys the batch loader omitted are remembered, and the next batch
// does not ask the loader for them (decision 0035, with 0034).
func TestABatchLoadRemembersAndSkipsAbsences(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()
	var asked [][]string
	ns := c.Namespace("rows", peercache.WithNegativeCaching(time.Hour), peercache.WithBatchLoader(
		func(_ context.Context, keys []string) (map[string][]byte, time.Duration, error) {
			asked = append(asked, append([]string(nil), keys...))
			return map[string][]byte{"here": []byte("v")}, 0, nil
		}))
	if _, err := ns.BatchGetOrLoad(ctx, []string{"here", "gone"}); err != nil {
		t.Fatalf("BatchGetOrLoad: %v", err)
	}
	if _, err := ns.BatchGetOrLoad(ctx, []string{"here", "gone", "new"}); err != nil {
		t.Fatalf("second BatchGetOrLoad: %v", err)
	}
	if len(asked) != 2 || strings.Join(asked[1], ",") != "new" {
		t.Fatalf("the loader was asked %v; the second batch should ask only for new", asked)
	}
	if got := c.Stats().NegativeHits; got != 1 {
		t.Fatalf("NegativeHits = %d, want 1: the second batch skipped gone", got)
	}
}

// misroutingTransport answers a caller's next batch load as if ownership had
// moved in flight: one key the owner returned is reported misrouted instead.
type misroutingTransport struct {
	core.Transport
	armed atomic.Bool
	moved atomic.Value // string: the key reported misrouted
}

func (t *misroutingTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &misroutingClient{PeerClient: inner, parent: t}, nil
}

// Ready forwards readiness; see countingTransport.Ready for why it must.
func (t *misroutingTransport) Ready() <-chan struct{} {
	if r, ok := t.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

type misroutingClient struct {
	core.PeerClient
	parent *misroutingTransport
}

func (c *misroutingClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	out, err := c.PeerClient.Invoke(ctx, method, req)
	if err != nil || method != transport.MethodBatchGet || !c.parent.armed.CompareAndSwap(true, false) {
		return out, err
	}
	var resp wire.BatchGetResponse
	if err := wire.Decode(out, &resp); err != nil || len(resp.Entries) == 0 {
		return out, err
	}
	moved := resp.Entries[0].Key
	c.parent.moved.Store(moved)
	resp.Entries = resp.Entries[1:]
	resp.Misrouted = append(resp.Misrouted, moved)
	return wire.Encode(&resp)
}

// TestACallerAsksAgainForMisroutedKeys is the other half of
// TestAMisroutedLoadNamesTheKeys: keys an owner names as not its own must be
// asked for again, one at a time, wherever they live now — not dropped from
// the result as if they did not exist.
func TestACallerAsksAgainForMisroutedKeys(t *testing.T) {
	var caller *misroutingTransport
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			if caller == nil {
				caller = &misroutingTransport{Transport: tr}
				return caller
			}
			return tr
		}, nil))
	r := newBatchRecorder()
	openBatch(f, r)
	f.settle(t)
	via := f.ids[0]

	var remote []string
	for _, k := range batchKeys(40) {
		if f.ownerOf(t, k) != via {
			remote = append(remote, k)
		}
	}
	if len(remote) == 0 {
		t.Fatal("no key owned elsewhere")
	}
	caller.armed.Store(true)
	got, err := f.caches[via].Namespace("rows").BatchGetOrLoad(context.Background(), remote)
	if err != nil {
		t.Fatalf("BatchGetOrLoad: %v", err)
	}
	moved, _ := caller.moved.Load().(string)
	if moved == "" {
		t.Fatal("no batch load was answered; the test proved nothing")
	}
	if _, ok := got[moved]; !ok {
		t.Fatalf("%s, reported misrouted, is missing from the result; it was dropped rather than "+
			"asked for again", moved)
	}
	if len(got) != len(remote) {
		t.Fatalf("%d of %d keys returned", len(got), len(remote))
	}
}
