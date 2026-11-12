package peercache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/testkit"
)

// The tests in this file were written from a coverage report: each covers a
// remote path that no test had executed, though the same operation on a single
// node was tested.

// TestBatchGetAcrossNodes: BatchGet had only ever run on one node, where every
// key is local. Across three, it splits keys by owner, asks each, and merges.
func TestBatchGetAcrossNodes(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("batch", peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	var keys []string
	want := map[string]string{}
	for i := 0; i < 60; i++ {
		key := fmt.Sprintf("batch/%02d", i)
		keys = append(keys, key)
		if i%5 == 4 {
			continue // absent: BatchGet omits it
		}
		want[key] = fmt.Sprintf("v%d", i)
		if err := openNS(f.ids[i%3]).Set(ctx, key, []byte(want[key]), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
	}

	owners := map[string]bool{}
	for _, k := range keys {
		owners[f.ownerOf(t, k)] = true
	}
	if len(owners) < 2 {
		t.Fatalf("the keys landed on %d owner(s); the test needs remote ones", len(owners))
	}

	for _, via := range f.ids {
		got, err := openNS(via).BatchGet(ctx, keys)
		if err != nil {
			t.Fatalf("BatchGet via %s: %v", via, err)
		}
		if len(got) != len(want) {
			t.Fatalf("BatchGet via %s returned %d keys, want %d", via, len(got), len(want))
		}
		for k, v := range want {
			if string(got[k]) != v {
				t.Fatalf("BatchGet via %s: %s = %q, want %q", via, k, got[k], v)
			}
		}
	}
}

// TestExistsAcrossNodes: Exists for a key owned elsewhere asks the owner.
func TestExistsAcrossNodes(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("exists", peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	// A key owned by some node other than the one asking.
	via := f.ids[0]
	key := ""
	for i := 0; i < 100 && key == ""; i++ {
		if k := fmt.Sprintf("exists/%d", i); f.ownerOf(t, k) != via {
			key = k
		}
	}
	if err := openNS(f.ownerOf(t, key)).Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	if ok, err := openNS(via).Exists(ctx, key); err != nil || !ok {
		t.Fatalf("Exists(%s) via %s = %v, %v; want true", key, via, ok, err)
	}
	if ok, err := openNS(via).Exists(ctx, key+"-absent"); err != nil || ok {
		t.Fatalf("Exists of an absent key via %s = %v, %v; want false", via, ok, err)
	}
}

// errBackend stands in for a loader's database being down.
var errBackend = errors.New("backend unavailable")

// TestRemoteGetOrLoadReportsWhatTheOwnersLoaderSaid: the loader runs on the
// owner, so its answer has to come back across the hop as the same answer.
func TestRemoteGetOrLoadReportsWhatTheOwnersLoaderSaid(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	loader := func(_ context.Context, key string) ([]byte, time.Duration, error) {
		switch {
		case strings.HasSuffix(key, "-missing"):
			return nil, 0, peercache.ErrNotFound
		case strings.HasSuffix(key, "-broken"):
			return nil, 0, errBackend
		}
		return []byte("loaded:" + key), 0, nil
	}
	withLoader := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("load", peercache.WithLoader(loader), peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		withLoader(id)
	}
	f.settle(t)

	via := f.ids[0]
	remote := func(suffix string) string {
		for i := 0; i < 200; i++ {
			if k := fmt.Sprintf("load/%d%s", i, suffix); f.ownerOf(t, k) != via {
				return k
			}
		}
		t.Fatalf("no remote key with suffix %q", suffix)
		return ""
	}

	key := remote("")
	got, err := withLoader(via).GetOrLoad(ctx, key)
	if err != nil || !bytes.Equal(got, []byte("loaded:"+key)) {
		t.Fatalf("GetOrLoad(%s) via %s = %q, %v", key, via, got, err)
	}

	if _, err := withLoader(via).GetOrLoad(ctx, remote("-missing")); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a remote loader reporting ErrNotFound came back as %v", err)
	}

	_, err = withLoader(via).GetOrLoad(ctx, remote("-broken"))
	if err == nil {
		t.Fatal("a remote loader's failure came back as success")
	}
	if errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a remote loader's failure came back as ErrNotFound (%v): the caller would "+
			"conclude the key does not exist when the backend was merely down", err)
	}
	if !strings.Contains(err.Error(), errBackend.Error()) {
		t.Fatalf("a remote loader's failure came back as %v, losing the loader's own message", err)
	}
}

// TestRemoteGetOrLoadWithoutALoaderOnTheOwner: a namespace opened without a
// loader reports ErrNoLoader whether the key is local or not.
func TestRemoteGetOrLoadWithoutALoaderOnTheOwner(t *testing.T) {
	f := newFabric(t, 3)
	openNS := func(id string) peercache.Namespace { return f.caches[id].Namespace("noload") }
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	via := f.ids[0]
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("noload/%d", i)
		if f.ownerOf(t, key) == via {
			continue
		}
		if _, err := openNS(via).GetOrLoad(context.Background(), key); !errors.Is(err, peercache.ErrNoLoader) {
			t.Fatalf("GetOrLoad of a remote key in a namespace with no loader returned %v, "+
				"want ErrNoLoader", err)
		}
		return
	}
	t.Fatal("no remote key found")
}

// TestLatencyHistogramsRecordRemoteOperations covers WithLatencyHistograms and
// RemoteLatency, which had benchmarks and no test. Every message is delayed by
// 2 ms of fake time, so a remote read is a round trip of at least 4 ms, and the
// histogram has to say so — in RemoteLatency and in the scrape output.
func TestLatencyHistogramsRecordRemoteOperations(t *testing.T) {
	const delay = 2 * time.Millisecond
	f := newFabricFull(t, 3, false, testkit.MembershipConfig{}, -1,
		withCacheOptions(peercache.WithLatencyHistograms()))
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("lat", peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	via := f.ids[0]
	key := ""
	for i := 0; i < 100 && key == ""; i++ {
		if k := fmt.Sprintf("lat/%d", i); f.ownerOf(t, k) != via {
			key = k
		}
	}
	if err := openNS(f.ownerOf(t, key)).Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}

	if p50, _, _ := f.caches[via].RemoteLatency(); p50 != 0 {
		t.Fatalf("p50 = %d before any remote operation", p50)
	}

	f.net.SetDelay(testkit.Wildcard, testkit.Wildcard, delay)
	t.Cleanup(func() { f.net.SetDelay(testkit.Wildcard, testkit.Wildcard, 0) })

	for i := 0; i < 20; i++ {
		done := make(chan error, 1)
		go func() {
			_, err := openNS(via).Get(ctx, key)
			done <- err
		}()
		// Each hop waits for the fake clock; move it in steps until the
		// round trip completes.
		var err error
		waitForCache(t, "a delayed remote read", func() bool {
			select {
			case err = <-done:
				return true
			default:
				f.clk.Advance(time.Millisecond)
				return false
			}
		})
		if err != nil {
			t.Fatalf("Get %s via %s: %v", key, via, err)
		}
	}

	// Only the lower bound means anything here. The loop above steps the
	// clock while it waits, so a read whose goroutine started late is
	// charged for every step it missed, and the upper quantiles grow with
	// scheduling noise rather than with the network.
	p50, p99, _ := f.caches[via].RemoteLatency()
	t.Logf("remote latency with %v per hop: p50 %v, p99 %v", delay, time.Duration(p50), time.Duration(p99))
	if time.Duration(p50) < 2*delay {
		t.Fatalf("p50 = %v with %v per hop; a round trip is at least %v", time.Duration(p50), delay, 2*delay)
	}

	var scrape bytes.Buffer
	if err := f.caches[via].WriteMetrics(&scrape); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(scrape.String(), "peercache_remote_duration_seconds_bucket") {
		t.Fatal("WithLatencyHistograms is set and the scrape output has no remote duration histogram")
	}

}

// TestLatencyHistogramsAreOffByDefault is the other half.
func TestLatencyHistogramsAreOffByDefault(t *testing.T) {
	c, _ := newCache(t)
	if p50, p99, p999 := c.RemoteLatency(); p50|p99|p999 != 0 {
		t.Fatalf("RemoteLatency = %d, %d, %d without WithLatencyHistograms", p50, p99, p999)
	}
	var scrape bytes.Buffer
	if err := c.WriteMetrics(&scrape); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(scrape.String(), "peercache_remote_duration_seconds") {
		t.Fatal("the remote duration histogram is exported without WithLatencyHistograms")
	}
}
