package tests

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

func hotKeysOf(t *testing.T, c peercache.Cache, ns string) []peercache.HotKey {
	t.Helper()
	for _, d := range c.Describe().Namespaces {
		if d.Name == ns {
			return d.HotKeys
		}
	}
	t.Fatalf("namespace %q not described", ns)
	return nil
}

// TestTheHottestKeyIsReportedFirst: one key read far more than a hundred
// others is at the top, with an estimate near its true count.
func TestTheHottestKeyIsReportedFirst(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("catalog", peercache.WithHotKeyTracking(3))
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		_ = ns.Set(ctx, fmt.Sprintf("item:%d", i), []byte("v"), 0)
	}
	for i := 0; i < 3200; i++ {
		_, _ = ns.Get(ctx, "item:7")
		if i%4 == 0 {
			_, _ = ns.Get(ctx, fmt.Sprintf("item:%d", i%100))
		}
	}
	hot := hotKeysOf(t, c, "catalog")
	if len(hot) == 0 || hot[0].Key != "item:7" {
		t.Fatalf("hot keys %+v; want item:7 first", hot)
	}
	if hot[0].Accesses < 2800 || hot[0].Accesses > 4200 {
		t.Fatalf("item:7 estimated at %d accesses, read about 3,200 times", hot[0].Accesses)
	}
	if len(hot) > 3 {
		t.Fatalf("%d hot keys reported, asked for 3", len(hot))
	}
}

// TestHotKeysDecay: a key hot an hour ago and untouched since is no longer
// reported; the list is about now.
func TestHotKeysDecay(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("catalog", peercache.WithHotKeyTracking(3))
	ctx := context.Background()
	for i := 0; i < 1600; i++ {
		_, _ = ns.Get(ctx, "yesterday")
	}
	fc.Advance(time.Hour)
	for i := 0; i < 160; i++ {
		_, _ = ns.Get(ctx, "today")
	}
	hot := hotKeysOf(t, c, "catalog")
	if len(hot) == 0 || hot[0].Key != "today" {
		t.Fatalf("hot keys %+v an hour later; want today first", hot)
	}
	for _, k := range hot {
		if k.Key == "yesterday" {
			t.Fatalf("a key untouched for an hour is still reported: %+v", hot)
		}
	}
}

// TestHotKeysCountWhatPeersAsk: reads arriving from other nodes are counted on
// the owner, where the load lands.
func TestHotKeysCountWhatPeersAsk(t *testing.T) {
	f := newFabric(t, 2)
	for _, id := range f.ids {
		f.caches[id].Namespace("catalog", peercache.WithHotKeyTracking(2), peercache.WithL1TTL(-1))
	}
	via := f.ids[0]
	key := traceRemoteKeyFor(t, f, via)
	ctx := context.Background()
	_ = f.caches[f.ownerOf(t, key)].Namespace("catalog").Set(ctx, key, []byte("v"), 0)
	for i := 0; i < 320; i++ {
		_, _ = f.caches[via].Namespace("catalog").Get(ctx, key)
	}
	hot := hotKeysOf(t, f.caches[f.ownerOf(t, key)], "catalog")
	if len(hot) == 0 || hot[0].Key != key {
		t.Fatalf("the owner's hot keys %+v; want %s, read 320 times by a peer", hot, key)
	}
}

// TestHotKeysAreOffByDefaultAndBounded: without the option nothing names a
// key; outside 0..100 is a configuration error.
func TestHotKeysAreOffByDefaultAndBounded(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("plain")
	for i := 0; i < 100; i++ {
		_, _ = ns.Get(context.Background(), "k")
	}
	if hot := hotKeysOf(t, c, "plain"); hot != nil {
		t.Fatalf("hot keys reported without the option: %+v", hot)
	}
	for _, n := range []int{-1, 101} {
		bad := c.Namespace(fmt.Sprintf("bad%d", n), peercache.WithHotKeyTracking(n))
		if _, err := bad.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
			t.Fatalf("WithHotKeyTracking(%d): %v, want ErrInvalidOption", n, err)
		}
	}
}

// traceRemoteKeyFor returns a key via does not own.
func traceRemoteKeyFor(t *testing.T, f *fabric, via string) string {
	t.Helper()
	for i := 0; i < 100; i++ {
		if k := fmt.Sprintf("h:%d", i); f.ownerOf(t, k) != via {
			return k
		}
	}
	t.Fatal("no key owned elsewhere")
	return ""
}
