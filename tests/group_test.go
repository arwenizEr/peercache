package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

func TestGroupSetGetDelete(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("user:123")
	ctx := context.Background()

	if g.ID() != "user:123" {
		t.Fatalf("ID() = %q", g.ID())
	}
	if _, err := g.Get(ctx, "absent"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get of an absent key = %v, want ErrNotFound", err)
	}
	// Deleting an absent key is not an error: the postcondition holds.
	if err := g.Delete(ctx, "absent"); err != nil {
		t.Fatalf("Delete of an absent key = %v", err)
	}

	if err := g.Set(ctx, "cart", []byte("3")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := g.Get(ctx, "cart")
	if err != nil || string(got) != "3" {
		t.Fatalf("Get = %q %v", got, err)
	}

	if err := g.Delete(ctx, "cart"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := g.Get(ctx, "cart"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

func TestGroupKeysAndItems(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("tenant:acme")
	ctx := context.Background()

	want := map[string][]byte{"orders": []byte("41"), "refunds": []byte("2"), "credits": []byte("7")}
	for k, v := range want {
		if err := g.Set(ctx, k, v); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	keys, err := g.Keys(ctx)
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	// Sorted, so that a group function iterating its own keys behaves the
	// same way on every pod.
	if len(keys) != 3 || keys[0] != "credits" || keys[1] != "orders" || keys[2] != "refunds" {
		t.Fatalf("Keys = %v, want them sorted", keys)
	}

	items, err := g.Items(ctx)
	if err != nil {
		t.Fatalf("Items: %v", err)
	}
	if len(items) != len(want) {
		t.Fatalf("Items returned %d entries, want %d", len(items), len(want))
	}
	for k, v := range want {
		if !bytes.Equal(items[k], v) {
			t.Fatalf("Items[%q] = %q, want %q", k, items[k], v)
		}
	}
}

// TestItemsSnapshotDoesNotAliasGroupState is the one place in this library that
// copies on read. Everything else hands back the stored slice, but this
// snapshot outlives its turn, so aliasing would let a caller mutate state that
// a later turn owns — with no lock between them.
func TestItemsSnapshotDoesNotAliasGroupState(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("g")
	ctx := context.Background()

	if err := g.Set(ctx, "k", []byte("original")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	items, err := g.Items(ctx)
	if err != nil {
		t.Fatalf("Items: %v", err)
	}
	copy(items["k"], "MUTATED!")

	got, err := g.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("mutating the Items snapshot changed the group: %q", got)
	}
}

func TestGroupSetCopiesTheCallersBuffer(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("g")
	ctx := context.Background()

	buf := []byte("original")
	if err := g.Set(ctx, "k", buf); err != nil {
		t.Fatalf("Set: %v", err)
	}
	copy(buf, "MUTATED!")

	got, err := g.Get(ctx, "k")
	if err != nil || string(got) != "original" {
		t.Fatalf("Get = %q %v; Set did not copy", got, err)
	}
}

func TestGroupsAreIsolated(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	if err := c.Group("a").Set(ctx, "k", []byte("from a")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := c.Group("b").Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a key set in one group was visible in another: %v", err)
	}
}

// TestCallSerializesConcurrentCallers is the acceptance criterion at the public
// API: correct arithmetic under concurrency is the proof of serialization,
// because a lost increment is what an overlapping turn would produce.
func TestCallSerializesConcurrentCallers(t *testing.T) {
	const callers = 500

	c, _ := newCache(t, peercache.WithGroupMailboxSize(callers))
	ctx := context.Background()

	if err := c.RegisterGroupFunc("increment", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		var n uint64
		if cur, ok := s.Get("count"); ok {
			n = binary.BigEndian.Uint64(cur)
		}
		n++
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		s.Set("count", buf)
		return buf, nil
	}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	g := c.Group("hot")
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := g.Call(ctx, "increment", nil); err != nil {
				t.Errorf("Call: %v", err)
			}
		}()
	}
	wg.Wait()

	got, err := g.Get(ctx, "count")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n := binary.BigEndian.Uint64(got); n != callers {
		t.Fatalf("counted %d of %d calls: turns overlapped", n, callers)
	}
}

// TestFlushDeactivatesButKeepsState covers the Flush contract: the goroutine
// goes, the state does not.
func TestFlushDeactivatesButKeepsState(t *testing.T) {
	c, _ := newCache(t)
	g := c.Group("g")
	ctx := context.Background()

	if err := g.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got := c.Stats().ActiveGroups; got != 1 {
		t.Fatalf("Stats().ActiveGroups = %d, want 1", got)
	}

	if err := g.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := c.Stats().ActiveGroups; got != 0 {
		t.Fatalf("Stats().ActiveGroups = %d after Flush, want 0", got)
	}
	if got := c.Stats().AddressableGroups; got != 1 {
		t.Fatalf("Stats().AddressableGroups = %d after Flush, want 1", got)
	}

	got, err := g.Get(ctx, "k")
	if err != nil || string(got) != "v" {
		t.Fatalf("Get after Flush = %q %v; state was lost", got, err)
	}
}

// TestGroupTimerFiresThroughThePublicAPI wires ScheduleTimer end to end on the
// injected clock.
func TestGroupTimerFiresThroughThePublicAPI(t *testing.T) {
	c, fc := newCache(t)
	ctx := context.Background()

	fired := make(chan []byte, 1)
	if err := c.RegisterGroupFunc("onTimer", func(_ context.Context, _ peercache.GroupState, arg []byte) ([]byte, error) {
		select {
		case fired <- arg:
		default:
		}
		return nil, nil
	}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	if err := c.RegisterGroupFunc("schedule", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		s.ScheduleTimer("wake", time.Minute, "onTimer", []byte("payload"))
		return nil, nil
	}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	if _, err := c.Group("g").Call(ctx, "schedule", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	fc.Advance(time.Minute)

	if got := <-fired; string(got) != "payload" {
		t.Fatalf("the timer delivered %q", got)
	}
}

// TestGroupFuncPanicIsContained checks that a bug in application code does not
// take the group offline permanently.
func TestGroupFuncPanicIsContained(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	if err := c.RegisterGroupFunc("boom", func(context.Context, peercache.GroupState, []byte) ([]byte, error) {
		panic("group function bug")
	}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}

	g := c.Group("g")
	if _, err := g.Call(ctx, "boom", nil); !errors.Is(err, peercache.ErrGroupFuncPanic) {
		t.Fatalf("Call = %v, want ErrGroupFuncPanic", err)
	}
	// Still usable.
	if err := g.Set(ctx, "k", []byte("v")); err != nil {
		t.Fatalf("Set after a panic: %v", err)
	}
}

// TestMaxActiveGroupsIsEnforced covers the cap through the public option.
func TestMaxActiveGroupsIsEnforced(t *testing.T) {
	const cap = 4
	const groups = 16

	c, _ := newCache(t,
		peercache.WithMaxActiveGroups(cap),
		peercache.WithGroupIdleTimeout(time.Hour),
	)
	ctx := context.Background()

	for i := 0; i < groups; i++ {
		if err := c.Group(fmt.Sprintf("g%d", i)).Set(ctx, "k", []byte("v")); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	waitForCache(t, "the active count to settle within the cap", func() bool {
		return c.Stats().ActiveGroups <= cap
	})
	if got := c.Stats().AddressableGroups; got != groups {
		t.Fatalf("Stats().AddressableGroups = %d, want %d", got, groups)
	}

	// Evicted groups keep their state.
	for i := 0; i < groups; i++ {
		id := fmt.Sprintf("g%d", i)
		got, err := c.Group(id).Get(ctx, "k")
		if err != nil || string(got) != "v" {
			t.Fatalf("group %s lost state across eviction: %q %v", id, got, err)
		}
	}
}

func waitForCache(t *testing.T, what string, cond func() bool) {
	t.Helper()
	const yields = 5_000_000
	for i := 0; !cond(); i++ {
		if i > yields {
			t.Fatalf("timed out waiting for %s", what)
		}
		gosched()
	}
}
