package peercache_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache"
)

// TestEventHandlerSeesEveryLifecycleEvent covers WithEventHandler, which no
// test had ever configured. It drives one group through its whole life and
// checks the events arrive, in order, on the handler.
//
// EventEvict is absent on purpose: nothing emits it. Group state has no TTL and
// no capacity limit, so there is nothing to evict it for — see open question 43.
func TestEventHandlerSeesEveryLifecycleEvent(t *testing.T) {
	var (
		mu     sync.Mutex
		events []peercache.Event
	)
	c, fc := newCache(t,
		peercache.WithGroupIdleTimeout(time.Second),
		peercache.WithEventHandler(func(e peercache.Event) {
			mu.Lock()
			events = append(events, e)
			mu.Unlock()
		}))

	if err := c.RegisterGroupFunc("step", func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
		switch string(arg) {
		case "create", "update":
			s.Set("k", arg)
		case "delete":
			s.Delete("k")
		case "emit":
			s.Emit(peercache.Event{Kind: peercache.EventCustom, Name: "hello"})
		}
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}

	g := c.Group("g1")
	for _, step := range []string{"create", "update", "delete", "emit"} {
		if _, err := g.Call(context.Background(), "step", []byte(step)); err != nil {
			t.Fatalf("Call %s: %v", step, err)
		}
	}
	fc.Advance(2 * time.Second) // past the idle timeout

	want := []peercache.EventKind{
		peercache.EventActivate, peercache.EventCreate, peercache.EventUpdate,
		peercache.EventDelete, peercache.EventCustom, peercache.EventDeactivate,
	}
	var got []peercache.EventKind
	waitForCache(t, "the deactivation event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		got = got[:0]
		for _, e := range events {
			got = append(got, e.Kind)
		}
		return len(got) >= len(want)
	})
	if !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, e := range events {
		if e.GroupID != "g1" {
			t.Fatalf("event %v carries group %q, want g1", e.Kind, e.GroupID)
		}
	}
	if events[1].Key != "k" || string(events[2].Value) != "update" {
		t.Fatalf("create/update events carry key %q and value %q", events[1].Key, events[2].Value)
	}
	if events[4].Name != "hello" {
		t.Fatalf("custom event name %q, want hello", events[4].Name)
	}
}
