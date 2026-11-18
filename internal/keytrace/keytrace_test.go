package keytrace_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/keytrace"
)

func TestANilTracerRecordsNothing(t *testing.T) {
	var tr *keytrace.Tracer
	tr.Record("ns", "k", keytrace.Event{Kind: keytrace.Write})
	if tr.Traced("ns", "k") || tr.History("ns", "k") != nil || tr.Dropped() != 0 {
		t.Fatal("a nil tracer traced something")
	}
	if keytrace.New(clock.NewFake(clock.Epoch), nil) != nil {
		t.Fatal("a tracer with no prefixes is not nil, so every hook pays for it")
	}
}

func TestOnlyKeysUnderAPrefixAreTraced(t *testing.T) {
	tr := keytrace.New(clock.NewFake(clock.Epoch), map[string][]string{"stock": {"item-"}, "all": {""}})
	for _, c := range []struct {
		ns, key string
		want    bool
	}{
		{"stock", "item-1", true},
		{"stock", "other", false},
		{"orders", "item-1", false},
		{"all", "anything", true},
	} {
		if got := tr.Traced(c.ns, c.key); got != c.want {
			t.Errorf("Traced(%q, %q) = %v, want %v", c.ns, c.key, got, c.want)
		}
	}
	tr.Record("stock", "other", keytrace.Event{Kind: keytrace.Write})
	if h := tr.History("stock", "other"); h != nil {
		t.Fatalf("an untraced key has a history: %v", h)
	}
}

func TestAHistoryKeepsTheNewestEventsOldestFirst(t *testing.T) {
	clk := clock.NewFake(clock.Epoch)
	tr := keytrace.New(clk, map[string][]string{"ns": {""}})
	const n = keytrace.PerKey + 10
	for i := 0; i < n; i++ {
		tr.Record("ns", "k", keytrace.Event{Kind: keytrace.Write, Order: uint64(i)})
		clk.Advance(1)
	}
	h := tr.History("ns", "k")
	if len(h) != keytrace.PerKey {
		t.Fatalf("history holds %d events, want the newest %d", len(h), keytrace.PerKey)
	}
	for i, e := range h {
		if want := uint64(n - keytrace.PerKey + i); e.Order != want {
			t.Fatalf("event %d has order %d, want %d: not the newest, oldest first", i, e.Order, want)
		}
		if i > 0 && e.AtNanos <= h[i-1].AtNanos {
			t.Fatalf("event %d is not stamped after event %d", i, i-1)
		}
	}
}

func TestKeysBeyondTheBoundAreCountedNotTraced(t *testing.T) {
	tr := keytrace.New(clock.NewFake(clock.Epoch), map[string][]string{"ns": {""}})
	for i := 0; i < keytrace.MaxKeys+5; i++ {
		tr.Record("ns", fmt.Sprintf("k%d", i), keytrace.Event{Kind: keytrace.Write})
	}
	if got := tr.Dropped(); got != 5 {
		t.Fatalf("dropped %d, want 5", got)
	}
	if h := tr.History("ns", fmt.Sprintf("k%d", keytrace.MaxKeys)); h != nil {
		t.Fatal("a key beyond the bound was traced")
	}
	if h := tr.History("ns", "k0"); len(h) != 1 {
		t.Fatalf("a key within the bound has %d events, want 1", len(h))
	}
}
