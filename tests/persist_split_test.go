package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// Tests for decision 0030: one object per flush, split into partitions later.

// putCounter counts Puts, and fails those whose name matches failing.
type putCounter struct {
	persist.BlobStore
	puts    atomic.Int64
	failing atomic.Value // string; "" fails nothing
}

func (s *putCounter) Put(ctx context.Context, name string, data []byte) error {
	s.puts.Add(1)
	if f, _ := s.failing.Load().(string); f != "" && strings.Contains(name, f) {
		return errors.New("store refuses " + f)
	}
	return s.BlobStore.Put(ctx, name, data)
}

func namesUnder(t *testing.T, store persist.BlobStore, part string) []string {
	t.Helper()
	names, err := store.List(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, n := range names {
		if strings.Contains(n, part) {
			out = append(out, n)
		}
	}
	return out
}

// TestAFlushWritesOneObject: a thousand keys spread over every partition are
// one request, not one per partition.
func TestAFlushWritesOneObject(t *testing.T) {
	store := &putCounter{BlobStore: persist.Memory()}
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	for i := 0; i < 1000; i++ {
		_ = ns.Set(context.Background(), fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	flushed(t, c, fc, 1)
	if got := store.puts.Load(); got != 1 {
		t.Fatalf("a flush of 1000 keys made %d requests, want 1", got)
	}
	if logs := namesUnder(t, store, "/log/"); len(logs) != 1 {
		t.Fatalf("logs after one flush: %v", logs)
	}
}

// TestASplitMovesLogsIntoPartitions: after a split no log is left, every
// partition segment is written once, and a cold start still reads every key.
func TestASplitMovesLogsIntoPartitions(t *testing.T) {
	store := &putCounter{BlobStore: persist.Memory()}
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	ctx := context.Background()
	const keys = 1000
	for round := 0; round < 3; round++ {
		for i := 0; i < keys; i++ {
			_ = ns.Set(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("r%d", round)), 0)
		}
		flushed(t, c, fc, 1)
	}
	before := store.puts.Load()
	split(t, c, fc)
	puts := store.puts.Load() - before
	if puts > 64 {
		t.Fatalf("a split wrote %d objects; at most one per partition, 64", puts)
	}
	if logs := namesUnder(t, store, "/log/"); len(logs) != 0 {
		t.Fatalf("logs left after a split: %v", logs)
	}
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}

	c2, _ := newCache(t)
	ns2 := c2.Namespace("orders", peercache.WithPersistence(store))
	for i := 0; i < keys; i++ {
		if v, err := ns2.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || string(v) != "r2" {
			t.Fatalf("k%d after a split and a cold start = %q, %v; want the last round", i, v, err)
		}
	}
}

// TestAFailedSplitLosesNothing: a split that cannot write its segments keeps
// the logs, so a restore still finds everything, and the next split finishes.
func TestAFailedSplitLosesNothing(t *testing.T) {
	store := &putCounter{BlobStore: persist.Memory()}
	c, fc := newCache(t)
	ns := c.Namespace("orders", peercache.WithPersistence(store))
	ctx := context.Background()
	for i := 0; i < 200; i++ {
		_ = ns.Set(ctx, fmt.Sprintf("k%d", i), []byte("v"), 0)
	}
	flushed(t, c, fc, 1)

	store.failing.Store("/p0") // every partition segment
	failures := c.Stats().PersistSplitFailures
	fc.Advance(splitInterval)
	waitFor(t, "the split to fail", func() bool { return c.Stats().PersistSplitFailures > failures })
	if logs := namesUnder(t, store, "/log/"); len(logs) != 1 {
		t.Fatalf("after a failed split the logs are %v; they must stay until the segments are durable", logs)
	}

	c2, _ := newCache(t)
	ns2 := c2.Namespace("orders", peercache.WithPersistence(store))
	if v, err := ns2.Get(ctx, "k7"); err != nil || string(v) != "v" {
		t.Fatalf("a cold start after a failed split reads %q, %v", v, err)
	}

	store.failing.Store("")
	split(t, c, fc)
	if logs := namesUnder(t, store, "/log/"); len(logs) != 0 {
		t.Fatalf("the next split left %v", logs)
	}
}

func TestWithFlushIntervalSetsTheCadence(t *testing.T) {
	store := persist.Memory()
	c, fc := newCache(t)
	ns := c.Namespace("slow", peercache.WithPersistence(store, persist.WithFlushInterval(time.Second)))
	_ = ns.Set(context.Background(), "k", []byte("v"), 0)

	fc.Advance(flushInterval)
	if got := c.Stats().PersistFlushes; got != 0 {
		t.Fatalf("flushed %d times after 100 ms with a one-second interval", got)
	}
	fc.Advance(time.Second - flushInterval)
	waitFor(t, "the one-second flush", func() bool { return c.Stats().PersistFlushes == 1 })
}

func TestANonPositiveFlushIntervalIsRefused(t *testing.T) {
	c, _ := newCache(t)
	for _, d := range []time.Duration{0, -time.Second} {
		ns := c.Namespace(fmt.Sprintf("bad-%d", d), peercache.WithPersistence(persist.Memory(), persist.WithFlushInterval(d)))
		if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
			t.Fatalf("WithFlushInterval(%v): Get = %v, want ErrInvalidOption", d, err)
		}
	}
}
