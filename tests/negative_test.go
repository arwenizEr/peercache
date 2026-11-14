package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/store"
)

// absentLoader reports every key absent unless present holds it, and counts
// its calls.
type absentLoader struct {
	calls   atomic.Int64
	present atomic.Pointer[string]
	err     error
}

func (l *absentLoader) load(_ context.Context, key string) ([]byte, time.Duration, error) {
	l.calls.Add(1)
	if p := l.present.Load(); p != nil && *p == key {
		return []byte("found:" + key), 0, nil
	}
	if l.err != nil {
		return nil, 0, l.err
	}
	return nil, 0, peercache.ErrNotFound
}

// TestARememberedAbsenceSavesTheLoad: within the TTL, repeated GetOrLoad of a
// key the loader reported missing answers ErrNotFound without calling it; past
// the TTL the loader is asked again.
func TestARememberedAbsenceSavesTheLoad(t *testing.T) {
	c, fc := newCache(t)
	l := &absentLoader{}
	ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Minute))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := ns.GetOrLoad(ctx, "user:404"); !errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("GetOrLoad #%d = %v, want ErrNotFound", i, err)
		}
	}
	if got := l.calls.Load(); got != 1 {
		t.Fatalf("the loader ran %d times for one absent key within the TTL, want 1", got)
	}
	if got := c.Stats().NegativeHits; got != 4 {
		t.Fatalf("NegativeHits = %d, want 4", got)
	}

	fc.Advance(time.Minute)
	if _, err := ns.GetOrLoad(ctx, "user:404"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("GetOrLoad after the TTL = %v", err)
	}
	if got := l.calls.Load(); got != 2 {
		t.Fatalf("the loader ran %d times after the TTL, want 2", got)
	}
}

// TestEveryWriteForgetsAnAbsence: a Set is served, a Delete — the application
// saying "the source changed" — makes the next GetOrLoad load again, and so
// does a value that was set and then expired. None may be answered from the
// absence.
func TestEveryWriteForgetsAnAbsence(t *testing.T) {
	ctx := context.Background()
	type step func(ns peercache.Namespace, fc interface{ Advance(time.Duration) }) error
	cases := map[string]step{
		"Delete": func(ns peercache.Namespace, _ interface{ Advance(time.Duration) }) error {
			return ns.Delete(ctx, "k")
		},
		"Set then expiry": func(ns peercache.Namespace, fc interface{ Advance(time.Duration) }) error {
			if err := ns.Set(ctx, "k", []byte("v"), time.Second); err != nil {
				return err
			}
			fc.Advance(time.Second)
			return nil
		},
		"Increment then expiry": func(ns peercache.Namespace, fc interface{ Advance(time.Duration) }) error {
			if _, err := ns.Increment(ctx, "k", 1, time.Second); err != nil {
				return err
			}
			fc.Advance(time.Second)
			return nil
		},
	}
	for name, write := range cases {
		t.Run(name, func(t *testing.T) {
			c, fc := newCache(t)
			l := &absentLoader{}
			ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Hour))
			if _, err := ns.GetOrLoad(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
				t.Fatalf("first GetOrLoad = %v", err)
			}
			key := "k"
			l.present.Store(&key)
			if err := write(ns, fc); err != nil {
				t.Fatalf("write: %v", err)
			}
			got, err := ns.GetOrLoad(ctx, "k")
			if err != nil || string(got) != "found:k" {
				t.Fatalf("GetOrLoad after %s = %q, %v; the remembered absence outlived a write", name, got, err)
			}
		})
	}

	t.Run("Set", func(t *testing.T) {
		c, _ := newCache(t)
		l := &absentLoader{}
		ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Hour))
		_, _ = ns.GetOrLoad(ctx, "k")
		if err := ns.Set(ctx, "k", []byte("set"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
		if got, err := ns.GetOrLoad(ctx, "k"); err != nil || string(got) != "set" {
			t.Fatalf("GetOrLoad after Set = %q, %v", got, err)
		}
	})
}

// TestOnlyAnAbsenceIsRemembered: a loader failing for any other reason — the
// database down — is asked again on the next call. Caching an outage as "not
// found" would keep answering it after the database came back.
func TestOnlyAnAbsenceIsRemembered(t *testing.T) {
	c, _ := newCache(t)
	l := &absentLoader{err: errors.New("connection refused")}
	ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Hour))
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := ns.GetOrLoad(ctx, "k"); err == nil || errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("GetOrLoad #%d = %v, want the loader's own error", i, err)
		}
	}
	if got := l.calls.Load(); got != 3 {
		t.Fatalf("the loader ran %d times for three failed loads, want 3", got)
	}
}

// TestAnAbsenceIsNotAValue: nothing that reads the store sees an absence.
func TestAnAbsenceIsNotAValue(t *testing.T) {
	c, _ := newCache(t)
	l := &absentLoader{}
	ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Hour))
	ctx := context.Background()
	_, _ = ns.GetOrLoad(ctx, "k")

	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get = %v, want ErrNotFound", err)
	}
	if ok, err := ns.Exists(ctx, "k"); err != nil || ok {
		t.Fatalf("Exists = %v, %v; an absence is not a key", ok, err)
	}
	if got := c.Stats().OwnedKeys; got != 0 {
		t.Fatalf("OwnedKeys = %d after remembering an absence, want 0", got)
	}
}

// TestWithoutNegativeCachingNothingIsRemembered is the default.
func TestWithoutNegativeCachingNothingIsRemembered(t *testing.T) {
	c, _ := newCache(t)
	l := &absentLoader{}
	ns := c.Namespace("users", peercache.WithLoader(l.load))
	for i := 0; i < 3; i++ {
		_, _ = ns.GetOrLoad(context.Background(), "k")
	}
	if got := l.calls.Load(); got != 3 {
		t.Fatalf("the loader ran %d times, want 3: negative caching is opt-in", got)
	}
	bad := c.Namespace("bad", peercache.WithNegativeCaching(-time.Second))
	if _, err := bad.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("a negative TTL: Get = %v, want ErrInvalidOption", err)
	}
}

// TestARemoteCallerGetsTheRememberedAbsence: a probe arriving through a
// non-owner reaches the owner, which answers from its absence, so the database
// is asked once however many pods the probes come through.
func TestARemoteCallerGetsTheRememberedAbsence(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	l := &absentLoader{}
	for _, id := range f.ids {
		f.caches[id].Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Minute))
	}
	f.settle(t)
	const key = "user:404"
	owner := f.ownerOf(t, key)
	for round := 0; round < 3; round++ {
		for _, id := range f.ids {
			if _, err := f.caches[id].Namespace("users").GetOrLoad(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
				t.Fatalf("GetOrLoad via %s = %v, want ErrNotFound", id, err)
			}
		}
	}
	if got := l.calls.Load(); got != 1 {
		t.Fatalf("the loader ran %d times for nine probes through three pods, want 1", got)
	}
	if got := f.caches[owner].Stats().NegativeHits; got != 8 {
		t.Fatalf("the owner answered %d probes from the absence, want 8", got)
	}
}

// TestAbsencesAreBoundedPerShard: a stream of distinct missing keys — the
// probing this exists for — cannot grow the owner's memory without bound.
func TestAbsencesAreBoundedPerShard(t *testing.T) {
	c, _ := newCache(t)
	l := &absentLoader{}
	const shards = 4
	ns := c.Namespace("users", peercache.WithLoader(l.load), peercache.WithNegativeCaching(time.Hour),
		peercache.WithShards(shards))
	for i := 0; i < shards*store.MaxAbsentPerShard*4; i++ {
		_, _ = ns.GetOrLoad(context.Background(), fmt.Sprintf("probe:%d", i))
	}
	if got := c.Describe().Namespaces[0].Absences; got > shards*store.MaxAbsentPerShard || got == 0 {
		t.Fatalf("%d absences remembered across %d shards, want 1..%d", got, shards, shards*store.MaxAbsentPerShard)
	}
}

// TestAWriteDuringTheLoadIsNotHiddenByItsAbsence: a Set lands while the loader
// is out, and the loader then reports the key missing — it read the source
// before the write. The absence must not be recorded over the write, or once
// the written value expired, GetOrLoad would answer "not found" without
// asking the loader, which by then finds the key.
func TestAWriteDuringTheLoadIsNotHiddenByItsAbsence(t *testing.T) {
	c, fc := newCache(t)
	ctx := context.Background()
	var calls atomic.Int64
	var ns peercache.Namespace
	ns = c.Namespace("users", peercache.WithNegativeCaching(time.Hour), peercache.WithLoader(
		func(_ context.Context, key string) ([]byte, time.Duration, error) {
			if calls.Add(1) == 1 {
				if err := ns.Set(ctx, key, []byte("written"), time.Second); err != nil {
					t.Errorf("Set during the load: %v", err)
				}
				return nil, 0, peercache.ErrNotFound
			}
			return []byte("found"), 0, nil
		}))
	_, _ = ns.GetOrLoad(ctx, "k")
	fc.Advance(time.Second)
	if got, err := ns.GetOrLoad(ctx, "k"); err != nil || string(got) != "found" {
		t.Fatalf("GetOrLoad after the written value expired = %q, %v; want the loader asked again", got, err)
	}
}
