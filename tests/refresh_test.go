package tests

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
)

// refreshLoader returns "v<n>" for its nth call with a ten-second TTL. While
// gate is non-nil every call after the first waits on it, and fail makes calls
// after the first fail.
type refreshLoader struct {
	calls atomic.Int64
	gate  chan struct{}
	fail  atomic.Bool
}

func (l *refreshLoader) load(ctx context.Context, _ string) ([]byte, time.Duration, error) {
	n := l.calls.Add(1)
	if n > 1 {
		if l.gate != nil {
			select {
			case <-l.gate:
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			}
		}
		if l.fail.Load() {
			return nil, 0, errors.New("the database is restarting")
		}
	}
	return []byte(fmt.Sprintf("v%d", n)), 10 * time.Second, nil
}

func refreshNS(t *testing.T, l *refreshLoader) (peercache.Cache, peercache.Namespace, interface{ Advance(time.Duration) }) {
	t.Helper()
	c, fc := newCache(t)
	ns := c.Namespace("hot", peercache.WithLoader(l.load), peercache.WithRefreshAhead(2*time.Second))
	if got, err := ns.GetOrLoad(context.Background(), "k"); err != nil || string(got) != "v1" {
		t.Fatalf("first GetOrLoad = %q, %v", got, err)
	}
	return c, ns, fc
}

// TestARefreshReloadsAKeyNearItsExpiry: a hit outside the window reloads
// nothing; a hit inside it returns the current value at once and reloads the
// key in the background, and the reloaded value lives a full TTL from then.
func TestARefreshReloadsAKeyNearItsExpiry(t *testing.T) {
	l := &refreshLoader{}
	c, ns, fc := refreshNS(t, l)
	ctx := context.Background()

	fc.Advance(7 * time.Second) // 3s left, outside the 2s window
	if got, _ := ns.GetOrLoad(ctx, "k"); string(got) != "v1" {
		t.Fatalf("GetOrLoad outside the window = %q", got)
	}
	// A reload would have been started by now; give it every chance to
	// show up before concluding there was none.
	for i := 0; i < 10_000; i++ {
		runtime.Gosched()
	}
	if got := l.calls.Load(); got != 1 {
		t.Fatalf("the loader ran %d times after a hit with 3s left of a 2s window, want 1", got)
	}
	fc.Advance(2 * time.Second) // 1s left, inside it
	if got, _ := ns.GetOrLoad(ctx, "k"); string(got) != "v1" {
		t.Fatalf("GetOrLoad inside the window = %q, want the current value at once", got)
	}
	waitFor(t, "the refresh", func() bool { return c.Stats().Refreshes == 1 })
	if got := l.calls.Load(); got != 2 {
		t.Fatalf("the loader ran %d times, want 2: one load, one refresh", got)
	}
	if got, _ := ns.Get(ctx, "k"); string(got) != "v2" {
		t.Fatalf("Get after the refresh = %q, want v2", got)
	}
	fc.Advance(5 * time.Second) // past the first value's expiry, inside the second's TTL
	if got, err := ns.Get(ctx, "k"); err != nil || string(got) != "v2" {
		t.Fatalf("Get past the old expiry = %q, %v; the refresh did not renew the lifetime", got, err)
	}
}

// TestConcurrentHitsStartOneRefresh: fifty reads inside the window while the
// reload is out start one reload, not fifty.
func TestConcurrentHitsStartOneRefresh(t *testing.T) {
	l := &refreshLoader{gate: make(chan struct{})}
	c, ns, fc := refreshNS(t, l)
	fc.Advance(9 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, _ := ns.GetOrLoad(context.Background(), "k"); string(got) != "v1" {
				t.Errorf("GetOrLoad during the refresh = %q", got)
			}
		}()
	}
	wg.Wait()
	close(l.gate)
	waitFor(t, "the refresh", func() bool { return c.Stats().Refreshes == 1 })
	if got := l.calls.Load(); got != 2 {
		t.Fatalf("the loader ran %d times for fifty hits inside the window, want 2", got)
	}
	// The loader count alone cannot tell one refresh from fifty: fifty would
	// join one single-flight load. Close waits for every refresh in flight,
	// and each counts itself, so what is left is how many were started.
	if err := c.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := c.Stats().Refreshes; got != 1 {
		t.Fatalf("fifty hits inside the window started %d refreshes, want 1", got)
	}
}

// TestAWriteDuringARefreshWins: a Set or a Delete that lands while the reload
// is out is newer than what the loader read; the reload's value is dropped.
func TestAWriteDuringARefreshWins(t *testing.T) {
	for name, write := range map[string]func(ns peercache.Namespace) error{
		"Set":    func(ns peercache.Namespace) error { return ns.Set(context.Background(), "k", []byte("app"), 0) },
		"Delete": func(ns peercache.Namespace) error { return ns.Delete(context.Background(), "k") },
	} {
		t.Run(name, func(t *testing.T) {
			l := &refreshLoader{gate: make(chan struct{})}
			c, ns, fc := refreshNS(t, l)
			fc.Advance(9 * time.Second)
			_, _ = ns.GetOrLoad(context.Background(), "k")
			waitFor(t, "the reload to start", func() bool { return l.calls.Load() == 2 })
			if err := write(ns); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			close(l.gate)
			waitFor(t, "the refresh", func() bool { return c.Stats().Refreshes == 1 })
			got, err := ns.Get(context.Background(), "k")
			switch name {
			case "Set":
				if string(got) != "app" {
					t.Fatalf("Get = %q, %v; the refresh stored over the Set", got, err)
				}
			case "Delete":
				if !errors.Is(err, peercache.ErrNotFound) {
					t.Fatalf("Get = %q, %v; the refresh put back a deleted key", got, err)
				}
			}
		})
	}
}

// TestAFailedRefreshKeepsTheValue: the database is down when the reload runs;
// the current value is served until its own expiry, and the failure counted.
func TestAFailedRefreshKeepsTheValue(t *testing.T) {
	l := &refreshLoader{}
	l.fail.Store(true)
	c, ns, fc := refreshNS(t, l)
	fc.Advance(9 * time.Second)
	_, _ = ns.GetOrLoad(context.Background(), "k")
	waitFor(t, "the failed refresh", func() bool { return c.Stats().RefreshFailures == 1 })
	if got, err := ns.Get(context.Background(), "k"); err != nil || string(got) != "v1" {
		t.Fatalf("Get after a failed refresh = %q, %v; want the current value kept", got, err)
	}
	if got := c.Stats().Refreshes; got != 0 {
		t.Fatalf("Refreshes = %d after a failed reload, want 0", got)
	}
}

// TestCloseWaitsForARefresh: Close does not close the store under a reload in
// flight.
func TestCloseWaitsForARefresh(t *testing.T) {
	l := &refreshLoader{gate: make(chan struct{})}
	c, ns, fc := refreshNS(t, l)
	fc.Advance(9 * time.Second)
	_, _ = ns.GetOrLoad(context.Background(), "k")
	waitFor(t, "the reload to start", func() bool { return l.calls.Load() == 2 })

	closed := make(chan struct{})
	go func() {
		_ = c.Close(context.Background())
		close(closed)
	}()
	for i := 0; i < 10_000; i++ {
		select {
		case <-closed:
			t.Fatal("Close returned while a refresh was in flight")
		default:
		}
		runtime.Gosched()
	}
	close(l.gate)
	<-closed
}

// TestRefreshAheadNeedsALoader: a window on a namespace that cannot reload is
// a misconfiguration, and so is a negative one.
func TestRefreshAheadNeedsALoader(t *testing.T) {
	c, _ := newCache(t)
	for name, ns := range map[string]peercache.Namespace{
		"no loader": c.Namespace("a", peercache.WithRefreshAhead(time.Second)),
		"negative":  c.Namespace("b", peercache.WithRefreshAhead(-time.Second), peercache.WithLoader((&refreshLoader{}).load)),
	} {
		if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
			t.Fatalf("%s: Get = %v, want ErrInvalidOption", name, err)
		}
	}
}
