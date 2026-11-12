package singleflight_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/singleflight"
)

func TestDoReturnsTheLoadersResult(t *testing.T) {
	var g singleflight.Group

	got := g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
		return []byte("value"), time.Minute, nil
	})
	if got.Err != nil {
		t.Fatalf("Err = %v", got.Err)
	}
	if string(got.Value) != "value" || got.TTL != time.Minute {
		t.Fatalf("Result = %q %v, want \"value\" 1m", got.Value, got.TTL)
	}
	if got.Shared {
		t.Fatal("Shared = true for an uncontended load")
	}
	if g.Loads() != 1 || g.Collapsed() != 0 {
		t.Fatalf("loads=%d collapsed=%d, want 1 and 0", g.Loads(), g.Collapsed())
	}
}

func TestDoPropagatesTheLoadersError(t *testing.T) {
	var g singleflight.Group
	sentinel := errors.New("database is on fire")

	got := g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
		return nil, 0, sentinel
	})
	if !errors.Is(got.Err, sentinel) {
		t.Fatalf("Err = %v, want the loader's error", got.Err)
	}
}

// TestCollapsesConcurrentLoads is the single-flight acceptance criterion: a loader that
// counts invocations under a thousand concurrent misses reports exactly one.
//
// Spawning a thousand goroutines proves nothing on its own — they could run one
// after another and each see the previous result. The loader is therefore held
// open until every caller is provably inside the group, which is what Inflight
// reports. No sleeping: the test yields until the count is reached.
func TestCollapsesConcurrentLoads(t *testing.T) {
	const callers = 1000

	var g singleflight.Group
	var invocations atomic.Int64
	release := make(chan struct{})

	results := make(chan singleflight.Result, callers)
	for i := 0; i < callers; i++ {
		go func() {
			results <- g.Do(context.Background(), "hot", func(context.Context) ([]byte, time.Duration, error) {
				invocations.Add(1)
				<-release
				return []byte("loaded once"), time.Minute, nil
			})
		}()
	}

	// Every caller is now attached to the same in-flight load.
	for g.Inflight() < callers {
		runtime.Gosched()
	}

	close(release)

	shared := 0
	for i := 0; i < callers; i++ {
		got := <-results
		if got.Err != nil {
			t.Fatalf("caller %d: %v", i, got.Err)
		}
		if string(got.Value) != "loaded once" {
			t.Fatalf("caller %d got %q", i, got.Value)
		}
		if got.Shared {
			shared++
		}
	}

	if got := invocations.Load(); got != 1 {
		t.Fatalf("loader ran %d times under %d concurrent misses, want exactly 1", got, callers)
	}
	if g.Loads() != 1 {
		t.Fatalf("Loads() = %d, want 1", g.Loads())
	}
	if g.Collapsed() != callers-1 {
		t.Fatalf("Collapsed() = %d, want %d", g.Collapsed(), callers-1)
	}
	if shared != callers {
		t.Fatalf("%d of %d results reported Shared, want all of them", shared, callers)
	}
}

// TestDifferentKeysDoNotCollapse is the other half: collapsing must be per key,
// not global, or one hot key would serialise the whole namespace.
func TestDifferentKeysDoNotCollapse(t *testing.T) {
	const keys = 50

	var g singleflight.Group
	var running atomic.Int64
	release := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < keys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g.Do(context.Background(), fmt.Sprintf("k%d", i), func(context.Context) ([]byte, time.Duration, error) {
				running.Add(1)
				<-release
				return nil, 0, nil
			})
		}(i)
	}

	// All fifty loads must be able to run at once.
	for running.Load() < keys {
		runtime.Gosched()
	}
	close(release)
	wg.Wait()

	if g.Loads() != keys {
		t.Fatalf("Loads() = %d, want %d", g.Loads(), keys)
	}
	if g.Collapsed() != 0 {
		t.Fatalf("Collapsed() = %d, want 0", g.Collapsed())
	}
}

// TestSequentialCallsDoNotCollapse guards against a call that is never removed
// from the map, which would serve one stale result forever.
func TestSequentialCallsDoNotCollapse(t *testing.T) {
	var g singleflight.Group
	var invocations atomic.Int64

	for i := 0; i < 5; i++ {
		i := i
		got := g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
			invocations.Add(1)
			return []byte(fmt.Sprintf("v%d", i)), 0, nil
		})
		if string(got.Value) != fmt.Sprintf("v%d", i) {
			t.Fatalf("call %d returned %q; a completed load was not released", i, got.Value)
		}
	}
	if got := invocations.Load(); got != 5 {
		t.Fatalf("loader ran %d times for 5 sequential calls, want 5", got)
	}
}

// TestCallerDeadlineDoesNotAbortTheLoad is the reason this is not
// golang.org/x/sync/singleflight. One impatient caller must not fail nine
// hundred patient ones.
func TestCallerDeadlineDoesNotAbortTheLoad(t *testing.T) {
	var g singleflight.Group
	release := make(chan struct{})
	loaderSawCancel := make(chan bool, 1)

	// The leader will give up; a second caller will not.
	leaderCtx, cancelLeader := context.WithCancel(context.Background())

	leaderResult := make(chan singleflight.Result, 1)
	go func() {
		leaderResult <- g.Do(leaderCtx, "k", func(loadCtx context.Context) ([]byte, time.Duration, error) {
			<-release
			loaderSawCancel <- loadCtx.Err() != nil
			return []byte("finished anyway"), time.Minute, nil
		})
	}()

	for g.Inflight() < 1 {
		runtime.Gosched()
	}

	followerResult := make(chan singleflight.Result, 1)
	go func() {
		followerResult <- g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
			t.Error("the follower started a second load")
			return nil, 0, nil
		})
	}()

	for g.Inflight() < 2 {
		runtime.Gosched()
	}

	cancelLeader()
	if got := <-leaderResult; !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("leader = %v, want context.Canceled", got.Err)
	}

	close(release)

	if cancelled := <-loaderSawCancel; cancelled {
		t.Fatal("the loader's context was cancelled with the leader's; it must be detached")
	}
	got := <-followerResult
	if got.Err != nil {
		t.Fatalf("follower = %v, want the completed load", got.Err)
	}
	if string(got.Value) != "finished anyway" {
		t.Fatalf("follower got %q", got.Value)
	}
}

func TestAlreadyCancelledContextDoesNotStartALoad(t *testing.T) {
	var g singleflight.Group
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	got := g.Do(ctx, "k", func(context.Context) ([]byte, time.Duration, error) {
		t.Error("the loader ran for an already-cancelled caller")
		return nil, 0, nil
	})
	if !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("Err = %v, want context.Canceled", got.Err)
	}
	if g.Loads() != 0 {
		t.Fatalf("Loads() = %d, want 0", g.Loads())
	}
}

// TestPanickingLoaderBecomesAnError is the behaviour that made hand-rolling
// worth it. A bug in application code becomes an error for the callers who
// asked for that key, not a panic in a thousand unrelated goroutines.
func TestPanickingLoaderBecomesAnError(t *testing.T) {
	const callers = 20

	var g singleflight.Group
	release := make(chan struct{})

	results := make(chan singleflight.Result, callers)
	for i := 0; i < callers; i++ {
		go func() {
			results <- g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
				<-release
				panic("loader bug")
			})
		}()
	}
	for g.Inflight() < callers {
		runtime.Gosched()
	}
	close(release)

	for i := 0; i < callers; i++ {
		got := <-results
		if !errors.Is(got.Err, core.ErrLoaderPanic) {
			t.Fatalf("caller %d: Err = %v, want ErrLoaderPanic", i, got.Err)
		}
	}

	// The group must still be usable afterwards. A panic that left the call
	// in the map would wedge the key permanently.
	after := g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
		return []byte("recovered"), 0, nil
	})
	if after.Err != nil || string(after.Value) != "recovered" {
		t.Fatalf("the group did not recover: %q %v", after.Value, after.Err)
	}
}

// TestGoexitingLoaderBecomesAnError covers the other abnormal exit: a loader
// that calls runtime.Goexit, which is what t.Fatal does. Without handling, the
// deferred close would publish a nil value and a nil error, which every caller
// would read as a successful load of nothing.
func TestGoexitingLoaderBecomesAnError(t *testing.T) {
	var g singleflight.Group

	got := g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
		runtime.Goexit()
		return nil, 0, nil
	})
	if !errors.Is(got.Err, core.ErrLoaderFailed) {
		t.Fatalf("Err = %v, want ErrLoaderFailed", got.Err)
	}
}

func TestInflightReturnsToZero(t *testing.T) {
	var g singleflight.Group
	for i := 0; i < 10; i++ {
		g.Do(context.Background(), "k", func(context.Context) ([]byte, time.Duration, error) {
			return nil, 0, nil
		})
	}
	if got := g.Inflight(); got != 0 {
		t.Fatalf("Inflight() = %d after every call returned, want 0", got)
	}
}

func TestConcurrentMixedKeysUnderRace(t *testing.T) {
	const goroutines = 64
	const perGoroutine = 200
	const keyspace = 16

	var g singleflight.Group
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				key := fmt.Sprintf("k%d", (i+j)%keyspace)
				got := g.Do(context.Background(), key, func(context.Context) ([]byte, time.Duration, error) {
					return []byte(key), time.Minute, nil
				})
				if got.Err != nil {
					t.Errorf("Do: %v", got.Err)
					return
				}
				if string(got.Value) != key {
					t.Errorf("key %q returned %q; results crossed between keys", key, got.Value)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	if got := g.Inflight(); got != 0 {
		t.Fatalf("Inflight() = %d, want 0", got)
	}
	if g.Loads()+g.Collapsed() != goroutines*perGoroutine {
		t.Fatalf("loads=%d collapsed=%d, want %d together",
			g.Loads(), g.Collapsed(), goroutines*perGoroutine)
	}
}
