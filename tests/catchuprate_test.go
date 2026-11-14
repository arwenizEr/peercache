package tests

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestCatchUpPullsAtTheConfiguredRate: two nodes, RF 2, two thousand keys, so
// a catch-up of either pulls every key once — the ones the peer owns and the
// copies it holds of the ones this node owns. At 1,000 entries a second it
// cannot finish in less than two seconds of the cache's clock; without a rate
// it finishes without the clock moving at all. (No upper bound: the test
// advances the clock while the puller is between pages, so the clock runs
// ahead of the waits it serves.)
func TestCatchUpPullsAtTheConfiguredRate(t *testing.T) {
	const keys, rate = 2000, 1000
	f := newFabricFull(t, 2, false, testkit.MembershipConfig{}, -1,
		withNodeOptions("n0", peercache.WithCatchUpRate(rate)))
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("data", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
	}
	f.settle(t)
	for i := 0; i < keys; i++ {
		if err := f.caches["n1"].Namespace("data").Set(ctx, fmt.Sprintf("k%04d", i), []byte("v"), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	if err := f.caches["n1"].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp without a rate: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- f.caches["n0"].CatchUp(ctx) }()
	// With the clock standing still, the unpaced catch-up above finished;
	// this one must be waiting for it.
	for i := 0; i < 100_000; i++ {
		select {
		case err := <-done:
			t.Fatalf("a paced CatchUp finished without the clock moving (%v)", err)
		default:
		}
		runtime.Gosched()
	}
	var advanced time.Duration
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("CatchUp: %v", err)
			}
			if min := keys * time.Second / rate; advanced < min {
				t.Fatalf("CatchUp of %d entries at %d/s finished after %v of the clock, want at least %v",
					keys, rate, advanced, min)
			}
			return
		default:
		}
		f.clk.Advance(time.Millisecond)
		advanced += time.Millisecond
		// Several yields per step: under -race on a loaded machine one
		// was too few for the puller to keep up, and a minute of fake
		// time ran out with the catch-up still paging.
		for i := 0; i < 20; i++ {
			runtime.Gosched()
		}
		if advanced > 30*time.Minute {
			t.Fatal("CatchUp never finished")
		}
	}
}

// TestCatchUpRateMustNotBeNegative is the option's validation.
func TestCatchUpRateMustNotBeNegative(t *testing.T) {
	_, err := peercache.New(peercache.WithNodeID("n0"), peercache.WithCatchUpRate(-1))
	if !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("New with a negative catch-up rate = %v, want ErrInvalidOption", err)
	}
}
