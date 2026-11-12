package peercache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestAStarvedNodeSlowsOnlyItsOwnKeys is M9's CPU starvation criterion: a node
// whose every message is delayed by 200 ms produces no split-brain detections
// and no change in latency for keys owned elsewhere.
//
// A throttled pod answers late. What must not happen is that its lateness
// spreads: a read of a key it does not own should never wait on it, and an
// asynchronous write whose replica it is should not either. Both are checked
// the strict way — the operation must complete without the fake clock moving at
// all, which is "no change in p99" with no statistics to argue about. A read of
// one of its own keys is checked the other way, to prove the delay is real.
//
// What this harness cannot show is the failure detector's side. Its membership
// follows reachability, not probe timings, so a slow node is never suspected
// here however slow it is. In production that is memberlist's job: the default
// LAN profile probes with a 500 ms timeout and scales it by up to 8 under its
// Lifeguard awareness when the local node is itself struggling, which is the
// case CPU throttling produces. See docs/api.md.
func TestAStarvedNodeSlowsOnlyItsOwnKeys(t *testing.T) {
	const (
		keys  = 200
		delay = 200 * time.Millisecond
	)
	f := newFabric(t, 5)
	ctx := context.Background()
	slow := "n4"

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("starved", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	owner := map[string]string{}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("starved/%03d", i)
		if err := openNS(f.ids[i%4]).Set(ctx, key, []byte("v"), 0); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
		owner[key] = f.ownerOf(t, key)
	}
	f.settle(t)

	f.net.SetDelay(slow, testkit.Wildcard, delay)
	f.net.SetDelay(testkit.Wildcard, slow, delay)
	t.Cleanup(func() {
		f.net.SetDelay(slow, testkit.Wildcard, 0)
		f.net.SetDelay(testkit.Wildcard, slow, 0)
	})

	// Completes without the clock moving, or the test fails.
	immediate := func(what string, op func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- op() }()
		var err error
		waitForCache(t, what+" to complete without the clock moving", func() bool {
			select {
			case err = <-done:
				return true
			default:
				return false
			}
		})
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}

	var elsewhere, own []string
	for key, o := range owner {
		if o == slow {
			own = append(own, key)
		} else {
			elsewhere = append(elsewhere, key)
		}
	}
	if len(own) == 0 || len(elsewhere) == 0 {
		t.Fatalf("%d keys owned by %s and %d elsewhere; the test needs both", len(own), slow, len(elsewhere))
	}

	healthy := []string{"n0", "n1", "n2", "n3"}
	for i, key := range elsewhere {
		via := healthy[i%len(healthy)]
		immediate("Get "+key+" via "+via, func() error {
			_, err := openNS(via).Get(ctx, key)
			return err
		})
		// An asynchronous write whose replica may be the slow node.
		immediate("Set "+key+" via "+via, func() error {
			return openNS(via).Set(ctx, key, []byte("w"), 0)
		})
	}

	// The delay is real. A read of one of the slow node's own keys, from a
	// node that has never read it and so holds no L1 copy, crosses to the slow
	// node and back, and must not complete until the clock covers that.
	slowKey := own[0]
	done := make(chan error, 1)
	go func() {
		_, err := openNS("n0").Get(ctx, slowKey)
		done <- err
	}()
	for i := 0; i < 100_000; i++ {
		select {
		case err := <-done:
			t.Fatalf("a read of %s, owned by the slow node, completed (err %v) without "+
				"the clock moving; the delay this test depends on is not in effect", slowKey, err)
		default:
		}
		gosched()
	}
	f.clk.Advance(2 * delay)
	waitForCache(t, "the slow node's read to complete after a round trip", func() bool {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Get %s after the round trip: %v", slowKey, err)
			}
			return true
		default:
			return false
		}
	})

	f.clk.Advance(time.Minute)
	f.settle(t)
	for _, id := range f.ids {
		st := f.caches[id].Stats()
		if st.SplitBrainDetections != 0 {
			t.Fatalf("%s detected %d split brains with one node slow, not partitioned",
				id, st.SplitBrainDetections)
		}
		if st.Members != 5 {
			t.Fatalf("%s sees %d members with one node slow; slow is not dead", id, st.Members)
		}
	}
}
