package testkit_test

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// These tests exist to force specific interleavings. The race detector only
// reports races on paths a test actually takes, so a hand-over bug in a window
// nothing exercises stays invisible until it produces an unreproducible
// failure in some later milestone.

// TestNetworkCloseRacesWithDelayedDelivery targets the window between a
// delivery being registered as pending and its timer being handed over. If
// Close drains pending in that window, the timer has already been created and
// nobody else holds a reference to it, so it outlives the network. The symptom
// is not a crash: it is the harness's own "no timer left armed" check failing
// intermittently, in whichever unrelated test happened to close while a
// delivery was in flight.
func TestNetworkCloseRacesWithDelayedDelivery(t *testing.T) {
	seed := testkit.Seed(t)

	for i := 0; i < 100; i++ {
		fc := clock.NewFake(clock.Epoch)
		fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })
		network := testkit.NewNetwork(fc, seed+int64(i))

		a := core.Node{ID: "a", Addr: "a:1"}
		b := core.Node{ID: "b", Addr: "b:1"}
		network.Register(a, testkit.EchoHandler(a))
		network.Register(b, testkit.EchoHandler(b))
		network.SetDelay("a", "b", time.Second)

		client, err := network.Transport(a).Dial(b)
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}

		var wg sync.WaitGroup
		for j := 0; j < 8; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = client.Invoke(context.Background(), "ping", nil)
			}()
		}

		// Close concurrently with the sends, so that some iterations land
		// inside the hand-over window.
		_ = network.Close()
		wg.Wait()

		if got := fc.Pending(); got != 0 {
			t.Fatalf("iteration %d: %d delivery timer(s) outlived the network", i, got)
		}
	}
}

// TestNetworkCloseUnblocksEveryWaiter checks the other half: no call may
// survive Close, whatever state it was in. A single call left blocked on a
// background context would hang the whole suite.
func TestNetworkCloseUnblocksEveryWaiter(t *testing.T) {
	const callers = 32

	c := testkit.NewCluster(t, testkit.WithNodes(2))
	c.Net.SetDelay("n0", "n1", time.Hour)
	c.Net.SetDropRate("n0", "n1", 0.5)

	client, err := c.Node("n0").Transport.Dial(c.Node("n1").Info)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	done := make(chan struct{}, callers)
	for i := 0; i < callers; i++ {
		go func() {
			_, _ = client.Invoke(context.Background(), "ping", nil)
			done <- struct{}{}
		}()
	}

	_ = c.Net.Close()
	for i := 0; i < callers; i++ {
		<-done
	}
}

// TestHardKillDuringInFlightCallDoesNotHang kills the callee while a delivery
// is scheduled. Whatever the outcome, the caller must be released; a hang here
// would mean a killed node can hold a caller's goroutine forever.
func TestHardKillDuringInFlightCallDoesNotHang(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	c.Net.SetDelay("n0", "n1", time.Second)

	client, err := c.Node("n0").Transport.Dial(c.Node("n1").Info)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	done := make(chan error, 1)
	go func() {
		_, err := client.Invoke(context.Background(), "ping", nil)
		done <- err
	}()

	c.Clock.BlockUntil(1) // the delivery is scheduled
	c.HardKill("n1")
	c.Advance(time.Second)

	<-done
}

// TestMembershipSettlesUnderConcurrentFaults hammers the lock ordering between
// Membership and Network from several directions at once, then asserts that the
// cluster still converges once the chaos stops.
//
// The assertion is deliberately only about the settled state. Asserting on the
// intermediate views would be asserting on the scheduler.
func TestMembershipSettlesUnderConcurrentFaults(t *testing.T) {
	const faulters = 4
	const rounds = 200

	c := testkit.NewCluster(t, testkit.WithNodes(5))
	ids := c.IDs()

	stop := make(chan struct{})
	var faultWG, advanceWG sync.WaitGroup

	for f := 0; f < faulters; f++ {
		faultWG.Add(1)
		go func(f int) {
			defer faultWG.Done()
			// Each goroutine gets its own generator: math/rand sources
			// are not safe for concurrent use, and sharing one would be
			// a race in the test rather than in the code.
			rng := rand.New(rand.NewSource(c.Seed + int64(f)))
			for i := 0; i < rounds; i++ {
				switch rng.Intn(4) {
				case 0:
					c.Net.Partition(ids[:rng.Intn(len(ids))], ids[rng.Intn(len(ids)):])
				case 1:
					c.Net.Heal()
				case 2:
					c.Net.Isolate(ids[rng.Intn(len(ids))])
				case 3:
					c.Net.Rejoin(ids[rng.Intn(len(ids))])
				}
			}
		}(f)
	}

	advanceWG.Add(1)
	go func() {
		defer advanceWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.Advance(time.Second)
		}
	}()

	// The advancer is on its own WaitGroup: waiting on one group that
	// contains both would deadlock, because the advancer only exits after a
	// close that happens after the wait.
	faultWG.Wait()
	close(stop)
	advanceWG.Wait()

	// Settle: whole network, nobody isolated, plenty of time.
	c.Net.Heal()
	for _, id := range ids {
		c.Net.Rejoin(id)
	}
	c.Advance(time.Hour)

	for _, id := range ids {
		wantMembers(t, c, id, ids...)
	}
}

// TestConcurrentInvokesAcrossAllPairs is a plain throughput-shaped exercise of
// the fabric under -race: every node calling every other node at once.
func TestConcurrentInvokesAcrossAllPairs(t *testing.T) {
	const perPair = 25

	c := testkit.NewCluster(t, testkit.WithNodes(5))
	ids := c.IDs()

	var wg sync.WaitGroup
	for _, from := range ids {
		for _, to := range ids {
			if from == to {
				continue
			}
			client, err := c.Node(from).Transport.Dial(c.Node(to).Info)
			if err != nil {
				t.Fatalf("Dial %s -> %s: %v", from, to, err)
			}
			wg.Add(1)
			go func(from, to string, client core.PeerClient) {
				defer wg.Done()
				defer func() { _ = client.Close() }()
				for i := 0; i < perPair; i++ {
					if _, err := client.Invoke(context.Background(), "ping", nil); err != nil {
						t.Errorf("%s -> %s: %v", from, to, err)
						return
					}
				}
			}(from, to, client)
		}
	}
	wg.Wait()
}
