package testkit_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/testkit"
)

// dial is the boilerplate every network test starts with.
func dial(t *testing.T, c *testkit.Cluster, from, to string) core.PeerClient {
	t.Helper()
	client, err := c.Node(from).Transport.Dial(c.Node(to).Info)
	if err != nil {
		t.Fatalf("dial %s -> %s: %v", from, to, err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestNetworkRoundTrip(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	client := dial(t, c, "n0", "n1")

	got, err := client.Invoke(context.Background(), "ping", []byte("hello"))
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if want := "n1|ping|hello"; string(got) != want {
		t.Fatalf("Invoke = %q, want %q", got, want)
	}
}

func TestNetworkUnknownNodeIsUnreachable(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(1))
	client, err := c.Node("n0").Transport.Dial(core.Node{ID: "ghost", Addr: "ghost:7946"})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	if _, err := client.Invoke(context.Background(), "ping", nil); !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("Invoke to an unregistered node = %v, want ErrNodeUnreachable", err)
	}
}

// TestNetworkDropWaitsForTheDeadline pins the most important property of the
// drop fault: a lost message is silence, not an error. Anything that turns a
// drop into an immediate failure would let callers accidentally depend on
// prompt notification of loss, which no real network provides.
func TestNetworkDropWaitsForTheDeadline(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	c.Net.SetDropRate("n0", "n1", 1)
	client := dial(t, c, "n0", "n1")

	ctx, cancel := clock.WithTimeout(context.Background(), c.Clock, 2*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Invoke(ctx, "ping", nil)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("Invoke returned %v before its deadline; a dropped message must look like silence", err)
	default:
	}

	c.Advance(2 * time.Second)
	if err := <-errCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invoke = %v, want DeadlineExceeded", err)
	}
}

func TestNetworkDelayHoldsMessageUntilAdvance(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	c.Net.SetDelay("n0", "n1", 100*time.Millisecond)
	client := dial(t, c, "n0", "n1")

	type reply struct {
		body []byte
		err  error
	}
	replies := make(chan reply, 1)
	go func() {
		body, err := client.Invoke(context.Background(), "ping", nil)
		replies <- reply{body, err}
	}()

	// The delivery timer being armed is proof that Invoke has committed to
	// sending and is now waiting. This is the sanctioned alternative to
	// sleeping and hoping.
	c.Clock.BlockUntil(1)
	select {
	case r := <-replies:
		t.Fatalf("delayed message was delivered without an advance: %q %v", r.body, r.err)
	default:
	}

	c.Advance(100 * time.Millisecond)
	r := <-replies
	if r.err != nil {
		t.Fatalf("Invoke: %v", r.err)
	}
	if want := "n1|ping|"; string(r.body) != want {
		t.Fatalf("Invoke = %q, want %q", r.body, want)
	}
}

// TestNetworkReorderChangesArrivalOrder proves the reorder fault does something
// observable. Messages are sent strictly in sequence, so the only thing that
// can shuffle them is the injected jitter.
func TestNetworkReorderChangesArrivalOrder(t *testing.T) {
	const messages = 20

	recorder := testkit.NewRecordingHandler(testkit.EchoHandler(core.Node{ID: "n1"}))
	c := testkit.NewCluster(t,
		testkit.WithNodes(2),
		testkit.WithHandler(func(self core.Node) core.Handler {
			if self.ID == "n1" {
				return recorder
			}
			return testkit.EchoHandler(self)
		}),
	)
	// A non-zero base delay guarantees every message is scheduled rather
	// than delivered inline, so the number of armed timers is a reliable
	// signal that the previous send has committed.
	c.Net.SetDelay("n0", "n1", time.Millisecond)
	c.Net.SetReorderWindow(50 * time.Millisecond)

	client := dial(t, c, "n0", "n1")
	var wg sync.WaitGroup
	for i := 0; i < messages; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := client.Invoke(context.Background(), strconv.Itoa(i), nil); err != nil {
				t.Errorf("Invoke %d: %v", i, err)
			}
		}(i)
		c.Clock.BlockUntil(i + 1)
	}

	c.Advance(time.Second)
	wg.Wait()

	got := recorder.Methods()
	if len(got) != messages {
		t.Fatalf("handler saw %d messages, want %d", len(got), messages)
	}
	inOrder := true
	for i, m := range got {
		if m != strconv.Itoa(i) {
			inOrder = false
			break
		}
	}
	if inOrder {
		t.Fatalf("reorder window had no effect; arrival order was %v", got)
	}
}

func TestNetworkPartitionSeparatesGroups(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	within := dial(t, c, "n0", "n1")
	across := dial(t, c, "n0", "n2")

	c.Net.Partition([]string{"n0", "n1"}, []string{"n2"})

	if _, err := within.Invoke(context.Background(), "ping", nil); err != nil {
		t.Fatalf("within a partition group: %v", err)
	}
	if _, err := across.Invoke(context.Background(), "ping", nil); !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("across partition groups = %v, want ErrNodeUnreachable", err)
	}

	c.Net.Heal()
	if _, err := across.Invoke(context.Background(), "ping", nil); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}

// TestNetworkPartitionIsolatesOmittedNodes makes a partial partition spec fail
// loudly. Silently leaving an unlisted node connected would produce tests that
// look like they exercise a split and do not.
func TestNetworkPartitionIsolatesOmittedNodes(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(3))
	client := dial(t, c, "n0", "n2")

	c.Net.Partition([]string{"n0", "n1"})

	if _, err := client.Invoke(context.Background(), "ping", nil); !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("omitted node was still reachable: %v", err)
	}
}

func TestNetworkIsolateAndRejoin(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	client := dial(t, c, "n0", "n1")

	c.Net.Isolate("n1")
	if _, err := client.Invoke(context.Background(), "ping", nil); !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("isolated node was reachable: %v", err)
	}

	c.Net.Rejoin("n1")
	if _, err := client.Invoke(context.Background(), "ping", nil); err != nil {
		t.Fatalf("after rejoin: %v", err)
	}
}

// TestNetworkLostResponseStillRanTheHandler is the asymmetry that matters. A
// caller that sees no reply must not conclude that nothing happened, and this
// harness has to be able to construct that situation on demand.
func TestNetworkLostResponseStillRanTheHandler(t *testing.T) {
	var calls atomic.Int64
	ran := make(chan struct{}, 1)

	c := testkit.NewCluster(t,
		testkit.WithNodes(2),
		testkit.WithHandler(func(self core.Node) core.Handler {
			return core.HandlerFunc(func(_ context.Context, _ core.Node, _ string, _ []byte) ([]byte, error) {
				if self.ID == "n1" {
					calls.Add(1)
					ran <- struct{}{}
				}
				return []byte("ok"), nil
			})
		}),
	)
	// Requests flow; replies do not.
	c.Net.SetDropRate("n1", "n0", 1)
	client := dial(t, c, "n0", "n1")

	ctx, cancel := clock.WithTimeout(context.Background(), c.Clock, time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Invoke(ctx, "mutate", nil)
		errCh <- err
	}()

	<-ran // the side effect has happened
	c.Advance(time.Second)

	if err := <-errCh; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Invoke = %v, want DeadlineExceeded", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler ran %d times, want 1", got)
	}
}

func TestNetworkCloseUnblocksInFlightCalls(t *testing.T) {
	c := testkit.NewCluster(t, testkit.WithNodes(2))
	c.Net.SetDropRate("n0", "n1", 1)
	client := dial(t, c, "n0", "n1")

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Invoke(context.Background(), "ping", nil)
		errCh <- err
	}()

	// Background context with a dropped message: only closing the fabric
	// can release this call. A harness that could not do so would hang
	// every test that forgot a deadline.
	if err := c.Net.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-errCh; !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("in-flight Invoke = %v, want ErrNodeUnreachable", err)
	}
}

func TestNetworkRequestBufferIsNotShared(t *testing.T) {
	c := testkit.NewCluster(t,
		testkit.WithNodes(2),
		testkit.WithHandler(func(core.Node) core.Handler {
			return core.HandlerFunc(func(_ context.Context, _ core.Node, _ string, req []byte) ([]byte, error) {
				// core.Handler forbids retaining req; a transport that
				// hands over the caller's slice would let this
				// corrupt the caller's data.
				for i := range req {
					req[i] = 'x'
				}
				return nil, nil
			})
		}),
	)
	client := dial(t, c, "n0", "n1")

	body := []byte("original")
	if _, err := client.Invoke(context.Background(), "scribble", body); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if string(body) != "original" {
		t.Fatalf("handler mutated the caller's buffer: %q", body)
	}
}

func TestTransportServeRegistersAndCloseReturns(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })
	net := testkit.NewNetwork(fc, testkit.Seed(t))
	defer func() { _ = net.Close() }()

	self := core.Node{ID: "a", Addr: "a:1"}
	tr := net.Transport(self)

	registered := make(chan struct{}, 1)
	net.OnChange(func() {
		if net.Reachable("a", "a") {
			select {
			case registered <- struct{}{}:
			default:
			}
		}
	})

	served := make(chan error, 1)
	go func() { served <- tr.Serve(nil, testkit.EchoHandler(self)) }()
	<-registered

	if err := tr.Close(); err != nil {
		t.Fatalf("Transport.Close: %v", err)
	}
	if err := <-served; err != nil {
		t.Fatalf("Serve returned %v, want nil", err)
	}
	if net.Reachable("a", "a") {
		t.Fatal("node still reachable after Transport.Close")
	}
}

func TestTransportServeRejectsNilHandler(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	net := testkit.NewNetwork(fc, testkit.Seed(t))
	defer func() { _ = net.Close() }()

	tr := net.Transport(core.Node{ID: "a"})
	if err := tr.Serve(nil, nil); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("Serve(nil handler) = %v, want ErrInvalidOption", err)
	}
}
