package tests

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/transport"
)

// nonOwner returns some node other than owner.
func (f *fabric) nonOwner(owner string) string {
	for _, id := range f.ids {
		if id != owner {
			return id
		}
	}
	return ""
}

// TestConcurrentRemoteMissesShareOneFetch is the first M13 acceptance
// criterion (docs/decisions/0028): a thousand concurrent reads of one
// non-owned key on one pod with a cold L1 cost the owner exactly one read RPC,
// and every caller gets the value.
//
// The reader-to-owner link is delayed on the fake clock, so the first fetch
// is held in flight until the test lets it go. Every caller has therefore
// missed L1 and asked by the time the reply can arrive — which is the
// stampede, made deterministic.
func TestConcurrentRemoteMissesShareOneFetch(t *testing.T) {
	f := newFabricCounting(t, 3)
	ctx := context.Background()

	const key = "hot/1"
	owner := f.ownerOf(t, key)
	reader := f.nonOwner(owner)
	if err := f.caches[owner].Namespace("data").Set(ctx, key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns := f.caches[reader].Namespace("data", peercache.WithL1TTL(30*time.Second))

	f.net.SetDelay(reader, owner, time.Millisecond)

	const callers = 1000
	var wg sync.WaitGroup
	errs := make([]error, callers)
	vals := make([][]byte, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			vals[i], errs[i] = ns.Get(ctx, key)
		}(i)
	}

	// Wait until every caller is accounted for: either it sent its own
	// request, or it joined one already in flight.
	waitFor(t, "every caller to miss and ask", func() bool {
		return f.counters[reader].get(transport.MethodGet)+
			int(f.caches[reader].Stats().CoalescedReads) == callers
	})
	allDone := make(chan struct{})
	go func() { wg.Wait(); close(allDone) }()
	advanceUntil(t, f, allDone)

	for i := 0; i < callers; i++ {
		if errs[i] != nil || string(vals[i]) != "v" {
			t.Fatalf("caller %d: %q, %v", i, vals[i], errs[i])
		}
	}
	if got := f.counters[reader].get(transport.MethodGet); got != 1 {
		t.Fatalf("%d concurrent misses of one key sent %d read RPCs to its owner, want 1",
			callers, got)
	}
}

// advanceUntil moves the fake clock a millisecond at a time until done closes.
//
// One Advance is not enough. The counting transport counts a request before
// the fake network schedules its delivery, so a test that waits on the count
// and then advances once can advance before the delivery exists — and the
// request is then held forever. Advancing until the answer arrives has no
// such window. Bounded, so a real hang still reports.
func advanceUntil(t *testing.T, f *fabric, done <-chan struct{}) {
	t.Helper()
	const maxSteps = 100_000
	for i := 0; ; i++ {
		select {
		case <-done:
			return
		default:
		}
		if i > maxSteps {
			t.Fatal("gave up advancing the clock: the request was never answered")
		}
		f.clk.Advance(time.Millisecond)
		runtime.Gosched()
	}
}

// advanceUntilRecv is advanceUntil for a result channel.
func advanceUntilRecv[T any](t *testing.T, f *fabric, ch <-chan T, out *T) {
	t.Helper()
	done := make(chan struct{})
	go func() { *out = <-ch; close(done) }()
	advanceUntil(t, f, done)
}

// waitFor yields until cond holds, failing the test if it never does. It is
// bounded by yields rather than a timer, so a failure reports instead of
// hanging, and nothing sleeps.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	const maxYields = 5_000_000
	for i := 0; !cond(); i++ {
		if i > maxYields {
			t.Fatalf("gave up waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// hotKeyFixture is a three-node counting fabric with key written on its owner
// and the reader's link to the owner held on the fake clock.
type hotKeyFixture struct {
	f             *fabric
	owner, reader string
	ns            peercache.Namespace
}

func newHotKeyFixture(t *testing.T, key string) hotKeyFixture {
	t.Helper()
	f := newFabricCounting(t, 3)
	owner := f.ownerOf(t, key)
	reader := f.nonOwner(owner)
	if err := f.caches[owner].Namespace("data").Set(context.Background(), key, []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	ns := f.caches[reader].Namespace("data", peercache.WithL1TTL(30*time.Second))
	f.net.SetDelay(reader, owner, time.Millisecond)
	return hotKeyFixture{f: f, owner: owner, reader: reader, ns: ns}
}

func (h hotKeyFixture) gets() int { return h.f.counters[h.reader].get(transport.MethodGet) }

func (h hotKeyFixture) coalesced() int64 { return h.f.caches[h.reader].Stats().CoalescedReads }

// TestACancelledWaiterLeavesTheSharedFetch: a caller that gives up while
// waiting on a shared fetch returns its own context error, and the fetch goes
// on for everyone else.
//
// The caller that quits is the one that started the fetch. That is the case
// that matters: a fetch run on its starter's context would be cancelled with
// it, and the patient caller would have to send a second request.
func TestACancelledWaiterLeavesTheSharedFetch(t *testing.T) {
	const key = "hot/cancel"
	h := newHotKeyFixture(t, key)

	ctx, cancel := context.WithCancel(context.Background())
	quitter := make(chan error, 1)
	go func() {
		_, err := h.ns.Get(ctx, key)
		quitter <- err
	}()
	waitFor(t, "the quitter's fetch", func() bool { return h.gets() == 1 })

	type res struct {
		val []byte
		err error
	}
	patient := make(chan res, 1)
	go func() {
		v, err := h.ns.Get(context.Background(), key)
		patient <- res{v, err}
	}()
	waitFor(t, "the patient caller to join", func() bool { return h.coalesced() == 1 })

	cancel()
	if err := <-quitter; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled caller returned %v, want context.Canceled", err)
	}

	var r res
	advanceUntilRecv(t, h.f, patient, &r)
	if r.err != nil || string(r.val) != "v" {
		t.Fatalf("patient caller got %q, %v after the starter quit; want the value", r.val, r.err)
	}
	if got := h.gets(); got != 1 {
		t.Fatalf("%d read RPCs, want 1: the starter quitting must not cancel the shared fetch", got)
	}
}

// TestALeadersDeadlineIsNotEveryones: the fetch runs under the deadline of
// the caller that started it, so when that deadline passes a waiter with time
// left must not inherit the failure. It fetches again and gets the value.
func TestALeadersDeadlineIsNotEveryones(t *testing.T) {
	const key = "hot/deadline"
	h := newHotKeyFixture(t, key)

	// The request is held for ten milliseconds of fake time and the
	// leader has one, so its deadline passes while the fetch is in flight.
	h.f.net.SetDelay(h.reader, h.owner, 10*time.Millisecond)
	leaderCtx, cancel := clock.WithTimeout(context.Background(), h.f.clk, time.Millisecond)
	defer cancel()
	leader := make(chan error, 1)
	go func() {
		_, err := h.ns.Get(leaderCtx, key)
		leader <- err
	}()
	waitFor(t, "the leader's fetch", func() bool { return h.gets() == 1 })

	waiter := make(chan []byte, 1)
	waitErr := make(chan error, 1)
	go func() {
		v, err := h.ns.Get(context.Background(), key)
		waiter <- v
		waitErr <- err
	}()
	waitFor(t, "the waiter to join", func() bool { return h.coalesced() == 1 })

	h.f.clk.Advance(time.Millisecond)
	if err := <-leader; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("leader returned %v, want its own deadline", err)
	}
	waitFor(t, "the waiter to fetch again", func() bool { return h.gets() == 2 })
	var v []byte
	advanceUntilRecv(t, h.f, waiter, &v)
	if err := <-waitErr; err != nil || string(v) != "v" {
		t.Fatalf("waiter got %q, %v; the leader's deadline became its own", v, err)
	}
}
