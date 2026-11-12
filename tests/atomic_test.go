package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// atomicFabric is a three-node cluster with one namespace opened identically on
// every node, the L1 tier off so every read is answered by the owner.
func atomicFabric(t *testing.T, opts ...peercache.NamespaceOption) (*fabric, func(string) peercache.Namespace) {
	t.Helper()
	f := newFabric(t, 3)
	all := append([]peercache.NamespaceOption{peercache.WithL1TTL(-1)}, opts...)
	openNS := func(id string) peercache.Namespace { return f.caches[id].Namespace("atomic", all...) }
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	return f, openNS
}

// TestConcurrentIncrementsFromEveryNodeLoseNothing is decision 0027's first
// criterion: a thousand increments of one key from every node of a cluster,
// at once, return a thousand distinct results and leave the key at a thousand.
//
// Distinct results are the stronger half. A total of 1000 could come from
// increments that were each applied but reported a stale value; distinct
// returns mean each one saw the one before it.
func TestConcurrentIncrementsFromEveryNodeLoseNothing(t *testing.T) {
	f, openNS := atomicFabric(t, peercache.WithReplicationFactor(2))
	const total = 1000
	ctx := context.Background()

	results := make(chan int64, total)
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(via string) {
			defer wg.Done()
			v, err := openNS(via).Increment(ctx, "hits", 1, 0)
			if err != nil {
				t.Errorf("Increment via %s: %v", via, err)
				return
			}
			results <- v
		}(f.ids[i%len(f.ids)])
	}
	wg.Wait()
	close(results)

	seen := make(map[int64]bool, total)
	for v := range results {
		if seen[v] {
			t.Fatalf("two increments both returned %d: one did not see the other", v)
		}
		seen[v] = true
	}
	for v := int64(1); v <= total; v++ {
		if !seen[v] {
			t.Fatalf("no increment returned %d; results are not 1..%d", v, total)
		}
	}
	for _, via := range f.ids {
		got, err := openNS(via).Get(ctx, "hits")
		if err != nil || string(got) != strconv.Itoa(total) {
			t.Fatalf("Get via %s = %q, %v; want %d", via, got, err, total)
		}
	}
}

// TestExactlyOneConcurrentSetIfAbsentWins: of many callers on every node
// racing to create one key, exactly one is told it stored, and that caller's
// value is the one every node reads.
func TestExactlyOneConcurrentSetIfAbsentWins(t *testing.T) {
	f, openNS := atomicFabric(t, peercache.WithReplicationFactor(2))
	const callers = 100
	ctx := context.Background()

	var mu sync.Mutex
	var winners []string
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mine := fmt.Sprintf("caller-%d", i)
			ok, err := openNS(f.ids[i%len(f.ids)]).SetIfAbsent(ctx, "lock", []byte(mine), 0)
			if err != nil {
				t.Errorf("SetIfAbsent: %v", err)
				return
			}
			if ok {
				mu.Lock()
				winners = append(winners, mine)
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if len(winners) != 1 {
		t.Fatalf("%d callers were told they created the key, want exactly 1: %v", len(winners), winners)
	}
	for _, via := range f.ids {
		got, err := openNS(via).Get(ctx, "lock")
		if err != nil || string(got) != winners[0] {
			t.Fatalf("Get via %s = %q, %v; want the winner's %q", via, got, err, winners[0])
		}
	}
}

// TestCompareAndSwapChangesOnlyWhatItWasShown: a stale expectation changes
// nothing, a current one succeeds, and a missing key never matches — locally
// and through a non-owner.
func TestCompareAndSwapChangesOnlyWhatItWasShown(t *testing.T) {
	f, openNS := atomicFabric(t)
	ctx := context.Background()
	const key = "cas/k"
	owner := f.ownerOf(t, key)
	other := survivorOf(f, owner)

	for _, via := range []string{owner, other} {
		ns := openNS(via)
		if ok, err := ns.CompareAndSwap(ctx, key+via, []byte("x"), []byte("y"), 0); err != nil || ok {
			t.Fatalf("via %s: CompareAndSwap on a missing key = %v, %v; want false", via, ok, err)
		}
	}

	ns := openNS(other)
	if err := ns.Set(ctx, key, []byte("v1"), 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := ns.CompareAndSwap(ctx, key, []byte("v0"), []byte("v2"), 0); err != nil || ok {
		t.Fatalf("CompareAndSwap with a stale old = %v, %v; want false", ok, err)
	}
	if got, _ := ns.Get(ctx, key); string(got) != "v1" {
		t.Fatalf("a refused swap changed the value to %q", got)
	}
	if ok, err := ns.CompareAndSwap(ctx, key, []byte("v1"), []byte("v2"), 0); err != nil || !ok {
		t.Fatalf("CompareAndSwap with the current old = %v, %v; want true", ok, err)
	}
	if got, _ := openNS(owner).Get(ctx, key); string(got) != "v2" {
		t.Fatalf("the owner holds %q after a successful swap, want v2", got)
	}
}

// TestCompareAndSwapComparesWhatGetReturns: on a compressed namespace the store
// holds framed bytes. A swap must compare against the value, not the framing.
func TestCompareAndSwapComparesWhatGetReturns(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("compressed", peercache.WithCompression("lz4", 16))
	ctx := context.Background()
	big := make([]byte, 4096)
	for i := range big {
		big[i] = byte('a' + i%3)
	}
	if err := ns.Set(ctx, "k", big, 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := ns.CompareAndSwap(ctx, "k", big, []byte("small"), 0); err != nil || !ok {
		t.Fatalf("CompareAndSwap against the value Get returns = %v, %v; want true", ok, err)
	}
	if got, _ := ns.Get(ctx, "k"); string(got) != "small" {
		t.Fatalf("Get after the swap = %q", got)
	}
}

// TestIncrementRefusesWhatIsNotAnInteger: a non-integer value and an overflow
// are ErrNotAnInteger, through the owner and through a peer, and leave the
// value as it was.
func TestIncrementRefusesWhatIsNotAnInteger(t *testing.T) {
	f, openNS := atomicFabric(t)
	ctx := context.Background()
	const text, big = "inc/text", "inc/big"
	for _, via := range f.ids {
		ns := openNS(via)
		if err := ns.Set(ctx, text, []byte("hello"), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ns.Increment(ctx, text, 1, 0); !errors.Is(err, peercache.ErrNotAnInteger) {
			t.Fatalf("via %s: Increment of %q = %v, want ErrNotAnInteger", via, "hello", err)
		}
		if err := ns.Set(ctx, big, []byte(strconv.FormatInt(1<<62, 10)), 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ns.Increment(ctx, big, 1<<62, 0); !errors.Is(err, peercache.ErrNotAnInteger) {
			t.Fatalf("via %s: an overflowing Increment = %v, want ErrNotAnInteger", via, err)
		}
		if got, _ := ns.Get(ctx, big); string(got) != strconv.FormatInt(1<<62, 10) {
			t.Fatalf("via %s: a refused Increment changed the value to %q", via, got)
		}
	}
}

// TestIncrementKeepsTheCountersDeadline: the ttl applies when the increment
// creates the key; later increments do not extend it, so a rate-limit window
// ends when it was meant to.
func TestIncrementKeepsTheCountersDeadline(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("window")
	ctx := context.Background()
	if _, err := ns.Increment(ctx, "k", 1, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	fc.Advance(6 * time.Second)
	if v, err := ns.Increment(ctx, "k", 1, 10*time.Second); err != nil || v != 2 {
		t.Fatalf("second Increment = %d, %v", v, err)
	}
	fc.Advance(5 * time.Second)
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("the counter outlived its first deadline: %v", err)
	}
	if v, err := ns.Increment(ctx, "k", 1, time.Second); err != nil || v != 1 {
		t.Fatalf("Increment after expiry = %d, %v; want a fresh counter at 1", v, err)
	}
}

// TestTouchOutlivesTheOriginalDeadlineAcrossFailover: a touched key lives past
// the deadline it was written with, on its owner and — because the new deadline
// is replicated — on the replica promoted when the owner dies.
func TestTouchOutlivesTheOriginalDeadlineAcrossFailover(t *testing.T) {
	f, openNS, key, owner := ttlFabric(t, 10*time.Second)
	ctx := context.Background()

	if ok, err := openNS(owner).Touch(ctx, key, time.Minute); err != nil || !ok {
		t.Fatalf("Touch = %v, %v", ok, err)
	}
	if ok, err := openNS(owner).Touch(ctx, key+"/absent", time.Minute); err != nil || ok {
		t.Fatalf("Touch of a missing key = %v, %v; want false", ok, err)
	}
	f.clk.Advance(20 * time.Second)
	if _, err := openNS(owner).Get(ctx, key); err != nil {
		t.Fatalf("the owner dropped a touched key at its original deadline: %v", err)
	}

	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)
	if got, err := openNS(survivorOf(f, owner)).Get(ctx, key); err != nil || string(got) != "v" {
		t.Fatalf("after failover the promoted replica serves %q, %v; the touch did not replicate", got, err)
	}
}

// TestAnIncrementWhoseOwnerFailedAfterApplyingIsNotRetried is the property
// that makes Increment safe to call over a network.
//
// The owner applies the increment, and before its reply arrives the
// connection fails and the owner dies — so by the time the caller could retry,
// the key has a new owner. That is the dangerous case: retrying to the same
// owner is already stopped by the check that the owner changed, but a retry to
// the *new* owner applies the increment a second time, on top of whatever that
// node holds. Only the decision to retry refusals and nothing else prevents it.
//
// A dropped reply cannot show this: it ends only in a deadline, and a retry
// under an expired context fails before it is sent.
func TestAnIncrementWhoseOwnerFailedAfterApplyingIsNotRetried(t *testing.T) {
	f := newFabricCounting(t, 3)
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("atomic", peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	const key = "inc/unreachable"
	owner := f.ownerOf(t, key)
	caller := survivorOf(f, owner)

	f.counters[caller].setReplyFault(func(method string, err error) error {
		if method != transport.MethodIncrement || err != nil {
			return err
		}
		// The owner has applied it. Now lose the reply and the owner, and
		// let the survivors agree on who owns the key instead.
		f.counters[caller].setReplyFault(nil)
		f.hardKill(t, owner)
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		f.convergeTo(t, 2)
		f.settle(t)
		return fmt.Errorf("connection reset after the owner answered: %w", peercache.ErrNodeUnreachable)
	})
	_, err := openNS(caller).Increment(context.Background(), key, 1, 0)

	if !errors.Is(err, peercache.ErrNodeUnreachable) {
		t.Fatalf("Increment = %v, want the ErrNodeUnreachable it was handed", err)
	}
	if n := f.counters[caller].get(transport.MethodIncrement); n != 1 {
		t.Fatalf("the caller sent %d Increment requests for one call, want 1", n)
	}
	for _, via := range f.live() {
		if got, gerr := openNS(via).Get(context.Background(), key); gerr == nil {
			t.Fatalf("after the owner died the key reads %q via %s: the increment was "+
				"applied again on the new owner", got, via)
		}
	}
}
