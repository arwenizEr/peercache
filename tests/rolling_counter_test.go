package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// counterLedger is what a caller of Increment can know: decrements it saw
// applied, and decrements whose outcome it could not learn.
type counterLedger struct {
	acked, uncertain map[string]int64
}

// decrement is the shop's checkout, reduced to its counter: a refusal is
// retried, an unknown outcome is not.
func (l *counterLedger) decrement(ctx context.Context, ns peercache.Namespace, key string) {
	for attempt := 0; attempt < 5; attempt++ {
		_, err := ns.Increment(ctx, key, -1, 0)
		switch {
		case err == nil, errors.Is(err, peercache.ErrUnderReplicated):
			l.acked[key]++
			return
		case errors.Is(err, peercache.ErrNotOwner), errors.Is(err, peercache.ErrStaleEpoch),
			errors.Is(err, peercache.ErrActivationPending), errors.Is(err, peercache.ErrClosed):
			continue
		default:
			l.uncertain[key]++
			return
		}
	}
}

// TestARollingRestartAppliesEachDecrementOnce: persisted, synchronously
// replicated counters, decremented through every live node throughout a
// rolling restart — each node drained, killed, brought back and caught up in
// turn. Every counter must end at its acknowledged decrements, or between
// that and those plus the uncertain ones. On kind every counter one restarted
// pod owned ended thousands of decrements below what was acknowledged
// (open question 55).
func TestARollingRestartAppliesEachDecrementOnce(t *testing.T) {
	f := newFabricFull(t, 4, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay, withCacheOptions(peercache.WithCatchUpGate()))
	store := newHeldStore()
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("stock",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithPersistence(store))
	}
	for _, id := range f.ids {
		open(id)
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)

	const counters = 12
	l := &counterLedger{acked: map[string]int64{}, uncertain: map[string]int64{}}
	round := func(times int) {
		for i := 0; i < times; i++ {
			for _, id := range f.live() {
				for k := 0; k < counters; k++ {
					l.decrement(ctx, open(id), fmt.Sprintf("item/%d", k))
				}
			}
			f.clk.Advance(flushInterval)
		}
	}
	round(3)

	for _, victim := range append([]string{}, f.ids...) {
		if err := f.caches[victim].Drain(ctx); err != nil {
			t.Fatalf("Drain(%s): %v", victim, err)
		}
		round(2)
		f.hardKill(t, victim)
		f.clk.Advance(2 * f.mcfg.FailureTimeout)
		f.convergeTo(t, len(f.ids)-1)
		f.settle(t)
		round(3)

		f.revive(t, victim)
		f.clk.Advance(2 * f.mcfg.JoinDelay)
		f.converge(t)
		open(victim)
		round(3) // back, not yet caught up
		if err := f.caches[victim].CatchUp(ctx); err != nil {
			t.Fatalf("CatchUp(%s): %v", victim, err)
		}
		round(3)
		f.clk.Advance(2 * ownership.DefaultActivationDelay)
		f.settle(t)
		round(3)
		// A second catch-up, as the shop's loop runs one every so often.
		if err := f.caches[victim].CatchUp(ctx); err != nil {
			t.Fatalf("second CatchUp(%s): %v", victim, err)
		}
		round(2)
	}
	f.settle(t)

	var wrong []string
	for k := 0; k < counters; k++ {
		key := fmt.Sprintf("item/%d", k)
		v, err := readWithRetryFrom(t, f, open, f.live()[0], key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		n, _ := strconv.ParseInt(v, 10, 64)
		applied := -n
		if applied < l.acked[key] || applied > l.acked[key]+l.uncertain[key] {
			wrong = append(wrong, fmt.Sprintf("%s applied %d, acknowledged %d, uncertain %d (owner %s)",
				key, applied, l.acked[key], l.uncertain[key], f.ownerOf(t, key)))
		}
	}
	if len(wrong) > 0 {
		t.Fatalf("%d of %d counters disagree with what callers were told:\n%v", len(wrong), counters, wrong)
	}
}

// fourNodeStock is a four-node fabric with a persisted, synchronously
// replicated counter namespace, caught up, and the key owned by f.ids[0].
func fourNodeStock(t *testing.T) (*fabric, func(string) peercache.Namespace, string) {
	t.Helper()
	f := newFabricFull(t, 4, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay, withCacheOptions(peercache.WithCatchUpGate()))
	store := newHeldStore()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("stock",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication(),
			peercache.WithPersistence(store))
	}
	for _, id := range f.ids {
		open(id)
		if err := f.caches[id].CatchUp(context.Background()); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	for i := 0; ; i++ {
		if k := fmt.Sprintf("item/%d", i); f.ownerOf(t, k) == f.ids[0] {
			return f, open, k
		}
	}
}

// TestADrainedNodeAppliesNoWrite: after Drain a node's own view can still
// name it the owner of keys it has handed on. A write through it — its own
// caller's, or a peer's that has not noticed the departure — was applied to a
// store nobody would read again, and acknowledged.
func TestADrainedNodeAppliesNoWrite(t *testing.T) {
	f, open, key := fourNodeStock(t)
	ctx := context.Background()
	drained := f.ids[0]
	if _, err := open(drained).Increment(ctx, key, -1, 0); err != nil {
		t.Fatalf("Increment before the drain: %v", err)
	}
	if err := f.caches[drained].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if v, err := open(drained).Increment(ctx, key, -1, 0); !errors.Is(err, peercache.ErrNotOwner) {
		t.Fatalf("Increment through the drained node = %d, %v; want ErrNotOwner: a write "+
			"applied there is lost", v, err)
	}
}

// TestANewOwnerCountsFromItsPromotedCopy: a node that has just become a key's
// owner holds the key's newest value as a replica until reconciliation
// promotes it. A local Increment used to run at once, on the owned store
// without it, counting from zero; the promotion then discarded the replica as
// older. It must wait out the activation delay, as a peer's request does.
func TestANewOwnerCountsFromItsPromotedCopy(t *testing.T) {
	f, open, key := fourNodeStock(t)
	ctx := context.Background()
	for _, id := range f.ids {
		if _, err := open(id).Increment(ctx, key, -1, 0); err != nil {
			t.Fatalf("Increment via %s: %v", id, err)
		}
	}
	if err := f.caches[f.ids[0]].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// The departure is applied at once, the ownership table on the guard's
	// own goroutine: wait for a survivor to see itself as the owner.
	successor := ""
	waitFor(t, "a survivor to see itself as the key's owner", func() bool {
		for _, id := range f.ids[1:] {
			if d, err := f.caches[id].DescribeKey("stock", key); err == nil && d.Owner == id {
				successor = id
				return true
			}
		}
		return false
	})
	if v, err := open(successor).Increment(ctx, key, -1, 0); !errors.Is(err, peercache.ErrActivationPending) {
		t.Fatalf("Increment through the new owner inside the activation delay = %d, %v; "+
			"want ErrActivationPending", v, err)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)
	v, err := open(successor).Increment(ctx, key, -1, 0)
	if err != nil {
		t.Fatalf("Increment through the new owner after the delay: %v", err)
	}
	if v != -5 {
		t.Fatalf("the new owner counted to %d, want -5: it did not count from the copy it "+
			"was promoted", v)
	}
}

// TestADrainedNodeRefusesAPeerThatMissedTheDeparture: a peer that did not
// hear a node's departure still routes its keys to it. The drained node must
// refuse the write rather than apply it to keys it has handed on.
func TestADrainedNodeRefusesAPeerThatMissedTheDeparture(t *testing.T) {
	f, open, key := fourNodeStock(t)
	ctx := context.Background()
	drained, unaware := f.ids[0], f.ids[1]
	// The unaware peer cannot hear the departure announcement.
	f.net.Partition([]string{drained}, []string{unaware})
	if err := f.caches[drained].Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	f.net.Heal()
	if d, err := f.caches[unaware].DescribeKey("stock", key); err != nil || d.Owner != drained {
		t.Fatalf("the unaware peer sees owner %q (%v); the test needs it to still route to %s", d.Owner, err, drained)
	}
	if v, err := open(unaware).Increment(ctx, key, -1, 0); !errors.Is(err, peercache.ErrNotOwner) &&
		!errors.Is(err, peercache.ErrActivationPending) {
		t.Fatalf("Increment from a peer that missed the departure = %d, %v; the drained "+
			"node applied it to a key it has handed on", v, err)
	}
}
