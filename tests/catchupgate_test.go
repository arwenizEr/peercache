package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestTheCatchUpGateHoldsForwardedWritesUntilCaughtUp is open question 49's
// second half. A restarted node owns its keys from the moment it joins, and its
// peers forward to it before its catch-up has brought their newest values back.
// Applied then, an Increment counts from nothing and the catch-up afterwards
// discards it as older: M16 lost 1 to 3 decrements on 5 of 50 stock counters
// that way under kill -9. With WithCatchUpGate the owner answers
// ErrActivationPending until it has caught up, and nothing is applied.
func TestTheCatchUpGateHoldsForwardedWritesUntilCaughtUp(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("gated",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}
	const key = "gated/count"
	victim := f.ownerOf(t, key)
	var peer string
	for _, id := range f.ids {
		if id != victim {
			peer = id
			break
		}
	}
	for i := 0; i < 5; i++ {
		if _, err := open(peer).Increment(ctx, key, 1, 0); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}

	if f.nodeOptions == nil {
		f.nodeOptions = map[string][]peercache.Option{}
	}
	f.nodeOptions[victim] = append(f.nodeOptions[victim], peercache.WithCatchUpGate())
	f.hardKill(t, victim)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, len(f.ids)-1)
	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(victim)

	if _, err := open(peer).Increment(ctx, key, 1, 0); !errors.Is(err, peercache.ErrActivationPending) {
		t.Fatalf("a forwarded Increment before the owner caught up returned %v; want ErrActivationPending, "+
			"or it is applied to a base that is not there yet", err)
	}
	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}
	v, err := open(peer).Increment(ctx, key, 1, 0)
	if err != nil || v != 6 {
		t.Fatalf("after catch-up the forwarded Increment returned %d, %v; want 6 — five before the "+
			"restart, none while it was held, one now", v, err)
	}
}

// TestWithoutTheCatchUpGateAForwardedWriteIsServed pins the default: a node
// whose application never calls CatchUp keeps serving its peers. The gate is
// opt-in because turning it on there would refuse them indefinitely.
func TestWithoutTheCatchUpGateAForwardedWriteIsServed(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	for _, id := range f.ids {
		f.caches[id].Namespace("ungated", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
	}
	const key = "ungated/k"
	owner := f.ownerOf(t, key)
	for _, id := range f.ids {
		if id != owner {
			if err := f.caches[id].Namespace("ungated").Set(ctx, key, []byte("v"), 0); err != nil {
				t.Fatalf("a forwarded Set to an owner that never caught up returned %v; the default "+
					"must keep serving", err)
			}
			return
		}
	}
}
