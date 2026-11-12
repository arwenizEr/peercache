package peercache_test

import (
	"context"
	"errors"

	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/testkit"
)

// ttlFabric is a three-node cluster at RF=2 with one key written under a TTL,
// and the node that owns it.
func ttlFabric(t *testing.T, ttl time.Duration) (*fabric, func(string) peercache.Namespace, string, string) {
	t.Helper()
	f := newFabric(t, 3)
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("ttl",
			peercache.WithReplicationFactor(2), peercache.WithSyncReplication(),
			// No L1, so every read is answered by the owner and nothing
			// cached elsewhere can mask what the owner holds.
			peercache.WithL1TTL(-1))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)

	const key = "ttl/k"
	owner := f.ownerOf(t, key)
	if err := openNS(owner).Set(context.Background(), key, []byte("v"), ttl); err != nil {
		t.Fatalf("Set: %v", err)
	}
	return f, openNS, key, owner
}

// survivorOf returns a live node other than id.
func survivorOf(f *fabric, id string) string {
	for _, other := range f.live() {
		if other != id {
			return other
		}
	}
	return ""
}

// TestAnExpiredKeyStaysExpiredAfterFailover: a key that has expired on its
// owner must not come back when a replica is promoted.
//
// A replica is held until the owner says otherwise, and an expiry says nothing
// — it is not replicated, it simply happens on every holder at the same instant.
// So a promoted replica must expire on the owner's schedule: the deadline the
// owner set, not a fresh lifetime counted from the moment of promotion.
func TestAnExpiredKeyStaysExpiredAfterFailover(t *testing.T) {
	f, openNS, key, owner := ttlFabric(t, 10*time.Second)

	f.clk.Advance(11 * time.Second)
	if _, err := openNS(owner).Get(context.Background(), key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("precondition: the owner still serves %s after its TTL: %v", key, err)
	}

	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	via := survivorOf(f, owner)
	got, err := openNS(via).Get(context.Background(), key)
	if err == nil {
		t.Fatalf("%s expired on its owner after 10s, the owner died, and the promoted replica "+
			"serves %q. An expired key came back", key, got)
	}
	if !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get %s after failover: %v", key, err)
	}
}

// TestAKeyExpiringDuringFailoverIsNotPromoted: a key alive when its owner died
// and expired by the time the failover completed must not be served by the node
// that took over.
//
// Two mechanisms can satisfy it, and in this harness only one is observed. The
// replica sweep runs on the cache's tick, and the fake ticker hands its tick to a
// goroutine that reads the clock after the advance has finished — so the sweep
// always sees the expiry first and removes the copy before promotion looks. In
// production a sweep runs at most a second before the expiry it misses, and in
// that window promotion's own check is what stops a replica being restored with
// a negative remainder — which the owned store would read as "never expires".
// That check is reasoned, not observed; docs/verification.md says so.
func TestAKeyExpiringDuringFailoverIsNotPromoted(t *testing.T) {
	f, openNS, key, owner := ttlFabric(t, 10*time.Second)

	f.clk.Advance(7 * time.Second)
	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	via := survivorOf(f, owner)
	if got, err := openNS(via).Get(context.Background(), key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("%s expired during the failover and the promoted node serves it (value %q, "+
			"err %v)", key, got, err)
	}
	// And it stays gone.
	f.clk.Advance(time.Hour)
	if _, err := openNS(via).Get(context.Background(), key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("%s reappeared an hour later: %v", key, err)
	}
}

// TestAPromotedKeyKeepsItsRemainingTTL is the same property for a key that has
// not expired yet: promoted at 8s into a 10s TTL, it has 2s left, not 10.
func TestAPromotedKeyKeepsItsRemainingTTL(t *testing.T) {
	const ttl = 20 * time.Second
	f, openNS, key, owner := ttlFabric(t, ttl)

	before := 10 * time.Second
	f.clk.Advance(before)
	f.hardKill(t, owner)
	detect := 2 * testkit.DefaultFailureTimeout
	if before+detect >= ttl {
		t.Fatalf("failure detection (%v) does not fit inside the TTL", detect)
	}
	f.clk.Advance(detect)
	f.convergeTo(t, 2)
	f.settle(t)

	via := survivorOf(f, owner)
	if _, err := openNS(via).Get(context.Background(), key); err != nil {
		t.Fatalf("Get %s after promotion, inside its TTL: %v", key, err)
	}
	// Past the original deadline: gone.
	f.clk.Advance(ttl - before - detect + time.Millisecond)
	if got, err := openNS(via).Get(context.Background(), key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("%s was written with a %v TTL and is still served after it, by the node "+
			"it was promoted to (value %q, err %v): promotion restarted its lifetime", key, ttl, got, err)
	}
}

// TestADrainedKeyKeepsItsRemainingTTL covers the hand-off path: a draining
// node sends each key with the time it has left, and the receiver must expire
// it on the original schedule.
func TestADrainedKeyKeepsItsRemainingTTL(t *testing.T) {
	f, openNS, key, owner := ttlFabric(t, 10*time.Second)

	f.clk.Advance(6 * time.Second)
	if err := f.caches[owner].Drain(context.Background()); err != nil {
		t.Fatalf("Drain(%s): %v", owner, err)
	}
	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)

	via := survivorOf(f, owner)
	elapsed := 6*time.Second + 2*testkit.DefaultFailureTimeout
	if elapsed < 10*time.Second {
		if _, err := openNS(via).Get(context.Background(), key); err != nil {
			t.Fatalf("Get %s after the drain, inside its TTL: %v", key, err)
		}
		f.clk.Advance(10*time.Second - elapsed + time.Millisecond)
	}
	if got, err := openNS(via).Get(context.Background(), key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("%s was written with a 10s TTL and is still served after 10s on the node it "+
			"was drained to (value %q, err %v)", key, got, err)
	}
}

// TestACaughtUpKeyKeepsItsRemainingTTL covers catch-up: a restarted replica
// pulls a key with the time it has left, and, if later promoted, must expire it
// on the owner's schedule.
func TestACaughtUpKeyKeepsItsRemainingTTL(t *testing.T) {
	// Long enough to fit a replica's death, return and catch-up, then the
	// owner's death, inside it.
	const ttl = 30 * time.Second
	f, openNS, key, owner := ttlFabric(t, ttl)
	ctx := context.Background()
	written := f.clk.Now()

	replica := ""
	for _, id := range f.ids {
		if id != owner && f.caches[id].Stats().ReplicaKeys > 0 {
			replica = id
		}
	}
	if replica == "" {
		t.Fatal("no node holds the replica")
	}
	f.hardKill(t, replica)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)
	f.revive(t, replica)
	openNS(replica)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.convergeTo(t, 3)
	f.settle(t)
	if err := f.caches[replica].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp(%s): %v", replica, err)
	}
	caughtUp := f.clk.Now()

	f.hardKill(t, owner)
	f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	f.convergeTo(t, 2)
	f.settle(t)
	promoted := f.clk.Now()

	// Just past the owner's deadline. A replica that held the time left at
	// catch-up as a duration and restarted it at promotion would still have
	// promoted-caughtUp of it to go.
	deadline := written.Add(ttl)
	if !promoted.Before(deadline) {
		t.Fatalf("promotion at %v is already past the deadline %v; the timings do not fit", promoted.Sub(written), ttl)
	}
	f.clk.Advance(deadline.Sub(promoted) + time.Millisecond)
	if got, err := openNS(replica).Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("%s, caught up %v after it was written and promoted %v after that, is still "+
			"served past its %v TTL (value %q, err %v)", key, caughtUp.Sub(written),
			promoted.Sub(caughtUp), ttl, got, err)
	}
}

// TestExpiredReplicasAreReclaimed: nothing replicates an expiry, so a replica
// store that never swept would hold every replica it was ever sent.
func TestExpiredReplicasAreReclaimed(t *testing.T) {
	f, _, _, owner := ttlFabric(t, 10*time.Second)
	replica := ""
	for _, id := range f.ids {
		if id != owner && f.caches[id].Stats().ReplicaKeys > 0 {
			replica = id
		}
	}
	if replica == "" {
		t.Fatal("no node holds the replica")
	}
	// Past the deadline, then one more sweep tick: the fake ticker collapses
	// missed ticks into the first deadline of an advance, which here is still
	// before the key expired.
	f.clk.Advance(11 * time.Second)
	f.clk.Advance(time.Second)
	waitForCache(t, "the expired replica to be reclaimed", func() bool {
		return f.caches[replica].Stats().ReplicaKeys == 0
	})
}
