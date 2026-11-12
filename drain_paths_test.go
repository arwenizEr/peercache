package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/arwenizEr/peercache"
)

// Drain paths a coverage report showed no test had run.

// TestASecondDrainDoesNothing: a PreStop hook that fires twice is a retry, and
// the second call must neither fail nor hand everything off again.
func TestASecondDrainDoesNothing(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("twice", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	for i := 0; i < 60; i++ {
		if err := openNS(f.ids[i%3]).Set(ctx, fmt.Sprintf("twice/%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}

	leaver := f.ids[2]
	if err := f.caches[leaver].Drain(ctx); err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	first := f.caches[leaver].Stats().KeysHandedOff
	if first == 0 {
		t.Fatal("the first drain handed off nothing; the second cannot be compared with it")
	}
	if err := f.caches[leaver].Drain(ctx); err != nil {
		t.Fatalf("second Drain returned %v; a repeated PreStop is a retry, not a mistake", err)
	}
	if again := f.caches[leaver].Stats().KeysHandedOff; again != first {
		t.Fatalf("the second Drain handed off %d more keys", again-first)
	}
}

// TestDrainUnderJoinProbationAnnouncesTheDeparture: with probation on, the
// membership the cache holds is the probation wrapper, and Drain's departure
// announcement goes through it. A wrapper that swallowed Leave would turn every
// graceful shutdown into a detected failure.
func TestDrainUnderJoinProbationAnnouncesTheDeparture(t *testing.T) {
	const keys = 120
	f, openNS := newProbationFabric(t, keys)
	ctx := context.Background()
	leaver := "n4"

	if err := f.caches[leaver].Drain(ctx); err != nil {
		t.Fatalf("Drain(%s): %v", leaver, err)
	}
	// No clock movement: a departure that was announced is known at once,
	// while one that was not would wait for the failure timeout.
	waitForCache(t, leaver+" to be gone from n0's membership without a failure timeout", func() bool {
		return !slices.Contains(f.memberships["n0"].MemberIDs(), leaver)
	})
	if f.caches[leaver].Stats().KeysHandedOff == 0 {
		t.Fatalf("%s drained under probation and handed nothing off", leaver)
	}
	f.settle(t)
	for i := 0; i < keys; i++ {
		if _, err := readWithRetryFrom(t, f, openNS, f.ids[i%4], fmt.Sprintf("probation/%04d", i)); err != nil {
			t.Fatalf("key %d after %s drained: %v", i, leaver, err)
		}
	}
}

// TestDrainReportsAFailedDepartureAndStillHandsOff: Drain's comment says an
// announcement that fails is reported, not fatal, because giving up would skip
// the transfer — the part that saves data.
func TestDrainReportsAFailedDepartureAndStillHandsOff(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("noannounce", peercache.WithReplicationFactor(2))
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.settle(t)
	for i := 0; i < 60; i++ {
		if err := openNS(f.ids[i%3]).Set(ctx, fmt.Sprintf("noannounce/%d", i), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}

	leaver := f.ids[2]
	// A membership that can no longer announce anything.
	f.memberships[leaver].Close()

	err := f.caches[leaver].Drain(ctx)
	if err == nil {
		t.Fatal("Drain returned nil although its departure could not be announced")
	}
	if !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("Drain returned %v; want the announcement's failure wrapped", err)
	}
	if f.caches[leaver].Stats().KeysHandedOff == 0 {
		t.Fatalf("the announcement failed and Drain handed nothing off (err %v); the transfer "+
			"is the part that saves data and must not be skipped", err)
	}
}
