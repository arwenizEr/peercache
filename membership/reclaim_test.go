package membership_test

import (
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/membership"
)

// TestADeadNodeRejoinsUnderItsNameFromANewAddress: a node that crashed and
// came back under the same name from a different address — a StatefulSet pod
// rescheduled after a crash — rejoins as soon as its peers have declared it
// dead, rather than after memberlist forgets it.
//
// With memberlist's default of never reclaiming a dead name, the new
// incarnation's alive message was refused as a conflicting address until the
// dead node was reaped: fifteen seconds on the local profile, thirty on the
// LAN one.
func TestADeadNodeRejoinsUnderItsNameFromANewAddress(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	a := newGossip(t, "a")

	first, err := newGossipAt(t, core.Node{ID: "b", Addr: "first:7947"})
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	if err := first.Join([]string{a.LocalAddr()}); err != nil {
		t.Fatalf("Join: %v", err)
	}
	awaitReal(t, "a to see b", 10*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 2
	})

	// A crash: no Leave, so a declares b dead rather than departed.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	awaitReal(t, "a to declare b dead", 15*time.Second, func() bool {
		members, _ := a.Members()
		return len(members) == 1
	})

	// The new incarnation binds a different gossip port: a new address
	// under the old name.
	second, err := testkit.RetryBind(func() (*membership.Gossip, error) {
		return membership.New(membership.Options{
			Self:   core.Node{ID: "b", Addr: "second:7947"},
			Config: localConfig("b"),
		})
	})
	if err != nil {
		t.Fatalf("membership.New: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if second.LocalAddr() == first.LocalAddr() {
		t.Fatalf("both incarnations bound %s; the test needs a new address", second.LocalAddr())
	}
	// Joining again every half second, as a deployment's join loop does
	// (examples/shop): a join inside the reclaim time is refused, and the
	// next one is not. Well inside the local profile's fifteen seconds
	// before a dead node is forgotten, which is how long every join was
	// refused before.
	lastJoin := time.Time{}
	awaitReal(t, "a to see b again, at its new address", 8*time.Second, func() bool {
		if time.Since(lastJoin) > 500*time.Millisecond {
			lastJoin = time.Now()
			_ = second.Join([]string{a.LocalAddr()})
		}
		members, _ := a.Members()
		for _, m := range members {
			if m.ID == "b" && m.Addr == "second:7947" {
				return true
			}
		}
		return false
	})
}
