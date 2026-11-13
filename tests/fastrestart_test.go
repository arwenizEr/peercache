package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestANodeRestartedFasterThanDetectionRecoversWhatItOwns is M16's kill -9.
//
// A pod killed with SIGKILL is restarted by its kubelet in a second or two —
// same name, same address — which is inside the failure detector's window. Its
// peers never declare it dead: no membership change, no epoch, and so no
// promotion of the replica copies they hold of its keys. The node comes back
// with empty memory to keys it still owns, and those keys' only copies are in
// its peers' *replica* stores.
//
// CatchUp is what a restarted node calls, and the sender used to read only its
// owned store. So the restarted owner recovered every key it holds as a
// replica, and none of the keys it owns: on kind, 60 acknowledged orders came
// back as missing or as an older value. A synchronous write held by RF nodes
// must survive the loss of one of them however fast it comes back.
func TestANodeRestartedFasterThanDetectionRecoversWhatItOwns(t *testing.T) {
	f := newFabric(t, 3)
	ctx := context.Background()
	open := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("fast",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		open(id)
	}

	const keys = 200
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("fast/%d", i)
		if err := open(f.ids[0]).Set(ctx, k, []byte(fmt.Sprintf("v%d", i)), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	victim := f.ids[1]
	var owned []string
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("fast/%d", i)
		if f.ownerOf(t, k) == victim {
			owned = append(owned, k)
		}
	}
	if len(owned) == 0 {
		t.Fatal("the victim owns nothing; the test cannot lose anything")
	}
	epochs := map[string]uint64{}
	for _, id := range f.ids {
		if id != victim {
			epochs[id] = f.caches[id].Describe().Epoch
		}
	}

	// Killed and back before anyone notices: the clock moves by the join
	// delay, well inside the failure timeout.
	f.hardKill(t, victim)
	f.revive(t, victim)
	f.clk.Advance(2 * testkit.DefaultJoinDelay)
	f.converge(t)
	open(victim)
	for id, before := range epochs {
		if after := f.caches[id].Describe().Epoch; after != before {
			t.Fatalf("%s moved from epoch %d to %d; this test is about a restart its peers never saw",
				id, before, after)
		}
	}

	if err := f.caches[victim].CatchUp(ctx); err != nil {
		t.Fatalf("CatchUp: %v", err)
	}

	var missing []string
	for _, k := range owned {
		got, err := open(victim).Get(ctx, k)
		if err != nil || string(got) != fmt.Sprintf("v%s", k[len("fast/"):]) {
			missing = append(missing, fmt.Sprintf("%s=%q,%v", k, got, err))
		}
	}
	if len(missing) > 0 {
		t.Fatalf("after catch-up, %d of the %d keys the restarted node owns are not back; "+
			"their copies sit in its peers' replica stores. First: %v",
			len(missing), len(owned), missing[:min(3, len(missing))])
	}
}
