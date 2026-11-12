package replication_test

import (
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/replication"
)

func entry(from string, epoch, version uint64, val string) replication.Entry {
	return replication.Entry{
		Value: []byte(val),
		Stamp: core.Stamp{Epoch: epoch, Version: version},
		From:  from,
	}
}

func TestPutAndGet(t *testing.T) {
	s := replication.NewStore()

	if _, ok := s.Get("ns", "k"); ok {
		t.Fatal("an empty store returned a value")
	}
	accepted, conflict := s.Put("ns", "k", entry("owner", 1, 1, "v"))
	if !accepted || conflict {
		t.Fatalf("first Put: accepted=%v conflict=%v, want true/false", accepted, conflict)
	}
	got, ok := s.Get("ns", "k")
	if !ok || string(got.Value) != "v" {
		t.Fatalf("Get = %q, %v", got.Value, ok)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
}

// TestAnOlderCopyIsRejected is the property async replication makes necessary.
//
// Fan-out is asynchronous by default, so two updates to one key can arrive out
// of order. A late delivery of an old value must not undo a newer one — without
// this check the replica would hold whichever packet the network happened to
// deliver last, which is not a policy anybody chose.
func TestAnOlderCopyIsRejected(t *testing.T) {
	s := replication.NewStore()

	s.Put("ns", "k", entry("owner", 1, 5, "new"))
	if accepted, _ := s.Put("ns", "k", entry("owner", 1, 4, "old")); accepted {
		t.Fatal("an older copy overwrote a newer one; out-of-order delivery would decide " +
			"the value of every key")
	}
	if got, _ := s.Get("ns", "k"); string(got.Value) != "new" {
		t.Fatalf("the replica holds %q, want the newer value", got.Value)
	}

	// A repeat of the resident stamp is a retransmission, not an update.
	if accepted, _ := s.Put("ns", "k", entry("owner", 1, 5, "again")); accepted {
		t.Fatal("an identical stamp was treated as an update")
	}
}

// TestVersionConflictsAreCountedButRetransmissionsAreNot covers the metric
// SPEC.md's M7 bullet names, and the distinction that makes it worth having.
//
// A strictly older stamp arriving is a real out-of-order delivery — ordinary
// under asynchronous replication, handled correctly by the stamp ordering, and
// still worth a number an operator can watch. An identical stamp is a
// retransmission and means nothing at all. Counting both would make the metric
// track retry volume instead of reordering, which is a different question with
// a much larger and less interesting answer.
func TestVersionConflictsAreCountedButRetransmissionsAreNot(t *testing.T) {
	s := replication.NewStore()

	if got := s.VersionConflicts(); got != 0 {
		t.Fatalf("a fresh store reports %d conflicts, want 0", got)
	}

	s.Put("ns", "k", entry("owner", 1, 5, "new"))
	if got := s.VersionConflicts(); got != 0 {
		t.Fatalf("accepting the first copy counted %d conflicts, want 0", got)
	}

	// Strictly older: a conflict.
	s.Put("ns", "k", entry("owner", 1, 4, "old"))
	if got := s.VersionConflicts(); got != 1 {
		t.Fatalf("an out-of-order copy counted %d conflicts, want 1", got)
	}

	// Identical stamp: a retransmission, and not a conflict.
	s.Put("ns", "k", entry("owner", 1, 5, "again"))
	if got := s.VersionConflicts(); got != 1 {
		t.Fatalf("a retransmission counted %d conflicts, want 1; the metric would "+
			"then measure retry volume rather than reordering", got)
	}

	// An older epoch is also out of order, whatever its version.
	s.Put("ns", "k", entry("owner", 0, 900, "older epoch"))
	if got := s.VersionConflicts(); got != 2 {
		t.Fatalf("a lower-epoch copy counted %d conflicts, want 2", got)
	}

	// And an accepted newer copy still does not count.
	s.Put("ns", "k", entry("owner", 1, 6, "newer"))
	if got := s.VersionConflicts(); got != 2 {
		t.Fatalf("accepting a newer copy counted %d conflicts, want 2", got)
	}
}

// TestALaterEpochWinsWhateverTheVersion is the same rule the L1 tier follows,
// and it matters more here.
//
// After ownership moves, the new owner often reconstructs a key and issues
// version 1 for it. A replica comparing versions alone would reject the new
// owner's value as stale and keep serving the dead node's, which is exactly the
// data a promotion is supposed to replace.
func TestALaterEpochWinsWhateverTheVersion(t *testing.T) {
	s := replication.NewStore()

	s.Put("ns", "k", entry("old-owner", 4, 900, "from the old owner"))
	if accepted, _ := s.Put("ns", "k", entry("new-owner", 5, 1, "from the new owner")); !accepted {
		t.Fatal("a value from a later epoch was rejected because its version was lower; " +
			"version sequences either side of an ownership change are unrelated")
	}
	if got, _ := s.Get("ns", "k"); string(got.Value) != "from the new owner" {
		t.Fatalf("the replica holds %q, want the new owner's value", got.Value)
	}
}

// TestTwoOwnersInOneEpochIsAConflict is the split-brain detector SPEC.md §4
// requires.
//
// One group has one owner, so two *different* nodes writing the same key under
// the same epoch cannot happen unless two of them believe they own it. That is
// the partition case, and the replica is the only party positioned to notice:
// neither owner can see the other.
func TestTwoOwnersInOneEpochIsAConflict(t *testing.T) {
	s := replication.NewStore()

	if _, conflict := s.Put("ns", "k", entry("node-a", 7, 1, "a")); conflict {
		t.Fatal("the first write reported a conflict")
	}
	_, conflict := s.Put("ns", "k", entry("node-b", 7, 2, "b"))
	if !conflict {
		t.Fatal("two different nodes wrote one key under the same epoch and it was not " +
			"reported; that is a split brain, and the replica is the only node that " +
			"can see it")
	}

	// The write still applies. Detection is not resolution: CLAUDE.md is
	// explicit that conflicting writes are not automatically merged, and
	// dropping one silently would be a merge policy chosen by accident.
	if got, _ := s.Get("ns", "k"); string(got.Value) != "b" {
		t.Fatalf("the replica holds %q; a detected conflict must still apply the newer "+
			"write rather than silently discarding it", got.Value)
	}
}

// TestTheSameOwnerWritingTwiceIsNotAConflict guards the obvious false positive.
// An owner updating its own key repeatedly within one epoch is the normal case.
func TestTheSameOwnerWritingTwiceIsNotAConflict(t *testing.T) {
	s := replication.NewStore()
	s.Put("ns", "k", entry("node-a", 7, 1, "one"))
	if _, conflict := s.Put("ns", "k", entry("node-a", 7, 2, "two")); conflict {
		t.Fatal("an owner updating its own key was reported as a split brain")
	}
}

// TestDifferentEpochsAreNotAConflict guards the other false positive. Ownership
// moving is the system working, not a split brain.
func TestDifferentEpochsAreNotAConflict(t *testing.T) {
	s := replication.NewStore()
	s.Put("ns", "k", entry("node-a", 7, 1, "before"))
	if _, conflict := s.Put("ns", "k", entry("node-b", 8, 1, "after")); conflict {
		t.Fatal("a handover between epochs was reported as a split brain; ownership " +
			"moving is the normal case and counting it would bury the real ones")
	}
}

func TestDelete(t *testing.T) {
	s := replication.NewStore()
	s.Put("ns", "k", entry("owner", 1, 1, "v"))

	if !s.Delete("ns", "k") {
		t.Fatal("Delete reported nothing removed")
	}
	if _, ok := s.Get("ns", "k"); ok {
		t.Fatal("the key survived Delete")
	}
	if s.Delete("ns", "k") {
		t.Fatal("deleting an absent key reported a removal")
	}
	if s.Delete("missing", "k") {
		t.Fatal("deleting from an unknown namespace reported a removal")
	}
}

// TestDrainTakesOnlyWhatItClaims is the promotion path: a node becoming the
// owner of keys it held as replicas takes exactly those and leaves the rest.
func TestDrainTakesOnlyWhatItClaims(t *testing.T) {
	s := replication.NewStore()
	for i := 0; i < 10; i++ {
		s.Put("ns", fmt.Sprintf("k%d", i), entry("owner", 1, uint64(i+1), fmt.Sprintf("v%d", i)))
	}

	// Take the even keys, as if this node had become their owner.
	taken := s.Drain("ns", func(key string) bool {
		var n int
		fmt.Sscanf(key, "k%d", &n)
		return n%2 == 0
	})
	if len(taken) != 5 {
		t.Fatalf("Drain took %d keys, want 5", len(taken))
	}
	if s.Len() != 5 {
		t.Fatalf("%d keys remain, want 5", s.Len())
	}

	// A drained key must not remain in the replica store. A key present in
	// both stores would be served from whichever the read path consulted
	// first, which is not a decision anybody made.
	for key := range taken {
		if _, ok := s.Get("ns", key); ok {
			t.Fatalf("key %q is in both the replica store and the drained set", key)
		}
	}
	for key, e := range taken {
		if len(e.Value) == 0 {
			t.Fatalf("key %q was drained without its value", key)
		}
		if e.Stamp.IsZero() {
			t.Fatalf("key %q was drained without its stamp; a promoted replica must "+
				"continue the version sequence rather than restart it", key)
		}
	}
}

func TestDrainOnAnUnknownNamespace(t *testing.T) {
	s := replication.NewStore()
	if got := s.Drain("missing", func(string) bool { return true }); got != nil {
		t.Fatalf("Drain on an unknown namespace = %v, want nil", got)
	}
}

// TestEvictDropsWhatIsNoLongerOurs covers the other half of a membership
// change: a node that stops being a replica for a key must let it go, or it
// holds memory nobody reads and a value that drifts further from the truth with
// every write it no longer receives.
func TestEvictDropsWhatIsNoLongerOurs(t *testing.T) {
	s := replication.NewStore()
	for i := 0; i < 6; i++ {
		s.Put("ns", fmt.Sprintf("k%d", i), entry("owner", 1, 1, "v"))
	}

	dropped := s.Evict("ns", func(key string) bool { return key == "k0" || key == "k1" })
	if dropped != 4 {
		t.Fatalf("Evict dropped %d, want 4", dropped)
	}
	if s.Len() != 2 {
		t.Fatalf("%d keys remain, want 2", s.Len())
	}
	if _, ok := s.Get("ns", "k0"); !ok {
		t.Fatal("Evict removed a key it was told to keep")
	}
}

func TestNamespacesAndKeys(t *testing.T) {
	s := replication.NewStore()
	s.Put("alpha", "a", entry("o", 1, 1, "v"))
	s.Put("beta", "b", entry("o", 1, 1, "v"))
	s.Put("beta", "c", entry("o", 1, 1, "v"))

	names := s.Namespaces()
	sort.Strings(names)
	if len(names) != 2 || names[0] != "alpha" || names[1] != "beta" {
		t.Fatalf("Namespaces = %v, want [alpha beta]", names)
	}

	keys := s.Keys("beta")
	sort.Strings(keys)
	if len(keys) != 2 || keys[0] != "b" || keys[1] != "c" {
		t.Fatalf("Keys(beta) = %v", keys)
	}
	if got := s.Keys("missing"); got != nil {
		t.Fatalf("Keys on an unknown namespace = %v, want nil", got)
	}
}

// TestConcurrentPutsKeepTheNewest is the race the stamp check exists to lose
// safely. Many writers, out of order, must leave the highest stamp resident.
func TestConcurrentPutsKeepTheNewest(t *testing.T) {
	const writers = 32
	const rounds = 20

	for round := 0; round < rounds; round++ {
		s := replication.NewStore()
		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup

		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(version uint64) {
				defer wg.Done()
				start.Wait()
				s.Put("ns", "k", replication.Entry{
					Value: []byte(fmt.Sprintf("v%d", version)),
					Stamp: core.Stamp{Epoch: 1, Version: version},
					From:  "owner",
				})
			}(uint64(w + 1))
		}
		start.Done()
		wg.Wait()

		got, ok := s.Get("ns", "k")
		if !ok {
			t.Fatalf("round %d: the key vanished", round)
		}
		if got.Stamp.Version != writers {
			t.Fatalf("round %d: resident version is %d, want the highest written (%d); "+
				"a concurrent Put overwrote a newer entry",
				round, got.Stamp.Version, writers)
		}
	}
}

// TestConcurrentDrainAndPut checks that promotion cannot strand a copy.
//
// If a Put could slip in behind a Drain, the key would stay in the replica
// store after this node had taken ownership of it — a second copy that no write
// will ever update.
func TestConcurrentDrainAndPut(t *testing.T) {
	for round := 0; round < 50; round++ {
		s := replication.NewStore()
		for i := 0; i < 50; i++ {
			s.Put("ns", fmt.Sprintf("k%d", i), entry("owner", 1, 1, "v"))
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var taken map[string]replication.Entry
		go func() {
			defer wg.Done()
			taken = s.Drain("ns", func(string) bool { return true })
		}()
		go func() {
			defer wg.Done()
			for i := 50; i < 100; i++ {
				s.Put("ns", fmt.Sprintf("k%d", i), entry("owner", 1, 1, "v"))
			}
		}()
		wg.Wait()

		// Every key is in exactly one place.
		for key := range taken {
			if _, ok := s.Get("ns", key); ok {
				t.Fatalf("round %d: key %q is in both stores", round, key)
			}
		}
		if total := len(taken) + s.Len(); total != 100 {
			t.Fatalf("round %d: %d keys drained plus %d remaining = %d, want 100",
				round, len(taken), s.Len(), total)
		}
	}
}

// TestEvictWhereSeesTheEntry: the RF=1 reconciliation needs to know who sent a
// copy, not only its key, to tell a hand-off still in flight from a surplus
// copy.
func TestEvictWhereSeesTheEntry(t *testing.T) {
	s := replication.NewStore()
	s.Put("ns", "from-a", entry("a", 1, 1, "x"))
	s.Put("ns", "from-b", entry("b", 1, 1, "y"))

	dropped := s.EvictWhere("ns", func(key string, e replication.Entry) bool { return e.From == "a" })
	if dropped != 1 {
		t.Fatalf("EvictWhere dropped %d, want 1", dropped)
	}
	if _, ok := s.Get("ns", "from-a"); !ok {
		t.Fatal("the kept entry is gone")
	}
	if _, ok := s.Get("ns", "from-b"); ok {
		t.Fatal("the evicted entry is still held")
	}
	if got := s.Bytes(); got != charge("from-a", entry("a", 1, 1, "x")) {
		t.Fatalf("Bytes = %d after EvictWhere, want the kept entry's charge", got)
	}
	if n := s.EvictWhere("absent", func(string, replication.Entry) bool { return false }); n != 0 {
		t.Fatalf("EvictWhere on an unknown namespace dropped %d", n)
	}
}
