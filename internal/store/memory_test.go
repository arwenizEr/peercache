package store_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/store"
)

// entrySize is what the store is documented to charge for one entry.
func entrySize(key string, val []byte) int64 {
	return int64(len(key)+len(val)) + store.EntryOverhead
}

// TestBytesTracksEveryWritePath checks the accounting against a model on every
// path that adds or removes an entry: Set, overwrite, Restore, Delete, Sweep.
//
// M9's memory budget is only as good as this number. A path that forgets to
// update it lets the store grow past the budget without the budget knowing, which
// is the OOM the budget exists to prevent.
func TestBytesTracksEveryWritePath(t *testing.T) {
	s, fc := newStore(t)

	if got := s.Bytes(); got != 0 {
		t.Fatalf("empty store: Bytes = %d, want 0", got)
	}

	s.Set("a", make([]byte, 100), 0)
	want := entrySize("a", make([]byte, 100))
	if got := s.Bytes(); got != want {
		t.Fatalf("after Set: Bytes = %d, want %d", got, want)
	}

	// An overwrite replaces the charge; it does not add a second one.
	s.Set("a", make([]byte, 10), 0)
	want = entrySize("a", make([]byte, 10))
	if got := s.Bytes(); got != want {
		t.Fatalf("after overwrite: Bytes = %d, want %d", got, want)
	}

	s.Restore("b", make([]byte, 50), 0, 7)
	want += entrySize("b", make([]byte, 50))
	if got := s.Bytes(); got != want {
		t.Fatalf("after Restore: Bytes = %d, want %d", got, want)
	}
	// A refused Restore changes nothing.
	s.Restore("b", make([]byte, 5000), 0, 3)
	if got := s.Bytes(); got != want {
		t.Fatalf("after a refused Restore: Bytes = %d, want %d", got, want)
	}

	s.Delete("a")
	want -= entrySize("a", make([]byte, 10))
	if got := s.Bytes(); got != want {
		t.Fatalf("after Delete: Bytes = %d, want %d", got, want)
	}
	s.Delete("never-there")
	if got := s.Bytes(); got != want {
		t.Fatalf("after deleting an absent key: Bytes = %d, want %d", got, want)
	}

	s.Set("c", make([]byte, 30), time.Second)
	fc.Advance(2 * time.Second)
	// Expired but not reclaimed: still resident, still charged.
	if got := s.Bytes(); got != want+entrySize("c", make([]byte, 30)) {
		t.Fatalf("expired, unswept: Bytes = %d, want %d", got, want+entrySize("c", make([]byte, 30)))
	}
	s.Sweep(s.Shards())
	if got := s.Bytes(); got != want {
		t.Fatalf("after Sweep: Bytes = %d, want %d", got, want)
	}
}

// TestBytesMatchesAModelUnderRandomOperations is the same property over a long
// random sequence, which is how question 18 was found and how a missed path
// would be.
func TestBytesMatchesAModelUnderRandomOperations(t *testing.T) {
	s, _ := newStore(t, func(o *store.Options) { o.Shards = 4 })
	r := rand.New(rand.NewSource(1))
	model := map[string]int{}

	for i := 0; i < 5000; i++ {
		key := fmt.Sprintf("k%d", r.Intn(200))
		switch r.Intn(4) {
		case 0, 1:
			n := r.Intn(64)
			s.Set(key, make([]byte, n), 0)
			model[key] = n
		case 2:
			s.Delete(key)
			delete(model, key)
		case 3:
			freed := s.Shed(int64(r.Intn(300)), func(k string, _ uint64) { delete(model, k) })
			if freed < 0 {
				t.Fatalf("Shed returned %d", freed)
			}
		}
		var want int64
		for k, n := range model {
			want += int64(len(k)+n) + store.EntryOverhead
		}
		if got := s.Bytes(); got != want {
			t.Fatalf("op %d: Bytes = %d, model says %d", i, got, want)
		}
	}
}

// TestShedFreesAtLeastTheTarget checks Shed's contract: it removes entries until
// it has freed at least the target or the store is empty, reports every key it
// removed, and the removed keys are gone.
func TestShedFreesAtLeastTheTarget(t *testing.T) {
	s, _ := newStore(t, func(o *store.Options) { o.Shards = 8 })
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("k%04d", i), make([]byte, 100), 0)
	}
	before := s.Bytes()

	var shed []string
	freed := s.Shed(10_000, func(k string, _ uint64) { shed = append(shed, k) })

	if freed < 10_000 {
		t.Fatalf("Shed(10000) freed %d", freed)
	}
	if got := before - s.Bytes(); got != freed {
		t.Fatalf("Shed reported %d freed, Bytes fell by %d", freed, got)
	}
	// It stops soon after the target rather than emptying the store: one
	// entry's worth of overshoot.
	if per := entrySize("k0000", make([]byte, 100)); freed >= 10_000+per {
		t.Fatalf("Shed(10000) freed %d, more than one entry (%d) past the target", freed, per)
	}
	for _, k := range shed {
		if s.Exists(k) {
			t.Fatalf("Shed reported %q removed and it is still present", k)
		}
	}
	if got := s.Stats().Shed; got != int64(len(shed)) {
		t.Fatalf("Stats().Shed = %d, want %d", got, len(shed))
	}

	// A target larger than the store empties it and returns what there was.
	rest := s.Bytes()
	if freed := s.Shed(1<<40, func(string, uint64) {}); freed != rest {
		t.Fatalf("Shed(huge) freed %d, the store held %d", freed, rest)
	}
	if s.Len() != 0 || s.Bytes() != 0 {
		t.Fatalf("after shedding everything: Len %d, Bytes %d", s.Len(), s.Bytes())
	}
}

// TestShedRotatesAcrossShards keeps repeated small sheds from always emptying the
// same shard, which would make shedding a function of the hash rather than of
// pressure and leave one key range permanently cold.
func TestShedRotatesAcrossShards(t *testing.T) {
	s, _ := newStore(t, func(o *store.Options) { o.Shards = 16 })
	for i := 0; i < 1600; i++ {
		s.Set(fmt.Sprintf("k%04d", i), make([]byte, 10), 0)
	}
	one := entrySize("k0000", make([]byte, 10))

	hit := map[int]bool{}
	for i := 0; i < 16; i++ {
		s.Shed(one, func(k string, _ uint64) { hit[s.ShardOf(k)] = true })
	}
	if len(hit) < 16 {
		t.Fatalf("16 single-entry sheds touched %d of 16 shards; shedding is not rotating", len(hit))
	}
}

// TestShedIsSafeAlongsideWrites runs shedding against concurrent writers under
// -race and then checks the accounting still balances.
func TestShedIsSafeAlongsideWrites(t *testing.T) {
	s, _ := newStore(t)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				s.Set(fmt.Sprintf("w%d-%d", w, i%300), make([]byte, i%50), 0)
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			s.Shed(500, func(string, uint64) {})
		}
	}()
	wg.Wait()

	// Recompute from the contents.
	var want int64
	page, more := s.Snapshot("", 1<<20, nil)
	if more {
		t.Fatal("snapshot paged; the test assumed one page")
	}
	for _, e := range page {
		want += int64(len(e.Key)+len(e.Value)) + store.EntryOverhead
	}
	if got := s.Bytes(); got != want {
		t.Fatalf("after concurrent writes and sheds: Bytes = %d, contents add up to %d", got, want)
	}
}

// TestARewriteAfterShedIsVersionedAboveWhatWasShed is what keeps a shed key
// from being resurrected at an older value, independently of any message
// arriving.
//
// A replica holds the owner's copy at the version the owner issued. If the
// owner sheds the key and is asked to write it again, an ordinary restart at
// version 1 would lose to the replica's copy — the rewrite would be refused
// there — and a failover would promote the older value. The owner's removal
// message to the replica would fix that if it arrived, but CLAUDE.md decision 6
// says correctness must never depend on a message arriving. So the store
// remembers, per shard, the highest version it ever shed, and a rewrite starts
// above it.
//
// Delete and Sweep do not raise the floor: a deletion replicates its own
// tombstone, and an expiry happens on every holder on the same schedule, so
// question 18's restart is harmless for both and stays as it was.
func TestARewriteAfterShedIsVersionedAboveWhatWasShed(t *testing.T) {
	s, _ := newStore(t, func(o *store.Options) { o.Shards = 1 })
	var v uint64
	for i := 0; i < 5; i++ {
		v = s.Set("k", []byte("old"), 0)
	}
	if v != 5 {
		t.Fatalf("five writes produced version %d", v)
	}
	s.Shed(1<<40, func(string, uint64) {})
	if s.Exists("k") {
		t.Fatal("Shed of everything left k")
	}
	if got := s.Set("k", []byte("new"), 0); got <= 5 {
		t.Fatalf("a rewrite after shedding version 5 got version %d; a replica still "+
			"holding version 5 would refuse it", got)
	}
	// The floor is per shard, so it covers every key that shard ever shed,
	// including ones never rewritten until later.
	if got := s.Set("other", []byte("x"), 0); got <= 5 {
		t.Fatalf("a first write to another key in a shard that shed version 5 got %d", got)
	}

	// Delete does not raise it: question 18 stands for deletions.
	d, _ := newStore(t, func(o *store.Options) { o.Shards = 1 })
	for i := 0; i < 5; i++ {
		d.Set("k", []byte("old"), 0)
	}
	d.Delete("k")
	if got := d.Set("k", []byte("new"), 0); got != 1 {
		t.Fatalf("a rewrite after Delete got version %d, want 1: only shedding raises the floor", got)
	}
}
