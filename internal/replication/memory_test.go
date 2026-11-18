package replication_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache/internal/replication"
)

func charge(key string, e replication.Entry) int64 {
	return int64(len(key)+len(e.Value)+len(e.From)) + replication.EntryOverhead
}

// TestReplicaBytesTracksEveryPath is the replica store's half of the memory
// budget's accounting: Put, a replacing Put, a refused Put, Delete, Drain and Evict.
func TestReplicaBytesTracksEveryPath(t *testing.T) {
	s := replication.NewStore()
	if s.Bytes() != 0 {
		t.Fatalf("empty: Bytes = %d", s.Bytes())
	}

	a1 := entry("n1", 1, 1, "aaaaaaaaaa")
	s.Put("ns", "a", a1)
	want := charge("a", a1)
	if got := s.Bytes(); got != want {
		t.Fatalf("after Put: %d, want %d", got, want)
	}

	a2 := entry("n1", 1, 2, "a")
	s.Put("ns", "a", a2)
	want = charge("a", a2)
	if got := s.Bytes(); got != want {
		t.Fatalf("after a replacing Put: %d, want %d", got, want)
	}

	s.Put("ns", "a", entry("n1", 1, 1, "a much longer and older value"))
	if got := s.Bytes(); got != want {
		t.Fatalf("after a refused Put: %d, want %d", got, want)
	}

	b := entry("n1", 1, 1, "bbbb")
	s.Put("other", "b", b)
	want += charge("b", b)
	s.Delete("ns", "a")
	want -= charge("a", a2)
	if got := s.Bytes(); got != want {
		t.Fatalf("after Delete: %d, want %d", got, want)
	}

	c := entry("n1", 1, 1, "cc")
	d := entry("n1", 1, 1, "ddd")
	s.Put("other", "c", c)
	s.Put("other", "d", d)
	s.Drain("other", func(k string) bool { return k == "c" })
	want += charge("d", d)
	if got := s.Bytes(); got != want {
		t.Fatalf("after Drain: %d, want %d", got, want)
	}

	s.Evict("other", func(string) bool { return false })
	if got := s.Bytes(); got != 0 {
		t.Fatalf("after evicting everything: %d, want 0", got)
	}
}

// TestReplicaShedFreesAtLeastTheTarget checks Shed across namespaces.
func TestReplicaShedFreesAtLeastTheTarget(t *testing.T) {
	s := replication.NewStore()
	for i := 0; i < 300; i++ {
		s.Put(fmt.Sprintf("ns%d", i%3), fmt.Sprintf("k%03d", i), entry("n1", 1, 1, "0123456789"))
	}
	before := s.Bytes()
	freed := s.Shed(2000, nil)
	if freed < 2000 {
		t.Fatalf("Shed(2000) freed %d", freed)
	}
	if got := before - s.Bytes(); got != freed {
		t.Fatalf("Shed reported %d, Bytes fell by %d", freed, got)
	}
	per := charge("k000", entry("n1", 1, 1, "0123456789"))
	if freed >= 2000+per {
		t.Fatalf("Shed(2000) freed %d, more than one entry (%d) past the target", freed, per)
	}
	if got, want := s.ShedCount(), int64(300-s.Len()); got != want {
		t.Fatalf("ShedCount = %d, but %d entries are gone", got, want)
	}

	// Repeated sheds visit every namespace rather than emptying one.
	touched := map[string]bool{}
	for i := 0; i < 6; i++ {
		lens := map[string]int{}
		for _, ns := range []string{"ns0", "ns1", "ns2"} {
			lens[ns] = len(s.Keys(ns))
		}
		s.Shed(per, nil)
		for _, ns := range []string{"ns0", "ns1", "ns2"} {
			if len(s.Keys(ns)) < lens[ns] {
				touched[ns] = true
			}
		}
	}
	if len(touched) != 3 {
		t.Fatalf("six single-entry sheds touched %d of 3 namespaces", len(touched))
	}

	rest := s.Bytes()
	if freed := s.Shed(1<<40, nil); freed != rest || s.Bytes() != 0 || s.Len() != 0 {
		t.Fatalf("Shed(huge) freed %d of %d; left Bytes %d Len %d", freed, rest, s.Bytes(), s.Len())
	}
}

// TestReplicaBytesUnderConcurrency runs Put, Delete and Shed together under
// -race and then checks the counter against the contents.
func TestReplicaBytesUnderConcurrency(t *testing.T) {
	s := replication.NewStore()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("k%d", r.Intn(200))
				if r.Intn(3) == 0 {
					s.Delete("ns", key)
				} else {
					s.Put("ns", key, entry("n1", 1, uint64(i+1), string(make([]byte, r.Intn(40)))))
				}
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			s.Shed(400, nil)
		}
	}()
	wg.Wait()

	var want int64
	for _, k := range s.Keys("ns") {
		e, ok := s.Get("ns", k)
		if !ok {
			t.Fatalf("Keys listed %q and Get cannot find it", k)
		}
		want += charge(k, e)
	}
	if got := s.Bytes(); got != want {
		t.Fatalf("Bytes = %d, contents add up to %d", got, want)
	}
}

// TestRemainingAndDeadline pins the arithmetic every holder of a replica uses
// to agree on when it expires.
func TestRemainingAndDeadline(t *testing.T) {
	if got := replication.DeadlineFrom(100, 0); got != 0 {
		t.Fatalf("DeadlineFrom(100, 0) = %d, want 0: no TTL means no expiry", got)
	}
	if got := replication.DeadlineFrom(100, -5); got != 0 {
		t.Fatalf("DeadlineFrom(100, -5) = %d, want 0", got)
	}
	if got := replication.DeadlineFrom(100, 50); got != 150 {
		t.Fatalf("DeadlineFrom(100, 50) = %d, want 150", got)
	}

	never := replication.Entry{}
	if left, live := never.Remaining(1 << 60); left != 0 || !live {
		t.Fatalf("an entry with no deadline: Remaining = %d, %v; want 0, true", left, live)
	}
	e := replication.Entry{ExpiresAt: 150}
	if left, live := e.Remaining(100); left != 50 || !live {
		t.Fatalf("Remaining(100) = %d, %v; want 50, true", left, live)
	}
	// At and past the deadline it is gone, never "zero left", which the
	// owned store would read as "does not expire".
	for _, now := range []int64{150, 151} {
		if _, live := e.Remaining(now); live || e.Live(now) {
			t.Fatalf("at %d, an entry expiring at 150 reports itself live", now)
		}
	}
}
