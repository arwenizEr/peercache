package replication_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/arwenizEr/peercache/internal/replication"
)

// TestShedKeepsCopiesFromTheFloor: a namespace's copies at or past the order
// keepFrom names are not shed, however much is asked for, and the rest of it
// and every other namespace are shed as before.
func TestShedKeepsCopiesFromTheFloor(t *testing.T) {
	s := replication.NewStore()
	for i := 0; i < 100; i++ {
		young := entry("n1", 1, 1, "v")
		young.Order = uint64(1000 + i)
		s.Put("durable", fmt.Sprintf("young%02d", i), young)
		old := entry("n1", 1, 1, "v")
		old.Order = uint64(i + 1)
		s.Put("durable", fmt.Sprintf("old%02d", i), old)
		s.Put("lossy", fmt.Sprintf("k%02d", i), young)
	}

	s.Shed(1<<40, func(ns string) uint64 {
		if ns == "durable" {
			return 1000
		}
		return math.MaxUint64
	})

	if n := len(s.Keys("lossy")); n != 0 {
		t.Fatalf("%d copies of a namespace with no floor survived a shed of everything", n)
	}
	kept := s.Keys("durable")
	if len(kept) != 100 {
		t.Fatalf("%d copies of the floored namespace kept, want the 100 at or past it", len(kept))
	}
	for _, k := range kept {
		if k[:5] != "young" {
			t.Fatalf("kept %s, a copy below the floor", k)
		}
	}
}

// BenchmarkReplicaShed sheds a tenth of 100,000 replicas, with no floor and
// with one that keeps the youngest tenth, which a shed must step over.
func BenchmarkReplicaShed(b *testing.B) {
	const n = 100_000
	fill := func() *replication.Store {
		s := replication.NewStore()
		for i := 0; i < n; i++ {
			e := entry("n1", 1, 1, "0123456789012345678901234567890123456789")
			e.Order = uint64(i + 1)
			s.Put("durable", fmt.Sprintf("key-%06d", i), e)
		}
		return s
	}
	for _, tc := range []struct {
		name     string
		keepFrom func(string) uint64
	}{
		{"no-floor", nil},
		{"youngest-tenth-kept", func(string) uint64 { return n - n/10 }},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				s := fill()
				target := s.Bytes() / 10
				b.StartTimer()
				s.Shed(target, tc.keepFrom)
			}
		})
	}
}

// TestShedVisitKeepsEachWritersCopiesFromItsOwnFloor: the floor is per
// writer. One writer's copies are kept from its floor and another's from
// theirs, in the same namespace, and a namespace given no floor function
// keeps nothing.
func TestShedVisitKeepsEachWritersCopiesFromItsOwnFloor(t *testing.T) {
	s := replication.NewStore()
	for i := 0; i < 100; i++ {
		for _, from := range []string{"a", "b"} {
			e := entry(from, 1, 1, "v")
			e.Order = uint64(i + 1)
			s.Put("durable", fmt.Sprintf("%s%02d", from, i), e)
		}
		s.Put("lossy", fmt.Sprintf("k%02d", i), entry("a", 1, 1, "v"))
	}

	shed := map[string]int{}
	s.ShedVisit(1<<40, func(ns string) func(string) uint64 {
		if ns != "durable" {
			return nil
		}
		return func(writer string) uint64 {
			if writer == "a" {
				return 91 // a's copies from order 91 are kept: 10 of them
			}
			return 51 // b's from 51: 50
		}
	}, func(ns, key string, _ replication.Entry) { shed[ns]++ })

	if n := len(s.Keys("lossy")); n != 0 {
		t.Fatalf("%d copies of a namespace with no floor survived a shed of everything", n)
	}
	kept := map[byte]int{}
	for _, k := range s.Keys("durable") {
		kept[k[0]]++
	}
	if kept['a'] != 10 || kept['b'] != 50 {
		t.Fatalf("kept %d of a's copies and %d of b's, want 10 and 50: each writer from its own floor",
			kept['a'], kept['b'])
	}
	if shed["durable"] != 140 || shed["lossy"] != 100 {
		t.Fatalf("visited %v, want 140 durable and 100 lossy copies shed", shed)
	}
}
