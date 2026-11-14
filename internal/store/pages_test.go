package store_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/pager"
)

// TestAPassSkipsKeysRemovedMidPassWithoutStopping: keys removed after a pass
// began are skipped, and a page they would have emptied is skipped too — the
// requester stops at an empty page, so an empty page mid-pass would end a
// catch-up early.
func TestAPassSkipsKeysRemovedMidPassWithoutStopping(t *testing.T) {
	s, fc := newStore(t)
	for i := 0; i < 300; i++ {
		s.Put(fmt.Sprintf("k%03d", i), []byte("v"), 0)
	}
	p := pager.New(fc, 0, 0)
	page, more := s.SnapshotFrom(p, "x", "", 50, nil)
	if len(page) != 50 || !more {
		t.Fatalf("first page %d entries, more=%v", len(page), more)
	}
	// Remove the whole next page's worth, and then some.
	for i := 50; i < 130; i++ {
		s.Remove(fmt.Sprintf("k%03d", i))
	}
	after, got := page[len(page)-1].Key, len(page)
	for {
		page, more = s.SnapshotFrom(p, "x", after, 50, nil)
		if len(page) == 0 && more {
			t.Fatal("an empty page mid-pass: the requester would stop here")
		}
		got += len(page)
		if len(page) > 0 {
			after = page[len(page)-1].Key
		}
		if !more {
			break
		}
	}
	if got != 300-80 {
		t.Fatalf("the pass returned %d entries, want %d", got, 300-80)
	}
}

// TestAPassReappliesItsFilter: a key the asker stops being entitled to after
// the pass began is not sent.
func TestAPassReappliesItsFilter(t *testing.T) {
	s, fc := newStore(t)
	for i := 0; i < 100; i++ {
		s.Put(fmt.Sprintf("k%03d", i), []byte("v"), 0)
	}
	allowed := true
	want := func(key string) bool { return allowed || key < "k050" }
	p := pager.New(fc, 0, 0)
	page, _ := s.SnapshotFrom(p, "x", "", 10, want)
	allowed = false
	after := page[len(page)-1].Key
	for {
		next, more := s.SnapshotFrom(p, "x", after, 10, want)
		for _, e := range next {
			if e.Key >= "k050" {
				t.Fatalf("sent %s, which the asker stopped being entitled to", e.Key)
			}
		}
		if len(next) == 0 || !more {
			break
		}
		after = next[len(next)-1].Key
	}
}
