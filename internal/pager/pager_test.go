package pager_test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/pager"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("k%05d", (i*7919)%n) // unsorted on purpose
	}
	return out
}

// pass pages through one whole pass, returning every key and how many times
// build ran.
func pass(p *pager.Pager, id string, all []string, limit int) ([]string, int) {
	builds := 0
	build := func() []string { builds++; return append([]string(nil), all...) }
	var got []string
	after := ""
	for {
		page, more := p.Page(id, after, limit, build)
		got = append(got, page...)
		if len(page) > 0 {
			after = page[len(page)-1]
		}
		if !more || len(page) == 0 {
			return got, builds
		}
	}
}

// TestAPassSortsOnceAndCoversEveryKey: a full pass returns every key once, in
// order, builds the list once, and drops it at the end.
func TestAPassSortsOnceAndCoversEveryKey(t *testing.T) {
	p := pager.New(clock.NewFake(clock.Epoch), 0, 0)
	all := keys(2000)
	got, builds := pass(p, "a", all, 64)
	want := append([]string(nil), all...)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("a pass returned %d keys, not the %d in order", len(got), len(want))
	}
	if builds != 1 {
		t.Fatalf("the key list was built %d times in one pass, want once", builds)
	}
	if p.Passes() != 0 {
		t.Fatalf("%d passes still held after the last page", p.Passes())
	}
}

// TestALostPassIsRebuiltFromItsCursor: a pass whose list was dropped — idle,
// or evicted — continues correctly from its cursor with a new list.
func TestALostPassIsRebuiltFromItsCursor(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	p := pager.New(fc, time.Minute, 2)
	all := keys(300)
	build := func() []string { return append([]string(nil), all...) }
	first, _ := p.Page("a", "", 100, build)
	fc.Advance(time.Minute)
	p.Page("b", "", 10, build) // a new pass evicts the idle one
	if p.Passes() != 1 {
		t.Fatalf("%d passes held, want only b", p.Passes())
	}
	next, more := p.Page("a", first[len(first)-1], 100, build)
	if !more || len(next) != 100 || next[0] <= first[len(first)-1] {
		t.Fatalf("a rebuilt pass resumed at %q (more=%v), after %q", next[0], more, first[len(first)-1])
	}
}

// TestPassesAreBounded: no more than max passes hold a list at once.
func TestPassesAreBounded(t *testing.T) {
	p := pager.New(clock.NewFake(clock.Epoch), 0, 3)
	build := func() []string { return keys(100) }
	for i := 0; i < 10; i++ {
		p.Page(fmt.Sprintf("p%d", i), "", 10, build)
	}
	if got := p.Passes(); got > 3 {
		t.Fatalf("%d passes held, bound is 3", got)
	}
}

// TestBytesCountsKeptListsAndDropGivesThemBack: a pass in progress is charged
// for its list until its last page; Drop frees every list, and a pass resumed
// after it rebuilds its list and still covers every key once.
func TestBytesCountsKeptListsAndDropGivesThemBack(t *testing.T) {
	p := pager.New(clock.NewFake(clock.Epoch), 0, 0)
	all := keys(1000)
	build := func() []string { return append([]string(nil), all...) }

	first, more := p.Page("a", "", 100, build)
	if !more || len(first) != 100 {
		t.Fatalf("first page: %d keys, more=%v", len(first), more)
	}
	p.Page("b", "", 100, build)
	if got, want := p.Bytes(), int64(2*1000*16); got < want {
		t.Fatalf("two passes of 1,000 keys charged %d bytes, want at least %d", got, want)
	}

	if freed := p.Drop(); freed == 0 || p.Bytes() != 0 || p.Passes() != 0 {
		t.Fatalf("Drop freed %d, left %d bytes in %d passes", freed, p.Bytes(), p.Passes())
	}

	got := append([]string(nil), first...)
	after := first[len(first)-1]
	builds := 0
	for {
		page, more := p.Page("a", after, 100, func() []string { builds++; return build() })
		got = append(got, page...)
		if !more {
			break
		}
		after = page[len(page)-1]
	}
	if builds != 1 {
		t.Fatalf("the resumed pass built its list %d times, want once", builds)
	}
	want := append([]string(nil), all...)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("the pass resumed after Drop returned %d keys, want the %d, each once", len(got), len(want))
	}
	if p.Bytes() != 0 {
		t.Fatalf("a finished pass is still charged %d bytes", p.Bytes())
	}
}
