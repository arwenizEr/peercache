package hlc_test

import (
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/hlc"
)

func newClock(t *testing.T) (*hlc.Clock, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })
	return hlc.New(fc), fc
}

func TestNextIsStrictlyIncreasingOnAStoppedClock(t *testing.T) {
	c, _ := newClock(t)
	prev := c.Next()
	for i := 0; i < 100_000; i++ {
		next := c.Next()
		if next <= prev {
			t.Fatalf("Next %d = %d after %d; an order must never repeat or go back", i, next, prev)
		}
		prev = next
	}
}

func TestNextFollowsTheWallClock(t *testing.T) {
	c, fc := newClock(t)
	first := c.Next()
	fc.Advance(time.Second)
	second := c.Next()
	if got := second.Millis() - first.Millis(); got != 1000 {
		t.Fatalf("one second of wall clock moved the physical part by %d ms", got)
	}
	if second.Logical() != 0 {
		t.Fatalf("logical part %d after the wall clock moved on, want 0", second.Logical())
	}
}

// TestAnObservedOrderIsOutranked is the property persistence depends on: a node
// that has seen another node's write must order its own next write after it,
// whatever its own wall clock says.
func TestAnObservedOrderIsOutranked(t *testing.T) {
	a, fa := newClock(t)
	b, _ := newClock(t)
	fa.Advance(5 * time.Second) // a's clock runs ahead of b's
	seen := a.Next()

	if !b.Observe(seen) {
		t.Fatal("an order five seconds ahead was refused; it is well inside the drift bound")
	}
	if next := b.Next(); next <= seen {
		t.Fatalf("after observing %d, Next = %d; the observer must order after what it saw", seen, next)
	}
}

func TestObservingAnOlderOrderChangesNothing(t *testing.T) {
	c, fc := newClock(t)
	fc.Advance(time.Minute)
	before := c.Next()
	c.Observe(hlc.FromMillis(1))
	if next := c.Next(); next <= before {
		t.Fatalf("an older observation moved the clock back: %d after %d", next, before)
	}
}

// TestADriftingPeerCannotDragTheClockAhead: an order from a node whose clock is
// hours ahead is refused and counted, so one bad clock cannot push every node's
// orders into the future.
func TestADriftingPeerCannotDragTheClockAhead(t *testing.T) {
	c, fc := newClock(t)
	far := hlc.FromMillis(fc.Now().Add(hlc.MaxDrift + time.Second).UnixMilli())
	if c.Observe(far) {
		t.Fatal("an order beyond the drift bound was accepted")
	}
	if c.Refused() != 1 {
		t.Fatalf("Refused = %d, want 1", c.Refused())
	}
	if next := c.Next(); next >= far {
		t.Fatalf("Next = %d, at or past the refused order %d", next, far)
	}
}

func TestNextIsUniqueUnderConcurrency(t *testing.T) {
	c, _ := newClock(t)
	const workers, each = 8, 10_000
	out := make([][]hlc.Order, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				out[w] = append(out[w], c.Next())
				if i%100 == 0 {
					c.Observe(out[w][len(out[w])-1])
				}
			}
		}(w)
	}
	wg.Wait()
	seen := make(map[hlc.Order]bool, workers*each)
	for _, orders := range out {
		for i, o := range orders {
			if seen[o] {
				t.Fatalf("order %d issued twice", o)
			}
			seen[o] = true
			if i > 0 && o <= orders[i-1] {
				t.Fatalf("one goroutine saw %d after %d", o, orders[i-1])
			}
		}
	}
}

func TestTheLogicalPartCarriesIntoTheMillisecond(t *testing.T) {
	c, _ := newClock(t)
	start := c.Next()
	var last hlc.Order
	for i := 0; i < 1<<17; i++ { // twice the logical range, on a stopped clock
		last = c.Next()
	}
	if last.Millis() <= start.Millis() {
		t.Fatalf("2^17 orders in one millisecond did not carry into the physical part")
	}
}
