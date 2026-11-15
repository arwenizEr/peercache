package hotkeys_test

import (
	"fmt"
	"testing"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/hotkeys"
)

// TestAKeyThatTurnsHotLateIsFound is Space-Saving's guarantee: the counters
// are full of keys counted earlier, and then one key takes two thirds of the
// traffic while the rest is a stream of keys seen once. A newcomer inherits
// the count of the counter it replaces, so the hot key climbs past the old
// ones; starting newcomers at one would let the one-off keys evict it every
// time and it would never be reported.
func TestAKeyThatTurnsHotLateIsFound(t *testing.T) {
	tr := hotkeys.New(clock.NewFake(clock.Epoch), 3) // 12 counters
	for i := 0; i < 12; i++ {
		for j := 0; j < 10*hotkeys.SampleEvery; j++ {
			tr.Seen(fmt.Sprintf("old:%d", i))
		}
	}
	// Period 3 is coprime with the sample rate, so samples hit the hot
	// key two times in three.
	for i := 0; i < 3*400*hotkeys.SampleEvery/2; i++ {
		if i%3 == 2 {
			tr.Seen(fmt.Sprintf("once:%d", i))
		} else {
			tr.Seen("hot")
		}
	}
	top := tr.Top()
	if len(top) == 0 || top[0].Key != "hot" {
		t.Fatalf("top %+v; want the late hot key first", top)
	}
}
