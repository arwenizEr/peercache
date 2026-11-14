package hotkeys

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// SampleEvery is how many accesses one sample stands for. Sampling keeps the
// mutex off all but one access in this many.
const SampleEvery = 16

// Window is how often every count is halved.
const Window = time.Minute

// Key is one tracked key and its estimated accesses, already scaled by
// SampleEvery.
type Key struct {
	Key      string
	Estimate uint64
}

// Tracker counts accesses to keys. It is safe for concurrent use.
type Tracker struct {
	clk      clock.Clock
	top      int
	capacity int
	tick     atomic.Uint64

	mu      sync.Mutex
	counts  map[string]uint64
	decayed time.Time
}

// New tracks enough keys to report the top n reliably: four counters per
// reported key, so a key in the true top n is not crowded out by noise.
func New(clk clock.Clock, n int) *Tracker {
	return &Tracker{clk: clk, top: n, capacity: 4 * n, counts: make(map[string]uint64, 4*n), decayed: clk.Now()}
}

// Seen records one access to key. One call in SampleEvery takes the lock.
func (t *Tracker) Seen(key string) {
	if t.tick.Add(1)%SampleEvery != 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.decayLocked()
	if c, ok := t.counts[key]; ok {
		t.counts[key] = c + 1
		return
	}
	if len(t.counts) < t.capacity {
		t.counts[key] = 1
		return
	}
	// Replace the smallest, inheriting its count: Space-Saving's bound.
	var minKey string
	var minCount uint64
	first := true
	for k, c := range t.counts {
		if first || c < minCount {
			minKey, minCount, first = k, c, false
		}
	}
	delete(t.counts, minKey)
	t.counts[key] = minCount + 1
}

// Top returns the n keys with the highest estimates, highest first.
func (t *Tracker) Top() []Key {
	t.mu.Lock()
	t.decayLocked()
	out := make([]Key, 0, len(t.counts))
	for k, c := range t.counts {
		out = append(out, Key{Key: k, Estimate: c * SampleEvery})
	}
	t.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Estimate != out[j].Estimate {
			return out[i].Estimate > out[j].Estimate
		}
		return out[i].Key < out[j].Key
	})
	if len(out) > t.top {
		out = out[:t.top]
	}
	return out
}

// decayLocked halves every count once per elapsed Window, dropping keys that
// reach zero.
func (t *Tracker) decayLocked() {
	now := t.clk.Now()
	for now.Sub(t.decayed) >= Window {
		t.decayed = t.decayed.Add(Window)
		for k, c := range t.counts {
			if c /= 2; c == 0 {
				delete(t.counts, k)
			} else {
				t.counts[k] = c
			}
		}
		if len(t.counts) == 0 {
			t.decayed = now
			return
		}
	}
}
