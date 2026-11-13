package hlc

import (
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// logicalBits is the width of the counter below the millisecond.
const logicalBits = 16

// MaxDrift is how far ahead of this node's wall clock an observed order may be
// and still be adopted. A minute is far beyond NTP's error on any healthy node
// and far short of the hours a misconfigured one can be off.
const MaxDrift = time.Minute

// Order is a hybrid logical timestamp. Larger is later.
type Order uint64

// FromMillis returns the first Order of a wall-clock millisecond.
func FromMillis(ms int64) Order {
	if ms < 0 {
		ms = 0
	}
	return Order(uint64(ms) << logicalBits)
}

// Millis returns the physical part, in milliseconds since the Unix epoch.
func (o Order) Millis() int64 { return int64(uint64(o) >> logicalBits) }

// Logical returns the counter within the millisecond.
func (o Order) Logical() uint16 { return uint16(uint64(o) & (1<<logicalBits - 1)) }

// Clock issues Orders. It is safe for concurrent use.
type Clock struct {
	wall    clock.Clock
	last    atomic.Uint64
	refused atomic.Int64
}

// New returns a Clock reading wall.
func New(wall clock.Clock) *Clock {
	return &Clock{wall: wall}
}

// Next returns an Order later than every Order this clock has issued or
// accepted from Observe: the current millisecond if that is later, and
// otherwise the last one plus one. A counter that overflows its sixteen bits
// carries into the millisecond, which keeps orders unique at the cost of
// running briefly ahead of the wall clock.
func (c *Clock) Next() Order {
	now := uint64(FromMillis(c.wall.Now().UnixMilli()))
	for {
		last := c.last.Load()
		next := now
		if next <= last {
			next = last + 1
		}
		if c.last.CompareAndSwap(last, next) {
			return Order(next)
		}
	}
}

// Observe advances the clock past o, so that the next Order outranks it, and
// reports whether it did. An o more than MaxDrift ahead of the wall clock is
// refused and counted; an o this clock is already past is accepted and changes
// nothing.
func (c *Clock) Observe(o Order) bool {
	limit := uint64(FromMillis(c.wall.Now().Add(MaxDrift).UnixMilli()))
	if uint64(o) > limit {
		c.refused.Add(1)
		return false
	}
	for {
		last := c.last.Load()
		if uint64(o) <= last || c.last.CompareAndSwap(last, uint64(o)) {
			return true
		}
	}
}

// Refused returns how many observed orders were beyond MaxDrift.
func (c *Clock) Refused() int64 { return c.refused.Load() }
