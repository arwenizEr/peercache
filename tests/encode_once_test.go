package tests

import (
	"bytes"
	"context"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/codec"
	"github.com/arwenizEr/peercache/persist"
)

// TestAWriteIsCompressedOnce pins that a Set frames its value once.
//
// The stored form is framed already, and it is also what persists and what
// replicas are sent. Until November 2026 a Set framed its value three times —
// for the store, the persister and the fan-out — so with compression on two
// thirds of every write's compression was wasted.
//
// Compression cannot be counted directly: a compressor without a frame code
// cannot frame, on purpose (internal/codec/frame.go), so a counting one cannot
// be registered. Allocations can. The same persistent Set, with and without
// compression, must differ by exactly what one Frame of the value allocates.
func TestAWriteIsCompressedOnce(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts are not meaningful under the race detector")
	}
	c, _ := newCache(t)
	ctx := context.Background()
	val := bytes.Repeat([]byte("compressible "), 80)

	framer, err := codec.NewFramer(codec.NameLZ4, 1)
	if err != nil {
		t.Fatalf("NewFramer: %v", err)
	}
	if len(framer.Frame(val)) >= len(val) {
		t.Fatalf("the value did not compress; the test would measure the raw path")
	}
	oneFrame := testing.AllocsPerRun(200, func() { _ = framer.Frame(val) })
	if oneFrame == 0 {
		t.Fatalf("a Frame allocated nothing; the comparison below would be vacuous")
	}

	// Persistent, so the persister records each write too: it framed the
	// value again for itself.
	plain := c.Namespace("once-plain", peercache.WithPersistence(persist.Memory()))
	packed := c.Namespace("once-packed",
		peercache.WithCompression(codec.NameLZ4, 1),
		peercache.WithPersistence(persist.Memory()))
	set := func(ns peercache.Namespace) func() {
		return func() {
			if err := ns.Set(ctx, "k", val, 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}
	// One of each first, so map growth and first-write work are not counted.
	set(plain)()
	set(packed)()

	extra := testing.AllocsPerRun(200, set(packed)) - testing.AllocsPerRun(200, set(plain))
	if extra != oneFrame {
		t.Fatalf("compression added %.1f allocations to a Set, and one Frame is %.1f: "+
			"the value is framed %.1f times, want once", extra, oneFrame, extra/oneFrame)
	}

	got, err := packed.Get(ctx, "k")
	if err != nil || !bytes.Equal(got, val) {
		t.Fatalf("Get = %d bytes, %v; want the %d written", len(got), err, len(val))
	}
}
