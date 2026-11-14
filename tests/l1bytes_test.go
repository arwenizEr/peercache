package tests

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/l1"
)

// TestTheL1TierIsBoundedInBytes reads two hundred 1 KiB keys owned elsewhere
// through a namespace whose L1 may hold 32 KiB, and checks the reader keeps no
// more copies than fit, while a namespace on the default bound keeps them all.
// The tier used to be bounded at 100,000 entries whatever their size.
func TestTheL1TierIsBoundedInBytes(t *testing.T) {
	f := newFabric(t, 2)
	ctx := context.Background()
	const maxBytes = 32 << 10
	val := make([]byte, 1024)

	reader := f.ids[0]
	var keys []string
	for i := 0; len(keys) < 200; i++ {
		k := fmt.Sprintf("big/%d", i)
		if f.ownerOf(t, k) != reader {
			keys = append(keys, k)
		}
	}
	for _, name := range []string{"bounded", "default"} {
		for _, k := range keys {
			if err := f.caches[f.ids[1]].Namespace(name).Set(ctx, k, val, 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
	}

	bounded := f.caches[reader].Namespace("bounded", peercache.WithL1MaxBytes(maxBytes))
	for _, k := range keys {
		if _, err := bounded.Get(ctx, k); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}
	// Each copy is charged its key, its value and the fixed overhead.
	fit := int64(maxBytes) / (int64(len(keys[0])+len(val)) + l1.EntryOverhead)
	// Eviction is asynchronous in the underlying library; yield until it has
	// caught up rather than asserting on a transient overshoot.
	for i := 0; i < 1_000_000 && f.caches[reader].Stats().L1Entries > fit; i++ {
		runtime.Gosched()
	}
	if got := f.caches[reader].Stats().L1Entries; got > fit || got == 0 {
		t.Fatalf("the reader holds %d copies of 1 KiB values under a 32 KiB bound; want 1..%d", got, fit)
	}
	var described int64 = -1
	for _, d := range f.caches[reader].Describe().Namespaces {
		if d.Name == "bounded" {
			described = d.L1MaxBytes
		}
	}
	if described != maxBytes {
		t.Fatalf("Describe reports L1MaxBytes %d, want %d", described, maxBytes)
	}

	before := f.caches[reader].Stats().L1Entries
	def := f.caches[reader].Namespace("default")
	for _, k := range keys {
		if _, err := def.Get(ctx, k); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}
	if got := f.caches[reader].Stats().L1Entries - before; got != int64(len(keys)) {
		t.Fatalf("the default bound kept %d of %d copies, want all of them", got, len(keys))
	}
}

// TestAnL1BoundMustBePositive checks that a namespace asking for no L1 memory
// at all is refused rather than given a tier that caches nothing; disabling
// the tier is WithL1TTL's job.
func TestAnL1BoundMustBePositive(t *testing.T) {
	f := newFabric(t, 2)
	for _, n := range []int64{0, -1} {
		ns := f.caches[f.ids[0]].Namespace(fmt.Sprintf("zero%d", n), peercache.WithL1MaxBytes(n))
		if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
			t.Fatalf("WithL1MaxBytes(%d): Get = %v, want ErrInvalidOption", n, err)
		}
	}
}
