package actor_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/actor"
	"github.com/arwenizEr/peercache/internal/core"
)

// M2 memory acceptance parameters, from SPEC.md section 4.
const (
	addressableGroups = 1_000_000
	activeGroups      = 20_000
)

// heapMiB returns the live heap after a collection.
func heapMiB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

// TestMemoryAtOneMillionAddressableGroups measures the M2 memory criterion:
// a million addressable groups with twenty thousand active.
//
// SPEC.md says the budget must be "measured and recorded", and does not say what
// it is. So this reports rather than asserting an invented threshold — except
// for one bound that *is* meaningful: an addressable group must cost far less
// than an active one, because that distinction is the entire reason records and
// activations are separate types. If they ever cost the same, the design has
// silently collapsed and a million idle groups would each hold a goroutine.
func TestMemoryAtOneMillionAddressableGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates on the order of a gigabyte; skipped under -short")
	}
	if raceEnabled {
		// The race detector allocates shadow memory for every access, so
		// a heap measurement under it describes the detector rather than
		// the runtime — and the run takes about a minute for a number
		// that means nothing.
		t.Skip("memory measurements are meaningless under the race detector")
	}

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MaxActive = activeGroups
		// Long, so that groups stay active for the measurement rather than
		// deactivating underneath it.
		o.IdleTimeout = time.Hour
		o.MailboxSize = 1
	})
	if err := rt.Register("touch", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	baseline := heapMiB()
	ctx := context.Background()

	// Phase one: make a million groups addressable. Each touch activates the
	// group, and the cap evicts all but the most recent, so what remains is
	// a million records and at most activeGroups goroutines.
	for i := 0; i < addressableGroups; i++ {
		if _, err := rt.Call(ctx, fmt.Sprintf("g%d", i), "touch", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}

	stats := rt.Stats()
	if stats.Addressable != addressableGroups {
		t.Fatalf("Addressable = %d, want %d", stats.Addressable, addressableGroups)
	}

	afterAddressable := heapMiB()
	perRecord := (afterAddressable - baseline) * (1 << 20) / addressableGroups

	t.Logf("baseline heap:                    %8.1f MiB", baseline)
	t.Logf("%d addressable, %d active: %8.1f MiB (%.0f B per addressable group)",
		addressableGroups, stats.Active, afterAddressable, perRecord)
	t.Logf("goroutines: %d", runtime.NumGoroutine())

	// The bound that matters. A goroutine's initial stack alone is several
	// kilobytes, so if an addressable group approached that, records and
	// activations would not really be separate.
	const perRecordCeiling = 1024
	if perRecord > perRecordCeiling {
		t.Errorf("an addressable group costs %.0f B, above the %d B ceiling; "+
			"records and activations are no longer meaningfully separate",
			perRecord, perRecordCeiling)
	}

	// The cap held throughout, so the active set never grew towards a million
	// goroutines.
	//
	// It is asserted as a settled bound rather than an instantaneous one.
	// Eviction is signalled after the call that tripped the cap is already
	// enqueued, and a victim exits when its current turn ends — making the
	// caller wait for someone else's turn would couple unrelated latencies.
	// So the count can sit a little above the cap for a moment, and demanding
	// otherwise would be asserting an implementation this deliberately does
	// not have.
	waitFor(t, "the active count to settle within the cap", func() bool {
		return rt.Stats().Active <= activeGroups
	})
	t.Logf("evictions: %d, settled active: %d", stats.Evictions, rt.Stats().Active)
}

// TestMemoryOfActiveGroups isolates what an activation costs on top of a
// record, which is the number that decides MaxActive.
func TestMemoryOfActiveGroups(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a few hundred megabytes; skipped under -short")
	}
	if raceEnabled {
		t.Skip("memory measurements are meaningless under the race detector")
	}

	rt, _ := newRuntime(t, func(o *actor.Options) {
		o.MaxActive = -1 // unlimited: measuring activations is the point
		o.IdleTimeout = time.Hour
		o.MailboxSize = 1
	})
	if err := rt.Register("touch", func(context.Context, core.GroupState, []byte) ([]byte, error) {
		return nil, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	baseline := heapMiB()
	baseGoroutines := runtime.NumGoroutine()
	ctx := context.Background()

	for i := 0; i < activeGroups; i++ {
		if _, err := rt.Call(ctx, fmt.Sprintf("g%d", i), "touch", nil); err != nil {
			t.Fatalf("Call: %v", err)
		}
	}

	stats := rt.Stats()
	if stats.Active != activeGroups {
		t.Fatalf("Active = %d, want %d", stats.Active, activeGroups)
	}

	after := heapMiB()
	perActive := (after - baseline) * (1 << 20) / activeGroups

	t.Logf("%d active groups: %.1f MiB heap (%.0f B each), %d goroutines",
		activeGroups, after-baseline, perActive, runtime.NumGoroutine()-baseGoroutines)
	t.Logf("note: goroutine stacks are not counted in HeapAlloc; at 8 KiB initial stack "+
		"that is roughly %.0f MiB more", float64(activeGroups)*8/1024)
}
