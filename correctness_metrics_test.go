package peercache_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/arwenizEr/peercache"
)

// SPEC.md's M7 bullet names five correctness metrics by name: split-brain
// detections, stale-epoch activation rejections, version conflicts, mailbox
// rejections, and transfer failures. Three had counters already. The two
// checked here did not — they were reachable conditions that nothing counted,
// which is the same as not being observable at all.
//
// Transfer failures are covered by ReplicationsFailed: the drain handoff reuses
// the replication path rather than inventing a transfer protocol, so a failed
// transfer is a failed replication and counting it twice would only let the two
// numbers disagree.

// TestMailboxRejectionsAreCounted saturates a group and checks the refusals
// reach both Stats and the metrics output.
//
// A rejection is not a fault — it is backpressure working, and CLAUDE.md is
// explicit that a full mailbox rejects rather than blocks. But it is the first
// observable sign that a group is saturated, and it arrives before the latency
// does. An operator who cannot see it learns about the problem from a timeout.
func TestMailboxRejectionsAreCounted(t *testing.T) {
	cache, err := peercache.New(
		peercache.WithNodeID("mailbox"),
		// One slot, so the second concurrent call is refused rather than
		// queued. The default of 64 would need 65 goroutines in flight to
		// prove the same thing.
		peercache.WithGroupMailboxSize(1),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cache.Close(context.Background())

	// A function that blocks until the test releases it, so the mailbox
	// stays occupied for as long as the assertion needs and not a moment
	// longer. No sleeping, and no dependence on how fast a turn runs.
	release := make(chan struct{})
	var releaseOnce sync.Once
	if err := cache.RegisterGroupFunc("block",
		func(ctx context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return nil, nil
		}); err != nil {
		t.Fatalf("RegisterGroupFunc: %v", err)
	}
	// Released on every exit path, so a failed assertion cannot leave the
	// callers parked forever.
	defer releaseOnce.Do(func() { close(release) })

	group := cache.Group("saturated")

	const callers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	var refused int
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := group.Call(context.Background(), "block", nil)
			if errors.Is(err, peercache.ErrGroupOverloaded) {
				mu.Lock()
				refused++
				mu.Unlock()
			}
		}()
	}

	// Wait for the counter to move rather than for the goroutines: they
	// cannot all finish until the function is released, and releasing it
	// before the mailbox filled would be a test that proves nothing.
	const maxYields = 5_000_000
	var rejections int64
	for i := 0; ; i++ {
		rejections = cache.Stats().MailboxRejections
		if rejections > 0 {
			break
		}
		if i > maxYields {
			t.Fatalf("%d concurrent calls against a 1-slot mailbox produced no "+
				"rejections; either the mailbox is not bounded or the counter is "+
				"not wired to Stats", callers)
		}
		runtime.Gosched()
	}

	releaseOnce.Do(func() { close(release) })
	wg.Wait()

	if refused == 0 {
		t.Error("no caller saw ErrGroupOverloaded, but the counter moved; the " +
			"counter and the error are reporting different events")
	}

	// And it has to be scrapeable, not merely present in Stats. A counter
	// nobody can alert on is a counter nobody has.
	var sb strings.Builder
	if err := cache.WriteMetrics(&sb); err != nil {
		t.Fatalf("WriteMetrics: %v", err)
	}
	if !strings.Contains(sb.String(), "peercache_mailbox_rejections_total ") {
		t.Fatal("peercache_mailbox_rejections_total is missing from the metrics output")
	}
	for _, line := range strings.Split(sb.String(), "\n") {
		if strings.HasPrefix(line, "peercache_mailbox_rejections_total ") {
			if strings.HasSuffix(line, " 0") {
				t.Errorf("Stats reports %d rejections but the metric reports 0; "+
					"the series is wired to the wrong field", rejections)
			}
		}
	}
}
