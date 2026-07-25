package clock_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
)

// TestWithTimeoutExpiresOnFakeClock is the reason this helper exists: a context
// deadline has to be as controllable as every other timer, or a test that wants
// to observe a timeout has to wait for one in real time.
func TestWithTimeoutExpiresOnFakeClock(t *testing.T) {
	fc := newFake(t)
	ctx, cancel := clock.WithTimeout(context.Background(), fc, 30*time.Second)
	defer cancel()

	if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(clock.Epoch.Add(30*time.Second)) {
		t.Fatalf("Deadline() = %v, %v", deadline, ok)
	}
	select {
	case <-ctx.Done():
		t.Fatal("context done before its deadline")
	default:
	}

	fc.Advance(30 * time.Second)
	<-ctx.Done()
	if err := ctx.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Err() = %v, want DeadlineExceeded", err)
	}
}

func TestWithTimeoutCancelReleasesTimer(t *testing.T) {
	fc := newFake(t)
	_, cancel := clock.WithTimeout(context.Background(), fc, time.Minute)
	if got := fc.Pending(); got != 1 {
		t.Fatalf("Pending() = %d before cancel, want 1", got)
	}
	cancel()
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after cancel, want 0: the deadline timer leaked", got)
	}
}

func TestWithTimeoutCancelIsIdempotent(t *testing.T) {
	fc := newFake(t)
	ctx, cancel := clock.WithTimeout(context.Background(), fc, time.Minute)
	cancel()
	cancel()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v, want Canceled", err)
	}
}

func TestWithTimeoutParentCancellationPropagates(t *testing.T) {
	fc := newFake(t)
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := clock.WithTimeout(parent, fc, time.Hour)
	defer cancel()

	cancelParent()
	<-ctx.Done()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v, want Canceled", err)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d after parent cancel, want 0", got)
	}
}

func TestWithTimeoutAlreadyDoneParent(t *testing.T) {
	fc := newFake(t)
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ctx, cancel := clock.WithTimeout(parent, fc, time.Hour)
	defer cancel()
	<-ctx.Done()
	if err := ctx.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err() = %v, want Canceled", err)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0: the timer was armed for an already-dead parent", got)
	}
}

func TestWithDeadlineInThePastIsImmediate(t *testing.T) {
	fc := newFake(t)
	ctx, cancel := clock.WithDeadline(context.Background(), fc, clock.Epoch.Add(-time.Second))
	defer cancel()

	<-ctx.Done()
	if err := ctx.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Err() = %v, want DeadlineExceeded", err)
	}
	if got := fc.Pending(); got != 0 {
		t.Fatalf("Pending() = %d, want 0", got)
	}
}

// TestWithDeadlineEarlierParentWins mirrors context.WithDeadline: a child
// cannot extend its parent's deadline.
func TestWithDeadlineEarlierParentWins(t *testing.T) {
	fc := newFake(t)
	parent, cancelParent := clock.WithDeadline(context.Background(), fc, clock.Epoch.Add(time.Minute))
	defer cancelParent()

	child, cancelChild := clock.WithDeadline(parent, fc, clock.Epoch.Add(time.Hour))
	defer cancelChild()

	deadline, ok := child.Deadline()
	if !ok || !deadline.Equal(clock.Epoch.Add(time.Minute)) {
		t.Fatalf("child Deadline() = %v, %v; want the parent's %v", deadline, ok, clock.Epoch.Add(time.Minute))
	}

	fc.Advance(time.Minute)
	<-child.Done()
	if err := child.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("child Err() = %v, want DeadlineExceeded", err)
	}
}

func TestWithTimeoutCarriesValues(t *testing.T) {
	type key struct{}
	fc := newFake(t)
	parent := context.WithValue(context.Background(), key{}, "kept")
	ctx, cancel := clock.WithTimeout(parent, fc, time.Minute)
	defer cancel()

	if got := ctx.Value(key{}); got != "kept" {
		t.Fatalf("Value() = %v, want \"kept\"", got)
	}
}

// TestWithTimeoutErrIsNeverNilWhenDone guards the context contract: a closed
// Done channel with a nil Err would break every caller that checks Err after
// selecting on Done.
func TestWithTimeoutErrIsNeverNilWhenDone(t *testing.T) {
	fc := newFake(t)
	ctx, cancel := clock.WithTimeout(context.Background(), fc, time.Minute)
	cancel()
	<-ctx.Done()
	if ctx.Err() == nil {
		t.Fatal("Done is closed but Err() is nil")
	}
}
