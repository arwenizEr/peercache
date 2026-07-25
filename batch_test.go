package peercache_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/peercache/peercache"
)

// countingContext reports success for the first n calls to Err and failure
// afterwards.
//
// The batch loops check the context every 64 keys rather than every key, to
// keep the check off the hot path while still bounding how far past a deadline
// a large batch can run. That arithmetic is easy to get wrong — off by one, or
// checking only at the start — and a real deadline cannot test it, because when
// it expires mid-loop is a matter of scheduling. Making expiry a function of
// call count makes it exact.
type countingContext struct {
	context.Context
	calls  int
	failAt int
}

func (c *countingContext) Err() error {
	c.calls++
	if c.calls > c.failAt {
		return context.DeadlineExceeded
	}
	return nil
}

// TestBatchGetAbortsMidwayAndReturnsWhatItHas covers the partial-result
// contract. A batch that runs out of time must hand back the values it already
// gathered, not throw them away.
func TestBatchGetAbortsMidwayAndReturnsWhatItHas(t *testing.T) {
	const keys = 300

	c, _ := newCache(t)
	ns := c.Namespace("batch")
	ctx := context.Background()

	all := make([]string, keys)
	for i := range all {
		all[i] = key(i)
		if err := ns.Set(ctx, all[i], []byte(strconv.Itoa(i)), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	// Survive the entry check plus the checks at key 0 and key 64, then fail
	// at key 128.
	cc := &countingContext{Context: ctx, failAt: 3}
	got, err := ns.BatchGet(cc, all)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("BatchGet = %v, want DeadlineExceeded", err)
	}
	if len(got) == 0 {
		t.Fatal("BatchGet discarded everything it had already read")
	}
	if len(got) >= keys {
		t.Fatalf("BatchGet returned all %d keys; it did not abort", len(got))
	}
	// It checked at 0 and 64 and gave up at 128, so it should hold exactly
	// the first 128 keys.
	if len(got) != 128 {
		t.Fatalf("BatchGet returned %d values, want the 128 read before the check failed", len(got))
	}
	for k, v := range got {
		if v == nil {
			t.Fatalf("key %q came back with a nil value", k)
		}
	}
}

// TestBatchSetAbortsMidwayAndPartialWritesAreVisible pins the other half. There
// is no cross-key atomicity and no rollback, so a caller that gives up midway
// must expect some keys written and some not.
func TestBatchSetAbortsMidwayAndPartialWritesAreVisible(t *testing.T) {
	const keys = 300

	c, _ := newCache(t)
	ns := c.Namespace("batch")
	ctx := context.Background()

	kv := make(map[string][]byte, keys)
	for i := 0; i < keys; i++ {
		kv[key(i)] = []byte(strconv.Itoa(i))
	}

	cc := &countingContext{Context: ctx, failAt: 2}
	if err := ns.BatchSet(cc, kv, time.Minute); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("BatchSet = %v, want DeadlineExceeded", err)
	}

	written := 0
	for k := range kv {
		if ok, err := ns.Exists(ctx, k); err != nil {
			t.Fatalf("Exists: %v", err)
		} else if ok {
			written++
		}
	}
	if written == 0 {
		t.Fatal("BatchSet aborted before writing anything; the check is too eager")
	}
	if written == keys {
		t.Fatal("BatchSet wrote every key; it did not abort")
	}
	t.Logf("partial application is visible and expected: %d of %d keys written", written, keys)
}

// TestBatchOperationsCheckTheContextAtAll is the degenerate case the periodic
// check must still handle: a context already expired on entry does no work.
func TestBatchOperationsCheckTheContextAtAll(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("batch")
	ctx := context.Background()

	if err := ns.Set(ctx, key(0), []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	expired, cancel := context.WithCancel(ctx)
	cancel()

	if _, err := ns.BatchGet(expired, []string{key(0)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("BatchGet = %v, want Canceled", err)
	}
	if err := ns.BatchSet(expired, map[string][]byte{"never": []byte("v")}, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("BatchSet = %v, want Canceled", err)
	}
	if ok, _ := ns.Exists(ctx, "never"); ok {
		t.Fatal("BatchSet wrote a key despite an expired context")
	}
}

// TestBrokenNamespaceDoesNotBreakTheCache puts a namespace that failed
// construction alongside a working one.
//
// A namespace with invalid options has no store at all. The sweeper walks every
// namespace on every tick, Stats aggregates across them, and Close shuts them
// down — three loops that would each dereference nil if the guard were missing,
// and none of which any other test exercises with a broken namespace present.
func TestBrokenNamespaceDoesNotBreakTheCache(t *testing.T) {
	c, fc := newCache(t)
	ctx := context.Background()

	broken := c.Namespace("broken", peercache.WithReplicationFactor(99))
	healthy := c.Namespace("healthy", peercache.WithShards(4))

	if _, err := broken.Get(ctx, "k"); !errors.Is(err, peercache.ErrInvalidOption) {
		t.Fatalf("the broken namespace = %v, want ErrInvalidOption", err)
	}
	if err := healthy.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Set on the healthy namespace: %v", err)
	}

	// Stats must aggregate across both without tripping over the one that
	// has no store.
	if got := c.Stats().OwnedKeys; got != 1 {
		t.Fatalf("Stats().OwnedKeys = %d, want 1", got)
	}

	// Drive the sweeper with the broken namespace registered.
	fc.Advance(2 * time.Minute)
	for c.Stats().OwnedKeys != 0 {
		gosched()
		fc.Advance(time.Second)
	}

	// And Close, which is the third loop.
	if err := c.Close(ctx); err != nil {
		t.Fatalf("Close with a broken namespace registered: %v", err)
	}
	if _, err := broken.Get(ctx, "k"); !errors.Is(err, peercache.ErrClosed) {
		t.Fatalf("after Close the broken namespace = %v, want ErrClosed", err)
	}
}
