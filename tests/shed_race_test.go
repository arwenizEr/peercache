package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// TestShedCountersRacingTheirWritersLoseNoDecrement: a key the budget sheds
// must be, at every instant, either in memory or marked for reading back. Two
// windows broke that with storage reachable: the shed removed the key and
// marked it only after releasing the shard (the mark came in the visit
// callback), and the read-back cleared the mark before installing what it had
// read. An Increment landing in either found the key absent and unmarked and
// counted from zero, with a write that outranks the stored value — open
// question 61's loss without any outage.
func TestShedCountersRacingTheirWritersLoseNoDecrement(t *testing.T) {
	const (
		budget   = 128 << 10
		counters = 8
		workers  = 4
		perWork  = 1500
	)
	c, _ := newCache(t, peercache.WithMemoryBudget(budget))
	stock := c.Namespace("stock", peercache.WithPersistence(persist.Memory()))
	bulk := c.Namespace("bulk")
	ctx := context.Background()

	var acked [counters]atomic.Int64
	var unknown [counters]atomic.Int64
	stop := make(chan struct{})
	var filler sync.WaitGroup
	filler.Add(1)
	go func() {
		defer filler.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = bulk.Set(ctx, fmt.Sprintf("b%d", i%4096), make([]byte, 1<<10), 0)
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWork; i++ {
				k := (w + i) % counters
				if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", k), -1, 0); err != nil {
					unknown[k].Add(1)
				} else {
					acked[k].Add(1)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	filler.Wait()

	if c.Stats().ShedOwned == 0 {
		t.Fatal("nothing was shed, so no write raced a shed")
	}
	for k := 0; k < counters; k++ {
		key := fmt.Sprintf("item-%d", k)
		v, err := stock.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			t.Fatalf("%s holds %q: %v", key, v, err)
		}
		lo, hi := acked[k].Load(), acked[k].Load()+unknown[k].Load()
		if -n < lo || -n > hi {
			t.Fatalf("%s = %d: %d decrements applied, %d acknowledged and %d unknown; "+
				"a shed counter was counted from zero", key, n, -n, lo, unknown[k].Load())
		}
	}
}

// TestShedFreshKeysLoseNoFirstWrite is the same property for a key's first
// write. Shed before the write recorded it, a fresh key is in neither storage
// nor this node's records, so the read-back finds nothing at all; it must wait
// for the record the mark says exists rather than settle on "absent", or the
// next write counts from zero and the first is gone.
func TestShedFreshKeysLoseNoFirstWrite(t *testing.T) {
	const (
		budget  = 128 << 10
		workers = 4
		keys    = 3000
	)
	c, _ := newCache(t, peercache.WithMemoryBudget(budget))
	stock := c.Namespace("stock", peercache.WithPersistence(persist.Memory()))
	bulk := c.Namespace("bulk")
	ctx := context.Background()

	stop := make(chan struct{})
	var filler sync.WaitGroup
	filler.Add(1)
	go func() {
		defer filler.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = bulk.Set(ctx, fmt.Sprintf("b%d", i%4096), make([]byte, 1<<10), 0)
		}
	}()

	var acked, unknown [keys]atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < keys; i++ {
				if _, err := stock.Increment(ctx, fmt.Sprintf("fresh-%d", i), -1, 0); err != nil {
					unknown[i].Add(1)
				} else {
					acked[i].Add(1)
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	filler.Wait()

	if c.Stats().ShedOwned == 0 {
		t.Fatal("nothing was shed, so no first write raced a shed")
	}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("fresh-%d", i)
		v, err := stock.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			t.Fatalf("%s holds %q: %v", key, v, err)
		}
		if lo, hi := acked[i].Load(), acked[i].Load()+unknown[i].Load(); -n < lo || -n > hi {
			t.Fatalf("%s = %d: %d acknowledged and %d unknown; a first write was lost to a shed",
				key, n, lo, unknown[i].Load())
		}
	}
}

// TestShedKeysAreNeverReadAsAbsent is open question 62 on the read paths. A
// read that checked the shed mark only before reading the store could see the
// budget shed the key in between and report it absent; a loader's store could
// then lay what the loader returns over the value storage holds. Every key
// here exists, with a value the loader never returns, so "not found" and the
// loader's value are both wrong answers.
func TestShedKeysAreNeverReadAsAbsent(t *testing.T) {
	const (
		budget  = 128 << 10
		keys    = 64
		workers = 4
		reads   = 1500
	)
	c, _ := newCache(t, peercache.WithMemoryBudget(budget))
	ns := c.Namespace("profiles",
		peercache.WithPersistence(persist.Memory()),
		peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) {
			return []byte("loaded"), 0, nil
		}))
	bulk := c.Namespace("bulk")
	ctx := context.Background()
	for i := 0; i < keys; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("p-%d", i), []byte(fmt.Sprintf("real-%d", i)), 0); err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var filler sync.WaitGroup
	filler.Add(1)
	go func() {
		defer filler.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_ = bulk.Set(ctx, fmt.Sprintf("b%d", i%4096), make([]byte, 1<<10), 0)
		}
	}()

	var wrong atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < reads && wrong.Load() == nil; i++ {
				k := (w*7 + i) % keys
				key, want := fmt.Sprintf("p-%d", k), fmt.Sprintf("real-%d", k)
				var got []byte
				var err error
				if i%2 == 0 {
					got, err = ns.Get(ctx, key)
				} else {
					got, err = ns.GetOrLoad(ctx, key)
				}
				switch {
				case errors.Is(err, peercache.ErrActivationPending):
				case errors.Is(err, peercache.ErrNotFound):
					wrong.Store(fmt.Sprintf("%s read as absent", key))
				case err != nil:
					wrong.Store(fmt.Sprintf("%s: %v", key, err))
				case string(got) != want:
					wrong.Store(fmt.Sprintf("%s = %q, want %q", key, got, want))
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	filler.Wait()

	if c.Stats().ShedOwned == 0 {
		t.Fatal("nothing was shed, so no read raced a shed")
	}
	if w := wrong.Load(); w != nil {
		t.Fatalf("a shed key was answered wrongly: %v", w)
	}
	for i := 0; i < keys; i++ {
		key, want := fmt.Sprintf("p-%d", i), fmt.Sprintf("real-%d", i)
		if got, err := ns.Get(ctx, key); err != nil || string(got) != want {
			t.Fatalf("after the run %s = %q, %v; want %q", key, got, err, want)
		}
	}
}
