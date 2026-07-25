package l1_test

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/l1"
)

func newCache(t *testing.T, opts ...func(*l1.Options)) (*l1.Cache, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	o := l1.Options{Clock: fc, Capacity: 1000}
	for _, opt := range opts {
		opt(&o)
	}
	c, err := l1.New(o)
	if err != nil {
		t.Fatalf("l1.New: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c, fc
}

func TestPutGet(t *testing.T) {
	c, _ := newCache(t)

	if _, _, ok := c.Get("absent"); ok {
		t.Fatal("Get of an absent key reported present")
	}
	if !c.Put("k", []byte("v"), 1, time.Second) {
		t.Fatal("Put reported that it did not store")
	}

	got, version, ok := c.Get("k")
	if !ok {
		t.Fatal("Get after Put reported absent")
	}
	if string(got) != "v" || version != 1 {
		t.Fatalf("Get = %q v%d, want \"v\" v1", got, version)
	}
}

func TestPutCopiesTheCallersBuffer(t *testing.T) {
	c, _ := newCache(t)
	buf := []byte("original")
	c.Put("k", buf, 1, time.Second)
	copy(buf, "MUTATED!")

	got, _, _ := c.Get("k")
	if string(got) != "original" {
		t.Fatalf("Get = %q; Put did not copy the caller's buffer", got)
	}
}

// TestEveryEntryIsTTLBounded is the rule that makes L1 safe at all. There must
// be no way to store an entry that outlives the staleness bound, because an
// invalidation broadcast may never arrive.
func TestEveryEntryIsTTLBounded(t *testing.T) {
	c, fc := newCache(t, func(o *l1.Options) {
		o.MaxTTL = 10 * time.Second
		o.DefaultTTL = 2 * time.Second
	})

	tests := map[string]time.Duration{
		"zero TTL becomes the default":     0,
		"negative TTL becomes the default": -time.Hour,
		"oversized TTL is clamped":         time.Hour,
		"a year is clamped":                365 * 24 * time.Hour,
	}
	for name, ttl := range tests {
		key := name
		c.Put(key, []byte("v"), 1, ttl)
	}

	fc.Advance(c.MaxTTL())
	for name := range tests {
		if _, _, ok := c.Get(name); ok {
			t.Errorf("%q survived MaxTTL; no entry may outlive the staleness bound", name)
		}
	}
}

func TestDefaultTTLApplies(t *testing.T) {
	c, fc := newCache(t, func(o *l1.Options) {
		o.MaxTTL = time.Minute
		o.DefaultTTL = 5 * time.Second
	})
	c.Put("k", []byte("v"), 1, 0)

	fc.Advance(4 * time.Second)
	if _, _, ok := c.Get("k"); !ok {
		t.Fatal("entry expired before the default TTL")
	}
	fc.Advance(2 * time.Second)
	if _, _, ok := c.Get("k"); ok {
		t.Fatal("entry survived the default TTL")
	}
}

func TestDefaultTTLIsClampedToMaxTTL(t *testing.T) {
	c, fc := newCache(t, func(o *l1.Options) {
		o.MaxTTL = time.Second
		o.DefaultTTL = time.Hour
	})
	c.Put("k", []byte("v"), 1, 0)

	fc.Advance(2 * time.Second)
	if _, _, ok := c.Get("k"); ok {
		t.Fatal("a default TTL larger than MaxTTL was honoured")
	}
}

// TestUnrepresentableTTLSaturates is the L1 half of the same overflow. Here the
// answer cannot be "never expires" — every entry must carry a deadline, because
// it is the only bound on staleness — so the deadline saturates instead.
func TestUnrepresentableTTLSaturates(t *testing.T) {
	c, fc := newCache(t, func(o *l1.Options) {
		// A nonsensical ceiling, which is exactly what provokes the
		// overflow.
		o.MaxTTL = time.Duration(math.MaxInt64)
		o.DefaultTTL = time.Hour
	})

	if !c.Put("k", []byte("v"), 1, time.Duration(math.MaxInt64)) {
		t.Fatal("Put rejected the maximum TTL")
	}
	if _, _, ok := c.Get("k"); !ok {
		t.Fatal("an entry with the maximum TTL expired immediately")
	}

	fc.Advance(200 * 365 * 24 * time.Hour)
	if _, _, ok := c.Get("k"); !ok {
		t.Fatal("a saturated deadline expired early")
	}
}

// TestExpiredEntriesAreDroppedOnRead covers the memory side. An expired
// non-owned copy has negative value: keeping it only makes a later bug more
// likely.
func TestExpiredEntriesAreDroppedOnRead(t *testing.T) {
	c, fc := newCache(t)
	c.Put("k", []byte("v"), 1, time.Second)
	if c.Len() != 1 {
		t.Fatalf("Len = %d, want 1", c.Len())
	}

	fc.Advance(2 * time.Second)
	c.Get("k")

	if c.Len() != 0 {
		t.Fatalf("Len = %d after reading an expired entry, want 0", c.Len())
	}
	if got := c.Stats().Expiries; got != 1 {
		t.Fatalf("Stats().Expiries = %d, want 1", got)
	}
}

// TestOlderVersionIsRejected is the version discipline. An invalidation and a
// repopulation racing on the network must not be able to leave the older value
// resident.
func TestOlderVersionIsRejected(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("new"), 5, time.Second)

	if c.Put("k", []byte("old"), 4, time.Second) {
		t.Fatal("an older version overwrote a newer one")
	}
	if c.Put("k", []byte("same"), 5, time.Second) {
		t.Fatal("an equal version overwrote the resident entry")
	}

	got, version, _ := c.Get("k")
	if string(got) != "new" || version != 5 {
		t.Fatalf("Get = %q v%d, want \"new\" v5", got, version)
	}
	if got := c.Stats().Rejects; got != 2 {
		t.Fatalf("Stats().Rejects = %d, want 2", got)
	}

	if !c.Put("k", []byte("newer"), 6, time.Second) {
		t.Fatal("a newer version was rejected")
	}
	if got, _, _ := c.Get("k"); string(got) != "newer" {
		t.Fatalf("Get = %q, want \"newer\"", got)
	}
}

// TestExpiredEntryDoesNotBlockAnOlderVersion covers the interaction that makes
// the TTL a correctness bound rather than a tuning knob. A key deleted and
// recreated on the owner restarts at version 1, so a legitimately fresh value
// can carry a *lower* version than a stale cached one. Only expiry rescues
// that, which is precisely why expiry is not optional.
func TestExpiredEntryDoesNotBlockAnOlderVersion(t *testing.T) {
	c, fc := newCache(t)
	c.Put("k", []byte("stale"), 99, time.Second)

	fc.Advance(2 * time.Second)
	if !c.Put("k", []byte("fresh, restarted at v1"), 1, time.Second) {
		t.Fatal("an expired entry blocked a lower-versioned replacement forever")
	}
	got, version, ok := c.Get("k")
	if !ok || version != 1 || string(got) != "fresh, restarted at v1" {
		t.Fatalf("Get = %q v%d ok=%v", got, version, ok)
	}
}

// TestConcurrentPutsNeverLeaveAnOlderVersion is the race the version check
// exists to prevent, driven hard enough to actually happen.
//
// Put reads the resident entry and then conditionally replaces it, and the
// underlying cache has no compare-and-set. Without serialising that sequence
// per key, two concurrent writers can both decide to write and the older one
// can land second. "The TTL cleans it up" is not an answer: the whole point of
// stamping versions is that stale data is recognised, not merely aged out.
func TestConcurrentPutsNeverLeaveAnOlderVersion(t *testing.T) {
	const writers = 16
	const rounds = 400

	c, _ := newCache(t)
	value := make([]byte, 32)

	for round := 0; round < rounds; round++ {
		k := fmt.Sprintf("k:%d", round)

		var start sync.WaitGroup
		start.Add(1)
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(version uint64) {
				defer wg.Done()
				start.Wait()
				c.Put(k, value, version, time.Minute)
			}(uint64(w + 1))
		}
		start.Done()
		wg.Wait()

		_, version, ok := c.Get(k)
		if !ok {
			t.Fatalf("round %d: the key vanished", round)
		}
		if version != writers {
			t.Fatalf("round %d: resident version is %d, want the highest written (%d); "+
				"a concurrent Put overwrote a newer entry", round, version, writers)
		}
	}
}

// TestExpiryDeleteDoesNotDropAFreshPut drives a reader observing an expired
// entry against a writer replacing it.
//
// Get notices the deadline has passed and removes the entry. Between noticing
// and removing, a Put may have installed a fresh value — and deleting that
// turns a hit into an avoidable network round trip to the owner. The delete is
// therefore done under the key's stripe and re-checks the deadline; this is the
// test that the re-check is load-bearing rather than decorative.
func TestExpiryDeleteDoesNotDropAFreshPut(t *testing.T) {
	const rounds = 300

	c, fc := newCache(t)
	value := []byte("fresh")

	for round := 0; round < rounds; round++ {
		k := fmt.Sprintf("k:%d", round)

		// An entry that is already expired, so every Get takes the
		// expiry-delete path.
		c.Put(k, []byte("stale"), 1, time.Second)
		fc.Advance(2 * time.Second)

		var wg sync.WaitGroup
		var start sync.WaitGroup
		start.Add(1)

		wg.Add(2)
		go func() {
			defer wg.Done()
			start.Wait()
			c.Get(k) // observes the expiry, deletes under the stripe
		}()
		go func() {
			defer wg.Done()
			start.Wait()
			c.Put(k, value, 2, time.Hour) // installs a fresh, live entry
		}()
		start.Done()
		wg.Wait()

		// Whatever the interleaving, the fresh entry must survive: the
		// Put either landed before the delete, in which case the
		// re-check sees a live entry and leaves it, or after it.
		got, version, ok := c.Get(k)
		if !ok {
			t.Fatalf("round %d: the fresh entry was deleted by an expiry sweep", round)
		}
		if version != 2 || string(got) != "fresh" {
			t.Fatalf("round %d: got %q v%d, want \"fresh\" v2", round, got, version)
		}
	}
}

func TestInvalidate(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("v"), 1, time.Second)
	c.Invalidate("k")

	if _, _, ok := c.Get("k"); ok {
		t.Fatal("Invalidate did not remove the entry")
	}
	// Invalidating an absent key is not an error.
	c.Invalidate("k")
}

// TestInvalidateOlderThanKeepsNewerCopies exists because losing a fresh copy
// costs a round trip while keeping a stale one costs correctness. A late
// invalidation must not throw away a value newer than the one it refers to.
func TestInvalidateOlderThanKeepsNewerCopies(t *testing.T) {
	c, _ := newCache(t)
	c.Put("k", []byte("v10"), 10, time.Second)

	if c.InvalidateOlderThan("k", 10) {
		t.Fatal("an invalidation for the resident version removed it")
	}
	if c.InvalidateOlderThan("k", 9) {
		t.Fatal("an invalidation for an older version removed a newer copy")
	}
	if _, version, ok := c.Get("k"); !ok || version != 10 {
		t.Fatalf("the newer copy was lost: v%d ok=%v", version, ok)
	}

	if !c.InvalidateOlderThan("k", 11) {
		t.Fatal("an invalidation for a newer version did not remove the stale copy")
	}
	if _, _, ok := c.Get("k"); ok {
		t.Fatal("the stale copy survived")
	}
	if c.InvalidateOlderThan("absent", 1) {
		t.Fatal("InvalidateOlderThan reported removing an absent key")
	}
}

func TestCapacityIsBounded(t *testing.T) {
	const capacity = 500
	c, _ := newCache(t, func(o *l1.Options) { o.Capacity = capacity })

	for i := 0; i < capacity*10; i++ {
		c.Put(fmt.Sprintf("k:%d", i), []byte("v"), 1, time.Minute)
	}
	// Eviction is asynchronous in the underlying library; yield until it has
	// caught up rather than asserting on a transient overshoot.
	for i := 0; i < 1_000_000 && c.Len() > capacity; i++ {
		runtime.Gosched()
	}
	if got := c.Len(); got > capacity {
		t.Fatalf("Len = %d, exceeds capacity %d", got, capacity)
	}
}

func TestClear(t *testing.T) {
	c, _ := newCache(t)
	for i := 0; i < 10; i++ {
		c.Put(fmt.Sprintf("k:%d", i), []byte("v"), 1, time.Minute)
	}
	c.Clear()
	if got := c.Len(); got != 0 {
		t.Fatalf("Len = %d after Clear, want 0", got)
	}
}

func TestStatsCountExpiredReadsAsMisses(t *testing.T) {
	c, fc := newCache(t)
	c.Put("k", []byte("v"), 1, time.Second)

	c.Get("k")
	fc.Advance(2 * time.Second)
	c.Get("k")
	c.Get("absent")

	stats := c.Stats()
	if stats.Hits != 1 {
		t.Fatalf("Hits = %d, want 1", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Fatalf("Misses = %d, want 2 (one expired, one absent)", stats.Misses)
	}
	if stats.Puts != 1 {
		t.Fatalf("Puts = %d, want 1", stats.Puts)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	tests := map[string]l1.Options{
		"no clock":             {Capacity: 10},
		"negative capacity":    {Clock: fc, Capacity: -1},
		"negative max TTL":     {Clock: fc, Capacity: 10, MaxTTL: -time.Second},
		"negative default TTL": {Clock: fc, Capacity: 10, DefaultTTL: -time.Second},
	}
	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := l1.New(opts); !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("New = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	const goroutines = 16
	const operations = 2000

	c, _ := newCache(t, func(o *l1.Options) { o.Capacity = 2000 })

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < operations; i++ {
				key := fmt.Sprintf("k:%d", i%256)
				switch i % 4 {
				case 0:
					c.Put(key, []byte("v"), uint64(i), time.Minute)
				case 1:
					c.Get(key)
				case 2:
					c.InvalidateOlderThan(key, uint64(i))
				case 3:
					c.Invalidate(key)
				}
			}
		}(g)
	}
	wg.Wait()
}
