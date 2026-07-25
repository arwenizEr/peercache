package store_test

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/store"
)

// newStore builds a store with the background sweeper disabled, so that expiry
// is driven explicitly by the test rather than by a goroutine the test would
// then have to synchronise with.
func newStore(t *testing.T, opts ...func(*store.Options)) (*store.Store, *clock.Fake) {
	t.Helper()
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	o := store.Options{Clock: fc}
	for _, opt := range opts {
		opt(&o)
	}
	s, err := store.New(o)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		if pending := fc.Pending(); pending != 0 {
			t.Errorf("timer leak: %d timer(s) armed after Close", pending)
		}
	})
	return s, fc
}

func TestSetGetDelete(t *testing.T) {
	s, _ := newStore(t)

	if _, _, ok := s.Get("absent"); ok {
		t.Fatal("Get of an absent key reported present")
	}
	if s.Exists("absent") {
		t.Fatal("Exists of an absent key reported true")
	}
	if s.Delete("absent") {
		t.Fatal("Delete of an absent key reported true")
	}

	version := s.Set("k", []byte("v"), 0)
	if version != 1 {
		t.Fatalf("first Set returned version %d, want 1", version)
	}

	got, gotVersion, ok := s.Get("k")
	if !ok {
		t.Fatal("Get after Set reported absent")
	}
	if string(got) != "v" || gotVersion != 1 {
		t.Fatalf("Get = %q v%d, want \"v\" v1", got, gotVersion)
	}
	if !s.Exists("k") {
		t.Fatal("Exists after Set reported false")
	}

	if !s.Delete("k") {
		t.Fatal("Delete of a present key reported false")
	}
	if _, _, ok := s.Get("k"); ok {
		t.Fatal("Get after Delete reported present")
	}
}

// TestVersionsIncrementPerKey pins the versioning contract. Versions are
// compared between two copies of one key, never across keys, so a per-key
// counter is both sufficient and cheaper than a global one.
func TestVersionsIncrementPerKey(t *testing.T) {
	s, _ := newStore(t)

	for i := 1; i <= 5; i++ {
		if got := s.Set("a", []byte("v"), 0); got != uint64(i) {
			t.Fatalf("Set %d returned version %d, want %d", i, got, i)
		}
	}
	if got := s.Set("b", []byte("v"), 0); got != 1 {
		t.Fatalf("first Set of a different key returned version %d, want 1", got)
	}

	// Deleting resets the sequence, because the version lives with the
	// entry. Recorded here as the observed behaviour so that M4's L1
	// invalidation is designed knowing it.
	s.Delete("a")
	if got := s.Set("a", []byte("v"), 0); got != 1 {
		t.Fatalf("Set after Delete returned version %d, want 1", got)
	}
}

func TestZeroLengthValueIsNotAbsence(t *testing.T) {
	s, _ := newStore(t)
	s.Set("empty", []byte{}, 0)

	got, _, ok := s.Get("empty")
	if !ok {
		t.Fatal("a zero-length value reported absent")
	}
	if len(got) != 0 {
		t.Fatalf("Get = %q, want empty", got)
	}
	if !s.Exists("empty") {
		t.Fatal("Exists of a zero-length value reported false")
	}
}

// TestSetCopiesTheCallersBuffer is the safe half of the aliasing contract:
// callers may reuse the buffer they passed to Set.
func TestSetCopiesTheCallersBuffer(t *testing.T) {
	s, _ := newStore(t)

	buf := []byte("original")
	s.Set("k", buf, 0)
	copy(buf, "MUTATED!")

	got, _, ok := s.Get("k")
	if !ok {
		t.Fatal("absent")
	}
	if string(got) != "original" {
		t.Fatalf("Get = %q; Set did not copy the caller's buffer", got)
	}
}

// TestGetReturnsTheStoredSlice is the sharp half. It is asserted rather than
// merely documented so that a future change to copy-on-read is a deliberate,
// visible decision instead of a silent performance regression.
func TestGetReturnsTheStoredSlice(t *testing.T) {
	s, _ := newStore(t)
	s.Set("k", []byte("original"), 0)

	first, _, _ := s.Get("k")
	second, _, _ := s.Get("k")
	if &first[0] != &second[0] {
		t.Fatal("Get copied; the documented contract is that it aliases the stored slice")
	}
}

// TestExpiryIsObservedOnRead is the M1 acceptance criterion for TTL: correct
// expiry under a fake clock, with no sweeper running at all.
func TestExpiryIsObservedOnRead(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v"), time.Minute)

	fc.Advance(time.Minute - time.Nanosecond)
	if _, _, ok := s.Get("k"); !ok {
		t.Fatal("key expired one nanosecond early")
	}
	if !s.Exists("k") {
		t.Fatal("Exists disagreed with Get before expiry")
	}

	fc.Advance(time.Nanosecond)
	if _, _, ok := s.Get("k"); ok {
		t.Fatal("key survived its deadline")
	}
	if s.Exists("k") {
		t.Fatal("Exists disagreed with Get after expiry")
	}

	// Nothing swept it, so it is still resident. Reads are the authority on
	// expiry; the sweeper only reclaims memory.
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1: the entry should still be resident", got)
	}
}

func TestDeleteOfAnExpiredKeyReportsAbsent(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v"), time.Minute)
	fc.Advance(2 * time.Minute)

	if s.Delete("k") {
		t.Fatal("Delete of an expired key reported it as present")
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len = %d after deleting the expired key, want 0", got)
	}
}

func TestOverwriteExtendsTheDeadline(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v1"), time.Minute)

	fc.Advance(50 * time.Second)
	s.Set("k", []byte("v2"), time.Minute)

	fc.Advance(30 * time.Second) // past the first deadline, inside the second
	got, version, ok := s.Get("k")
	if !ok {
		t.Fatal("the overwritten key expired on its old deadline")
	}
	if string(got) != "v2" || version != 2 {
		t.Fatalf("Get = %q v%d, want \"v2\" v2", got, version)
	}
}

func TestNonPositiveTTLNeverExpires(t *testing.T) {
	s, fc := newStore(t)
	s.Set("zero", []byte("v"), 0)
	s.Set("negative", []byte("v"), -time.Hour)

	fc.Advance(100 * 365 * 24 * time.Hour)

	for _, key := range []string{"zero", "negative"} {
		if _, _, ok := s.Get(key); !ok {
			t.Fatalf("%q expired despite a non-positive TTL", key)
		}
	}
}

// TestSweepReclaimsExpiredEntries covers the memory side. Sweeping must not
// change what any reader sees, only what the process holds.
func TestSweepReclaimsExpiredEntries(t *testing.T) {
	s, fc := newStore(t)

	const keys = 500
	for i := 0; i < keys; i++ {
		s.Set(fmt.Sprintf("expiring:%d", i), []byte("v"), time.Minute)
		s.Set(fmt.Sprintf("permanent:%d", i), []byte("v"), 0)
	}
	if got := s.Len(); got != 2*keys {
		t.Fatalf("Len = %d, want %d", got, 2*keys)
	}

	fc.Advance(2 * time.Minute)

	// One full rotation over every shard.
	removed := s.Sweep(s.Shards())
	if removed != keys {
		t.Fatalf("Sweep removed %d entries, want %d", removed, keys)
	}
	if got := s.Len(); got != keys {
		t.Fatalf("Len = %d after sweeping, want %d", got, keys)
	}
	for i := 0; i < keys; i++ {
		if _, _, ok := s.Get(fmt.Sprintf("permanent:%d", i)); !ok {
			t.Fatalf("sweep removed a permanent key")
		}
	}
	if got := s.Stats().Expired; got != int64(keys) {
		t.Fatalf("Stats().Expired = %d, want %d", got, keys)
	}
}

// TestSweepIsIncremental is the property that keeps expiry from becoming a
// periodic stall proportional to the keyspace.
func TestSweepIsIncremental(t *testing.T) {
	s, fc := newStore(t, func(o *store.Options) { o.Shards = 16 })

	const keys = 2000
	for i := 0; i < keys; i++ {
		s.Set(fmt.Sprintf("k:%d", i), []byte("v"), time.Minute)
	}
	fc.Advance(2 * time.Minute)

	// Four of sixteen shards: some entries survive this pass.
	if removed := s.Sweep(4); removed == 0 || removed >= keys {
		t.Fatalf("a partial sweep removed %d of %d entries; expected a proper subset", removed, keys)
	}

	// Rotation eventually covers everything.
	for i := 0; i < s.Shards(); i++ {
		s.Sweep(4)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len = %d after a full rotation, want 0", got)
	}
}

func TestSweepZeroOrNegativeIsANoOp(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v"), time.Minute)
	fc.Advance(2 * time.Minute)

	if removed := s.Sweep(0); removed != 0 {
		t.Fatalf("Sweep(0) removed %d", removed)
	}
	if removed := s.Sweep(-1); removed != 0 {
		t.Fatalf("Sweep(-1) removed %d", removed)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
}

// TestBackgroundSweeperRuns proves the ticker is actually wired to Sweep. The
// test synchronises on the sweeper's effect through a channel rather than by
// advancing and hoping the goroutine was scheduled.
func TestBackgroundSweeperRuns(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	fc.SetMisuseHandler(func(msg string) { t.Errorf("clock misuse: %s", msg) })

	s, err := store.New(store.Options{
		Clock:              fc,
		Shards:             4,
		SweepInterval:      time.Second,
		SweepShardsPerTick: 4,
	})
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	defer func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
		if pending := fc.Pending(); pending != 0 {
			t.Errorf("timer leak: %d timer(s) armed after Close", pending)
		}
	}()

	for i := 0; i < 100; i++ {
		s.Set(fmt.Sprintf("k:%d", i), []byte("v"), time.Minute)
	}

	// The sweeper is parked on the ticker; one armed timer proves it.
	fc.BlockUntil(1)
	fc.Advance(2 * time.Minute)

	// Advance releases the tick, but the sweeper goroutine still has to be
	// scheduled. Yield to it and re-check rather than sleeping; the go test
	// timeout bounds the wait if the wiring is broken.
	for s.Len() != 0 {
		runtime.Gosched()
		fc.Advance(time.Second)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	tests := map[string]store.Options{
		"no clock":        {Shards: 4},
		"negative shards": {Clock: fc, Shards: -1},
		"too many shards": {Clock: fc, Shards: store.MaxShards + 1},
	}
	for name, opts := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := store.New(opts); !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("New = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	tests := map[int]int{0: store.DefaultShards, 1: 1, 2: 2, 3: 4, 100: 128, 256: 256, 257: 512}
	for given, want := range tests {
		s, _ := newStore(t, func(o *store.Options) { o.Shards = given })
		if got := s.Shards(); got != want {
			t.Errorf("Shards(%d) = %d, want %d", given, got, want)
		}
	}
}

// TestKeysSpreadAcrossShards catches a shard function that ignores part of the
// key, collapses to a constant, or uses only the low bits of a poor hash. Any
// of those turns 256 shards into one contended lock, which would not fail any
// other test — it would just quietly cost an order of magnitude under load.
//
// Distribution is measured through the public API: expire everything, then
// sweep one shard at a time and count what each gives up.
func TestKeysSpreadAcrossShards(t *testing.T) {
	const shards = 64
	const perShard = 200
	const keys = shards * perShard

	s, fc := newStore(t, func(o *store.Options) { o.Shards = shards })
	for i := 0; i < keys; i++ {
		s.Set(fmt.Sprintf("user:%d/cart", i), []byte("v"), time.Minute)
	}
	if got := s.Len(); got != keys {
		t.Fatalf("Len = %d, want %d", got, keys)
	}
	fc.Advance(2 * time.Minute)

	counts := make([]int, shards)
	total := 0
	for i := range counts {
		counts[i] = s.Sweep(1)
		total += counts[i]
	}
	if total != keys {
		t.Fatalf("sweeping every shard once removed %d entries, want %d", total, keys)
	}

	minCount, maxCount := counts[0], counts[0]
	for _, c := range counts {
		if c < minCount {
			minCount = c
		}
		if c > maxCount {
			maxCount = c
		}
	}
	if minCount == 0 {
		t.Fatalf("at least one shard received no keys; counts = %v", counts)
	}
	// Expected 200 per shard with a standard deviation near 14, so a
	// three-to-one spread is far outside anything a uniform hash produces
	// and far inside anything a broken one would.
	if maxCount > 3*minCount {
		t.Fatalf("shard occupancy spread from %d to %d; the hash is not distributing keys",
			minCount, maxCount)
	}
}

func TestConcurrentAccessIsSafe(t *testing.T) {
	const writers = 8
	const readers = 8
	const perGoroutine = 2000

	s, _ := newStore(t)

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				key := fmt.Sprintf("w%d:%d", w, i%64)
				s.Set(key, []byte("value"), time.Minute)
				if i%8 == 0 {
					s.Delete(key)
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				s.Get(fmt.Sprintf("w%d:%d", i%writers, i%64))
				s.Exists(fmt.Sprintf("w%d:%d", i%writers, i%64))
			}
		}(r)
	}
	wg.Wait()

	stats := s.Stats()
	if stats.Sets != writers*perGoroutine {
		t.Fatalf("Stats().Sets = %d, want %d", stats.Sets, writers*perGoroutine)
	}
	if stats.Hits+stats.Misses != readers*perGoroutine {
		t.Fatalf("hits+misses = %d, want %d", stats.Hits+stats.Misses, readers*perGoroutine)
	}
}

func TestStatsCountExpiredReadsAsMisses(t *testing.T) {
	s, fc := newStore(t)
	s.Set("k", []byte("v"), time.Minute)

	s.Get("k")
	fc.Advance(2 * time.Minute)
	s.Get("k")

	stats := s.Stats()
	if stats.Hits != 1 || stats.Misses != 1 {
		t.Fatalf("hits=%d misses=%d, want 1 and 1: an expired read is a miss to the caller",
			stats.Hits, stats.Misses)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	fc := clock.NewFake(clock.Epoch)
	s, err := store.New(store.Options{Clock: fc, SweepInterval: time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if pending := fc.Pending(); pending != 0 {
		t.Fatalf("timer leak: %d timer(s) armed after Close", pending)
	}
}
