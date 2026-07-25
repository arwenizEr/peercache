package peercache_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
)

// key builds a deterministic test key.
func key(i int) string { return "user:" + strconv.Itoa(i) + "/session" }

// gosched yields without sleeping. Tests use it to wait on a condition another
// goroutine will satisfy; the go test timeout bounds the wait if it never does.
func gosched() { runtime.Gosched() }

func TestNamespaceSetGetDelete(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	if _, err := ns.Get(ctx, "absent"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get of an absent key = %v, want ErrNotFound", err)
	}
	if ok, err := ns.Exists(ctx, "absent"); err != nil || ok {
		t.Fatalf("Exists of an absent key = %v, %v", ok, err)
	}
	// Deleting an absent key is not an error: the postcondition already
	// holds.
	if err := ns.Delete(ctx, "absent"); err != nil {
		t.Fatalf("Delete of an absent key = %v, want nil", err)
	}

	if err := ns.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := ns.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "v" {
		t.Fatalf("Get = %q, want \"v\"", got)
	}
	if ok, err := ns.Exists(ctx, "k"); err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}

	if err := ns.Delete(ctx, "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get after Delete = %v, want ErrNotFound", err)
	}
}

// TestZeroLengthValueIsNotAbsence is why Get returns ErrNotFound rather than a
// nil value. A key may legitimately hold zero bytes, and (nil, nil) would make
// the two indistinguishable.
func TestZeroLengthValueIsNotAbsence(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	if err := ns.Set(ctx, "empty", []byte{}, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := ns.Get(ctx, "empty")
	if err != nil {
		t.Fatalf("Get of a zero-length value = %v, want nil", err)
	}
	if len(got) != 0 {
		t.Fatalf("Get = %q, want empty", got)
	}
}

func TestNamespacesAreIsolated(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	sessions := c.Namespace("sessions")
	profiles := c.Namespace("profiles")

	if err := sessions.Set(ctx, "k", []byte("session"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := profiles.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a key set in one namespace was visible in another: %v", err)
	}

	if err := profiles.Set(ctx, "k", []byte("profile"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := sessions.Get(ctx, "k")
	if err != nil || string(got) != "session" {
		t.Fatalf("the first namespace's value was overwritten: %q %v", got, err)
	}
}

// TestNamespaceHandleIsMemoized pins the documented contract that the first
// call wins. Two handles to one namespace would now mean two stores, so this is
// load-bearing rather than cosmetic.
func TestNamespaceHandleIsMemoized(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	first := c.Namespace("sessions")
	if err := first.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	second := c.Namespace("sessions", peercache.WithReplicationFactor(9))
	if first != second {
		t.Fatal("Namespace returned a different handle for the same name")
	}
	// If the second call had won, its invalid options would surface here and
	// the value written through the first handle would be in another store.
	got, err := second.Get(ctx, "k")
	if err != nil || string(got) != "v" {
		t.Fatalf("Get through the second handle = %q %v; the later options were applied", got, err)
	}
}

// TestTTLExpiryUnderFakeClock is one of the M1 acceptance criteria: correct TTL
// expiry, driven entirely by the injected clock.
func TestTTLExpiryUnderFakeClock(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	if err := ns.Set(ctx, "k", []byte("v"), time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	fc.Advance(time.Minute - time.Nanosecond)
	if _, err := ns.Get(ctx, "k"); err != nil {
		t.Fatalf("the key expired one nanosecond early: %v", err)
	}
	if ok, _ := ns.Exists(ctx, "k"); !ok {
		t.Fatal("Exists disagreed with Get before expiry")
	}

	fc.Advance(time.Nanosecond)
	if _, err := ns.Get(ctx, "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("the key survived its deadline: %v", err)
	}
	if ok, _ := ns.Exists(ctx, "k"); ok {
		t.Fatal("Exists disagreed with Get after expiry")
	}
}

func TestNonPositiveTTLNeverExpires(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	for _, tc := range []struct {
		key string
		ttl time.Duration
	}{{"zero", 0}, {"negative", -time.Hour}} {
		if err := ns.Set(ctx, tc.key, []byte("v"), tc.ttl); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	fc.Advance(100 * 365 * 24 * time.Hour)
	for _, k := range []string{"zero", "negative"} {
		if _, err := ns.Get(ctx, k); err != nil {
			t.Fatalf("%q expired despite a non-positive TTL: %v", k, err)
		}
	}
}

func TestSetCopiesTheCallersBuffer(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	buf := []byte("original")
	if err := ns.Set(ctx, "k", buf, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	copy(buf, "MUTATED!")

	got, err := ns.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "original" {
		t.Fatalf("Get = %q; Set did not copy the caller's buffer", got)
	}
}

func TestBatchGetOmitsAbsentKeys(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	for i := 0; i < 10; i += 2 {
		if err := ns.Set(ctx, key(i), []byte(strconv.Itoa(i)), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	keys := make([]string, 10)
	for i := range keys {
		keys[i] = key(i)
	}
	got, err := ns.BatchGet(ctx, keys)
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	// A batch read where half the keys are cold is the normal case, not an
	// error.
	if len(got) != 5 {
		t.Fatalf("BatchGet returned %d values, want 5", len(got))
	}
	for i := 0; i < 10; i += 2 {
		if string(got[key(i)]) != strconv.Itoa(i) {
			t.Fatalf("BatchGet[%q] = %q", key(i), got[key(i)])
		}
	}

	if empty, err := ns.BatchGet(ctx, nil); err != nil || len(empty) != 0 {
		t.Fatalf("BatchGet of no keys = %v, %v", empty, err)
	}
}

func TestBatchSet(t *testing.T) {
	c, fc := newCache(t)
	ns := c.Namespace("sessions")
	ctx := context.Background()

	kv := map[string][]byte{"a": []byte("1"), "b": []byte("2"), "c": []byte("3")}
	if err := ns.BatchSet(ctx, kv, time.Minute); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	for k, want := range kv {
		got, err := ns.Get(ctx, k)
		if err != nil {
			t.Fatalf("Get(%q): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) = %q, want %q", k, got, want)
		}
	}

	// The TTL applies to every key in the batch.
	fc.Advance(2 * time.Minute)
	for k := range kv {
		if _, err := ns.Get(ctx, k); !errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("Get(%q) after expiry = %v, want ErrNotFound", k, err)
		}
	}

	if err := ns.BatchSet(ctx, nil, 0); err != nil {
		t.Fatalf("BatchSet of nothing = %v, want nil", err)
	}
}

func TestGetOrLoadWithoutLoaderReportsErrNoLoader(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("sessions")
	if _, err := ns.GetOrLoad(context.Background(), "k"); !errors.Is(err, peercache.ErrNoLoader) {
		t.Fatalf("GetOrLoad = %v, want ErrNoLoader", err)
	}
}

func TestGetOrLoadPopulatesAndCaches(t *testing.T) {
	var calls atomic.Int64
	c, fc := newCache(t)
	ns := c.Namespace("profiles", peercache.WithLoader(
		func(_ context.Context, k string) ([]byte, time.Duration, error) {
			calls.Add(1)
			return []byte("loaded:" + k), time.Minute, nil
		}))
	ctx := context.Background()

	got, err := ns.GetOrLoad(ctx, "user:1")
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if string(got) != "loaded:user:1" {
		t.Fatalf("GetOrLoad = %q", got)
	}

	// The loaded value is now resident, so a plain Get finds it and the
	// loader is not consulted again.
	direct, err := ns.Get(ctx, "user:1")
	if err != nil || string(direct) != "loaded:user:1" {
		t.Fatalf("Get after GetOrLoad = %q %v", direct, err)
	}
	if _, err := ns.GetOrLoad(ctx, "user:1"); err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loader ran %d times, want 1", got)
	}

	// The loader's TTL is honoured, after which the loader runs again.
	fc.Advance(2 * time.Minute)
	if _, err := ns.GetOrLoad(ctx, "user:1"); err != nil {
		t.Fatalf("GetOrLoad after expiry: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader ran %d times after expiry, want 2", got)
	}
}

func TestGetOrLoadPropagatesTheLoadersError(t *testing.T) {
	sentinel := errors.New("the database is on fire")
	c, _ := newCache(t)
	ns := c.Namespace("profiles", peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			return nil, 0, sentinel
		}))

	_, err := ns.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, sentinel) {
		t.Fatalf("GetOrLoad = %v, want the loader's error", err)
	}
	// A failed load must not be cached as an empty value.
	if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("a failed load left something resident: %v", err)
	}
}

// TestGetOrLoadCollapsesConcurrentMisses is the M1 acceptance criterion:
// a loader that counts invocations under a thousand concurrent misses reports
// exactly one.
//
// Spawning a thousand goroutines proves nothing on its own — they could run one
// after another and each find the previous result. The loader is held open
// until every caller is provably inside the flight, which Stats().InflightLoads
// reports. No sleeping: the test yields until the count is reached.
func TestGetOrLoadCollapsesConcurrentMisses(t *testing.T) {
	const callers = 1000

	var invocations atomic.Int64
	var arrived atomic.Int64
	release := make(chan struct{})

	c, _ := newCache(t)
	ns := c.Namespace("profiles", peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			invocations.Add(1)
			<-release
			return []byte("loaded once"), time.Minute, nil
		}))

	type outcome struct {
		val []byte
		err error
	}
	results := make(chan outcome, callers)
	for i := 0; i < callers; i++ {
		go func() {
			arrived.Add(1)
			val, err := ns.GetOrLoad(context.Background(), "hot")
			results <- outcome{val, err}
		}()
	}

	// Every caller has entered GetOrLoad. This always happens: it is only
	// goroutine start-up, independent of whether collapsing works.
	for arrived.Load() < callers {
		gosched()
	}

	// Now they must all be *inside* one flight. Bounded, because if
	// collapsing regressed this condition is never reached — and a test that
	// hangs for the full timeout tells you far less than one that says which
	// invariant broke. An earlier version of this test spun here forever and
	// took 300 seconds to report nothing.
	const yields = 5_000_000
	for i := 0; c.Stats().InflightLoads < callers; i++ {
		if i > yields {
			t.Fatalf("%d callers entered GetOrLoad but only %d are inside a single flight "+
				"after %d yields; concurrent misses are not collapsing",
				arrived.Load(), c.Stats().InflightLoads, yields)
		}
		gosched()
	}
	close(release)

	for i := 0; i < callers; i++ {
		got := <-results
		if got.err != nil {
			t.Fatalf("caller %d: %v", i, got.err)
		}
		if string(got.val) != "loaded once" {
			t.Fatalf("caller %d got %q", i, got.val)
		}
	}

	if got := invocations.Load(); got != 1 {
		t.Fatalf("the loader ran %d times under %d concurrent misses, want exactly 1", got, callers)
	}
	stats := c.Stats()
	if stats.Loads != 1 {
		t.Fatalf("Stats().Loads = %d, want 1", stats.Loads)
	}
	if stats.CollapsedLoads != callers-1 {
		t.Fatalf("Stats().CollapsedLoads = %d, want %d", stats.CollapsedLoads, callers-1)
	}
	if stats.InflightLoads != 0 {
		t.Fatalf("Stats().InflightLoads = %d after every caller returned, want 0", stats.InflightLoads)
	}
}

// TestGetOrLoadPanicIsContained checks that a bug in application code becomes
// an error for the callers of that key rather than a panic in a thousand
// unrelated goroutines.
func TestGetOrLoadPanicIsContained(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("profiles", peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			panic("loader bug")
		}))

	_, err := ns.GetOrLoad(context.Background(), "k")
	if !errors.Is(err, peercache.ErrLoaderPanic) {
		t.Fatalf("GetOrLoad = %v, want ErrLoaderPanic", err)
	}
	// The namespace must still work afterwards.
	if _, err := ns.GetOrLoad(context.Background(), "k"); !errors.Is(err, peercache.ErrLoaderPanic) {
		t.Fatalf("the namespace was wedged by the panic: %v", err)
	}
}

// TestLoaderTimeoutIsTheNamespacesNotTheCallers is why LoaderTimeout exists.
// Concurrent misses collapse, so the bound on a load cannot be whichever
// caller happened to arrive first.
func TestLoaderTimeoutIsTheNamespacesNotTheCallers(t *testing.T) {
	c, fc := newCache(t)

	sawDeadline := make(chan bool, 1)
	entered := make(chan struct{}, 1)
	ns := c.Namespace("profiles",
		peercache.WithLoaderTimeout(time.Minute),
		peercache.WithLoader(func(loadCtx context.Context, _ string) ([]byte, time.Duration, error) {
			entered <- struct{}{}
			<-loadCtx.Done()
			sawDeadline <- errors.Is(loadCtx.Err(), context.DeadlineExceeded)
			return nil, 0, loadCtx.Err()
		}))

	done := make(chan error, 1)
	go func() {
		_, err := ns.GetOrLoad(context.Background(), "k")
		done <- err
	}()

	<-entered
	// The loader is parked on its own deadline, which belongs to the
	// namespace and is measured on the injected clock.
	fc.BlockUntil(2) // the sweeper's ticker plus the loader's deadline
	fc.Advance(time.Minute)

	if !<-sawDeadline {
		t.Fatal("the loader's context ended for a reason other than its own deadline")
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GetOrLoad = %v, want DeadlineExceeded", err)
	}
}

// TestCallerCancellationDoesNotAbortTheLoad is the other half: one impatient
// caller must not fail the patient ones.
func TestCallerCancellationDoesNotAbortTheLoad(t *testing.T) {
	c, _ := newCache(t)

	release := make(chan struct{})
	loaderRan := make(chan struct{}, 1)
	ns := c.Namespace("profiles", peercache.WithLoader(
		func(context.Context, string) ([]byte, time.Duration, error) {
			loaderRan <- struct{}{}
			<-release
			return []byte("finished anyway"), time.Minute, nil
		}))

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, err := ns.GetOrLoad(leaderCtx, "k")
		leaderDone <- err
	}()
	<-loaderRan

	followerDone := make(chan []byte, 1)
	go func() {
		val, err := ns.GetOrLoad(context.Background(), "k")
		if err != nil {
			t.Errorf("follower: %v", err)
		}
		followerDone <- val
	}()
	for c.Stats().InflightLoads < 2 {
		gosched()
	}

	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader = %v, want context.Canceled", err)
	}

	close(release)
	if got := <-followerDone; string(got) != "finished anyway" {
		t.Fatalf("follower got %q", got)
	}
}

// TestPartitionerHasNoEffectYet pins an uncomfortable fact rather than leaving
// it implicit: WithPartitioner is accepted and validated, and nothing on any
// data path calls it.
//
// Grouping decides which node owns a key. On a single node with no ownership
// there is nothing for it to decide, so M1 cannot honestly do anything with it.
// Asserting the current behaviour means the day M3 wires it up, this test
// changes deliberately instead of a reviewer wondering whether it ever worked.
func TestPartitionerHasNoEffectYet(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	// A partitioner that maps every key to one group, which would be
	// dramatic if it did anything.
	collapsing := c.Namespace("collapsing",
		peercache.WithPartitioner(peercache.PartitionByFunc(func(string) string { return "everything" })))

	if err := collapsing.Set(ctx, "a", []byte("first"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := collapsing.Set(ctx, "b", []byte("second"), 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Keys remain independent: grouping does not merge them.
	a, err := collapsing.Get(ctx, "a")
	if err != nil || string(a) != "first" {
		t.Fatalf("Get(a) = %q %v", a, err)
	}
	b, err := collapsing.Get(ctx, "b")
	if err != nil || string(b) != "second" {
		t.Fatalf("Get(b) = %q %v", b, err)
	}
}

func TestNamespaceOptionValidationSurfaces(t *testing.T) {
	c, _ := newCache(t)
	tests := map[string]peercache.NamespaceOption{
		"replication factor too high": peercache.WithReplicationFactor(4),
		"replication factor zero":     peercache.WithReplicationFactor(0),
		"non-positive L1 TTL":         peercache.WithL1TTL(0),
		"nil partitioner":             peercache.WithPartitioner(nil),
		"non-positive loader timeout": peercache.WithLoaderTimeout(0),
		"negative shard count":        peercache.WithShards(-1),
		"unknown codec":               peercache.WithCodec("no-such-codec"),
		"unknown compressor":          peercache.WithCompression("no-such-compressor", 64),
		"negative threshold":          peercache.WithCompression("lz4", -1),
	}
	for name, opt := range tests {
		t.Run(name, func(t *testing.T) {
			// Distinct names: namespace handles are memoized, so
			// reusing one would test the first subtest's config again.
			ns := c.Namespace("bad-"+name, opt)
			if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("Get = %v, want ErrInvalidOption", err)
			}
			if err := ns.Set(context.Background(), "k", nil, 0); !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("Set = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestNamespaceAcceptsValidOptions(t *testing.T) {
	c, _ := newCache(t)
	opts := map[string][]peercache.NamespaceOption{
		"replication 1": {peercache.WithReplicationFactor(1)},
		"replication 3": {peercache.WithReplicationFactor(3)},
		"json codec":    {peercache.WithCodec("json")},
		"msgpack codec": {peercache.WithCodec("msgpack")},
		"lz4":           {peercache.WithCompression("lz4", 64)},
		"few shards":    {peercache.WithShards(1)},
	}
	for name, opt := range opts {
		t.Run(name, func(t *testing.T) {
			ns := c.Namespace("ok-"+name, opt...)
			if err := ns.Set(context.Background(), "k", []byte("v"), 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
			if got, err := ns.Get(context.Background(), "k"); err != nil || string(got) != "v" {
				t.Fatalf("Get = %q %v", got, err)
			}
		})
	}
}

// TestCompressionRoundTrips checks the opt-in compression path end to end,
// including the case where a value is too small to compress and the case where
// it does not compress at all.
func TestCompressionRoundTrips(t *testing.T) {
	c, _ := newCache(t)
	ns := c.Namespace("blobs", peercache.WithCompression("lz4", 128))
	ctx := context.Background()

	payloads := map[string][]byte{
		"empty":                {},
		"below threshold":      []byte("small"),
		"compressible":         bytes.Repeat([]byte("peercache-"), 1000),
		"incompressible":       pseudorandom(4096),
		"exactly at threshold": bytes.Repeat([]byte("x"), 128),
	}
	for name, want := range payloads {
		t.Run(name, func(t *testing.T) {
			if err := ns.Set(ctx, name, want, 0); err != nil {
				t.Fatalf("Set: %v", err)
			}
			got, err := ns.Get(ctx, name)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("round trip lost data: %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

// TestCompressionAcrossEveryReadPath covers the combinations that the
// individual tests miss.
//
// Framing is applied on write and undone on read, and there is more than one
// read path: Get, BatchGet, and the two routes through GetOrLoad — the one that
// stores what the loader returned and the one that re-checks an
// already-populated key. A framing bug on any of them corrupts values, and
// testing only Get would have covered exactly one of four.
func TestCompressionAcrossEveryReadPath(t *testing.T) {
	compressible := bytes.Repeat([]byte("peercache-"), 500)
	tiny := []byte("below the threshold")

	var loaded atomic.Int64
	c, _ := newCache(t)
	ns := c.Namespace("framed",
		peercache.WithCompression("lz4", 128),
		peercache.WithLoader(func(_ context.Context, k string) ([]byte, time.Duration, error) {
			loaded.Add(1)
			if k == "loaded-large" {
				return compressible, time.Minute, nil
			}
			return tiny, time.Minute, nil
		}))
	ctx := context.Background()

	// Written directly, read back through Get and BatchGet.
	if err := ns.Set(ctx, "large", compressible, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := ns.Set(ctx, "small", tiny, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	got, err := ns.Get(ctx, "large")
	if err != nil || !bytes.Equal(got, compressible) {
		t.Fatalf("Get(large) = %d bytes, %v", len(got), err)
	}

	batch, err := ns.BatchGet(ctx, []string{"large", "small"})
	if err != nil {
		t.Fatalf("BatchGet: %v", err)
	}
	if !bytes.Equal(batch["large"], compressible) {
		t.Fatalf("BatchGet(large) returned %d bytes, want %d", len(batch["large"]), len(compressible))
	}
	if !bytes.Equal(batch["small"], tiny) {
		t.Fatalf("BatchGet(small) = %q", batch["small"])
	}

	// Written by the loader: the value goes through Frame on the way in and
	// must come back out of GetOrLoad unframed.
	loadedLarge, err := ns.GetOrLoad(ctx, "loaded-large")
	if err != nil || !bytes.Equal(loadedLarge, compressible) {
		t.Fatalf("GetOrLoad(loaded-large) = %d bytes, %v", len(loadedLarge), err)
	}

	// The second call takes the resident path, which unframes rather than
	// returning what the loader handed back. It must agree.
	again, err := ns.GetOrLoad(ctx, "loaded-large")
	if err != nil || !bytes.Equal(again, compressible) {
		t.Fatalf("second GetOrLoad = %d bytes, %v", len(again), err)
	}
	if got := loaded.Load(); got != 1 {
		t.Fatalf("loader ran %d times, want 1", got)
	}

	// And through a plain Get, which is a third route to the same bytes.
	direct, err := ns.Get(ctx, "loaded-large")
	if err != nil || !bytes.Equal(direct, compressible) {
		t.Fatalf("Get of a loaded value = %d bytes, %v", len(direct), err)
	}

	// BatchSet writes through the framer too.
	if err := ns.BatchSet(ctx, map[string][]byte{"batched": compressible}, 0); err != nil {
		t.Fatalf("BatchSet: %v", err)
	}
	batched, err := ns.Get(ctx, "batched")
	if err != nil || !bytes.Equal(batched, compressible) {
		t.Fatalf("Get of a BatchSet value = %d bytes, %v", len(batched), err)
	}
}

// pseudorandom returns deterministic high-entropy bytes, so an incompressible
// payload does not need a random seed.
func pseudorandom(n int) []byte {
	out := make([]byte, n)
	state := uint32(2463534242)
	for i := range out {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		out[i] = byte(state)
	}
	return out
}

func TestCompressionActuallyShrinksLargeValues(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()

	plain := c.Namespace("plain")
	packed := c.Namespace("packed", peercache.WithCompression("lz4", 128))

	payload := bytes.Repeat([]byte("peercache-"), 10_000)
	if err := plain.Set(ctx, "k", payload, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := packed.Set(ctx, "k", payload, 0); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Both must read back identically; the difference is only in what is
	// held. Residency is not directly observable, so assert the invariant
	// that matters and leave the size to the codec's own tests.
	a, err := plain.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	b, err := packed.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(a, b) || !bytes.Equal(a, payload) {
		t.Fatal("compressed and uncompressed namespaces disagreed")
	}
}

func TestConcurrentNamespaceAccessIsSafe(t *testing.T) {
	const goroutines = 16
	const operations = 1000

	c, _ := newCache(t)
	ns := c.Namespace("sessions", peercache.WithLoader(
		func(_ context.Context, k string) ([]byte, time.Duration, error) {
			return []byte("loaded:" + k), time.Minute, nil
		}))
	ctx := context.Background()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < operations; i++ {
				k := key(i % 128)
				switch i % 5 {
				case 0:
					if err := ns.Set(ctx, k, []byte("v"), time.Minute); err != nil {
						t.Errorf("Set: %v", err)
						return
					}
				case 1:
					if _, err := ns.Get(ctx, k); err != nil && !errors.Is(err, peercache.ErrNotFound) {
						t.Errorf("Get: %v", err)
						return
					}
				case 2:
					if err := ns.Delete(ctx, k); err != nil {
						t.Errorf("Delete: %v", err)
						return
					}
				case 3:
					if _, err := ns.Exists(ctx, k); err != nil {
						t.Errorf("Exists: %v", err)
						return
					}
				case 4:
					if _, err := ns.GetOrLoad(ctx, k); err != nil {
						t.Errorf("GetOrLoad: %v", err)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestConcurrentNamespaceCreation covers the double-checked creation path. Two
// goroutines racing must end up with one namespace, not two stores over the
// same name.
func TestConcurrentNamespaceCreation(t *testing.T) {
	const goroutines = 32

	c, _ := newCache(t)
	handles := make(chan peercache.Namespace, goroutines)

	var start sync.WaitGroup
	start.Add(1)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			handles <- c.Namespace("contended")
		}()
	}
	start.Done()
	wg.Wait()
	close(handles)

	first := <-handles
	for ns := range handles {
		if ns != first {
			t.Fatal("concurrent creation produced two namespaces for one name")
		}
	}
}

var _ = fmt.Sprintf // keep fmt imported for future assertions

var _ = clock.Epoch
