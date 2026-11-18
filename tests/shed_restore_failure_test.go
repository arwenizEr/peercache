package tests

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// cutOffStore fails every operation while cut is set: the object store as seen
// from a node whose network is gone.
type cutOffStore struct {
	persist.BlobStore
	cut atomic.Bool
}

var errCutOff = errors.New("store unreachable")

func (s *cutOffStore) Put(ctx context.Context, name string, data []byte) error {
	if s.cut.Load() {
		return errCutOff
	}
	return s.BlobStore.Put(ctx, name, data)
}

func (s *cutOffStore) Get(ctx context.Context, name string) ([]byte, error) {
	if s.cut.Load() {
		return nil, errCutOff
	}
	return s.BlobStore.Get(ctx, name)
}

func (s *cutOffStore) List(ctx context.Context, prefix string) ([]string, error) {
	if s.cut.Load() {
		return nil, errCutOff
	}
	return s.BlobStore.List(ctx, prefix)
}

func (s *cutOffStore) Delete(ctx context.Context, name string) error {
	if s.cut.Load() {
		return errCutOff
	}
	return s.BlobStore.Delete(ctx, name)
}

// TestAShedCounterIsNotCountedFromZeroWhenStorageIsUnreachable is open
// question 57 on the memory budget's path rather than the restore's.
//
// A persistent namespace's owner sheds a key under memory pressure: the key
// leaves memory and stays in storage, and its next access reads it back. If
// that read fails, the key is neither in memory nor known — and answering from
// memory then reports it absent, so an Increment counts from zero and its
// write outranks the stored value forever. On kind on 2026-11-18 the pods cut
// off from the object store were over their budget, and four stock counters
// came out hundreds of decrements above the order ledger.
//
// The answer must be the one a failed partition restore gives: ask again.
func TestAShedCounterIsNotCountedFromZeroWhenStorageIsUnreachable(t *testing.T) {
	const (
		budget   = 256 << 10
		counters = 50
		before   = 10
	)
	store := &cutOffStore{BlobStore: persist.Memory()}
	c, fc := newCache(t, peercache.WithMemoryBudget(budget))
	stock := c.Namespace("stock", peercache.WithPersistence(store))
	ctx := context.Background()

	for i := 0; i < counters; i++ {
		for j := 0; j < before; j++ {
			if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
				t.Fatalf("Increment: %v", err)
			}
		}
	}
	flushed(t, c, fc, 1)

	// Pressure: owned data is shed from every namespace in turn.
	bulk := c.Namespace("bulk")
	for i := 0; i < 4000; i++ {
		if err := bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	if c.Stats().ShedOwned == 0 {
		t.Fatal("4 MB written into a 256 KB budget and nothing was shed")
	}

	store.cut.Store(true)
	refused := 0
	for i := 0; i < counters; i++ {
		key := fmt.Sprintf("item-%d", i)
		// A read first: it has no second check under the shard lock, so it
		// depends on the read-back's refusal alone. "Not found" for a key
		// storage holds is the lie an Increment would then count from.
		if _, err := stock.Get(ctx, key); errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("Get(%s) with storage unreachable reported it absent; it was shed, "+
				"not deleted, and storage holds it", key)
		}
		got, err := stock.Increment(ctx, key, -1, 0)
		if err != nil {
			if !errors.Is(err, peercache.ErrActivationPending) {
				t.Fatalf("Increment(%s) with storage unreachable: %v; want ErrActivationPending", key, err)
			}
			refused++
			continue
		}
		if got != -(before + 1) {
			t.Fatalf("Increment(%s) = %d with storage unreachable, want %d: a counter the budget "+
				"shed was counted from zero", key, got, -(before + 1))
		}
	}
	if refused == 0 {
		t.Fatal("no stock counter was shed, so this test checked nothing")
	}

	// Storage back, and the retry interval past: every counter continues
	// from its own value.
	store.cut.Store(false)
	fc.Advance(2 * time.Second)
	for i := 0; i < counters; i++ {
		key := fmt.Sprintf("item-%d", i)
		v, err := stock.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get(%s) after storage returned: %v", key, err)
		}
		n, err := strconv.Atoi(string(v))
		if err != nil {
			t.Fatalf("%s holds %q: %v", key, v, err)
		}
		if n > -before {
			t.Fatalf("%s = %d after storage returned; it was %d before the outage", key, n, -before)
		}
	}
}

// listCountingStore counts listings, which every read-back of a partition
// starts with.
type listCountingStore struct {
	*cutOffStore
	lists atomic.Int64
}

func (s *listCountingStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.lists.Add(1)
	return s.cutOffStore.List(ctx, prefix)
}

// TestAShedReadBackBacksOffWhileStorageIsDown: a read-back that failed is not
// retried by every access until the retry interval passes, as a failed
// partition restore is not. Without that, each access to a shed key during an
// outage read a whole partition against a store that was not answering, held
// one of the restore slots doing it, and counted one more restore failure:
// the failure count measured the request rate, not the outage.
func TestAShedReadBackBacksOffWhileStorageIsDown(t *testing.T) {
	const (
		budget   = 256 << 10
		counters = 50
		accesses = 200
	)
	store := &listCountingStore{cutOffStore: &cutOffStore{BlobStore: persist.Memory()}}
	c, fc := newCache(t, peercache.WithMemoryBudget(budget))
	stock := c.Namespace("stock", peercache.WithPersistence(store))
	ctx := context.Background()
	for i := 0; i < counters; i++ {
		if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	bulk := c.Namespace("bulk")
	for i := 0; i < 4000; i++ {
		_ = bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0)
	}

	store.cut.Store(true)
	before, failuresBefore := store.lists.Load(), c.Stats().PersistRestoreFailures
	refused := 0
	for i := 0; i < accesses; i++ {
		_, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i%counters), -1, 0)
		if errors.Is(err, peercache.ErrActivationPending) {
			refused++
		}
	}
	if refused < accesses/4 {
		t.Fatalf("only %d of %d accesses refused; too few shed keys to say anything", refused, accesses)
	}
	lists := store.lists.Load() - before
	failures := c.Stats().PersistRestoreFailures - failuresBefore
	// One attempt per partition per retry interval, and the clock does not
	// move here: 50 keys fall in at most 50 partitions.
	if lists > counters || failures > counters {
		t.Fatalf("%d refused accesses listed storage %d times and counted %d restore failures; "+
			"a failed read-back must wait out the retry interval, not retry on every access",
			refused, lists, failures)
	}

	// After the interval, with storage back, the keys come back.
	store.cut.Store(false)
	fc.Advance(2 * time.Second)
	for i := 0; i < counters; i++ {
		if _, err := stock.Get(ctx, fmt.Sprintf("item-%d", i)); err != nil {
			t.Fatalf("Get(item-%d) after storage returned: %v", i, err)
		}
	}
}

// TestAShedKeyThisNodeStillHoldsIsServedDuringAnOutage is open question 64.
// A key written during a store outage stays in this node's pending records,
// unflushed; shed, its read-back fails. The shed's mark names the order of the
// value memory held, and a record of this node's of at least that order is
// that value or a later write of its own: as right to serve as memory was
// before the shed. Refusing it made recently written keys unavailable for the
// whole outage.
func TestAShedKeyThisNodeStillHoldsIsServedDuringAnOutage(t *testing.T) {
	const counters = 50
	store := &cutOffStore{BlobStore: persist.Memory()}
	c, fc := newCache(t, peercache.WithMemoryBudget(256<<10))
	stock := c.Namespace("stock", peercache.WithPersistence(store))
	ctx := context.Background()

	// One decrement each with storage up, so the partitions are restored
	// and the keys are this node's; then the outage, and two more each that
	// stay pending, because every flush fails.
	for i := 0; i < counters; i++ {
		if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	store.cut.Store(true)
	t.Cleanup(func() { store.cut.Store(false) }) // before Close flushes
	for i := 0; i < counters; i++ {
		for j := 0; j < 2; j++ {
			if _, err := stock.Increment(ctx, fmt.Sprintf("item-%d", i), -1, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	bulk := c.Namespace("bulk")
	for i := 0; i < 4000; i++ {
		_ = bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0)
	}
	if c.Stats().ShedOwned == 0 {
		t.Fatal("nothing was shed")
	}
	for i := 0; i < counters; i++ {
		key := fmt.Sprintf("item-%d", i)
		got, err := stock.Increment(ctx, key, -1, 0)
		if err != nil {
			t.Fatalf("Increment(%s) during the outage: %v; this node holds the key's newest write "+
				"in its own records and can answer", key, err)
		}
		if got != -4 {
			t.Fatalf("Increment(%s) = %d, want -4", key, got)
		}
	}
}

// TestAShedLoadedOrExpiredKeyIsNotRefusedForever: two shed keys whose
// read-back finds no record at all, legitimately. A value a loader stored is
// never recorded — it can be loaded again — and a value whose TTL has passed
// is dropped from storage by compaction. The shed mark keeps the order of the
// value memory held; treating "no record" as older than it refused such a key
// on every access, forever, and the loader could not store it again either.
func TestAShedLoadedOrExpiredKeyIsNotRefusedForever(t *testing.T) {
	// The expiring keys' records are swallowed, as a compaction past the
	// tombstone grace drops an expired record: a read-back finds nothing.
	store := &blackholeStore{BlobStore: persist.Memory()}
	c, fc := newCache(t, peercache.WithMemoryBudget(128<<10))
	ns := c.Namespace("profiles",
		peercache.WithPersistence(store),
		peercache.WithLoader(func(_ context.Context, key string) ([]byte, time.Duration, error) {
			return []byte("loaded-" + key), 0, nil
		}))
	ctx := context.Background()
	const keys = 40
	for i := 0; i < keys; i++ {
		if _, err := ns.GetOrLoad(ctx, fmt.Sprintf("loaded-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	store.dropping.Store(true)
	for i := 0; i < keys; i++ {
		if err := ns.Set(ctx, fmt.Sprintf("expiring-%d", i), []byte("v"), time.Second); err != nil {
			t.Fatal(err)
		}
	}
	flushed(t, c, fc, 1)
	store.dropping.Store(false)
	bulk := c.Namespace("bulk")
	for i := 0; i < 4000; i++ {
		_ = bulk.Set(ctx, fmt.Sprintf("b%05d", i), make([]byte, 1<<10), 0)
	}
	if c.Stats().ShedOwned == 0 {
		t.Fatal("nothing was shed")
	}
	fc.Advance(2 * time.Second) // past the deadline

	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("loaded-%d", i)
		v, err := ns.GetOrLoad(ctx, key)
		if err != nil || string(v) != "loaded-"+key {
			t.Fatalf("GetOrLoad(%s) after a shed = %q, %v; the loader must answer it again", key, v, err)
		}
		key = fmt.Sprintf("expiring-%d", i)
		if _, err := ns.Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
			t.Fatalf("Get(%s) past its TTL after a shed: %v; want ErrNotFound", key, err)
		}
	}
}
