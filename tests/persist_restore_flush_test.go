package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// heldStore is a blob store whose reads can be held and whose writes can be
// made to fail, so a test can stop a restore in the middle of reading storage
// and decide whether a flush reaches it.
type heldStore struct {
	persist.BlobStore
	held    atomic.Bool
	failPut atomic.Bool
	waiting atomic.Int64

	mu      sync.Mutex
	release chan struct{}

	putHeld    atomic.Bool
	putWaiting atomic.Int64
	putRelease chan struct{}
}

func newHeldStore() *heldStore {
	return &heldStore{BlobStore: persist.Memory(), release: make(chan struct{}), putRelease: make(chan struct{})}
}

// hold makes every Get from now on wait until open.
func (s *heldStore) hold() { s.held.Store(true) }

// open lets every held Get, and every later one, through.
func (s *heldStore) open() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held.Swap(false) {
		close(s.release)
		s.release = make(chan struct{})
	}
}

func (s *heldStore) Get(ctx context.Context, name string) ([]byte, error) {
	if s.held.Load() {
		s.mu.Lock()
		release := s.release
		s.mu.Unlock()
		s.waiting.Add(1)
		select {
		case <-release:
		case <-ctx.Done():
			s.waiting.Add(-1)
			return nil, ctx.Err()
		}
		s.waiting.Add(-1)
	}
	return s.BlobStore.Get(ctx, name)
}

// holdPuts makes every Put from now on wait until openPuts. Call once.
func (s *heldStore) holdPuts() { s.putHeld.Store(true) }

// openPuts lets every held Put through.
func (s *heldStore) openPuts() {
	if s.putHeld.Swap(false) {
		close(s.putRelease)
	}
}

func (s *heldStore) Put(ctx context.Context, name string, data []byte) error {
	if s.putHeld.Load() {
		s.putWaiting.Add(1)
		select {
		case <-s.putRelease:
		case <-ctx.Done():
			s.putWaiting.Add(-1)
			return ctx.Err()
		}
		s.putWaiting.Add(-1)
	}
	if s.failPut.Load() {
		return errors.New("heldStore: put refused")
	}
	return s.BlobStore.Put(ctx, name, data)
}

// keysOwnedBy returns the keys among k0..k(n-1) that id owns.
func keysOwnedBy(t *testing.T, f *fabric, id string, n int) []string {
	t.Helper()
	var keys []string
	for i := 0; i < n; i++ {
		if k := fmt.Sprintf("k%d", i); f.ownerOf(t, k) == id {
			keys = append(keys, k)
		}
	}
	return keys
}

// TestAFlushIsNotHeldUpByARestore: a restore reading storage must not stop
// this node's flushes. After a membership change every partition restores
// again on first access; while one does, writes to partitions already
// restored are acknowledged and wait in pending for a flush. When the restore
// held the flush lock, nothing flushed until it finished, and a crash in
// between lost every one of them — on kind, 58,000 acknowledged orders on a
// pod OOM-killed during its restores (open question 55). The documented loss
// window is one flush interval, not the length of a restore.
func TestAFlushIsNotHeldUpByARestore(t *testing.T) {
	f := newFabric(t, 3)
	store := newHeldStore()
	openDurable(f, store)
	ctx := context.Background()
	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }

	const keys = 200
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := durable(f.ids[0]).Set(ctx, k, []byte("v1"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	flushAll(t, f)
	// Split, so each partition's restore reads its own segments. Unsplit,
	// the first restore reads the shared logs and the others take them from
	// it without touching the store (TestARestoreReadsEachLogOnce), so none
	// would be caught reading.
	splitAll(t, f)

	owner := f.ids[0]
	mine := keysOwnedBy(t, f, owner, keys)
	if len(mine) < 2 {
		t.Fatalf("%s owns %d keys; need two partitions' worth", owner, len(mine))
	}
	// A membership change, after which every partition restores again.
	killAndSettle(t, f, f.ids[2])

	// The first key's partition restores, from a store that answers.
	first := mine[0]
	if _, err := durable(owner).Get(ctx, first); err != nil {
		t.Fatalf("Get(%s): %v", first, err)
	}

	// Every other partition's restore now stops in the middle of reading.
	var readers sync.WaitGroup
	defer readers.Wait() // after open, which runs first
	store.hold()
	defer store.open()
	for _, k := range mine[1:] {
		readers.Add(1)
		go func(k string) {
			defer readers.Done()
			_, _ = durable(owner).Get(ctx, k)
		}(k)
	}
	waitFor(t, "a restore to be reading storage", func() bool { return store.waiting.Load() > 0 })

	// A write to the partition already restored is acknowledged...
	if err := durable(owner).Set(ctx, first, []byte("v2"), 0); err != nil {
		t.Fatalf("Set(%s) while another partition restores: %v", first, err)
	}
	before := f.caches[owner].Stats()
	if before.PersistPending == 0 {
		t.Fatal("the write is not pending; the test would prove nothing")
	}
	// ...and must reach storage at the next flush, restore or no restore.
	f.clk.Advance(flushInterval)
	waitFor(t, "a flush while a restore is reading storage", func() bool {
		s := f.caches[owner].Stats()
		return s.PersistFlushes > before.PersistFlushes && s.PersistPending == 0
	})
	if got := f.caches[owner].Stats().PersistRecordsWritten; got <= before.PersistRecordsWritten {
		t.Fatalf("records written %d, was %d: the flush during the restore wrote nothing",
			got, before.PersistRecordsWritten)
	}
}

// TestADeleteFlushedDuringARestoreIsNotUndone is the hazard the flush lock
// was guarding. A restore reads storage, then lays over it what this node
// wrote that storage may not show yet. A deletion that a flush moves out of
// pending and into storage after the restore listed storage is in neither
// place the restore looks — unless the flusher keeps it for the restore —
// and the restore would bring the deleted value back.
func TestADeleteFlushedDuringARestoreIsNotUndone(t *testing.T) {
	f := newFabric(t, 3)
	store := newHeldStore()
	openDurable(f, store)
	ctx := context.Background()
	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }

	const keys = 200
	owner := f.ids[0]
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := durable(owner).Set(ctx, k, []byte("v1"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	flushAll(t, f)
	key := keysOwnedBy(t, f, owner, keys)[0]

	// One membership change, the key's partition restored after it, and a
	// deletion that cannot reach storage: it stays pending.
	killAndSettle(t, f, f.ids[1])
	if _, err := durable(owner).Get(ctx, key); err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	store.failPut.Store(true)
	if err := durable(owner).Delete(ctx, key); err != nil {
		t.Fatalf("Delete(%s): %v", key, err)
	}

	// A second membership change: the partition restores again on its next
	// access, and that restore stops after listing storage, before reading.
	killAndSettle(t, f, f.ids[2])
	store.hold()
	defer store.open()
	got := make(chan error, 1)
	go func() {
		_, err := durable(owner).Get(ctx, key)
		got <- err
	}()
	waitFor(t, "the restore to be reading storage", func() bool { return store.waiting.Load() > 0 })

	// Now the deletion reaches storage — after the restore listed it.
	store.failPut.Store(false)
	before := f.caches[owner].Stats().PersistRecordsWritten
	f.clk.Advance(flushInterval)
	waitFor(t, "the deletion to be flushed", func() bool {
		return f.caches[owner].Stats().PersistRecordsWritten > before
	})

	store.open()
	if err := <-got; !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get(%s) after the restore = %v; it was deleted, and a restore that missed "+
			"the deletion's flush brought the old value back", key, err)
	}
	if _, err := durable(owner).Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get(%s) again = %v; the deleted value came back", key, err)
	}
}

// TestADeleteBeingFlushedDuringARestoreIsNotUndone: the same hazard with the
// flush still writing when the restore lays its records over storage. The
// deletion has left pending and is not in storage yet; the restore must find
// it in the batch being written.
func TestADeleteBeingFlushedDuringARestoreIsNotUndone(t *testing.T) {
	f := newFabric(t, 3)
	store := newHeldStore()
	openDurable(f, store)
	ctx := context.Background()
	durable := func(id string) peercache.Namespace { return f.caches[id].Namespace("durable") }

	const keys = 200
	owner := f.ids[0]
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := durable(owner).Set(ctx, k, []byte("v1"), 0); err != nil {
			t.Fatalf("Set(%s): %v", k, err)
		}
	}
	flushAll(t, f)
	key := keysOwnedBy(t, f, owner, keys)[0]

	killAndSettle(t, f, f.ids[1])
	if _, err := durable(owner).Get(ctx, key); err != nil {
		t.Fatalf("Get(%s): %v", key, err)
	}
	store.failPut.Store(true)
	if err := durable(owner).Delete(ctx, key); err != nil {
		t.Fatalf("Delete(%s): %v", key, err)
	}

	killAndSettle(t, f, f.ids[2])
	store.failPut.Store(false)
	store.hold()
	defer store.open()
	got := make(chan error, 1)
	go func() {
		_, err := durable(owner).Get(ctx, key)
		got <- err
	}()
	waitFor(t, "the restore to be reading storage", func() bool { return store.waiting.Load() > 0 })

	// The flush takes the deletion out of pending and stops writing it.
	store.holdPuts()
	defer store.openPuts()
	f.clk.Advance(flushInterval)
	waitFor(t, "the flush to be writing the deletion", func() bool { return store.putWaiting.Load() > 0 })

	// The restore finishes while the deletion is in neither pending nor
	// storage.
	store.open()
	if err := <-got; !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get(%s) after the restore = %v; it was deleted, and a restore that ran "+
			"while the deletion was being flushed brought the old value back", key, err)
	}
	store.openPuts()
	if _, err := durable(owner).Get(ctx, key); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get(%s) again = %v; the deleted value came back", key, err)
	}
}
