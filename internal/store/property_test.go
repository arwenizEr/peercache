package store_test

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/store"
	"github.com/peercache/peercache/internal/testkit"
)

// This file tests the store against a reference implementation under randomised
// operation sequences.
//
// Every other test in this package checks something I already thought of. The
// interactions that actually break a sharded, versioned, expiring store are the
// ones nobody thought of: an overwrite that lands in the same nanosecond as an
// expiry, a delete that resets a version sequence a later reader depends on, a
// sweep that runs between a Set and its Get. A model does not need to have
// thought of them.
//
// The reference is deliberately the dumbest thing that could work: one map, no
// shards, no sweeper, expiry checked inline. If the real store and the obvious
// store ever disagree, the real one is wrong.

// modelEntry mirrors what the store keeps per key.
type modelEntry struct {
	val       []byte
	expiresAt int64 // Unix nanoseconds; 0 means never
	version   uint64
}

// model is a single-map reference store.
type model struct {
	entries map[string]modelEntry
	nowNs   func() int64
	// floor is the store's per-shard version floor: the highest version a
	// shard has deleted or shed. A first write to an absent key starts above
	// it (open question 42).
	floor   map[int]uint64
	shardOf func(string) int
}

func newModel(nowNs func() int64, shardOf func(string) int) *model {
	return &model{entries: make(map[string]modelEntry), nowNs: nowNs,
		floor: make(map[int]uint64), shardOf: shardOf}
}

func (m *model) live(e modelEntry) bool {
	return e.expiresAt == 0 || m.nowNs() < e.expiresAt
}

func (m *model) get(key string) ([]byte, uint64, bool) {
	e, ok := m.entries[key]
	if !ok || !m.live(e) {
		return nil, 0, false
	}
	return e.val, e.version, true
}

func (m *model) set(key string, val []byte, ttl time.Duration) uint64 {
	var expiresAt int64
	if ttl > 0 {
		// Mirrors the store's overflow rule: an unrepresentable deadline
		// means "never".
		deadline := m.nowNs() + int64(ttl)
		if deadline > m.nowNs() && deadline <= maxRepresentableNs {
			expiresAt = deadline
		}
	}
	old, had := m.entries[key]
	version := old.version + 1
	if f := m.floor[m.shardOf(key)]; !had && version <= f {
		version = f + 1
	}
	stored := make([]byte, len(val))
	copy(stored, val)
	m.entries[key] = modelEntry{val: stored, expiresAt: expiresAt, version: version}
	return version
}

func (m *model) delete(key string) bool {
	e, ok := m.entries[key]
	delete(m.entries, key)
	if ok && e.version > m.floor[m.shardOf(key)] {
		m.floor[m.shardOf(key)] = e.version
	}
	return ok && m.live(e)
}

// sweep drops everything already expired, which is the most a rotation of the
// real sweeper can ever remove.
func (m *model) sweep() {
	for key, e := range m.entries {
		if !m.live(e) {
			delete(m.entries, key)
		}
	}
}

// maxRepresentableNs is the last instant time.Time.UnixNano can express.
const maxRepresentableNs = math.MaxInt64

// TestStoreMatchesReferenceModel drives random operation sequences against both
// the real store and a one-map reference, and requires them to agree on every
// read.
func TestStoreMatchesReferenceModel(t *testing.T) {
	const operations = 20_000
	const keyspace = 64

	seed := testkit.Seed(t)
	rng := rand.New(rand.NewSource(seed))

	s, fc := newStore(t, func(o *store.Options) { o.Shards = 8 })
	ref := newModel(func() int64 { return fc.Now().UnixNano() }, s.ShardOf)

	keys := make([]string, keyspace)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d/session", i)
	}

	ttls := []time.Duration{0, -time.Second, time.Nanosecond, time.Millisecond, time.Second, time.Hour}

	for op := 0; op < operations; op++ {
		key := keys[rng.Intn(len(keys))]

		switch rng.Intn(10) {
		case 0, 1, 2, 3: // Set, weighted heavily so entries exist to read
			ttl := ttls[rng.Intn(len(ttls))]
			val := []byte(fmt.Sprintf("v%d", op))

			gotVersion := s.Set(key, val, ttl)
			wantVersion := ref.set(key, val, ttl)
			if gotVersion != wantVersion {
				t.Fatalf("op %d: Set(%q, ttl=%v) returned version %d, model says %d",
					op, key, ttl, gotVersion, wantVersion)
			}

		case 4: // Delete
			got := s.Delete(key)
			want := ref.delete(key)
			if got != want {
				t.Fatalf("op %d: Delete(%q) = %v, model says %v", op, key, got, want)
			}

		case 5: // Sweep every shard
			// A full rotation on both sides, not a partial one.
			//
			// Reclaiming an entry resets its version to 1, so a
			// partial sweep here would make the two disagree about
			// versions for reasons that are entirely about which
			// shards the cursor happened to reach. That is a real
			// property of the store, recorded in
			// TestVersionRestartsOnlyIfTheEntryWasReclaimed and in
			// docs/decisions/open.md, and it is not what this model
			// is for. Partial sweeps are covered by
			// TestSweepIsIncremental.
			s.Sweep(s.Shards())
			ref.sweep()

		case 6: // Advance the clock
			fc.Advance(time.Duration(rng.Intn(3)) * time.Millisecond)

		case 7: // Exists must agree with Get about presence
			_, _, want := ref.get(key)
			if got := s.Exists(key); got != want {
				t.Fatalf("op %d: Exists(%q) = %v, model says %v", op, key, got, want)
			}

		default: // Get
			gotVal, gotVersion, gotOK := s.Get(key)
			wantVal, wantVersion, wantOK := ref.get(key)

			if gotOK != wantOK {
				t.Fatalf("op %d: Get(%q) present=%v, model says %v (now=%v)",
					op, key, gotOK, wantOK, fc.Now())
			}
			if !gotOK {
				continue
			}
			if string(gotVal) != string(wantVal) {
				t.Fatalf("op %d: Get(%q) = %q, model says %q", op, key, gotVal, wantVal)
			}
			if gotVersion != wantVersion {
				t.Fatalf("op %d: Get(%q) version %d, model says %d",
					op, key, gotVersion, wantVersion)
			}
		}
	}

	// Final agreement across the whole keyspace.
	for _, key := range keys {
		gotVal, gotVersion, gotOK := s.Get(key)
		wantVal, wantVersion, wantOK := ref.get(key)
		if gotOK != wantOK || gotVersion != wantVersion || string(gotVal) != string(wantVal) {
			t.Fatalf("final state for %q: store has (%q, v%d, %v), model has (%q, v%d, %v)",
				key, gotVal, gotVersion, gotOK, wantVal, wantVersion, wantOK)
		}
	}
}

// TestSweepDoesNotDisturbLiveEntries drives the sweeper concurrently with
// writers and readers.
//
// Sweep takes each shard's write lock and deletes while other goroutines are
// setting and getting. Nothing in the hand-written tests runs those together,
// so nothing would catch a sweep that dropped a key written a nanosecond
// earlier.
func TestSweepDoesNotDisturbLiveEntries(t *testing.T) {
	const writers = 8
	const perWriter = 3000

	s, _ := newStore(t, func(o *store.Options) { o.Shards = 16 })

	done := make(chan struct{})
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		for {
			select {
			case <-done:
				return
			default:
			}
			s.Sweep(4)
		}
	}()

	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			for i := 0; i < perWriter; i++ {
				key := fmt.Sprintf("w%d:%d", w, i)
				// No TTL, so the sweeper has no business touching it.
				s.Set(key, []byte("v"), 0)
				if _, _, ok := s.Get(key); !ok {
					errs <- fmt.Errorf("writer %d: key %q vanished immediately after Set", w, key)
					return
				}
			}
			errs <- nil
		}(w)
	}

	for w := 0; w < writers; w++ {
		if err := <-errs; err != nil {
			close(done)
			<-swept
			t.Fatal(err)
		}
	}
	close(done)
	<-swept

	if got := s.Len(); got != writers*perWriter {
		t.Fatalf("Len = %d, want %d: the sweeper reclaimed entries that never expired",
			got, writers*perWriter)
	}
}
