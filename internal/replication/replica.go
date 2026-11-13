package replication

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/arwenizEr/peercache/internal/core"
)

// Store holds copies of keys this node does not own.
//
// It is deliberately separate from the owned store rather than a flag on an
// entry, for three reasons that all became clear while designing promotion.
//
// Ownership moves. A key can be owned, then replicated, then owned again as
// members come and go, and a single store would have to rewrite a per-entry
// flag on every membership change — touching every key to record something that
// is derivable from the ownership table in nanoseconds.
//
// The two have different lifecycles. Both expire at the owner's deadline, but
// a replica also leaves when the owner deletes it or this node stops being a
// replica for it, and promotion moves it into the owned store. Sweeping them
// together would need two policies in one loop; [Store.SweepExpired] is the
// replica half.
//
// And [Stats.ReplicaKeys] has to mean something. An operator asking "how much
// of this pod's memory is data it does not own" gets an answer from a separate
// map and a guess from a shared one.
//
// A Store is safe for concurrent use.
type Store struct {
	// shards are keyed by namespace, because promotion happens a namespace
	// at a time and a returning node catches up a namespace at a time.
	mu sync.RWMutex
	ns map[string]*namespaceReplicas

	// versionConflicts counts copies rejected because what is already held
	// is newer.
	//
	// "Version conflicts" is a correctness metric in its own right, separately from split-brain detections, and the two
	// are genuinely different questions. A conflict is two nodes writing one
	// key under one epoch — a correctness failure. This is one node's writes
	// arriving out of order, which asynchronous replication makes ordinary
	// and which the stamp ordering then handles correctly.
	//
	// It is exported because a rising rate still means something an operator
	// wants to know: reordering under load, or a replica being fed through a
	// path that keeps redelivering stale copies. A steady low rate is normal.
	versionConflicts atomic.Int64

	// bytes is the accounted size of every held replica: key, value and
	// sender ID, plus [EntryOverhead] each. Changed only under the owning
	// namespace's lock.
	bytes atomic.Int64
	// shed counts replicas dropped by [Store.Shed], and shedCursor rotates
	// which namespace the next Shed starts from.
	shed       atomic.Int64
	shedCursor atomic.Uint64
}

// EntryOverhead is what the store charges for one replica beyond its key, value
// and sender ID.
//
// An estimate, on the high side for the same reason as the owned store's: a map
// slot holding a string header and an Entry (a slice, a stamp, a TTL and a
// string — about 80 bytes on 64-bit), bucket overhead, and the value's allocation
// rounding.
const EntryOverhead int64 = 128

// size is what one replica is charged.
func size(key string, e Entry) int64 {
	return int64(len(key)+len(e.Value)+len(e.From)) + EntryOverhead
}

// namespaceReplicas is one namespace's worth of replicated keys.
type namespaceReplicas struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// Entry is one replicated value and the stamp the owner issued for it.
//
// The stamp is what makes a replica usable after promotion: a node that becomes
// the owner of a key must be able to continue the version sequence rather than
// restart it, or every caller holding a cached copy would see the version go
// backwards. See [core.Stamp] and docs/decisions/open.md question 18.
type Entry struct {
	Value []byte
	Stamp core.Stamp
	// ExpiresAt is the owner's deadline for this key in Unix nanoseconds, or
	// zero for no expiry.
	//
	// A deadline, not a duration. It was a duration until a test showed what
	// that cost: a promoted replica restarted the key's lifetime at promotion,
	// and a key that had already expired on its owner came back with a fresh
	// one. An expiry is never replicated — it happens on every holder at the
	// same instant — so every holder has to agree on the instant.
	ExpiresAt int64
	// ReceivedAt is when this node accepted the copy, in Unix nanoseconds
	// by its own clock. It is set by the receiver and never crosses the
	// network. Replica reads serve a copy only while it is younger than the
	// namespace's L1 TTL; see docs/decisions/0028-hot-key-reads.md.
	ReceivedAt int64
	// Order is the write's persistence order, zero in a namespace without
	// persistence. Promotion installs it unchanged.
	Order uint64
	// From is the node that sent this copy. It is what makes split-brain
	// detectable: two different nodes writing the same key under the same
	// epoch means two of them believe they own it.
	From string
}

// NewStore returns an empty replica store.
func NewStore() *Store {
	return &Store{ns: make(map[string]*namespaceReplicas)}
}

// namespaceFor returns the per-namespace map, creating it if needed.
func (s *Store) namespaceFor(namespace string) *namespaceReplicas {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if ok {
		return nr
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if nr, ok := s.ns[namespace]; ok {
		return nr
	}
	nr = &namespaceReplicas{entries: make(map[string]Entry)}
	s.ns[namespace] = nr
	return nr
}

// Put stores a replicated value, reporting whether it was accepted and whether
// accepting it revealed a conflict.
//
// A copy is rejected if what is already held is newer, by [core.Stamp]
// ordering. That is not merely an optimisation: replication is asynchronous by
// default, so two updates to one key can arrive out of order, and a late
// delivery of an old value must not undo a new one.
//
// conflict reports that a *different* node wrote this key under the *same*
// epoch. Under one owner per group that cannot happen, so it is direct evidence
// that two nodes believed they owned the same key at the same time — the
// split-brain that must be detected and counted. It is reported, not resolved:
// conflicting writes are never automatically merged.
func (s *Store) Put(namespace, key string, e Entry) (accepted, conflict bool) {
	nr := s.namespaceFor(namespace)

	nr.mu.Lock()
	defer nr.mu.Unlock()

	existing, held := nr.entries[key]
	if held {
		if existing.Stamp.Epoch == e.Stamp.Epoch && existing.From != "" && e.From != "" &&
			existing.From != e.From {
			conflict = true
		}
		if !e.Stamp.NewerThan(existing.Stamp) {
			// Equal stamps from the same writer are a retransmission,
			// which is normal and not worth counting. A strictly older
			// stamp is a genuine out-of-order delivery, and is.
			if existing.Stamp.NewerThan(e.Stamp) {
				s.versionConflicts.Add(1)
			}
			return false, conflict
		}
	}
	nr.entries[key] = e
	delta := size(key, e)
	if held {
		delta -= size(key, existing)
	}
	s.bytes.Add(delta)
	return true, conflict
}

// Get returns a replicated value.
func (s *Store) Get(namespace, key string) (Entry, bool) {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}

	nr.mu.RLock()
	defer nr.mu.RUnlock()
	e, held := nr.entries[key]
	return e, held
}

// Delete removes a replicated key.
func (s *Store) Delete(namespace, key string) bool {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return false
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()
	e, held := nr.entries[key]
	if !held {
		return false
	}
	delete(nr.entries, key)
	s.bytes.Add(-size(key, e))
	return true
}

// DeleteUnlessNewer removes a replicated key unless the copy held is newer than
// stamp, and reports whether it removed anything.
//
// It is the deletion counterpart of [Store.Put]'s ordering. A tombstone from an
// owner on an older epoch — arriving after a partition heals, say — must not
// remove a copy a newer owner wrote, any more than an old write may replace it.
func (s *Store) DeleteUnlessNewer(namespace, key string, stamp core.Stamp) bool {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return false
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()
	e, held := nr.entries[key]
	if !held || e.Stamp.NewerThan(stamp) {
		return false
	}
	delete(nr.entries, key)
	s.bytes.Add(-size(key, e))
	return true
}

// VersionConflicts returns the number of copies rejected because a newer one
// was already held.
func (s *Store) VersionConflicts() int64 { return s.versionConflicts.Load() }

// Len reports how many replicated keys this node holds, across namespaces.
func (s *Store) Len() int {
	s.mu.RLock()
	namespaces := make([]*namespaceReplicas, 0, len(s.ns))
	for _, nr := range s.ns {
		namespaces = append(namespaces, nr)
	}
	s.mu.RUnlock()

	total := 0
	for _, nr := range namespaces {
		nr.mu.RLock()
		total += len(nr.entries)
		nr.mu.RUnlock()
	}
	return total
}

// Namespaces returns the namespaces holding at least one replica, so that
// promotion and catch-up can iterate without knowing what exists.
func (s *Store) Namespaces() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.ns))
	for name := range s.ns {
		out = append(out, name)
	}
	return out
}

// Drain removes and returns every replica in a namespace whose key satisfies
// keep, and reports the rest.
//
// This is the promotion path. A node that has become the owner of keys it held
// as replicas takes them out of here and puts them in its owned store, and the
// two must not both hold the same key: a key present in both would be served
// from whichever the read path consulted first, which is not a decision anybody
// made.
//
// Removal and return happen under one lock so that a concurrent Put cannot slip
// a copy in behind the drain and leave it stranded in the replica store after
// this node has taken ownership.
func (s *Store) Drain(namespace string, keep func(key string) bool) map[string]Entry {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return nil
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()

	var taken map[string]Entry
	for key, e := range nr.entries {
		if !keep(key) {
			continue
		}
		if taken == nil {
			taken = make(map[string]Entry)
		}
		taken[key] = e
		delete(nr.entries, key)
		s.bytes.Add(-size(key, e))
	}
	return taken
}

// Evict removes every replica in a namespace that this node should no longer
// hold, reporting how many it dropped.
//
// A node stops being a replica for a key when membership changes, and keeping
// the copy afterwards is not free: it is memory nobody will read and a value
// that will drift further from the truth with every write it does not receive.
//
// # This is not how a moved replica is dropped
//
// `stillMine` is answered from the local ranking, which says nothing about
// whether a node that *now* ranks for the key holds a copy. After a partition
// heals it usually does not, and dropping on that basis loses the key — see
// handoff.go in the root package and docs/decisions/0020-hand-off-before-evicting.md.
//
// The caller therefore uses [Store.Drain] with the inverted predicate to take
// the orphans out and hand them on, and reaches for this only where there is no
// replica set to hand them to: a namespace at replication factor 1, where a copy
// held here is a leftover from a configuration that no longer applies.
func (s *Store) Evict(namespace string, stillMine func(key string) bool) int {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return 0
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()

	dropped := 0
	for key, e := range nr.entries {
		if stillMine(key) {
			continue
		}
		delete(nr.entries, key)
		s.bytes.Add(-size(key, e))
		dropped++
	}
	return dropped
}

// Live reports whether e is unexpired at now (Unix nanoseconds).
func (e Entry) Live(now int64) bool { return e.ExpiresAt == 0 || now < e.ExpiresAt }

// Remaining returns the time e has left at now, and whether it has any. A key
// with no deadline has all the time in the world, reported as zero and true:
// zero is the owned store's "does not expire".
func (e Entry) Remaining(now int64) (int64, bool) {
	if e.ExpiresAt == 0 {
		return 0, true
	}
	left := e.ExpiresAt - now
	return left, left > 0
}

// DeadlineFrom converts a TTL received over the wire into a deadline at the
// moment of arrival. Zero or negative means no expiry, as on the owner.
func DeadlineFrom(now, ttlNanos int64) int64 {
	if ttlNanos <= 0 {
		return 0
	}
	return now + ttlNanos
}

// SweepExpired removes every replica whose deadline has passed and returns how
// many it removed.
//
// The owned store sweeps its own expired entries; until this existed nothing
// swept replicas, so under a TTL workload every replica a node had ever held
// stayed resident after its key expired everywhere else.
func (s *Store) SweepExpired(now int64) int {
	s.mu.RLock()
	namespaces := make([]*namespaceReplicas, 0, len(s.ns))
	for _, nr := range s.ns {
		namespaces = append(namespaces, nr)
	}
	s.mu.RUnlock()

	removed := 0
	for _, nr := range namespaces {
		nr.mu.Lock()
		for key, e := range nr.entries {
			if !e.Live(now) {
				delete(nr.entries, key)
				s.bytes.Add(-size(key, e))
				removed++
			}
		}
		nr.mu.Unlock()
	}
	return removed
}

// Bytes returns the accounted size of every replica held, across namespaces.
func (s *Store) Bytes() int64 { return s.bytes.Load() }

// ShedCount returns how many replicas [Store.Shed] has dropped.
func (s *Store) ShedCount() int64 { return s.shed.Load() }

// Shed drops replicas until it has freed at least target bytes, or none are
// left, and returns how many bytes it freed.
//
// It is the memory budget's middle tier. Dropping a replica lowers that key's
// replication factor until the owner next writes it or this node next catches
// up; it never loses the key while the owner holds it, which is why replicas are
// shed before owned data. Nothing is sent anywhere: the owner's copy is the
// authority and is untouched.
//
// Namespaces are visited from a rotating start and entries taken in map
// iteration order, so repeated small sheds spread across the keyspace rather
// than emptying one namespace. Overshoot is at most one entry.
func (s *Store) Shed(target int64) int64 {
	if target <= 0 {
		return 0
	}
	s.mu.RLock()
	names := make([]string, 0, len(s.ns))
	for name := range s.ns {
		names = append(names, name)
	}
	s.mu.RUnlock()
	if len(names) == 0 {
		return 0
	}
	// Map order is random; sort so that the rotating cursor means something.
	sort.Strings(names)

	var freed int64
	start := s.shedCursor.Add(1) - 1
	for i := 0; i < len(names) && freed < target; i++ {
		s.mu.RLock()
		nr := s.ns[names[(start+uint64(i))%uint64(len(names))]]
		s.mu.RUnlock()
		if nr == nil {
			continue
		}
		nr.mu.Lock()
		for key, e := range nr.entries {
			if freed >= target {
				break
			}
			delete(nr.entries, key)
			n := size(key, e)
			s.bytes.Add(-n)
			s.shed.Add(1)
			freed += n
		}
		nr.mu.Unlock()
	}
	return freed
}

// EvictWhere removes every replica in a namespace for which keep returns false,
// reporting how many it dropped. It is [Store.Evict] with the entry visible to
// the predicate, for decisions that depend on who sent a copy and not only on
// its key.
func (s *Store) EvictWhere(namespace string, keep func(key string, e Entry) bool) int {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return 0
	}

	nr.mu.Lock()
	defer nr.mu.Unlock()

	dropped := 0
	for key, e := range nr.entries {
		if keep(key, e) {
			continue
		}
		delete(nr.entries, key)
		s.bytes.Add(-size(key, e))
		dropped++
	}
	return dropped
}

// Keys returns the keys held for a namespace, sorted for determinism.
//
// Used by catch-up, which compares what a node holds against what it should
// hold. Allocating a slice per call is acceptable because catch-up runs on a
// membership change, not on a request.
func (s *Store) Keys(namespace string) []string {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return nil
	}

	nr.mu.RLock()
	defer nr.mu.RUnlock()
	out := make([]string, 0, len(nr.entries))
	for key := range nr.entries {
		out = append(out, key)
	}
	return out
}

// KeyedEntry is one key's worth of a [Store.Snapshot].
type KeyedEntry struct {
	Key string
	Entry
}

// Snapshot returns up to limit live replicas in namespace whose keys sort
// after after and satisfy want, in key order, and whether more remain.
//
// It is catch-up's view of the replica store, paged and sorted for the same
// reasons as the owned store's Snapshot: a stable cursor, and no message
// proportional to the namespace. It exists because a node restarted faster than
// failure detection still owns its keys, and their newest copies are held here,
// as replicas, by its peers; nobody was ever promoted to hold them as owner.
func (s *Store) Snapshot(namespace, after string, limit int, now int64, want func(key string) bool) ([]KeyedEntry, bool) {
	if limit <= 0 {
		return nil, false
	}
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}

	nr.mu.RLock()
	var candidates []string
	for key, e := range nr.entries {
		if key <= after || (e.ExpiresAt != 0 && now >= e.ExpiresAt) {
			continue
		}
		if want != nil && !want(key) {
			continue
		}
		candidates = append(candidates, key)
	}
	nr.mu.RUnlock()

	sort.Strings(candidates)
	more := len(candidates) > limit
	if more {
		candidates = candidates[:limit]
	}

	page := make([]KeyedEntry, 0, len(candidates))
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	for _, key := range candidates {
		e, ok := nr.entries[key]
		if !ok || (e.ExpiresAt != 0 && now >= e.ExpiresAt) {
			continue // removed between the passes; the cursor still advances
		}
		e.Value = append([]byte(nil), e.Value...)
		page = append(page, KeyedEntry{Key: key, Entry: e})
	}
	return page, more
}

// String implements [fmt.Stringer] for debugging.
func (s *Store) String() string {
	return fmt.Sprintf("replication.Store(%d keys across %d namespaces)", s.Len(), len(s.Namespaces()))
}
