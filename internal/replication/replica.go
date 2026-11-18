package replication

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/digest"
	"github.com/arwenizEr/peercache/internal/pager"
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
// Measured, at the worst point, as the owned store's is: a map that has just
// doubled. A slot holds a string header and an Entry (a slice, a stamp, three
// deadlines and orders, a string — 104 bytes on 64-bit), and pays its share of
// the empty slots and control words, and the key's and value's rounding. With
// 22-byte keys and 48-byte values, a million replicas took 237 bytes each
// beyond key, value and sender in live heap; TestAccountedBytesCoverTheLiveHeap
// holds the figure to it. It was 128, an estimate that called itself high.
//
// It is charged per slot the map keeps, not per live entry: on the most
// entries the map has held since it was last built, which deleting does not
// lower; see namespaceReplicas.shrink.
const EntryOverhead int64 = 240

// dataSize is what one replica's key, value and sender take.
func dataSize(key string, e Entry) int64 {
	return int64(len(key) + len(e.Value) + len(e.From))
}

// replicaShards is how many maps one namespace's copies are split across, by
// the low bits of the key's hash.
//
// # Why a namespace is sharded
//
// A shed rebuilds what it shed from, since a Go map gives back no slots until
// it is rebuilt (see shrink). With one map a namespace, every relief round
// rebuilt the whole namespace under its one lock: about 160 ms for a million
// replicas, during which every copy arriving for the namespace waited. Split
// 64 ways, a shed empties shards whole, each rebuilt for nothing, and copies
// what is left of only the last one it took from; every other pass — a drain,
// a sweep chunk, an eviction — holds one shard's lock at a time, and a write
// waits only for its own shard. The owned store has been split the same way,
// 256 shards a store, since M3.
const replicaShards = 64

// namespaceReplicas is one namespace's worth of replicated keys.
type namespaceReplicas struct {
	shards [replicaShards]replicaShard
	// bytes is the store's accounted size, which put, del and shrink keep.
	bytes *atomic.Int64
	// digest summarises every copy's key and owner version; see
	// [Store.Digest]. Kept by put and del, the only writers of entries; its
	// counters are atomic, so the shards share it.
	digest digest.Set
	// cursor rotates the shard a shed, a bounded drain or a sweep chunk
	// starts from, so that repeated passes spread over the namespace.
	cursor atomic.Uint64
}

// replicaShard is one of a namespace's maps.
type replicaShard struct {
	mu      sync.RWMutex
	entries map[string]Entry
	// high is the most entries the map has held since it was last built;
	// see shrink. Each is charged EntryOverhead.
	high int
}

func newNamespaceReplicas(bytes *atomic.Int64) *namespaceReplicas {
	nr := &namespaceReplicas{bytes: bytes}
	for i := range nr.shards {
		nr.shards[i].entries = make(map[string]Entry)
	}
	return nr
}

// shard returns the shard that holds key.
func (nr *namespaceReplicas) shard(key string) *replicaShard {
	return &nr.shards[uint64(digest.KeyOf(key))%replicaShards]
}

// start returns where the next rotating pass begins.
func (nr *namespaceReplicas) start() int {
	return int((nr.cursor.Add(1) - 1) % replicaShards)
}

// len reports how many copies the namespace holds, taking each shard's lock
// in turn.
func (nr *namespaceReplicas) len() int {
	n := 0
	for i := range nr.shards {
		sh := &nr.shards[i]
		sh.mu.RLock()
		n += len(sh.entries)
		sh.mu.RUnlock()
	}
	return n
}

// put stores e under key in sh, keeping the digest and the accounted size in
// step. Callers hold sh.mu.
func (nr *namespaceReplicas) put(sh *replicaShard, key string, e Entry) {
	k := digest.KeyOf(key)
	delta := dataSize(key, e)
	if old, held := sh.entries[key]; held {
		nr.digest.Replace(k, old.Stamp.Version, e.Stamp.Version)
		delta -= dataSize(key, old)
	} else {
		nr.digest.Add(k, e.Stamp.Version)
	}
	sh.entries[key] = e
	if len(sh.entries) > sh.high {
		delta += int64(len(sh.entries)-sh.high) * EntryOverhead
		sh.high = len(sh.entries)
	}
	nr.bytes.Add(delta)
}

// shrink rebuilds a shard's map once it holds under three quarters of the
// entries it has held, or whenever it holds fewer if force is set, and returns
// the bytes that frees. Callers hold sh.mu, and call it after removing rather
// than from del, which runs inside ranges over the map.
//
// A Go map never gives back the slots of what is deleted from it. A store
// that took a million copies and was shed or promoted down to 400,000 still
// held the million's slots, while Bytes, which charged each live entry its
// share of a map at its worst point, said 400,000: on kind a pod's replica map
// held 239 MB of heap after a failover promoted two thirds of it, for 136 MB
// accounted, and the pod was OOM-killed with its budget reading 460 MB of 512.
// So the overhead is charged on the slots kept, high, and only a rebuild
// gives it back. At three quarters the copy is paid for by the deletions
// before it, and the map never holds more than a third more slots than its
// entries were last built with.
func (nr *namespaceReplicas) shrink(sh *replicaShard, force bool) int64 {
	n := len(sh.entries)
	if n == sh.high || (!force && n*4 >= sh.high*3) {
		return 0
	}
	fresh := make(map[string]Entry, n)
	for key, e := range sh.entries {
		fresh[key] = e
	}
	freed := int64(sh.high-n) * EntryOverhead
	sh.entries, sh.high = fresh, n
	nr.bytes.Add(-freed)
	return freed
}

// unlockShrinking shrinks a shard's map if it has become sparse, then
// releases its lock. Every path that removes entries leaves through it.
func (nr *namespaceReplicas) unlockShrinking(sh *replicaShard) {
	nr.shrink(sh, false)
	sh.mu.Unlock()
}

// del removes key from sh with its digest contribution. Callers hold sh.mu;
// it is safe inside a range over sh.entries.
func (nr *namespaceReplicas) del(sh *replicaShard, key string) {
	if old, held := sh.entries[key]; held {
		nr.digest.Remove(digest.KeyOf(key), old.Stamp.Version)
		delete(sh.entries, key)
		nr.bytes.Add(-dataSize(key, old))
	}
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
	// Order is the write's order from its owner's hybrid logical clock. It
	// is what orders copies (see [Newer]); zero only from a peer on a release
	// that ordered persistent namespaces alone. Promotion installs it
	// unchanged.
	Order uint64
	// Prev is the order of the value this write replaced on its owner, zero
	// if none or unknown. With Order and From it is what fork detection
	// compares; see [Store.Put].
	Prev uint64
	// From is the node that sent this copy. With Order and Prev it is what
	// makes split brain detectable; see [Forked].
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
	nr = newNamespaceReplicas(&s.bytes)
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
// conflict reports that the held copy and e fork the key's history (see
// [Forked]): two nodes wrote it with neither replacing the other's write, which
// under one owner per group is direct evidence of the split brain that must be
// detected and counted. It is reported, not resolved: conflicting writes are
// never automatically merged.
func (s *Store) Put(namespace, key string, e Entry) (accepted, conflict bool) {
	nr := s.namespaceFor(namespace)
	sh := nr.shard(key)

	sh.mu.Lock()
	defer sh.mu.Unlock()

	existing, held := sh.entries[key]
	if held {
		conflict = Forked(existing, e)
		if !Newer(e, existing) {
			// Equal copies from the same writer are a retransmission,
			// which is normal and not worth counting. A strictly older
			// copy is a genuine out-of-order delivery, and is.
			if Newer(existing, e) {
				s.versionConflicts.Add(1)
			}
			return false, conflict
		}
	}
	nr.put(sh, key, e)
	return true, conflict
}

// Forked reports whether two copies of one key, from different writers, show
// the key's history forking: neither write descends from the other.
//
// A write carries Prev, the order of the value it replaced on its owner. A new
// owner that caught up before writing replaces the old owner's last value, so
// its Prev is at least that value's order: a handover. A replica that missed
// some writes sees a Prev above what it holds: also linear. Only when each
// write's Prev is below the other's order did each writer replace something
// older than the other's write — two owners writing at once, or one writing
// over a value it never received. That is the split brain decision 5 says is
// detected and never merged.
//
// It replaced a test of "two writers in one epoch", and epochs are per view: on
// different nodes they coincide by accident as easily as they differ, so after
// one rolling restart on kind the old test counted tens of thousands of splits
// that never happened (M16, open question 50). A copy without the information
// — no Prev, as from an earlier release, a first write, or a key its sender
// owns and relays — is not judged: a missed fork is a smaller harm than an
// invented one in the counter whose non-zero value means investigate.
func Forked(held, in Entry) bool {
	if held.From == "" || in.From == "" || held.From == in.From {
		return false
	}
	if held.Order == 0 || in.Order == 0 || held.Prev == 0 || in.Prev == 0 {
		return false
	}
	return in.Prev < held.Order && held.Prev < in.Order
}

// Newer reports whether a supersedes b.
//
// By order when both carry one: an order is issued by a hybrid logical clock
// every owner advances past whatever it has received, so it agrees with the
// key's history across ownership changes. The stamp is the fallback for a copy
// from a peer that issues no order — an earlier release — and is only sound
// within one owner's lineage, because its epoch is per view: a returning owner
// holds a smaller epoch than the interim owner whose copy it supersedes.
func Newer(a, b Entry) bool {
	if a.Order != 0 && b.Order != 0 {
		return a.Order > b.Order
	}
	return a.Stamp.NewerThan(b.Stamp)
}

// Get returns a replicated value.
func (s *Store) Get(namespace, key string) (Entry, bool) {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}

	sh := nr.shard(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	e, held := sh.entries[key]
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

	sh := nr.shard(key)
	sh.mu.Lock()
	defer nr.unlockShrinking(sh)
	_, held := sh.entries[key]
	if !held {
		return false
	}
	nr.del(sh, key)
	return true
}

// DeleteUnlessNewer removes a replicated key unless the copy held is newer than
// stamp, and reports whether it removed anything.
//
// It is the deletion counterpart of [Store.Put]'s ordering. A tombstone from an
// owner on an older epoch — arriving after a partition heals, say — must not
// remove a copy a newer owner wrote, any more than an old write may replace it.
func (s *Store) DeleteUnlessNewer(namespace, key string, stamp core.Stamp, order uint64) bool {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return false
	}

	sh := nr.shard(key)
	sh.mu.Lock()
	defer nr.unlockShrinking(sh)
	e, held := sh.entries[key]
	if !held || Newer(e, Entry{Stamp: stamp, Order: order}) {
		return false
	}
	nr.del(sh, key)
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
		total += nr.len()
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
// Removal and return happen under the key's shard lock so that a concurrent Put
// cannot slip a copy in behind the drain and leave it stranded in the replica
// store after this node has taken ownership: a Put to a shard the drain has
// passed lands after it, as a Put after a drain of one map always could, and
// is promoted by the replicate handler's own ownership check.
func (s *Store) Drain(namespace string, keep func(key string) bool) map[string]Entry {
	return s.DrainUpTo(namespace, -1, keep)
}

// DrainUpTo is [Store.Drain] that stops after max entries; a negative max is
// no limit, and zero takes nothing.
//
// It exists for a consumer with bounded room. Hand-off can queue only so many
// orphans; draining the rest only to put them back, as reconciliation once
// did, cycled every orphan beyond the queue through the store once a second —
// 10.5 million returns on one kind pod to rehome 97,000. What is not taken
// stays where it is, untouched, for the next round.
func (s *Store) DrainUpTo(namespace string, max int, keep func(key string) bool) map[string]Entry {
	if max == 0 {
		return nil
	}
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return nil
	}

	var taken map[string]Entry
	first := nr.start()
	for i := 0; i < replicaShards; i++ {
		if max > 0 && len(taken) >= max {
			break
		}
		sh := &nr.shards[(first+i)%replicaShards]
		sh.mu.Lock()
		for key, e := range sh.entries {
			if max > 0 && len(taken) >= max {
				break
			}
			if !keep(key) {
				continue
			}
			if taken == nil {
				taken = make(map[string]Entry)
			}
			taken[key] = e
			nr.del(sh, key)
		}
		nr.unlockShrinking(sh)
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

	return nr.evictWhere(func(key string, _ Entry) bool { return stillMine(key) })
}

// evictWhere removes every copy for which keep returns false, a shard at a
// time, and reports how many it removed.
func (nr *namespaceReplicas) evictWhere(keep func(key string, e Entry) bool) int {
	dropped := 0
	for i := range nr.shards {
		sh := &nr.shards[i]
		sh.mu.Lock()
		for key, e := range sh.entries {
			if keep(key, e) {
				continue
			}
			nr.del(sh, key)
			dropped++
		}
		nr.unlockShrinking(sh)
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

// Sweep sizing; see [Store.SweepExpired].
const (
	// SweepWholeBelow is the size under which a namespace is swept whole.
	SweepWholeBelow = 4096
	// SweepChunk is how many entries one hold of a namespace's lock visits.
	SweepChunk = 1024
	// SweepShare is the fraction of a namespace one sweep visits at least:
	// one in SweepShare.
	SweepShare = 32
	// sweepMoreAbove is the expired share of a chunk, as one in
	// sweepMoreAbove, above which a sweep goes on past its share.
	sweepMoreAbove = 4
	// sweepMostShare bounds one sweep at one in sweepMostShare entries,
	// however many it finds expired.
	sweepMostShare = 4
)

// SweepExpired removes replicas whose deadline has passed and returns how many
// it removed and how many entries it visited. It visits a sample, not every entry: an expired replica is never
// served — every read checks the deadline — so a sweep reclaims memory, and
// can take its time.
//
// The owned store sweeps its own expired entries; until this existed nothing
// swept replicas, so under a TTL workload every replica a node had ever held
// stayed resident after its key expired everywhere else.
//
// # Why a sample
//
// It visited every entry, every second, with the namespace's lock held, which
// every copy arriving for that namespace waits on. On kind at 1.35 million
// replicas a pod that was a quarter of the pod's CPU and a stall of tens of
// milliseconds a second for every replicated write, which as the store grew
// was most of the throughput the shop lost.
//
// It is Redis's active expiry, shaped to a Go map. A namespace under
// [SweepWholeBelow] entries is swept whole, as before. A larger one is visited
// [SweepChunk] entries at a time, from a rotating shard, holding one shard's
// lock at a time (see replicaShards), until a
// [SweepShare]th of it has been seen; and on while a chunk finds more than a
// quarter of what it visits expired, up to a quarter of the namespace. A range
// over a Go map starts at a random place, so each chunk is a fresh sample, and
// an expired replica is found within a few dozen sweeps.
func (s *Store) SweepExpired(now int64) (removed, visited int) {
	s.mu.RLock()
	namespaces := make([]*namespaceReplicas, 0, len(s.ns))
	for _, nr := range s.ns {
		namespaces = append(namespaces, nr)
	}
	s.mu.RUnlock()

	for _, nr := range namespaces {
		r, v := s.sweepNamespace(nr, now)
		removed += r
		visited += v
	}
	return removed, visited
}

// sweepNamespace is SweepExpired for one namespace.
func (s *Store) sweepNamespace(nr *namespaceReplicas, now int64) (removed, visited int) {
	held := nr.len()
	if held < SweepWholeBelow {
		return s.sweepChunk(nr, now, held)
	}

	share, most := held/SweepShare, held/sweepMostShare
	for visited < most {
		found, seen := s.sweepChunk(nr, now, SweepChunk)
		removed += found
		visited += seen
		if seen == 0 || (visited >= share && found*sweepMoreAbove <= seen) {
			break
		}
	}
	return removed, visited
}

// sweepChunk visits up to n entries, from a rotating shard and wherever its
// map's range starts, on into the next shards until it has seen n, and
// removes the expired ones. It holds one shard's lock at a time.
func (s *Store) sweepChunk(nr *namespaceReplicas, now int64, n int) (removed, seen int) {
	first := nr.start()
	for i := 0; i < replicaShards && seen < n; i++ {
		sh := &nr.shards[(first+i)%replicaShards]
		sh.mu.Lock()
		for key, e := range sh.entries {
			if seen == n {
				break
			}
			seen++
			if !e.Live(now) {
				nr.del(sh, key)
				removed++
			}
		}
		nr.unlockShrinking(sh)
	}
	return removed, seen
}

// Slots returns how many map slots the store charges [EntryOverhead] for,
// across namespaces: at least [Store.Len], and more by what has been removed
// since a namespace's map was last rebuilt.
func (s *Store) Slots() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, nr := range s.ns {
		for i := range nr.shards {
			sh := &nr.shards[i]
			sh.mu.RLock()
			n += int64(sh.high)
			sh.mu.RUnlock()
		}
	}
	return n
}

// Bytes returns the accounted size of every replica held, across namespaces:
// keys, values and sender IDs, and [EntryOverhead] per slot (see [Store.Slots]).
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
// Namespaces, and each namespace's shards, are visited from a rotating start
// and entries taken in map iteration order, so repeated small sheds spread
// across the keyspace rather than emptying one namespace; a shard holds a
// hash-chosen 64th of its namespace's keys, so emptying a few is still a
// random sample of them. Overshoot is at most one entry. Each shard it sheds
// from is rebuilt afterwards, which is what frees the slots; see
// namespaceReplicas.shrink and replicaShards.
//
// keepFrom, when not nil, names for each namespace the order from which copies
// are kept: a copy whose Order is at or past it is not shed. It is how the
// budget spares the copies of persistent writes their owner may not have
// flushed yet, which are then the write's only other holder; see
// docs/decisions/open.md question 60. A namespace it returns math.MaxUint64
// for keeps nothing.
func (s *Store) Shed(target int64, keepFrom func(namespace string) uint64) int64 {
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
		name := names[(start+uint64(i))%uint64(len(names))]
		s.mu.RLock()
		nr := s.ns[name]
		s.mu.RUnlock()
		if nr == nil {
			continue
		}
		floor := uint64(math.MaxUint64)
		if keepFrom != nil {
			floor = keepFrom(name)
		}
		first := nr.start()
		for j := 0; j < replicaShards && freed < target; j++ {
			sh := &nr.shards[(first+j)%replicaShards]
			sh.mu.Lock()
			// What a rebuild after the loop gives back: the slots already
			// spare, and one per copy shed.
			var data int64
			slots := int64(sh.high-len(sh.entries)) * EntryOverhead
			for key, e := range sh.entries {
				if freed+data+slots >= target {
					break
				}
				if e.Order >= floor {
					continue
				}
				data += dataSize(key, e)
				slots += EntryOverhead
				nr.del(sh, key)
				s.shed.Add(1)
			}
			// Rebuilt whatever share was shed: shedding is for memory, and
			// a map gives none of its slots back until it is rebuilt. A
			// shard shed whole is rebuilt for nothing.
			freed += data + nr.shrink(sh, true)
			sh.mu.Unlock()
		}
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

	return nr.evictWhere(keep)
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

	var out []string
	nr.each(func(key string, _ Entry) { out = append(out, key) })
	return out
}

// each calls fn for every copy, a shard at a time under its read lock.
func (nr *namespaceReplicas) each(fn func(key string, e Entry)) {
	for i := range nr.shards {
		sh := &nr.shards[i]
		sh.mu.RLock()
		for key, e := range sh.entries {
			fn(key, e)
		}
		sh.mu.RUnlock()
	}
}

// lookup returns key's copy with its value cloned, if it is held and live at
// now and want, when not nil, accepts it.
func (nr *namespaceReplicas) lookup(key string, now int64, want func(key string) bool) (Entry, bool) {
	sh := nr.shard(key)
	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()
	if !ok || (e.ExpiresAt != 0 && now >= e.ExpiresAt) || (want != nil && !want(key)) {
		return Entry{}, false
	}
	e.Value = append([]byte(nil), e.Value...)
	return e, true
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

	var candidates []string
	nr.each(func(key string, e Entry) {
		if key <= after || (e.ExpiresAt != 0 && now >= e.ExpiresAt) {
			return
		}
		if want != nil && !want(key) {
			return
		}
		candidates = append(candidates, key)
	})

	sort.Strings(candidates)
	more := len(candidates) > limit
	if more {
		candidates = candidates[:limit]
	}

	page := make([]KeyedEntry, 0, len(candidates))
	for _, key := range candidates {
		// Removed between the passes, and skipped; the cursor still
		// advances.
		if e, ok := nr.lookup(key, now, nil); ok {
			page = append(page, KeyedEntry{Key: key, Entry: e})
		}
	}
	return page, more
}

// String implements [fmt.Stringer] for debugging.
func (s *Store) String() string {
	return fmt.Sprintf("replication.Store(%d keys across %d namespaces)", s.Len(), len(s.Namespaces()))
}

// Digest returns the running digest of every copy held for namespace, without
// walking them. See docs/decisions/0039-cluster-view.md.
func (s *Store) Digest(namespace string) [digest.Parts]digest.Pair {
	return s.namespaceFor(namespace).digest.Snapshot()
}

// SnapshotFrom is [Store.Snapshot] for a pass through a namespace's copies,
// with the pass's sorted key list kept by p under id; see
// store.Store.SnapshotFrom, whose rules it follows.
func (s *Store) SnapshotFrom(p *pager.Pager, id, namespace, after string, limit int, now int64, want func(key string) bool) ([]KeyedEntry, bool) {
	s.mu.RLock()
	nr, ok := s.ns[namespace]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	build := func() []string {
		var keys []string
		nr.each(func(key string, e Entry) {
			if e.ExpiresAt != 0 && now >= e.ExpiresAt {
				return
			}
			if want != nil && !want(key) {
				return
			}
			keys = append(keys, key)
		})
		return keys
	}
	for {
		keys, more := p.Page(id, after, limit, build)
		page := make([]KeyedEntry, 0, len(keys))
		for _, key := range keys {
			if e, ok := nr.lookup(key, now, want); ok {
				page = append(page, KeyedEntry{Key: key, Entry: e})
			}
		}
		if len(page) > 0 || !more {
			return page, more
		}
		after = keys[len(keys)-1]
	}
}
