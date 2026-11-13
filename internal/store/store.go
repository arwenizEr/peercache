package store

import (
	"fmt"
	"math"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
)

// Defaults for [Options].
const (
	// DefaultShards is the default shard count. It is
	// well above any realistic core count, so lock contention comes from
	// key collisions rather than from the shard map itself.
	DefaultShards = 256
	// MaxShards bounds the fan-out. Beyond this the per-shard maps get
	// small enough that their overhead dominates.
	MaxShards = 1 << 16
	// DefaultSweepInterval is how often the background sweeper wakes.
	DefaultSweepInterval = time.Second
	// DefaultSweepShardsPerTick bounds the work one sweep does. With the
	// default shard count and interval the whole keyspace is visited every
	// 32 seconds, and no single tick ever walks more than 1/32 of it.
	DefaultSweepShardsPerTick = 8
)

// EntryOverhead is what the store charges for one entry beyond its key and
// value bytes, in [Store.Bytes].
//
// It is an estimate, not a measurement, and it is deliberately on the high side.
// One entry costs a map slot (the key's string header and the entry struct,
// about 56 bytes on 64-bit), the slot's share of bucket and tophash overhead
// at Go's load factor, and the value's own allocation header and size-class
// rounding. 96 covers that for small values and overstates it for large ones,
// where the value's length dominates anyway. Overstating is the safe direction
// for a figure a memory budget is enforced against.
const EntryOverhead int64 = 96

// Options configures a [Store].
type Options struct {
	// Shards is the number of independently locked partitions. It is
	// rounded up to a power of two so that shard selection is a mask
	// rather than a division. Defaults to [DefaultShards].
	Shards int
	// Clock is the time source used for expiry. Required.
	Clock clock.Clock
	// SweepInterval is how often the background sweeper runs. A
	// non-positive value disables it, which is what tests that drive
	// [Store.Sweep] directly should use.
	SweepInterval time.Duration
	// SweepShardsPerTick is how many shards one sweep visits.
	SweepShardsPerTick int
	// Order, if set, issues the persistence order of every write. It is
	// called under the shard lock, so a key's orders rise in the same
	// sequence as its versions: two concurrent writes can never be stored
	// in one order and persisted in the other. Nil for a namespace without
	// persistence, whose entries carry order zero. See
	// docs/decisions/0029-persistence-as-a-segment-log.md.
	Order func() uint64
}

// entry is one stored value.
//
// expiresAt is Unix nanoseconds rather than a [time.Time] deliberately: it
// saves sixteen bytes per entry, which is sixteen megabytes at a million keys,
// and reduces the expiry check to an integer comparison. Zero means the key
// does not expire on its own.
type entry struct {
	val       []byte
	expiresAt int64
	version   uint64
	// order is the write's order from the hybrid logical clock, or zero
	// in a store built without [Options.Order].
	order uint64
}

// shard is one independently locked partition.
//
// Counters live per shard rather than on the Store so that a read on one shard
// does not invalidate another core's cache line. Each shard is separately
// allocated, so they do not share one.
type shard struct {
	mu      sync.RWMutex
	entries map[string]entry

	hits    atomic.Int64
	misses  atomic.Int64
	sets    atomic.Int64
	deletes atomic.Int64
	expired atomic.Int64
	shed    atomic.Int64

	// bytes is the shard's accounted size: every resident entry's key and
	// value plus [EntryOverhead]. Written only under mu, read without it by
	// [Store.Bytes], hence atomic.
	bytes atomic.Int64

	// floor is the highest version this shard has shed, guarded by mu. A
	// first write to an absent key starts above it, so a key shed and then
	// rewritten is never versioned at or below a copy a replica may still
	// hold. See [Store.Shed].
	floor uint64
}

// size is what one entry is charged.
func size(key string, val []byte) int64 {
	return int64(len(key)+len(val)) + EntryOverhead
}

// Store is the owned, in-memory, sharded key/value store.
//
// It is safe for concurrent use. Every method except [Store.Close] may be
// called from any goroutine at any time.
type Store struct {
	shards []*shard
	mask   uint64
	clk    clock.Clock
	// order issues persistence orders; see [Options.Order].
	order func() uint64

	sweepPerTick int
	// cursor is the next shard the sweeper will visit. It is only touched
	// by Sweep, which may be called concurrently, so it is atomic.
	cursor atomic.Uint64
	// shedCursor is the shard the next Shed starts from, for the same reason.
	shedCursor atomic.Uint64

	closeOnce sync.Once
	done      chan struct{}
	stopped   chan struct{}
	ticker    clock.Ticker
}

// New returns a [Store].
//
// If the options enable the background sweeper, New starts one goroutine that
// runs until [Store.Close]. A Store whose Close is never called leaks that
// goroutine and, on a fake clock, an armed ticker.
func New(opts Options) (*Store, error) {
	if opts.Clock == nil {
		return nil, fmt.Errorf("store: a clock is required: %w", core.ErrInvalidOption)
	}
	if opts.Shards == 0 {
		opts.Shards = DefaultShards
	}
	if opts.Shards < 1 {
		return nil, fmt.Errorf("store: shard count %d must be positive: %w", opts.Shards, core.ErrInvalidOption)
	}
	if opts.Shards > MaxShards {
		return nil, fmt.Errorf("store: shard count %d exceeds the %d limit: %w",
			opts.Shards, MaxShards, core.ErrInvalidOption)
	}
	if opts.SweepShardsPerTick <= 0 {
		opts.SweepShardsPerTick = DefaultSweepShardsPerTick
	}

	count := roundUpPow2(opts.Shards)
	s := &Store{
		shards:       make([]*shard, count),
		mask:         uint64(count - 1),
		clk:          opts.Clock,
		order:        opts.Order,
		sweepPerTick: opts.SweepShardsPerTick,
		done:         make(chan struct{}),
		stopped:      make(chan struct{}),
	}
	for i := range s.shards {
		s.shards[i] = &shard{entries: make(map[string]entry)}
	}

	if opts.SweepInterval > 0 {
		s.ticker = opts.Clock.NewTicker(opts.SweepInterval)
		go s.sweepLoop()
	} else {
		// Nothing to wait for; unblock Close immediately.
		close(s.stopped)
	}
	return s, nil
}

// maxRepresentable is the last instant [time.Time.UnixNano] can express.
// Beyond it the value silently wraps negative.
var maxRepresentable = time.Unix(0, math.MaxInt64)

// deadlineNanos returns now+ttl as Unix nanoseconds, or zero — meaning "does
// not expire" — if that instant cannot be represented.
//
// UnixNano is only defined for roughly 1678 to 2262. Without this check a
// caller asking for the maximum time.Duration, about 292 years, overflows to a
// negative deadline and the key expires *immediately* — the exact opposite of
// what was asked for, and silently. Treating an unrepresentably distant
// deadline as "never" is both the safe direction and what the caller meant.
func deadlineNanos(now time.Time, ttl time.Duration) int64 {
	deadline := now.Add(ttl)
	if deadline.Before(now) || deadline.After(maxRepresentable) {
		// Before(now) catches time.Time's own wraparound on an absurd
		// duration; After catches the UnixNano range.
		return 0
	}
	return deadline.UnixNano()
}

// roundUpPow2 returns the smallest power of two at least as large as n.
func roundUpPow2(n int) int {
	if n <= 1 {
		return 1
	}
	if n&(n-1) == 0 {
		return n
	}
	return 1 << bits.Len(uint(n-1))
}

// Shards returns the number of shards, after rounding up to a power of two.
func (s *Store) Shards() int { return len(s.shards) }

// shardFor selects the shard holding key.
func (s *Store) shardFor(key string) *shard {
	return s.shards[xxhash.Sum64String(key)&s.mask]
}

// Get returns the value and version stored under key.
//
// The returned slice is the stored one. Modifying it corrupts the store for
// every subsequent reader; copy it if you need to keep or change it.
//
// An expired key reports absent even if the sweeper has not reached it yet.
// Reads are the authority on expiry; the sweeper only reclaims memory.
func (s *Store) Get(key string) ([]byte, uint64, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()

	if !ok {
		sh.misses.Add(1)
		return nil, 0, false
	}
	// The clock is only read for keys that actually have a deadline, so a
	// namespace that never sets a TTL never pays for one.
	if e.expiresAt != 0 && s.clk.Now().UnixNano() >= e.expiresAt {
		sh.misses.Add(1)
		return nil, 0, false
	}
	sh.hits.Add(1)
	return e.val, e.version, true
}

// Exists reports whether key is present and unexpired, without returning the
// value.
//
// It is not a race-free precondition for anything. Between Exists and the
// operation it was meant to guard, the key may expire or be deleted.
func (s *Store) Exists(key string) bool {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()

	if !ok {
		return false
	}
	return e.expiresAt == 0 || s.clk.Now().UnixNano() < e.expiresAt
}

// Set stores val under key for ttl and returns the new version.
//
// A non-positive ttl means the key does not expire on its own, as does a ttl so
// large that the resulting deadline cannot be represented. val is copied, so
// the caller may reuse the buffer.
//
// Versions increase by one per write and are per key, not global: they exist so
// that a cached copy elsewhere can be recognised as stale, which only ever
// compares two versions of the same key. A global counter would add contention
// for a total order nobody reads.
//
// A version is not monotonic across a key's whole lifetime. It lives with the
// entry, so an entry the sweeper reclaims restarts the sequence at 1 — and
// whether an expired entry has been reclaimed depends on when the sweeper last
// passed. A deleted or shed entry does not restart it: the shard keeps a floor
// at the highest version it removed that way, and a first write starts above it,
// because a replica may still hold that version. Do not treat
// "lower version" as proof that a value is stale; see docs/decisions/open.md
// question 18.
func (s *Store) Set(key string, val []byte, ttl time.Duration) uint64 {
	return s.Put(key, val, ttl).Version
}

// Write is what one write to the store assigned.
type Write struct {
	Version uint64
	// Order is the persistence order, or zero without [Options.Order].
	Order uint64
	// ExpiresAt is the stored deadline in Unix nanoseconds, zero for none.
	ExpiresAt int64
}

// Put is [Store.Set], also reporting the order the write was given.
func (s *Store) Put(key string, val []byte, ttl time.Duration) Write {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = deadlineNanos(s.clk.Now(), ttl)
	}

	// Copy before taking the lock: the allocation is the expensive part and
	// it does not need to be serialised.
	stored := make([]byte, len(val))
	copy(stored, val)

	sh := s.shardFor(key)
	sh.mu.Lock()
	old, had := sh.entries[key]
	version := old.version + 1
	if !had && version <= sh.floor {
		version = sh.floor + 1
	}
	order := s.nextOrder()
	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version, order: order}
	delta := size(key, stored)
	if had {
		delta -= size(key, old.val)
	}
	sh.bytes.Add(delta)
	sh.mu.Unlock()

	sh.sets.Add(1)
	return Write{Version: version, Order: order, ExpiresAt: expiresAt}
}

// nextOrder issues an order, or zero without [Options.Order]. Callers hold the
// shard lock.
func (s *Store) nextOrder() uint64 {
	if s.order == nil {
		return 0
	}
	return s.order()
}

// Order returns the persistence order of key's live entry. A restore from
// storage merges a record only if it is newer than this.
func (s *Store) Order(key string) (uint64, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, ok := sh.entries[key]
	sh.mu.RUnlock()
	if !ok || (e.expiresAt != 0 && s.clk.Now().UnixNano() >= e.expiresAt) {
		return 0, false
	}
	return e.order, true
}

// SetIfAbsent stores val under key for ttl unless a live entry is already
// there, and reports whether it stored. When it does not, it returns the
// resident value — the same stored slice [Store.Get] would.
//
// It is what a load stores with. A load reads its source, then stores what it
// read; a Set that lands in between is newer than the load's value, and an
// unconditional store would put the older value back over it. An expired entry
// counts as absent, as it does to every reader.
func (s *Store) SetIfAbsent(key string, val []byte, ttl time.Duration) ([]byte, bool) {
	var expiresAt int64
	now := s.clk.Now()
	if ttl > 0 {
		expiresAt = deadlineNanos(now, ttl)
	}
	stored := make([]byte, len(val))
	copy(stored, val)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	old, had := sh.entries[key]
	if had && (old.expiresAt == 0 || now.UnixNano() < old.expiresAt) {
		return old.val, false
	}
	version := old.version + 1
	if !had && version <= sh.floor {
		version = sh.floor + 1
	}
	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version, order: s.nextOrder()}
	delta := size(key, stored)
	if had {
		delta -= size(key, old.val)
	}
	sh.bytes.Add(delta)
	sh.sets.Add(1)
	return stored, true
}

// Snapshot returns up to limit unexpired entries whose keys sort after the
// given key, in key order, and reports whether more remain.
//
// It exists for catch-up, which has to enumerate an owner's keys for a
// restarted replica. Two properties matter and neither is free.
//
// It is *paged*, because a namespace can hold millions of keys and a single
// response proportional to the keyspace is the shape of every out-of-memory
// bug this project has already fixed once.
//
// It is *sorted*, because paging needs a stable cursor. Sorting a shard's keys
// per call is real work, which is why this is not on any request path: it runs
// when a node rejoins, not when a caller reads.
//
// The result is a point-in-time sample per shard rather than a consistent
// snapshot across shards. Catch-up does not need one: anything written during
// the scan is also being replicated live, so a key missed here arrives by the
// ordinary path.
func (s *Store) Snapshot(after string, limit int, want func(key string) bool) ([]SnapshotEntry, bool) {
	if limit <= 0 {
		return nil, false
	}

	// Keys first, values only for the page. Copying every candidate's value
	// before cutting to the limit, as an earlier version did, made each page
	// allocate in proportion to the whole store.
	var candidates []string
	now := s.clk.Now().UnixNano()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for key, e := range sh.entries {
			if key <= after {
				continue
			}
			if e.expiresAt != 0 && now >= e.expiresAt {
				continue
			}
			if want != nil && !want(key) {
				continue
			}
			candidates = append(candidates, key)
		}
		sh.mu.RUnlock()
	}

	sort.Strings(candidates)
	more := len(candidates) > limit
	if more {
		candidates = candidates[:limit]
	}

	page := make([]SnapshotEntry, 0, len(candidates))
	for _, key := range candidates {
		sh := s.shardFor(key)
		sh.mu.RLock()
		e, ok := sh.entries[key]
		if ok && (e.expiresAt == 0 || now < e.expiresAt) {
			val := make([]byte, len(e.val))
			copy(val, e.val)
			page = append(page, SnapshotEntry{Key: key, Value: val, Version: e.version, ExpiresAt: e.expiresAt, Order: e.order})
		}
		// A key removed between the two passes is simply absent from the
		// page. The caller's cursor is the last key returned, so the next
		// page continues correctly either way.
		sh.mu.RUnlock()
	}
	return page, more
}

// SnapshotEntry is one key's worth of a [Store.Snapshot].
type SnapshotEntry struct {
	Key   string
	Value []byte
	// Version is the owner's version, carried so a replica that is later
	// promoted continues the sequence rather than restarting it.
	Version uint64
	// ExpiresAt is the entry's deadline in Unix nanoseconds, or zero for no
	// expiry. Sent as an absolute instant rather than a remaining duration
	// so that a slow transfer does not extend the key's life.
	ExpiresAt int64
	// Order is the entry's persistence order, carried for the same reason
	// as Version: a node that later owns the key must continue it.
	Order uint64
}

// Restore installs a value at a version chosen by the caller, reporting
// whether it was installed.
//
// This is the promotion path and nothing else should use it. A node taking
// ownership of keys it held as replicas must continue the previous owner's
// version sequence rather than start a new one: every cached copy in the
// cluster is stamped against that sequence, and restarting it would make stale
// copies look newer than the truth and be kept in preference to it.
//
// A version not newer than the resident one is refused. Promotion races with
// ordinary writes — a caller can write to this node between the moment it
// becomes the owner and the moment the drain reaches a key — and the newer
// value must win whichever arrived first.
//
// order is the persistence order the value was written under, carried with the
// replica; it is installed as it is, never reissued, so that the promoted value
// keeps its place in the key's history.
func (s *Store) Restore(key string, val []byte, ttl time.Duration, version, order uint64) bool {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = deadlineNanos(s.clk.Now(), ttl)
	}

	stored := make([]byte, len(val))
	copy(stored, val)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	existing, ok := sh.entries[key]
	// By order when both carry one, for the reason replicas do: a version
	// restarts when a key changes hands, an order does not.
	if ok && order != 0 && existing.order != 0 {
		if existing.order >= order {
			return false
		}
	} else if ok && existing.version >= version {
		return false
	}
	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version, order: order}
	delta := size(key, stored)
	if ok {
		delta -= size(key, existing.val)
	}
	sh.bytes.Add(delta)
	sh.sets.Add(1)
	return true
}

// Delete removes key and reports whether it was present and unexpired.
//
// Deleting an absent key is not an error: the caller's postcondition already
// holds. The boolean distinguishes the two for callers that care, such as
// metrics.
func (s *Store) Delete(key string) bool {
	existed, _ := s.Remove(key)
	return existed
}

// Remove is [Store.Delete], also returning the order the deletion was given.
// A deletion gets an order whether or not the key was present: its tombstone
// is persisted either way, because an older write of the key may already be in
// storage and must not come back on restore.
func (s *Store) Remove(key string) (bool, uint64) {
	sh := s.shardFor(key)

	sh.mu.Lock()
	order := s.nextOrder()
	e, ok := sh.entries[key]
	if ok {
		delete(sh.entries, key)
		sh.bytes.Add(-size(key, e.val))
		// The deletion's tombstone is replicated asynchronously and may
		// not arrive, leaving a replica holding this version. A rewrite
		// must start above it, as after a shed. See
		// docs/decisions/open.md question 42.
		if e.version > sh.floor {
			sh.floor = e.version
		}
	}
	sh.mu.Unlock()

	sh.deletes.Add(1)
	if !ok {
		return false, order
	}
	if e.expiresAt != 0 && s.clk.Now().UnixNano() >= e.expiresAt {
		// It was already gone as far as any reader was concerned, so
		// report it as absent even though bytes were reclaimed.
		return false, order
	}
	return true, order
}

// Len returns the number of stored entries.
//
// The count includes entries that have expired but not yet been swept, because
// counting exactly would mean walking every shard under a read lock and
// consulting the clock per entry. Use it for capacity reporting, not for
// assertions about visibility.
func (s *Store) Len() int {
	total := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		total += len(sh.entries)
		sh.mu.RUnlock()
	}
	return total
}

// Counters is a snapshot of a store's activity.
type Counters struct {
	// Hits and Misses count reads that found and did not find a live value.
	// An expired key counts as a miss, which is what a caller experienced.
	Hits   int64
	Misses int64
	// Sets and Deletes count write operations, including deletes of absent
	// keys.
	Sets    int64
	Deletes int64
	// Expired counts entries reclaimed by the sweeper. It does not count
	// expired keys observed by a reader, because those are still resident.
	Expired int64
	// Entries is [Store.Len] at the time of the snapshot.
	Entries int64
	// Shed counts entries removed by [Store.Shed] to relieve memory pressure.
	Shed int64
}

// Stats returns a snapshot of the counters.
//
// The fields are read independently, so the snapshot is not a consistent
// instant. Counters are for trends, and a lock that made them consistent would
// cost more than the information is worth.
func (s *Store) Stats() Counters {
	var c Counters
	for _, sh := range s.shards {
		c.Hits += sh.hits.Load()
		c.Misses += sh.misses.Load()
		c.Sets += sh.sets.Load()
		c.Deletes += sh.deletes.Load()
		c.Expired += sh.expired.Load()
		c.Shed += sh.shed.Load()

		sh.mu.RLock()
		c.Entries += int64(len(sh.entries))
		sh.mu.RUnlock()
	}
	return c
}

// Bytes returns the store's accounted size: every resident entry's key and
// value, plus [EntryOverhead] each.
//
// Expired entries the sweeper has not reclaimed are counted, because they are
// still holding the memory. The figure is the sum of per-shard counters read
// independently, so under concurrent writes it is approximate in the same way
// [Store.Stats] is, and exact at rest.
func (s *Store) Bytes() int64 {
	var total int64
	for _, sh := range s.shards {
		total += sh.bytes.Load()
	}
	return total
}

// Shed removes entries until it has freed at least target bytes, or the store
// is empty, and returns how many bytes it freed.
//
// It is the memory budget's pressure valve, and it is not an eviction policy:
// entries are taken in map iteration order, which Go randomises, starting from a
// shard that rotates on every call. An LRU would need every read to record an
// access, and the read path is the one this store exists to keep cheap.
//
// visit is called once per removed entry with its key and version, after the
// shard's lock is released, so it may do real work — the caller uses it to
// replicate the removal. It must not call back into this store's shard it was
// removed from while expecting the entry to be present.
//
// Overshoot is at most one entry: Shed checks the target after each removal.
//
// # Why it raises a version floor
//
// A replica holds the owner's copy at the owner's version. If this store shed a
// key at version 5 and then started its rewrite at version 1, as an ordinary
// absent key does, the replica would refuse the rewrite and a
// failover would promote the shed value over the one last written. The caller
// replicates a removal through visit, which would prevent that if it arrived —
// but correctness must not depend on a message arriving.
// So each shard remembers the highest version it shed, and a first write to an
// absent key in that shard starts above it. Delete raises the same floor, for
// the same reason: its tombstone is replicated asynchronously and may be lost.
// Sweep does not, because an expiry happens on every holder
// at the same deadline and leaves no copy for a rewrite to have to outrank.
func (s *Store) Shed(target int64, visit func(key string, version uint64)) int64 {
	if target <= 0 {
		return 0
	}
	type removed struct {
		key     string
		version uint64
	}
	var freed int64
	start := s.shedCursor.Add(1) - 1
	for i := 0; i < len(s.shards) && freed < target; i++ {
		sh := s.shards[(start+uint64(i))&s.mask]
		var out []removed
		sh.mu.Lock()
		for key, e := range sh.entries {
			if freed >= target {
				break
			}
			delete(sh.entries, key)
			if e.version > sh.floor {
				sh.floor = e.version
			}
			n := size(key, e.val)
			sh.bytes.Add(-n)
			freed += n
			out = append(out, removed{key, e.version})
		}
		sh.mu.Unlock()
		sh.shed.Add(int64(len(out)))
		if visit != nil {
			for _, r := range out {
				visit(r.key, r.version)
			}
		}
	}
	return freed
}

// ShardOf returns the index of the shard holding key. It exists so that tests
// can observe how work spreads across shards.
func (s *Store) ShardOf(key string) int {
	return int(xxhash.Sum64String(key) & s.mask)
}

// Sweep visits the next n shards in rotation and removes expired entries,
// returning how many it removed.
//
// It is exported so that a test can drive expiry deterministically instead of
// advancing a clock and hoping the sweeper goroutine has been scheduled.
func (s *Store) Sweep(n int) int {
	if n <= 0 {
		return 0
	}
	if n > len(s.shards) {
		n = len(s.shards)
	}
	now := s.clk.Now().UnixNano()
	removed := 0

	start := s.cursor.Add(uint64(n)) - uint64(n)
	for i := 0; i < n; i++ {
		sh := s.shards[(start+uint64(i))&s.mask]
		fromShard := 0
		sh.mu.Lock()
		for key, e := range sh.entries {
			if e.expiresAt != 0 && now >= e.expiresAt {
				delete(sh.entries, key)
				sh.bytes.Add(-size(key, e.val))
				fromShard++
			}
		}
		sh.mu.Unlock()
		if fromShard > 0 {
			sh.expired.Add(int64(fromShard))
			removed += fromShard
		}
	}
	return removed
}

// sweepLoop runs the background sweeper until Close.
func (s *Store) sweepLoop() {
	defer close(s.stopped)
	for {
		select {
		case <-s.done:
			return
		case <-s.ticker.C():
			s.Sweep(s.sweepPerTick)
		}
	}
}

// Close stops the background sweeper and waits for it to exit.
//
// It is idempotent. Data is not written anywhere, because there is nowhere to
// write it: this store is memory only by design.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		if s.ticker != nil {
			s.ticker.Stop()
		}
	})
	<-s.stopped
	return nil
}

// MergeIfNewer applies a record read back from persistent storage, reporting
// whether it changed anything and, for a value, the version it was installed
// at — the caller replicates it under that version.
//
// A record is applied only if this store holds nothing live for key, or holds
// something with a lower order: memory that is newer than storage always wins,
// which is what makes a restore safe to run concurrently with writes, more than
// once, and in any order. A value is installed at a fresh version, as a first
// write would be, above the shard's floor; a tombstone removes an older live
// entry and does nothing otherwise. The caller drops expired records first.
func (s *Store) MergeIfNewer(key string, val []byte, expiresAt int64, order uint64, tombstone bool) (uint64, bool) {
	var stored []byte
	if !tombstone {
		stored = make([]byte, len(val))
		copy(stored, val)
	}

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	old, had := sh.entries[key]
	live := had && (old.expiresAt == 0 || s.clk.Now().UnixNano() < old.expiresAt)
	if live && old.order >= order {
		return 0, false
	}
	if tombstone {
		if !live {
			return 0, false
		}
		delete(sh.entries, key)
		sh.bytes.Add(-size(key, old.val))
		if old.version > sh.floor {
			sh.floor = old.version
		}
		sh.deletes.Add(1)
		return old.version, true
	}

	version := old.version + 1
	if !had && version <= sh.floor {
		version = sh.floor + 1
	}
	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version, order: order}
	delta := size(key, stored)
	if had {
		delta -= size(key, old.val)
	}
	sh.bytes.Add(delta)
	sh.sets.Add(1)
	return version, true
}
