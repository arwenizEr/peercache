package l1

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	otter "github.com/maypok86/otter"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
)

// writeStripes is the number of locks guarding read-modify-write sequences.
//
// The version check reads the resident entry and then conditionally replaces
// it, and the underlying cache offers no compare-and-set. Without a lock, two
// concurrent Puts can both observe no newer entry and both write, leaving
// whichever finished last resident — which may be the older version. That is
// precisely the failure version stamping exists to prevent, so "the TTL will
// clean it up eventually" is not an acceptable answer.
//
// Reads never take these locks. Stripes are keyed by the same hash as the key
// itself, so unrelated keys never contend.
const writeStripes = 256

// Defaults and limits for [Options].
const (
	// DefaultCapacity is the number of non-owned entries kept before
	// eviction begins.
	DefaultCapacity = 100_000
	// DefaultMaxTTL is the hard ceiling on how stale a non-owned read may
	// be. It is short on purpose: this is the only bound that exists on
	// staleness, since invalidation broadcasts may be lost.
	DefaultMaxTTL = 30 * time.Second
	// DefaultTTL applies when a caller supplies none.
	DefaultTTL = 5 * time.Second
)

// EntryOverhead is what the tier charges for one entry beyond its key and value
// bytes, in [Cache.Bytes]. An estimate on the high side: the entry struct (a
// slice, a stamp and a deadline), the library's node and its policy metadata,
// and the value's allocation rounding.
const EntryOverhead int64 = 128

// size is what one entry is charged.
func size(key string, e entry) int64 { return int64(len(key)+len(e.val)) + EntryOverhead }

// Options configures a [Cache].
type Options struct {
	// Capacity is the maximum number of entries. Defaults to
	// [DefaultCapacity].
	Capacity int
	// Clock is the time source used for expiry. Required.
	Clock clock.Clock
	// MaxTTL is the ceiling applied to every entry, whatever the caller
	// asks for. Defaults to [DefaultMaxTTL].
	MaxTTL time.Duration
	// DefaultTTL applies to entries stored with a non-positive TTL.
	// Defaults to [DefaultTTL] and is itself clamped to MaxTTL.
	DefaultTTL time.Duration
}

// entry is one cached copy of a key owned elsewhere.
type entry struct {
	val []byte
	// stamp is the owner's (epoch, version) pair rather than a bare
	// version. A version restarts when the owner reclaims its store entry,
	// so comparing versions across an ownership change can reject a fresh
	// value as stale — see docs/decisions/open.md question 18.
	stamp     core.Stamp
	expiresAt int64
	// floor marks an entry that holds no value, only a stamp: what an
	// invalidation leaves behind in a namespace with replica reads. It
	// reads as a miss and refuses any Put older than its stamp. See
	// [Cache.Floor].
	floor bool
}

// Cache is a bounded, TTL-bounded, version-stamped cache of non-owned keys.
//
// It is safe for concurrent use.
type Cache struct {
	inner      otter.Cache[string, entry]
	clk        clock.Clock
	maxTTL     time.Duration
	defaultTTL time.Duration

	// stripes serialise read-modify-write sequences per key. See
	// [writeStripes].
	stripes [writeStripes]sync.Mutex

	hits     atomic.Int64
	misses   atomic.Int64
	expiries atomic.Int64
	puts     atomic.Int64
	rejects  atomic.Int64
	shed     atomic.Int64

	// bytes is the tier's accounted size. Puts add to it directly; removals
	// are reported by the library's deletion listener, which runs on its
	// maintenance goroutine, so the figure lags a removal briefly and never
	// lags an addition. That errs towards overstating, the safe direction
	// for a budget. It is a pointer because the listener is built before the
	// Cache that owns it.
	bytes *atomic.Int64
}

// New returns a [Cache].
//
// The caller must call [Cache.Close]; the underlying library runs a maintenance
// goroutine.
func New(opts Options) (*Cache, error) {
	if opts.Clock == nil {
		return nil, fmt.Errorf("l1: a clock is required: %w", core.ErrInvalidOption)
	}
	if opts.Capacity == 0 {
		opts.Capacity = DefaultCapacity
	}
	if opts.Capacity < 1 {
		return nil, fmt.Errorf("l1: capacity %d must be positive: %w", opts.Capacity, core.ErrInvalidOption)
	}
	if opts.MaxTTL == 0 {
		opts.MaxTTL = DefaultMaxTTL
	}
	if opts.MaxTTL < 0 {
		return nil, fmt.Errorf("l1: max TTL %v must be positive: %w", opts.MaxTTL, core.ErrInvalidOption)
	}
	if opts.DefaultTTL == 0 {
		opts.DefaultTTL = DefaultTTL
	}
	if opts.DefaultTTL < 0 {
		return nil, fmt.Errorf("l1: default TTL %v must be positive: %w", opts.DefaultTTL, core.ErrInvalidOption)
	}
	if opts.DefaultTTL > opts.MaxTTL {
		opts.DefaultTTL = opts.MaxTTL
	}

	bytes := new(atomic.Int64)
	inner, err := otter.MustBuilder[string, entry](opts.Capacity).
		// Every cause is a removal of the reported value, including
		// Replaced: the listener is told about the old value, and the
		// Put that replaced it charged the new one.
		DeletionListener(func(key string, e entry, _ otter.DeletionCause) {
			bytes.Add(-size(key, e))
		}).
		Build()
	if err != nil {
		return nil, fmt.Errorf("l1: building the cache: %w", err)
	}
	return &Cache{
		bytes:      bytes,
		inner:      inner,
		clk:        opts.Clock,
		maxTTL:     opts.MaxTTL,
		defaultTTL: opts.DefaultTTL,
	}, nil
}

// MaxTTL returns the ceiling applied to every entry.
func (c *Cache) MaxTTL() time.Duration { return c.maxTTL }

// stripeFor returns the lock guarding read-modify-write sequences on key.
func (c *Cache) stripeFor(key string) *sync.Mutex {
	return &c.stripes[xxhash.Sum64String(key)%writeStripes]
}

// Get returns the cached value and the owner-issued version it was stamped
// with.
//
// An entry past its deadline reports absent and is removed. There is no way to
// read an expired entry, which is what makes the TTL a real bound on staleness
// rather than a hint.
//
// The returned slice is the stored one; do not modify it.
func (c *Cache) Get(key string) ([]byte, core.Stamp, bool) {
	e, ok := c.inner.Get(key)
	if !ok {
		c.misses.Add(1)
		return nil, core.Stamp{}, false
	}
	if c.clk.Now().UnixNano() >= e.expiresAt {
		// Drop it here rather than waiting for capacity pressure: an
		// expired non-owned copy has negative value, since holding it
		// only makes a later bug more likely.
		//
		// Under the stripe, and re-checked: a Put may have replaced the
		// entry with a fresh one between the read above and here, and
		// deleting that would turn a hit into an avoidable round trip.
		stripe := c.stripeFor(key)
		stripe.Lock()
		if current, ok := c.inner.Get(key); ok && c.clk.Now().UnixNano() >= current.expiresAt {
			c.inner.Delete(key)
			c.expiries.Add(1)
		}
		stripe.Unlock()

		c.misses.Add(1)
		return nil, core.Stamp{}, false
	}
	if e.floor {
		c.misses.Add(1)
		return nil, core.Stamp{}, false
	}
	c.hits.Add(1)
	return e.val, e.stamp, true
}

// Put caches val under key with the owner-issued stamp.
//
// ttl is clamped into (0, MaxTTL]: a non-positive value becomes the configured
// default, and anything larger than MaxTTL becomes MaxTTL. There is deliberately
// no way to store an unbounded entry.
//
// A Put whose stamp is not newer than the stored one is rejected. Without that
// check, an invalidation and a repopulation racing on the network could leave
// the older value resident — the exact failure that version stamping exists to
// prevent.
//
// "Newer" is [core.Stamp.NewerThan]: the epoch dominates the version, so a
// value from a new owner supersedes one from the previous owner even if the new
// owner's version sequence has restarted at 1.
//
// It reports whether the value was stored.
func (c *Cache) Put(key string, val []byte, stamp core.Stamp, ttl time.Duration) bool {
	if ttl <= 0 {
		ttl = c.defaultTTL
	}
	if ttl > c.maxTTL {
		ttl = c.maxTTL
	}

	// Copy outside the lock: the allocation is the expensive part and does
	// not need to be serialised.
	stored := make([]byte, len(val))
	copy(stored, val)

	stripe := c.stripeFor(key)
	stripe.Lock()
	defer stripe.Unlock()

	now := c.clk.Now().UnixNano()
	if existing, ok := c.inner.Get(key); ok {
		live := now < existing.expiresAt
		// A floor admits its own stamp: the owner's answer to the read an
		// invalidation provoked carries exactly that stamp, and refusing
		// it would send every read to the owner until the floor expired.
		stale := !stamp.NewerThan(existing.stamp)
		if existing.floor {
			stale = existing.stamp.NewerThan(stamp)
		}
		if live && stale {
			c.rejects.Add(1)
			return false
		}
	}

	c.puts.Add(1)
	e := entry{
		val:       stored,
		stamp:     stamp,
		expiresAt: saturatingDeadline(now, ttl),
	}
	// Charged before the Set, so that a removal the listener reports for this
	// very entry can never be subtracted before it was added.
	c.bytes.Add(size(key, e))
	if !c.inner.Set(key, e) {
		// The library refused it outright; nothing is resident.
		c.bytes.Add(-size(key, e))
		return false
	}
	return true
}

// saturatingDeadline returns now+ttl in Unix nanoseconds, clamped to the
// largest representable instant rather than wrapping.
//
// Unlike the owned store, this tier cannot answer an unrepresentable deadline
// with "never expires": every L1 entry must have one, because the TTL is the
// only bound on how stale a non-owned read can be. So it saturates. Wrapping
// would produce a negative deadline and expire the entry immediately, which is
// the opposite failure and just as silent.
func saturatingDeadline(now int64, ttl time.Duration) int64 {
	deadline := now + int64(ttl)
	if deadline < now {
		return math.MaxInt64
	}
	return deadline
}

// Invalidate removes key unconditionally.
//
// This is the local half of an invalidation broadcast. The broadcast itself is
// best effort; this is not.
func (c *Cache) Invalidate(key string) { c.inner.Delete(key) }

// InvalidateOlderThan removes key if the resident copy predates stamp,
// reporting whether it removed anything.
//
// It exists so that an invalidation which arrives after a newer value has
// already been cached does not throw that newer value away. Losing a fresh copy
// costs a round trip; keeping a stale one costs correctness, so the asymmetry
// matters.
func (c *Cache) InvalidateOlderThan(key string, stamp core.Stamp) bool {
	stripe := c.stripeFor(key)
	stripe.Lock()
	defer stripe.Unlock()

	e, ok := c.inner.Get(key)
	if !ok || !stamp.NewerThan(e.stamp) {
		return false
	}
	c.inner.Delete(key)
	return true
}

// Floor replaces a live copy older than stamp with a floor: a marker that holds
// no value, reads as a miss, and refuses any [Cache.Put] older than stamp until
// ttl passes. It reports whether it set one.
//
// It is the invalidation path for namespaces with replica reads
// (docs/decisions/0028). Invalidation and replication leave the owner on
// separate queues, so this node can hear "stamp S exists" before the replica it
// would refill from has S. Deleting the copy, as [Cache.InvalidateOlderThan]
// does, would let that refill put the old value straight back.
//
// Nothing is set for a key with no live copy. A node that holds nothing for a
// key has not read it within a TTL, and a floor for every invalidated key on
// every node would fill the tier with markers for keys nobody here reads.
// Without a floor such a read is still bounded by the TTL; the floor only makes
// it fresher sooner.
func (c *Cache) Floor(key string, stamp core.Stamp, ttl time.Duration) bool {
	if ttl <= 0 || ttl > c.maxTTL {
		ttl = c.maxTTL
	}
	stripe := c.stripeFor(key)
	stripe.Lock()
	defer stripe.Unlock()

	now := c.clk.Now().UnixNano()
	existing, ok := c.inner.Get(key)
	if !ok || now >= existing.expiresAt || !stamp.NewerThan(existing.stamp) {
		return false
	}
	e := entry{stamp: stamp, expiresAt: saturatingDeadline(now, ttl), floor: true}
	c.bytes.Add(size(key, e))
	if !c.inner.Set(key, e) {
		c.bytes.Add(-size(key, e))
		return false
	}
	return true
}

// Below reports whether a live entry for key — a copy or a floor — carries a
// stamp newer than stamp. A reader uses it to discard a replica's answer that
// this node already knows to be superseded.
func (c *Cache) Below(key string, stamp core.Stamp) bool {
	e, ok := c.inner.Get(key)
	if !ok || c.clk.Now().UnixNano() >= e.expiresAt {
		return false
	}
	return e.stamp.NewerThan(stamp)
}

// Clear removes everything.
//
// The library does not report a clear to the deletion listener, so the
// accounting is reset here. Removals queued before the clear are reported
// before it completes — the library processes them in order and waits — so
// none of them can arrive afterwards and drive the figure negative.
func (c *Cache) Clear() {
	c.inner.Clear()
	c.bytes.Store(0)
}

// Bytes returns the tier's accounted size: every resident entry's key and
// value, plus [EntryOverhead] each. Removals are reflected once the library's
// maintenance goroutine has reported them.
func (c *Cache) Bytes() int64 { return c.bytes.Load() }

// Shed empties the tier to relieve memory pressure and returns the bytes it
// held.
//
// It is the first tier the memory budget sheds, and it is shed whole rather
// than in part. Everything here is a copy of a key owned elsewhere with a TTL
// of seconds, so dropping it costs round trips and never data; and a clear is
// accounted exactly, where a partial removal would be accounted only when the
// library's maintenance goroutine got round to it — late enough that the budget
// would go on to shed the next tier for pressure already relieved.
func (c *Cache) Shed() int64 {
	held := c.bytes.Load()
	c.shed.Add(int64(c.inner.Size()))
	c.Clear()
	return held
}

// Len returns the number of resident entries, including any that have expired
// but have not been read since.
func (c *Cache) Len() int { return c.inner.Size() }

// Capacity returns the configured maximum number of entries.
func (c *Cache) Capacity() int { return c.inner.Capacity() }

// Counters is a snapshot of a cache's activity.
type Counters struct {
	// Hits and Misses count reads. An expired entry counts as a miss, which
	// is what the caller experienced.
	Hits   int64
	Misses int64
	// Expiries counts entries dropped on read because their deadline had
	// passed. It is the observable size of the staleness bound doing work.
	Expiries int64
	// Puts counts stored entries; Rejects counts writes refused because the
	// resident copy was newer. A rising Rejects means updates are arriving
	// out of order.
	Puts    int64
	Rejects int64
	// Entries is [Cache.Len] at the time of the snapshot.
	Entries int64
	// Shed counts entries dropped by [Cache.Shed] under memory pressure.
	Shed int64
}

// Stats returns a snapshot of the counters. Fields are read independently, so
// the snapshot is not a consistent instant.
func (c *Cache) Stats() Counters {
	return Counters{
		Hits:     c.hits.Load(),
		Misses:   c.misses.Load(),
		Expiries: c.expiries.Load(),
		Puts:     c.puts.Load(),
		Rejects:  c.rejects.Load(),
		Entries:  int64(c.inner.Size()),
		Shed:     c.shed.Load(),
	}
}

// Close releases the cache and stops its maintenance goroutine. It is
// idempotent.
func (c *Cache) Close() error {
	c.inner.Close()
	return nil
}
