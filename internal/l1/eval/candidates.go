package eval

import (
	"runtime"

	ristretto "github.com/dgraph-io/ristretto/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	otterv1 "github.com/maypok86/otter"
	otter "github.com/maypok86/otter/v2"
)

// Candidate is the slice of a cache library's API that the L1 tier would
// actually use: bounded capacity, get, put, and a size estimate.
//
// TTL and versioning are deliberately absent. The L1 tier owns both, because
// they are correctness rules (docs/design.md, principle 6) and every one of these
// libraries implements expiry against the wall clock, which cannot be tested
// under a fake clock and which no injected clock of ours would reach. What is
// being evaluated is eviction and throughput, nothing else.
type Candidate interface {
	Name() string
	Get(key string) ([]byte, bool)
	Put(key string, val []byte)
	Size() int
	// SettlesAsynchronously reports whether a Put may not be visible to an
	// immediately following Get.
	SettlesAsynchronously() bool
	// Settle blocks until every preceding Put is visible.
	//
	// Without this, a benchmark that fills a cache and then measures reads
	// would measure misses on any candidate with buffered writes — and
	// misses are much faster than hits, so the dishonest candidate would
	// win by an order of magnitude. The first run of this harness did
	// exactly that.
	Settle()
	Close()
}

// Factory names a candidate and builds a fresh instance of it.
//
// Candidates are handed out as factories rather than as instances because a
// benchmark body runs several times — once or more to calibrate b.N, then again
// per -count — and a single shared instance would be reused after its own
// deferred Close. For a library whose Close permanently stops its background
// goroutines that produces a silent 0% hit ratio, which is exactly what the
// first version of this harness measured.
type Factory struct {
	Name string
	New  func(capacity int) Candidate
}

// Factories returns every candidate under evaluation.
func Factories() []Factory {
	return []Factory{
		{Name: "hashicorp/golang-lru", New: newHashicorpLRU},
		{Name: "maypok86/otter-v1", New: newOtterV1},
		{Name: "maypok86/otter-v2", New: newOtter},
		{Name: "dgraph-io/ristretto", New: newRistretto},
	}
}

// hashicorpLRU wraps github.com/hashicorp/golang-lru/v2: an exact LRU behind a
// single mutex. The baseline — old, small, widely deployed, and the thing any
// alternative has to beat.
type hashicorpLRU struct{ c *lru.Cache[string, []byte] }

func newHashicorpLRU(capacity int) Candidate {
	c, err := lru.New[string, []byte](capacity)
	if err != nil {
		// Only possible for a non-positive capacity, which the harness
		// never passes.
		panic(err)
	}
	return &hashicorpLRU{c: c}
}

func (h *hashicorpLRU) Name() string { return "hashicorp/golang-lru" }

func (h *hashicorpLRU) Get(key string) ([]byte, bool) { return h.c.Get(key) }

func (h *hashicorpLRU) Put(key string, val []byte) { h.c.Add(key, val) }

func (h *hashicorpLRU) Size() int { return h.c.Len() }

func (h *hashicorpLRU) SettlesAsynchronously() bool { return false }

func (h *hashicorpLRU) Settle() {}

func (h *hashicorpLRU) Close() {}

// otterV1Cache wraps github.com/maypok86/otter v1: the same S3-FIFO-derived
// design as v2, from before the module raised its Go floor to 1.24.
//
// It is in the evaluation because v2 requires Go 1.24 and docs/design.md fixes this
// project's floor at 1.23. A candidate the project cannot actually adopt is not
// a candidate; measuring the version that can be adopted is the whole point.
type otterV1Cache struct {
	c        otterv1.Cache[string, []byte]
	capacity int
}

func newOtterV1(capacity int) Candidate {
	c, err := otterv1.MustBuilder[string, []byte](capacity).Build()
	if err != nil {
		panic(err)
	}
	return &otterV1Cache{c: c, capacity: capacity}
}

func (o *otterV1Cache) Name() string { return "maypok86/otter-v1" }

func (o *otterV1Cache) Get(key string) ([]byte, bool) { return o.c.Get(key) }

func (o *otterV1Cache) Put(key string, val []byte) { o.c.Set(key, val) }

func (o *otterV1Cache) Size() int { return o.c.Size() }

// SettlesAsynchronously is false: v1's writes are immediately visible. Its
// *eviction* is asynchronous, which is a different property and is what Settle
// waits for.
func (o *otterV1Cache) SettlesAsynchronously() bool { return false }

// Settle yields until eviction has brought residency back within capacity.
//
// v1 exposes no CleanUp, so the only way to distinguish "overshoots capacity
// permanently" from "overshoots capacity briefly while a background goroutine
// catches up" is to yield and re-measure. The bound stops this from hanging if
// the answer turns out to be the first one.
func (o *otterV1Cache) Settle() {
	for i := 0; i < 1_000_000 && o.c.Size() > o.capacity; i++ {
		runtime.Gosched()
	}
}

func (o *otterV1Cache) Close() { o.c.Close() }

// otterCache wraps github.com/maypok86/otter/v2: W-TinyLFU admission over an
// S3-FIFO-derived eviction policy, with a lock-free read path.
type otterCache struct{ c *otter.Cache[string, []byte] }

func newOtter(capacity int) Candidate {
	return &otterCache{c: otter.Must(&otter.Options[string, []byte]{MaximumSize: capacity})}
}

func (o *otterCache) Name() string { return "maypok86/otter-v2" }

func (o *otterCache) Get(key string) ([]byte, bool) { return o.c.GetIfPresent(key) }

func (o *otterCache) Put(key string, val []byte) { o.c.Set(key, val) }

func (o *otterCache) Size() int { return o.c.EstimatedSize() }

func (o *otterCache) SettlesAsynchronously() bool { return false }

// Settle drains otter's write buffers. Otter's Set is synchronous with respect
// to visibility, but CleanUp forces any pending maintenance so that resident
// counts are comparable.
func (o *otterCache) Settle() { o.c.CleanUp() }

func (o *otterCache) Close() { o.c.InvalidateAll() }

// ristrettoCache wraps github.com/dgraph-io/ristretto/v2: TinyLFU admission
// with batched, asynchronous writes.
//
// That last property is the interesting one. A Set is buffered and applied by a
// background goroutine, so a Get immediately afterwards may miss. The harness
// measures whether that actually happens rather than taking the documentation's
// word for it.
type ristrettoCache struct {
	c *ristretto.Cache[string, []byte]
}

func newRistretto(capacity int) Candidate {
	c, err := ristretto.NewCache(&ristretto.Config[string, []byte]{
		// The library's own guidance: ten counters per expected entry.
		NumCounters: int64(capacity) * 10,
		MaxCost:     int64(capacity),
		BufferItems: 64,
		// Without this, ristretto adds its own per-item storage overhead
		// to the cost of 1 passed to Set, so a MaxCost of 100,000 holds
		// roughly a thousand entries rather than a hundred thousand.
		// The first run of this harness measured a 0% hit ratio because
		// of it. Counting entries, as the other candidates do, is the
		// only way to compare like with like.
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(err)
	}
	return &ristrettoCache{c: c}
}

func (r *ristrettoCache) Name() string { return "dgraph-io/ristretto" }

func (r *ristrettoCache) Get(key string) ([]byte, bool) { return r.c.Get(key) }

func (r *ristrettoCache) Put(key string, val []byte) { r.c.Set(key, val, 1) }

func (r *ristrettoCache) Size() int { return int(r.c.MaxCost() - r.c.RemainingCost()) }

func (r *ristrettoCache) SettlesAsynchronously() bool { return true }

// Settle blocks until ristretto's buffered writes have been applied.
func (r *ristrettoCache) Settle() { r.c.Wait() }

func (r *ristrettoCache) Close() { r.c.Close() }
