package peercache

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/hlc"
	"github.com/arwenizEr/peercache/internal/segment"
)

// Persistence defaults. See docs/decisions/0029-persistence-as-a-segment-log.md.
const (
	// persistPartitions is how many partitions a persistent namespace's keys
	// are spread over. Fixed: a key's partition is part of where its records
	// live, so changing it would orphan every record already written.
	persistPartitions = 256
	// persistFlushInterval is the loss window's main term: a write is
	// in memory only until the next flush.
	persistFlushInterval = 100 * time.Millisecond
	// persistFlushBytes flushes early when this much is pending, so one
	// burst cannot build an unbounded segment.
	persistFlushBytes = 1 << 20
	// persistCompactInterval is how often a node looks at the partitions
	// it compacts.
	persistCompactInterval = time.Minute
	// persistCompactAfter is how many uncovered segments a partition
	// collects before it is compacted: few enough that a restore reads a
	// handful of objects, many enough that compaction is not a rewrite of
	// the partition per flush.
	persistCompactAfter = 8
	// persistTombstoneGrace is how long a deletion is kept after it was
	// written. A segment from a slow writer can arrive late with an older
	// write of the key; while the tombstone is kept it still wins.
	persistTombstoneGrace = time.Hour
	// persistFlushTimeout bounds one flush. A store that does not answer
	// must not hold the flusher, and the records stay pending for the next.
	persistFlushTimeout = 10 * time.Second
)

// persister writes one namespace's changes behind to its blob store.
//
// # The dirty map
//
// Every write on this node's owned store records the key's new state here,
// replacing whatever was pending for it: a key written a thousand times
// between flushes costs one record. The map holds the record itself, value
// included, so nothing a flush must write can be lost by the owned store
// shedding or expiring the key meanwhile (decision 0029, finding 6). Its size
// is bounded by distinct keys written since the last successful flush.
//
// # A flush
//
// The map is swapped out, its records are grouped by partition, and each
// partition's records become one immutable segment object. A segment that
// fails to write puts its records back, unless a newer record for the key
// arrived meanwhile, and they go out with the next flush. Nothing is retried
// in a loop: the next tick is the retry.
type persister struct {
	ns    *namespace
	store core.BlobStore
	clk   clock.Clock

	mu      sync.Mutex
	pending map[string]*segment.Record
	bytes   int
	// shed holds keys the memory budget dropped from the owned store. Their
	// records are still in storage, or still pending here; the first access
	// to one restores that key alone (see restoreShed). Restoring the whole
	// partition instead would bring every shed key back at once and undo
	// the shedding that made room. Bounded by the owned keys shed.
	shed map[string]struct{}

	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
	// flushMu serialises flushes: the ticker's, a kick's and Close's.
	flushMu sync.Mutex

	flushes  atomic.Int64
	written  atomic.Int64
	failures atomic.Int64

	compactions     atomic.Int64
	compactFailures atomic.Int64

	// rmu guards parts, the per-partition restore state.
	rmu             sync.Mutex
	parts           [persistPartitions]partitionState
	restored        atomic.Int64
	restoreFailures atomic.Int64
}

func newPersister(ns *namespace, store core.BlobStore) *persister {
	p := &persister{
		ns:      ns,
		store:   store,
		clk:     ns.cache.opts.Clock,
		pending: map[string]*segment.Record{},
		shed:    map[string]struct{}{},
		kick:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	// The ticker is created here, not in run: a ticker armed on the
	// goroutine misses any clock advance that lands before the goroutine is
	// scheduled, and the first flush then comes an interval late — on the
	// fake clock, never.
	ticker := p.clk.NewTicker(persistFlushInterval)
	compact := p.clk.NewTicker(persistCompactInterval)
	p.wg.Add(1)
	go p.run(ticker, compact)
	return p
}

// record notes a key's new state for the next flush. value is the stored
// (framed) form; a tombstone has none.
func (p *persister) record(key string, value []byte, expiresAt int64, order uint64, tombstone bool) {
	r := &segment.Record{
		Namespace: p.ns.name,
		Key:       key,
		Writer:    p.ns.cache.self().ID,
		Order:     order,
		ExpiresAt: expiresAt,
		Tombstone: tombstone,
	}
	if !tombstone {
		r.Value = append([]byte(nil), value...)
	}

	p.mu.Lock()
	// The shed mark is deliberately left alone. Clearing it here, as the
	// first version did, lost keys: a write's own memory accounting can
	// shed the key it just stored, before the write records it, and the
	// cleared mark then meant it was never restored. Restoring a key that
	// was written since costs a read and changes nothing, because the
	// merge keeps the newer value in memory.
	if cur, ok := p.pending[key]; ok {
		if !r.Newer(cur) {
			// An older write recorded late — a promotion racing a
			// fresh write — must not replace the newer one.
			p.mu.Unlock()
			return
		}
		p.bytes -= recordSize(cur)
	}
	p.pending[key] = r
	p.bytes += recordSize(r)
	full := p.bytes >= persistFlushBytes
	p.mu.Unlock()

	if full {
		p.flushSoon()
	}
}

// noteShed records that the memory budget dropped key from the owned store.
func (p *persister) noteShed(key string) {
	p.mu.Lock()
	p.shed[key] = struct{}{}
	p.mu.Unlock()
}

// pendingBytes is what the dirty map holds, for the memory budget.
func (p *persister) pendingBytes() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(p.bytes)
}

// flushSoon asks the flusher to run now rather than at its next tick.
func (p *persister) flushSoon() {
	select {
	case p.kick <- struct{}{}:
	default:
	}
}

func recordSize(r *segment.Record) int {
	return len(r.Namespace) + len(r.Key) + len(r.Writer) + len(r.Value) + 32
}

func (p *persister) run(ticker, compact clock.Ticker) {
	defer p.wg.Done()
	defer ticker.Stop()
	defer compact.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C():
		case <-p.kick:
		case <-compact.C():
			ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
			p.compactOwned(ctx)
			cancel()
			continue
		}
		ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
		_ = p.flush(ctx)
		cancel()
	}
}

// flush writes everything pending, one segment per partition.
func (p *persister) flush(ctx context.Context) error {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	p.mu.Lock()
	batch := p.pending
	if len(batch) == 0 {
		p.mu.Unlock()
		return nil
	}
	p.pending = map[string]*segment.Record{}
	p.bytes = 0
	p.mu.Unlock()

	byPartition := map[int][]segment.Record{}
	for key, r := range batch {
		part := persistPartition(p.ns.groupKeyFor(key))
		byPartition[part] = append(byPartition[part], *r)
	}

	var failed error
	for part, records := range byPartition {
		sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
		var maxOrder uint64
		for i := range records {
			if records[i].Order > maxOrder {
				maxOrder = records[i].Order
			}
		}
		name := segmentName(p.ns.name, part, maxOrder, p.ns.cache.self().ID)
		data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})
		if err := p.store.Put(ctx, name, data); err != nil {
			p.failures.Add(1)
			failed = errors.Join(failed, fmt.Errorf("peercache: persisting %s: %w", name, err))
			p.restorePending(records)
			continue
		}
		p.written.Add(int64(len(records)))
	}
	p.flushes.Add(1)
	return failed
}

// restorePending puts records from a failed segment back, keeping any newer
// record that arrived while the flush ran.
func (p *persister) restorePending(records []segment.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range records {
		r := records[i]
		if cur, ok := p.pending[r.Key]; ok && !r.Newer(cur) {
			continue
		} else if ok {
			p.bytes -= recordSize(cur)
		}
		p.pending[r.Key] = &r
		p.bytes += recordSize(&r)
	}
}

// Close stops the flusher and flushes what is pending, under ctx.
func (p *persister) Close(ctx context.Context) error {
	close(p.done)
	p.wg.Wait()
	return p.flush(ctx)
}

// persistCounters is what a persister reports for Stats.
type persistCounters struct {
	pending                      int
	flushes, written, failures   int64
	restored, restoreFailures    int64
	compactions, compactFailures int64
}

func (p *persister) counters() persistCounters {
	p.mu.Lock()
	pending := len(p.pending)
	p.mu.Unlock()
	return persistCounters{
		pending: pending, flushes: p.flushes.Load(), written: p.written.Load(), failures: p.failures.Load(),
		restored: p.restored.Load(), restoreFailures: p.restoreFailures.Load(),
		compactions: p.compactions.Load(), compactFailures: p.compactFailures.Load(),
	}
}

// persistPartition is the partition a group key's records live in.
func persistPartition(groupKey string) int {
	return int(xxhash.Sum64String(groupKey) % persistPartitions)
}

// partitionPrefix is the object-name prefix of one partition of a namespace.
// The namespace name is escaped, because it is arbitrary application text and
// an object name is a path.
func partitionPrefix(namespace string, part int) string {
	return fmt.Sprintf("%s/p%03d/", url.PathEscape(namespace), part)
}

// segmentName names a segment by the largest order it holds and its writer,
// hex-encoded for the same reason as the namespace. Two segments from one
// writer never share a largest order, because a writer's orders never repeat.
func segmentName(namespace string, part int, maxOrder uint64, writer string) string {
	return fmt.Sprintf("%sseg-%016x-%s.bin", partitionPrefix(namespace, part), maxOrder, hex.EncodeToString([]byte(writer)))
}

// restoreRetry is how long after a failed restore a partition is tried again.
// Until then its keys are served from memory alone, degraded rather than
// unavailable: decision 0029 keeps a store outage from taking the namespace
// down with it.
const restoreRetry = time.Second

// partitionState tracks one partition's restore on this node.
type partitionState struct {
	// restored is the restore generation, plus one, this partition was last
	// restored at; zero means never.
	restored uint64
	// running is closed when the restore in flight finishes.
	running chan struct{}
	// failedAt is when the last restore failed, in Unix nanoseconds.
	failedAt int64
}

// await blocks until the partition holding key has been restored since the
// last membership change, or ctx ends.
//
// # Why every local access waits, writes included
//
// A restore merges storage into memory, newest wins. A read that ran ahead of
// it would report a key missing that the restore was about to bring back, and a
// delete that ran ahead of it would be undone by the older stored value. So
// reads and writes of a key wait for its partition, and the first one starts
// the restore. A namespace that is never touched after a membership change is
// never re-read.
func (p *persister) await(ctx context.Context, key string) error {
	if err := p.awaitPartition(ctx, key); err != nil {
		return err
	}
	return p.restoreShed(ctx, key)
}

func (p *persister) awaitPartition(ctx context.Context, key string) error {
	part := persistPartition(p.ns.groupKeyFor(key))
	for {
		want := p.ns.cache.restoreGen.Load() + 1
		p.rmu.Lock()
		st := &p.parts[part]
		if st.restored >= want {
			p.rmu.Unlock()
			return nil
		}
		if st.running == nil {
			if st.failedAt != 0 && p.clk.Now().UnixNano()-st.failedAt < int64(restoreRetry) {
				// Failed recently: serve from memory, retry later.
				p.rmu.Unlock()
				return nil
			}
			st.running = make(chan struct{})
			go p.restorePartition(part, want, st.running)
		}
		running := st.running
		p.rmu.Unlock()

		select {
		case <-running:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// restorePartition runs one restore of part and publishes its outcome.
func (p *persister) restorePartition(part int, gen uint64, done chan struct{}) {
	ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
	err := p.restore(ctx, part)
	cancel()

	p.rmu.Lock()
	st := &p.parts[part]
	if err == nil {
		if gen > st.restored {
			st.restored = gen
		}
		st.failedAt = 0
	} else {
		st.failedAt = p.clk.Now().UnixNano()
		p.restoreFailures.Add(1)
	}
	st.running = nil
	p.rmu.Unlock()
	close(done)
}

// restore merges one partition's stored records into the owned store: the
// newest snapshot, every segment it does not name, newest record per key, kept
// only for keys this node owns and not expired.
func (p *persister) restore(ctx context.Context, part int) error {
	// Held across the read and the overlay of pending records: a flush in
	// between would move a record out of pending and into storage after we
	// read storage, and the restore would see it in neither place.
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	prefix := partitionPrefix(p.ns.name, part)
	// A compactor can delete a segment between our List and our Get. The
	// snapshot that covers it is then newer than the one we chose, so
	// listing again finds it; three tries covers a compaction per try.
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var newest map[string]*segment.Record
		newest, err = p.read(ctx, prefix)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		// A record still pending here is newer than storage for its key
		// whenever it exists: this node wrote it and has not flushed it.
		p.mu.Lock()
		for key, r := range p.pending {
			if persistPartition(p.ns.groupKeyFor(key)) != part {
				continue
			}
			if cur, ok := newest[key]; !ok || r.Newer(cur) {
				newest[key] = r
			}
		}
		p.mu.Unlock()
		p.apply(newest)
		return nil
	}
	return err
}

// read collects the newest record per key under prefix.
func (p *persister) read(ctx context.Context, prefix string) (map[string]*segment.Record, error) {
	sc, err := p.scan(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return sc.newest, nil
}

// scanned is one partition as a reader found it.
type scanned struct {
	newest map[string]*segment.Record
	// merged names every object whose records are in newest, snapshots and
	// segments alike: what a snapshot made from newest covers.
	merged []string
	// segments counts the segments not covered by any snapshot.
	segments int
}

// scan reads every snapshot under prefix and every segment none of them
// names, keeping the newest record per key.
//
// Every snapshot, not only the newest. Two compactors at once — a partition,
// decision 5 — each write one and each delete only the segments their own
// covers, so a record can survive in an older snapshot alone. The union is
// always right: records merge as a last-writer-wins map, and a segment is
// deleted only after a snapshot covering it is durable.
func (p *persister) scan(ctx context.Context, prefix string) (*scanned, error) {
	names, err := p.store.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("peercache: listing %s: %w", prefix, err)
	}
	sc := &scanned{newest: map[string]*segment.Record{}}
	merge := func(f *segment.File) {
		for i := range f.Records {
			r := &f.Records[i]
			if r.Namespace != p.ns.name {
				continue
			}
			if cur, ok := sc.newest[r.Key]; !ok || r.Newer(cur) {
				sc.newest[r.Key] = r
			}
		}
	}
	covered := map[string]bool{}
	for _, name := range names {
		if !strings.HasPrefix(name[len(prefix):], "snap-") {
			continue
		}
		f, err := p.load(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, c := range f.Covered {
			covered[c] = true
		}
		merge(f)
		sc.merged = append(sc.merged, name)
	}
	for _, name := range names {
		if covered[name] || !strings.HasPrefix(name[len(prefix):], "seg-") {
			continue
		}
		f, err := p.load(ctx, name)
		if err != nil {
			return nil, err
		}
		merge(f)
		sc.merged = append(sc.merged, name)
		sc.segments++
	}
	return sc, nil
}

func (p *persister) load(ctx context.Context, name string) (*segment.File, error) {
	data, err := p.store.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("peercache: reading %s: %w", name, err)
	}
	f, err := segment.Decode(data)
	if err != nil {
		// A corrupt object is not skipped: restoring around it could bring
		// back an older value it superseded. The restore fails, is counted,
		// and the partition is served from memory until it is repaired.
		return nil, fmt.Errorf("peercache: %s: %w", name, err)
	}
	return f, nil
}

// apply merges records into the owned store.
func (p *persister) apply(newest map[string]*segment.Record) {
	now := p.clk.Now().UnixNano()
	c := p.ns.cache
	var maxOrder uint64
	for key, r := range newest {
		if r.Order > maxOrder {
			maxOrder = r.Order
		}
		if !p.owns(key) {
			continue
		}
		if !r.Tombstone && r.ExpiresAt != 0 && now >= r.ExpiresAt {
			continue
		}
		if p.ns.store.MergeIfNewer(key, r.Value, r.ExpiresAt, r.Order, r.Tombstone) {
			p.restored.Add(1)
		}
	}
	// This node's next write to any of these keys must outrank what it
	// just read, whatever its own clock says.
	c.observeOrder(maxOrder)
}

// owns reports whether this node owns key under its current view.
func (p *persister) owns(key string) bool {
	c := p.ns.cache
	if c.guard == nil {
		return true
	}
	owner, _, err := c.guard.Table().Owner(p.ns.groupKeyFor(key))
	return err == nil && owner.ID == c.self().ID
}

// restoreShed brings back one key the memory budget shed, on its first access.
//
// It reads the key's partition and merges that key alone, preferring a record
// still pending here — the newest write may not have been flushed when the key
// was shed. A failure leaves the key marked, counted, and served from memory
// as a miss; the next access tries again. Two accesses racing may both read
// the partition; the merge makes that harmless.
func (p *persister) restoreShed(ctx context.Context, key string) error {
	p.mu.Lock()
	_, shed := p.shed[key]
	p.mu.Unlock()
	if !shed {
		return nil
	}

	// See restore for why a flush must not run between the read and the
	// look at pending.
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	newest, err := p.read(ctx, partitionPrefix(p.ns.name, persistPartition(p.ns.groupKeyFor(key))))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.restoreFailures.Add(1)
		return nil
	}

	p.mu.Lock()
	delete(p.shed, key)
	r := newest[key]
	if pending, ok := p.pending[key]; ok && (r == nil || pending.Newer(r)) {
		r = pending
	}
	p.mu.Unlock()

	if r != nil {
		p.apply(map[string]*segment.Record{key: r})
	}
	return nil
}

// compactOwned compacts every partition this node is the compactor for that
// has collected enough segments.
func (p *persister) compactOwned(ctx context.Context) {
	for part := 0; part < persistPartitions; part++ {
		if ctx.Err() != nil {
			return
		}
		if !p.compactsPartition(part) {
			continue
		}
		if err := p.compact(ctx, part, persistCompactAfter); err != nil {
			p.compactFailures.Add(1)
		}
	}
}

// compactsPartition reports whether this node is part's compactor: the
// rendezvous owner of a synthetic group key, so every node agrees without
// asking and the work spreads across the cluster.
func (p *persister) compactsPartition(part int) bool {
	c := p.ns.cache
	if c.guard == nil {
		return true
	}
	key := fmt.Sprintf("persist/%s/%d", p.ns.name, part)
	owner, _, err := c.guard.Table().Owner(key)
	return err == nil && owner.ID == c.self().ID
}

// compact folds part's snapshots and segments into one snapshot when at
// least threshold segments are uncovered, then deletes what it folded.
func (p *persister) compact(ctx context.Context, part, threshold int) error {
	prefix := partitionPrefix(p.ns.name, part)
	sc, err := p.scan(ctx, prefix)
	if err != nil {
		return err
	}
	if sc.segments < threshold {
		return nil
	}

	cutoff := uint64(hlc.FromMillis(p.clk.Now().Add(-persistTombstoneGrace).UnixMilli()))
	now := p.clk.Now().UnixNano()
	keys := make([]string, 0, len(sc.newest))
	for key := range sc.newest {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	records := make([]segment.Record, 0, len(keys))
	var maxOrder uint64
	for _, key := range keys {
		r := sc.newest[key]
		if r.Order > maxOrder {
			maxOrder = r.Order
		}
		// Past the grace a deletion has done its work: every segment old
		// enough to hold an older write of the key has been folded. A value
		// that expired that long ago is as dead as a tombstone.
		if r.Order < cutoff && (r.Tombstone || (r.ExpiresAt != 0 && now >= r.ExpiresAt)) {
			continue
		}
		records = append(records, *r)
	}

	name := fmt.Sprintf("%ssnap-%016x-%s.bin", prefix, maxOrder, hex.EncodeToString([]byte(p.ns.cache.self().ID)))
	snap := segment.Encode(&segment.File{Kind: segment.KindSnapshot, Covered: sc.merged, Records: records})
	if err := p.store.Put(ctx, name, snap); err != nil {
		return fmt.Errorf("peercache: writing %s: %w", name, err)
	}
	// Only now, with the snapshot durable, is anything it covers deleted.
	for _, covered := range sc.merged {
		if covered == name {
			continue
		}
		if err := p.store.Delete(ctx, covered); err != nil {
			// Left behind, it is merged again and harmlessly; the next
			// compaction covers it.
			return fmt.Errorf("peercache: deleting %s: %w", covered, err)
		}
	}
	p.compactions.Add(1)
	return nil
}
