package peercache

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
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
	//
	// 64, not the 256 decision 0029 first said. A flush writes one durable
	// object per partition it touches, and durable writes overlap only about
	// threefold on Machine A's disk: a flush touching all of 256 took 40 to
	// 600 ms, past its own 100 ms interval, where 64 takes about 5. A restore
	// still reads a sixty-fourth of the namespace.
	persistPartitions = 64
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
	// persistSplitInterval is how often a namespace's splitter moves its
	// logs into partition segments. It bounds how many logs a restore reads:
	// a few seconds of flushes per node.
	persistSplitInterval = 10 * time.Second
	// persistSplitParallel is how many partition segments one split writes
	// at once.
	persistSplitParallel = 16
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

	mu sync.Mutex
	// pending holds records by value, not pointer: one allocation fewer per
	// write, which was a third of a persistent Set's bytes.
	pending map[string]segment.Record
	// spare is the map the last flush emptied, cleared and kept so that the
	// next flush's pending map does not regrow from nothing. Touched only by
	// flush, under flushMu.
	spare map[string]segment.Record
	// recBuf is the last flush's record slice, kept for the same reason.
	recBuf []segment.Record
	// pendingHigh and spareHigh are the most entries pending and spare have
	// held since they were made: a cleared map keeps that many buckets.
	pendingHigh, spareHigh int
	bytes                  int
	// shed holds keys the memory budget dropped from the owned store. Their
	// records are still in storage, or still pending here; the first access
	// to one restores that key alone (see restoreShed). Restoring the whole
	// partition instead would bring every shed key back at once and undo
	// the shedding that made room. Bounded by the owned keys shed.
	shed map[string]struct{}

	// inflight is the batch a flush has taken from pending and not yet
	// finished writing; flushedDuring holds the batches flushes wrote while
	// at least one restore was running (restoring counts them). Under mu.
	//
	// They exist so that a restore need not hold the flush lock while it
	// reads storage. A restore overlays what this node wrote but storage
	// may not show yet: before, that was pending alone, and keeping a flush
	// from moving a record out of pending meant holding flushMu across the
	// whole read. On kind that stopped flushes for the length of every
	// restore after every membership change - 58,000 acknowledged records
	// pending on one pod when it was OOM-killed (open question 55). Now a
	// record taken from pending after a restore began is, when the restore
	// overlays, in pending, in inflight, or in flushedDuring.
	inflight      map[string]segment.Record
	flushedDuring []map[string]segment.Record
	restoring     int

	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
	// flushMu serialises flushes: the ticker's, a kick's and Close's.
	flushMu sync.Mutex
	// restoreSlots bounds the restores running at once, and with them how
	// many partitions' records are held in memory together. It was a mutex —
	// one partition at a time — and a node restarted onto a large namespace
	// then restored its 64 partitions in a queue while every caller of a
	// partition near the back waited for all of those in front: on kind, a
	// slow minute after each restart. A flush never takes a slot.
	restoreSlots chan struct{}

	flushes  atomic.Int64
	written  atomic.Int64
	failures atomic.Int64

	compactions     atomic.Int64
	compactFailures atomic.Int64
	splits          atomic.Int64
	splitFailures   atomic.Int64

	// rmu guards parts, the per-partition restore state, and rclosed.
	rmu   sync.Mutex
	parts [persistPartitions]partitionState
	// Restores run on their own goroutines, under rctx. Close cancels it and
	// waits on rwg: a restore left running outlived the cache.
	rctx            context.Context
	rcancel         context.CancelFunc
	rwg             sync.WaitGroup
	rclosed         bool
	restored        atomic.Int64
	restoreFailures atomic.Int64

	// logMu guards logs, logBytes and logUsers: the unsplit logs scans have
	// read, decoded and cut by partition, shared by the scans after them,
	// and how many scans and restores are under way. See logRecords.
	logMu    sync.Mutex
	logs     map[string]*logEntry
	logBytes int
	logUsers int
}

// logEntry is one unsplit log as scans share it: its records by partition,
// or why it could not be read.
type logEntry struct {
	ready chan struct{} // closed once parts or err is set
	parts [persistPartitions][]segment.Record
	bytes int
	err   error
}

// persistLogCacheBytes bounds the records the shared logs hold. Past it a
// log is read for the scan that asked and not kept: the restore is no worse
// than before the cache, one read of the log per partition.
//
// The memory budget does not count these bytes, which is why the bound is
// small: logs are what the splitter has not moved yet, a few seconds of the
// cluster's distinct keys written to the namespace, which past this bound
// are better read again than held. Per persistent namespace.
const persistLogCacheBytes = 16 << 20

func newPersister(ns *namespace, store core.BlobStore) *persister {
	p := &persister{
		restoreSlots: make(chan struct{}, restoreParallelism),
		ns:           ns,
		store:        store,
		clk:          ns.cache.opts.Clock,
		pending:      map[string]segment.Record{},
		shed:         map[string]struct{}{},
		kick:         make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
	p.rctx, p.rcancel = context.WithCancel(context.Background())
	// The ticker is created here, not in run: a ticker armed on the
	// goroutine misses any clock advance that lands before the goroutine is
	// scheduled, and the first flush then comes an interval late — on the
	// fake clock, never.
	interval := ns.opts.PersistFlushInterval
	if interval <= 0 {
		interval = persistFlushInterval
	}
	ticker := p.clk.NewTicker(interval)
	compact := p.clk.NewTicker(persistCompactInterval)
	split := p.clk.NewTicker(persistSplitInterval)
	p.wg.Add(2)
	go p.run(ticker)
	go p.maintain(compact, split)
	return p
}

// record notes a key's new state for the next flush. value is the stored
// (framed) form; a tombstone has none.
func (p *persister) record(key string, value []byte, expiresAt int64, order uint64, tombstone bool) {
	r := segment.Record{
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
		if !r.Newer(&cur) {
			// An older write recorded late — a promotion racing a
			// fresh write — must not replace the newer one.
			p.mu.Unlock()
			return
		}
		p.bytes -= recordSize(&cur)
	}
	p.pending[key] = r
	p.bytes += recordSize(&r)
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

// run is the flusher. It does nothing else: whatever shares its goroutine
// delays every flush behind it, and a delayed flush is a longer loss window.
// Compaction and splitting once ran here, and on kind, with a million orders,
// each compaction held flushes off for seconds every minute (open question
// 55): pending records rose from ~150 to ~1,500 at every compaction tick.
func (p *persister) run(ticker clock.Ticker) {
	defer p.wg.Done()
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C():
		case <-p.kick:
		}
		ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
		_ = p.flush(ctx)
		cancel()
	}
}

// maintain compacts partitions and splits logs, on its own goroutine so that
// neither holds up a flush. Running alongside this node's flushes is safe for
// the reason running alongside other nodes' compactors and splitters is
// (decision 0030): a compaction deletes only the objects its snapshot
// covers, a split deletes logs only once their segments are durable, and
// readers take the union of whatever is there.
func (p *persister) maintain(compact, split clock.Ticker) {
	defer p.wg.Done()
	defer compact.Stop()
	defer split.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-compact.C():
			ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
			p.compactOwned(ctx)
			cancel()
		case <-split.C():
			p.dropIdleLogs()
			if p.splitsLogs() {
				ctx, cancel := clock.WithTimeout(context.Background(), p.clk, persistFlushTimeout)
				if err := p.split(ctx); err != nil {
					p.splitFailures.Add(1)
				}
				cancel()
			}
		}
	}
}

// flush writes everything pending as one log object, whatever partitions its
// records belong to: one request per flush, which is what an object store
// charges for (decision 0030). The splitter moves the records into their
// partitions later.
func (p *persister) flush(ctx context.Context) error {
	p.flushMu.Lock()
	defer p.flushMu.Unlock()

	p.mu.Lock()
	batch := p.pending
	if len(batch) == 0 {
		p.mu.Unlock()
		return nil
	}
	batchHigh := max(p.pendingHigh, len(batch))
	if p.spare != nil {
		p.pending, p.pendingHigh = p.spare, p.spareHigh
		p.spare = nil
	} else {
		p.pending, p.pendingHigh = make(map[string]segment.Record, len(batch)), 0
	}
	p.bytes = 0
	p.inflight = batch
	p.mu.Unlock()

	records := p.recBuf[:0]
	var maxOrder uint64
	for _, r := range batch {
		records = append(records, r)
		if r.Order > maxOrder {
			maxOrder = r.Order
		}
	}
	// Recycled while the workload still fills it. clear keeps a map's
	// buckets, so a map that once held a burst of a million keys would hold
	// that memory — unaccounted for by the memory budget — for the life of
	// the namespace. A batch under a quarter of the map's high-water mark
	// lets it go instead. Measured on Machine A, recycling took a persistent
	// Set from 705 to 421 bytes and 4 to 3 allocations.
	kept := false // handed to flushedDuring, so not ours to recycle
	defer func() {
		clear(records) // drop the values the slice still points at
		if kept {
			p.recBuf = records[:0]
			return
		}
		if len(batch)*4 < batchHigh {
			p.spare, p.spareHigh, p.recBuf = nil, 0, nil
			return
		}
		clear(batch)
		p.spare, p.spareHigh = batch, batchHigh
		p.recBuf = records[:0]
	}()
	sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
	name := logName(p.ns.name, maxOrder, p.ns.cache.self().ID)
	data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})
	// Counted whether or not the write succeeded: PersistFlushes is flush
	// attempts, and a waiter on it is waiting for the attempt to be over.
	defer p.flushes.Add(1)
	if err := p.store.Put(ctx, name, data); err != nil {
		p.failures.Add(1)
		p.restorePending(records)
		return fmt.Errorf("peercache: persisting %s: %w", name, err)
	}
	p.mu.Lock()
	if p.restoring > 0 {
		p.flushedDuring = append(p.flushedDuring, batch)
		kept = true
	}
	p.inflight = nil
	p.mu.Unlock()
	p.written.Add(int64(len(records)))
	return nil
}

// restorePending puts records from a failed segment back, keeping any newer
// record that arrived while the flush ran.
func (p *persister) restorePending(records []segment.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// In the same critical section as the records going back: a restore
	// overlaying in between would otherwise find them in neither place.
	p.inflight = nil
	for i := range records {
		r := records[i]
		if cur, ok := p.pending[r.Key]; ok && !r.Newer(&cur) {
			continue
		} else if ok {
			p.bytes -= recordSize(&cur)
		}
		p.pending[r.Key] = r
		p.bytes += recordSize(&r)
	}
}

// Close stops the flusher and flushes what is pending, under ctx.
func (p *persister) Close(ctx context.Context) error {
	close(p.done)
	p.wg.Wait()
	// Restores before the final flush, and cancelled rather than left to
	// finish: one reading a store that no longer answers would never end.
	p.rmu.Lock()
	p.rclosed = true
	p.rmu.Unlock()
	p.rcancel()
	p.rwg.Wait()
	return p.flush(ctx)
}

// persistCounters is what a persister reports for Stats.
type persistCounters struct {
	pending                      int
	flushes, written, failures   int64
	restored, restoreFailures    int64
	compactions, compactFailures int64
	splits, splitFailures        int64
}

func (p *persister) counters() persistCounters {
	p.mu.Lock()
	pending := len(p.pending)
	p.mu.Unlock()
	return persistCounters{
		pending: pending, flushes: p.flushes.Load(), written: p.written.Load(), failures: p.failures.Load(),
		restored: p.restored.Load(), restoreFailures: p.restoreFailures.Load(),
		compactions: p.compactions.Load(), compactFailures: p.compactFailures.Load(),
		splits: p.splits.Load(), splitFailures: p.splitFailures.Load(),
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

// logPrefix is the object-name prefix of a namespace's unsplit logs.
func logPrefix(namespace string) string {
	return url.PathEscape(namespace) + "/log/"
}

// logName names one flush's log object by the largest order it holds and its
// writer. A writer's orders never repeat, so neither do its log names.
func logName(namespace string, maxOrder uint64, writer string) string {
	return fmt.Sprintf("%sseg-%016x-%s-%s.bin", logPrefix(namespace), maxOrder, hex.EncodeToString([]byte(writer)), nameNonce())
}

// nameNonce makes a log name unique even when its order and writer are not.
// They usually are: a writer's orders never repeat. But a node restarted under
// the same name, whose clock has not yet observed its predecessor's last
// order, can issue it again, and the second log would replace the first in
// storage with every record the first held gone. Two caches in one test did.
func nameNonce() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0"
	}
	return hex.EncodeToString(b[:])
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
		if st.running == nil && p.rclosed {
			// Closing: no new restore starts, and memory answers.
			p.rmu.Unlock()
			return nil
		}
		if st.running == nil {
			if st.failedAt != 0 && p.clk.Now().UnixNano()-st.failedAt < int64(restoreRetry) {
				// Failed recently: memory answers until the retry, for
				// what memory holds. See errPartitionUnrestored.
				p.rmu.Unlock()
				return errPartitionUnrestored
			}
			st.running = make(chan struct{})
			p.rwg.Add(1)
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

// errPartitionUnrestored reports that a key's partition could not be restored
// from storage and is answered from memory until the next attempt.
//
// Memory is the right answer for a key memory holds: this node wrote or was
// sent it. It is the wrong one for a key memory does not hold. The restore is
// what brings back keys this node has just come to own; until it succeeds,
// an absent key may exist, and "not found" — or an Increment counting from
// zero, a SetIfAbsent succeeding — is a lie the next restore cannot correct,
// because the write it made outranks the record it would restore. On kind a
// restarted owner whose restore failed served a stock counter from zero and
// lost every decrement before (open question 57). See namespace.awaitRestore.
//
// That holds for a blind write too. A Set looks as if it cannot be wrong — it
// reads nothing — but its order comes from this node's clock, which after a
// failed restore has observed none of the records storage holds, and can be
// behind them: the next restore would then lay the stored value over the Set
// and lose it. A test caught exactly that, the new write's order equal to the
// record's, so the write is refused like the rest.
var errPartitionUnrestored = errors.New("peercache: partition not restored from storage")

// restorePartition runs one restore of part and publishes its outcome.
func (p *persister) restorePartition(part int, gen uint64, done chan struct{}) {
	defer p.rwg.Done()
	// From before the slot is taken: restores queued behind the slots keep
	// the logs that the ones running have read.
	defer p.useLogs()()
	ctx, cancel := clock.WithTimeout(p.rctx, p.clk, persistFlushTimeout)
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
	if err != nil && p.rctx.Err() == nil {
		// Counted, and said: a partition that failed to restore is served
		// from memory until it is retried, and which error it was decides
		// whether that is an outage or a race to fix.
		p.ns.cache.opts.Logger.Warn("persistent namespace: restoring a partition failed",
			"namespace", p.ns.name, "partition", part, "error", err)
	}
}

// restore merges one partition's stored records into the owned store: the
// newest snapshot, every segment it does not name, newest record per key, kept
// only for keys this node owns and not expired.
func (p *persister) restore(ctx context.Context, part int) error {
	if err := p.takeRestoreSlot(ctx); err != nil {
		return err
	}
	defer p.releaseRestoreSlot()
	// Registered before the read: a flush that moves a record out of pending
	// while storage is read keeps it for the overlay below.
	mark := p.beginRestore()
	defer p.endRestore()
	// A compactor can delete a segment between our List and our Get. The
	// snapshot that covers it is then newer than the one we chose, so
	// listing again finds it; three tries covers a compaction per try.
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		var newest map[string]*segment.Record
		newest, err = p.read(ctx, part)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		p.overlay(newest, mark, func(key string) bool {
			return persistPartition(p.ns.groupKeyFor(key)) == part
		})
		p.apply(newest)
		return nil
	}
	return err
}

// restoreParallelism is how many partitions of one namespace restore at once.
//
// Eight of 64 holds an eighth of the namespace's records in memory at the peak
// rather than a sixty-fourth, and cut the time to a namespace answering
// everywhere after a restart by the factor docs/benchmarks.md records. The
// restores are independent: each reads its own partition, and each merges
// newest-wins into the store, so two at once cannot undo each other.
const restoreParallelism = 8

// takeRestoreSlot waits for a restore slot, or for ctx.
func (p *persister) takeRestoreSlot(ctx context.Context) error {
	select {
	case p.restoreSlots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *persister) releaseRestoreSlot() { <-p.restoreSlots }

// beginRestore registers a restore with the flusher and returns its mark:
// the batches flushed from here on are kept for its overlay.
func (p *persister) beginRestore() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restoring++
	return len(p.flushedDuring)
}

// endRestore lets the flusher forget what it kept once no restore needs it.
func (p *persister) endRestore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.restoring--
	if p.restoring == 0 {
		p.flushedDuring = nil
	}
}

// overlay puts into newest, for the keys want selects, every record this
// node wrote that the storage read may have missed: still pending, being
// flushed, or flushed since mark. Where one exists it is newer than storage
// for its key, since this node wrote it; the newest of them wins.
func (p *persister) overlay(newest map[string]*segment.Record, mark int, want func(string) bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	add := func(m map[string]segment.Record) {
		for key, r := range m {
			if !want(key) {
				continue
			}
			if cur, ok := newest[key]; !ok || r.Newer(cur) {
				r := r // a copy: the map's value must not be aliased
				newest[key] = &r
			}
		}
	}
	for _, b := range p.flushedDuring[mark:] {
		add(b)
	}
	add(p.inflight)
	add(p.pending)
}

// read collects the newest record per key of one partition.
func (p *persister) read(ctx context.Context, part int) (map[string]*segment.Record, error) {
	sc, err := p.scan(ctx, part)
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
//
// Logs not yet split are read too, keeping only this partition's records
// (decision 0030). They are not counted in merged or segments: compaction
// neither folds nor deletes them, the splitter does.
func (p *persister) scan(ctx context.Context, part int) (*scanned, error) {
	defer p.useLogs()()
	prefix := partitionPrefix(p.ns.name, part)
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
	// An object listed and then gone has been folded or split: its records
	// are in a snapshot or a partition segment made before it was deleted
	// (decision 0030), which listing again shows. So a missing object is
	// skipped and the listing repeated, reading only what it shows anew,
	// until a pass finds nothing gone. Starting the whole read over instead
	// re-read every log each time; on kind a restore reading a hundred logs
	// while a splitter deleted them every ten seconds failed its three tries
	// over and over (open question 57).
	loaded := map[string]bool{}
	covered := map[string]bool{}
	gone := func(err error) bool { return errors.Is(err, ErrNotFound) }
	for pass := 0; ; pass++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		vanished := false
		names, err := p.store.List(ctx, prefix)
		if err != nil {
			return nil, fmt.Errorf("peercache: listing %s: %w", prefix, err)
		}
		for _, name := range names {
			if loaded[name] || !strings.HasPrefix(name[len(prefix):], "snap-") {
				continue
			}
			f, err := p.load(ctx, name)
			if gone(err) {
				vanished = true
				continue
			}
			if err != nil {
				return nil, err
			}
			loaded[name] = true
			for _, c := range f.Covered {
				covered[c] = true
			}
			merge(f)
			sc.merged = append(sc.merged, name)
		}
		for _, name := range names {
			if loaded[name] || covered[name] || !strings.HasPrefix(name[len(prefix):], "seg-") {
				continue
			}
			f, err := p.load(ctx, name)
			if gone(err) {
				vanished = true
				continue
			}
			if err != nil {
				return nil, err
			}
			loaded[name] = true
			merge(f)
			sc.merged = append(sc.merged, name)
			sc.segments++
		}

		logs, err := p.store.List(ctx, logPrefix(p.ns.name))
		if err != nil {
			return nil, fmt.Errorf("peercache: listing the logs of %q: %w", p.ns.name, err)
		}
		p.pruneLogs(logs)
		for _, name := range logs {
			if loaded[name] {
				continue
			}
			records, err := p.logRecords(ctx, name, part)
			if gone(err) {
				vanished = true
				continue
			}
			if err != nil {
				return nil, err
			}
			loaded[name] = true
			merge(&segment.File{Records: records})
		}
		if !vanished {
			return sc, nil
		}
		if pass+1 >= scanPasses {
			return nil, fmt.Errorf("peercache: partition %d: objects kept vanishing over %d listings: %w",
				part, scanPasses, ErrNotFound)
		}
	}
}

// scanPasses bounds how many times a scan lists again after objects vanished
// under it. Each pass reads only what is new, so it is cheap; the bound is for
// a store that lists objects it cannot return.
const scanPasses = 16

// logRecords returns the records of the unsplit log name that fall in part.
//
// Logs hold records for every partition, so every partition's scan reads
// every log. Read separately that was each log fetched and decoded once per
// partition: a restore of all 64 after a membership change, with a few
// hundred logs waiting for the splitter, made tens of thousands of reads of
// the same few hundred objects. So the first scan to want a log reads it once
// and cuts it by partition, and every scan running meanwhile takes its share
// from that, and so does every scan after it until the log is gone.
//
// A log is immutable until the splitter deletes it, and a deleted log's
// records are in the partition segments a scan lists as well, so a kept copy
// is never wrong, only spare. Kept logs are dropped when a listing no longer
// shows them (pruneLogs), and all of them at the split interval when no scan
// or restore is under way (dropIdleLogs): a node that stopped restoring holds
// none for longer than that.
func (p *persister) logRecords(ctx context.Context, name string, part int) ([]segment.Record, error) {
	p.logMu.Lock()
	e := p.logs[name]
	if e == nil {
		e = &logEntry{ready: make(chan struct{})}
		if p.logs == nil {
			p.logs = map[string]*logEntry{}
		}
		p.logs[name] = e
		p.logMu.Unlock()

		f, err := p.load(ctx, name)
		if err == nil {
			for _, r := range f.Records {
				i := persistPartition(p.ns.groupKeyFor(r.Key))
				e.parts[i] = append(e.parts[i], r)
				e.bytes += recordSize(&r)
			}
		}
		p.logMu.Lock()
		e.err = err
		if p.logs[name] == e {
			if err != nil || p.logBytes+e.bytes > persistLogCacheBytes {
				// Failed — perhaps on this caller's context alone — or
				// over the bound: the next asker reads it for itself.
				delete(p.logs, name)
			} else {
				p.logBytes += e.bytes
			}
		}
		p.logMu.Unlock()
		close(e.ready)
	} else {
		p.logMu.Unlock()
	}

	select {
	case <-e.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if e.err != nil {
		if errors.Is(e.err, ErrNotFound) {
			return nil, e.err
		}
		// Another scan's failure, maybe its deadline: try for this one.
		return p.loadLogPart(ctx, name, part)
	}
	return e.parts[part], nil
}

// loadLogPart reads the log name for one scan alone and keeps part's records.
func (p *persister) loadLogPart(ctx context.Context, name string, part int) ([]segment.Record, error) {
	f, err := p.load(ctx, name)
	if err != nil {
		return nil, err
	}
	kept := f.Records[:0]
	for _, r := range f.Records {
		if persistPartition(p.ns.groupKeyFor(r.Key)) == part {
			kept = append(kept, r)
		}
	}
	return kept, nil
}

// useLogs marks a scan or restore under way, which keeps dropIdleLogs from
// dropping the logs it reads, and returns the function that ends it.
func (p *persister) useLogs() (done func()) {
	p.logMu.Lock()
	p.logUsers++
	p.logMu.Unlock()
	return func() {
		p.logMu.Lock()
		p.logUsers--
		p.logMu.Unlock()
	}
}

// pruneLogs drops the kept logs a listing of them no longer shows: split and
// deleted. A listing older than a log just read drops it too, which costs that
// log one more read.
func (p *persister) pruneLogs(listed []string) {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	if len(p.logs) == 0 {
		return
	}
	present := make(map[string]struct{}, len(listed))
	for _, name := range listed {
		present[name] = struct{}{}
	}
	for name, e := range p.logs {
		if _, ok := present[name]; ok {
			continue
		}
		select {
		case <-e.ready:
			p.logBytes -= e.bytes
			delete(p.logs, name)
		default: // still being read; its reader accounts for it
		}
	}
}

// dropIdleLogs drops every kept log when no scan or restore is under way.
func (p *persister) dropIdleLogs() {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	if p.logUsers == 0 {
		p.logs, p.logBytes = nil, 0
	}
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
		version, merged := p.ns.store.MergeIfNewer(key, r.Value, r.ExpiresAt, r.Order, r.Tombstone)
		if !merged {
			continue
		}
		p.restored.Add(1)
		p.replicateRestored(key, r, version, now)
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

	if err := p.takeRestoreSlot(ctx); err != nil {
		return err
	}
	defer p.releaseRestoreSlot()
	mark := p.beginRestore()
	defer p.endRestore()
	newest, err := p.read(ctx, persistPartition(p.ns.groupKeyFor(key)))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p.restoreFailures.Add(1)
		return nil
	}

	p.mu.Lock()
	delete(p.shed, key)
	p.mu.Unlock()
	// The newest write may not have been flushed when the key was shed.
	p.overlay(newest, mark, func(k string) bool { return k == key })
	r := newest[key]

	if r != nil {
		p.apply(map[string]*segment.Record{key: r})
	}
	return nil
}

// compactOwned compacts every partition this node is the compactor for that
// has collected enough segments.
func (p *persister) compactOwned(ctx context.Context) {
	defer p.useLogs()() // the partitions compacted this round read the logs once
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
	// Counted from the listing before anything is read. Every node looks at
	// the partitions it compacts once a minute, and most minutes most of
	// them are under the threshold; reading each in full — its snapshots,
	// its segments and every unsplit log — to learn that was a minute's
	// worth of a partition's reads, per partition, for nothing. A segment
	// already covered by a snapshot is counted here too, so this can only
	// read more often than needed, never less; the scan's count decides.
	names, err := p.store.List(ctx, prefix)
	if err != nil {
		return fmt.Errorf("peercache: listing %s: %w", prefix, err)
	}
	listed := 0
	for _, name := range names {
		if strings.HasPrefix(name[len(prefix):], "seg-") {
			listed++
		}
	}
	if listed < threshold {
		return nil
	}
	sc, err := p.scan(ctx, part)
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

// splitsLogs reports whether this node is the namespace's splitter: the
// rendezvous owner of a synthetic group key, as a partition's compactor is.
func (p *persister) splitsLogs() bool {
	c := p.ns.cache
	if c.guard == nil {
		return true
	}
	owner, _, err := c.guard.Table().Owner(fmt.Sprintf("persist/%s/log", p.ns.name))
	return err == nil && owner.ID == c.self().ID
}

// split moves every log's records into partition segments, then deletes the
// logs (decision 0030).
//
// The logs are deleted only once every segment is durable. A split that stops
// part-way leaves logs and segments both holding the same records, which the
// merge reads as once; the next split writes the segments again and finishes.
// A segment's name carries a hash of the logs it came from, so two splits of
// different log sets never write to the same name.
func (p *persister) split(ctx context.Context) error {
	logs, err := p.store.List(ctx, logPrefix(p.ns.name))
	if err != nil || len(logs) == 0 {
		return err
	}
	byPartition := map[int]map[string]*segment.Record{}
	for _, name := range logs {
		f, err := p.load(ctx, name)
		if err != nil {
			return err
		}
		for i := range f.Records {
			r := &f.Records[i]
			part := persistPartition(p.ns.groupKeyFor(r.Key))
			keys := byPartition[part]
			if keys == nil {
				keys = map[string]*segment.Record{}
				byPartition[part] = keys
			}
			if cur, ok := keys[r.Key]; !ok || r.Newer(cur) {
				keys[r.Key] = r
			}
		}
	}

	h := xxhash.New()
	for _, name := range logs {
		_, _ = h.WriteString(name)
	}
	tag := fmt.Sprintf("%016x", h.Sum64())
	writer := hex.EncodeToString([]byte(p.ns.cache.self().ID))

	var (
		failed  error
		errMu   sync.Mutex
		wg      sync.WaitGroup
		workers = make(chan struct{}, persistSplitParallel)
	)
	for part, keys := range byPartition {
		records := make([]segment.Record, 0, len(keys))
		var maxOrder uint64
		for _, r := range keys {
			records = append(records, *r)
			if r.Order > maxOrder {
				maxOrder = r.Order
			}
		}
		sort.Slice(records, func(i, j int) bool { return records[i].Key < records[j].Key })
		name := fmt.Sprintf("%sseg-%016x-%s-%s.bin", partitionPrefix(p.ns.name, part), maxOrder, writer, tag)
		data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})

		wg.Add(1)
		workers <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-workers }()
			if err := p.store.Put(ctx, name, data); err != nil {
				errMu.Lock()
				failed = errors.Join(failed, fmt.Errorf("peercache: writing %s: %w", name, err))
				errMu.Unlock()
			}
		}()
	}
	wg.Wait()
	if failed != nil {
		return failed
	}
	for _, name := range logs {
		if err := p.store.Delete(ctx, name); err != nil {
			return fmt.Errorf("peercache: deleting %s: %w", name, err)
		}
	}
	p.splits.Add(1)
	return nil
}

// replicateRestored sends a value or deletion a restore just installed to the
// key's replicas.
//
// A restore runs because this node came to own keys it did not hold — at
// start, or because every holder of them died. What it installs exists on
// this node alone, so without this the key sat at a replication factor of one
// until its next write, and losing this node as well meant restoring from
// storage again. Always asynchronous: a restore has no caller waiting for
// durability, and it runs inside one, where waiting on a slow replica would
// stall the read that triggered it.
func (p *persister) replicateRestored(key string, r *segment.Record, version uint64, now int64) {
	ns := p.ns
	if ns.opts.ReplicationFactor < 2 {
		return
	}
	targets, _, err := ns.cache.replicaTargets(ns.groupKeyFor(key), ns.opts.ReplicationFactor)
	if err != nil || len(targets) == 0 {
		return
	}
	w := replicaWork{namespace: ns.name, key: key, order: r.Order, targets: targets}
	if r.Tombstone {
		w.stamp = ns.stamp(math.MaxUint64)
		w.deleted = true
	} else {
		w.value = r.Value
		w.stamp = ns.stamp(version)
		if r.ExpiresAt != 0 {
			w.ttl = time.Duration(r.ExpiresAt - now)
		}
	}
	_ = ns.cache.replicator.replicate(context.Background(), false, w)
}
