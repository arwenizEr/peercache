package peercache

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
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

	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
	// flushMu serialises flushes: the ticker's, a kick's and Close's.
	flushMu sync.Mutex

	flushes  atomic.Int64
	written  atomic.Int64
	failures atomic.Int64
}

func newPersister(ns *namespace, store core.BlobStore) *persister {
	p := &persister{
		ns:      ns,
		store:   store,
		clk:     ns.cache.opts.Clock,
		pending: map[string]*segment.Record{},
		kick:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	// The ticker is created here, not in run: a ticker armed on the
	// goroutine misses any clock advance that lands before the goroutine is
	// scheduled, and the first flush then comes an interval late — on the
	// fake clock, never.
	ticker := p.clk.NewTicker(persistFlushInterval)
	p.wg.Add(1)
	go p.run(ticker)
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
		select {
		case p.kick <- struct{}{}:
		default:
		}
	}
}

func recordSize(r *segment.Record) int {
	return len(r.Namespace) + len(r.Key) + len(r.Writer) + len(r.Value) + 32
}

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

// counters reports pending keys, flushes, records written and failed segments.
func (p *persister) counters() (pending int, flushes, written, failures int64) {
	p.mu.Lock()
	pending = len(p.pending)
	p.mu.Unlock()
	return pending, p.flushes.Load(), p.written.Load(), p.failures.Load()
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
