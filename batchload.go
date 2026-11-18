package peercache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// BatchGetOrLoad returns the values for keys, loading on each key's owner the
// ones it does not hold. Keys go to their owners as BatchGet's do, one request
// per owner; each owner calls the namespace's batch loader once with the keys
// it is missing, or its per-key loader for each. See
// docs/decisions/0035-batch-loads.md.
func (n *namespace) BatchGetOrLoad(ctx context.Context, keys []string) (map[string][]byte, error) {
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := n.status(); err != nil {
		return nil, err
	}
	if n.opts.Loader == nil && n.opts.BatchLoader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	out := make(map[string][]byte, len(keys))
	return out, n.batchGetOrLoadInto(ctx, keys, out, true)
}

// batchGetOrLoadInto is BatchGetOrLoad's body, writing into out. reask says
// whether keys an owner names as not its own are asked for again; it is true
// for the caller's request and false for that one retry, so a key that keeps
// moving is reported rather than chased.
func (n *namespace) batchGetOrLoadInto(ctx context.Context, keys []string, out map[string][]byte, reask bool) error {
	byOwner, unresolved, err := n.cache.route.groupKeysByOwner(n, keys)
	if err != nil {
		return err
	}
	var failures error
	if len(unresolved) > 0 {
		failures = fmt.Errorf("peercache: %d key(s) have no resolvable owner: %w",
			len(unresolved), ErrNotFound)
	}
	for id, batch := range byOwner {
		if err := ctx.Err(); err != nil {
			return errors.Join(failures, err)
		}
		if id == localBucket {
			if err := n.localBatchGetOrLoad(ctx, batch.keys, out); err != nil {
				failures = errors.Join(failures, err)
			}
			continue
		}
		if err := n.remoteBatchGetOrLoad(ctx, batch, out, reask); err != nil {
			failures = errors.Join(failures, err)
		}
	}
	return failures
}

// localBatchGetOrLoad answers the keys this node owns: what the store holds,
// then one load of the rest.
func (n *namespace) localBatchGetOrLoad(ctx context.Context, keys []string, out map[string][]byte) error {
	var missing []string
	for i, key := range keys {
		if i%64 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		stored, ok, err := n.getResident(ctx, key)
		if err != nil {
			return err
		}
		if ok {
			val, err := n.decode(stored)
			if err != nil {
				return err
			}
			out[key] = val
			continue
		}
		missing = append(missing, key)
	}
	if len(missing) == 0 {
		return nil
	}
	return n.loadMissing(ctx, missing, out)
}

// loadMissing loads keys this node owns and does not hold, and adds what it
// found to out. A key the loader reports absent is left out.
func (n *namespace) loadMissing(ctx context.Context, missing []string, out map[string][]byte) error {
	// Keys this owner remembers its loader found missing are not asked
	// for again (decision 0034).
	if n.opts.NegativeTTL > 0 {
		kept := missing[:0:0]
		for _, key := range missing {
			if !n.knownAbsent(key) {
				kept = append(kept, key)
			}
		}
		if missing = kept; len(missing) == 0 {
			return nil
		}
	}
	if n.opts.BatchLoader == nil {
		// The per-key loader, through the same single-flight GetOrLoad
		// uses: the round trips are saved, the queries stay per key.
		var failures error
		for _, key := range missing {
			val, err := n.localGetOrLoad(ctx, key)
			switch {
			case err == nil:
				out[key] = val
			case errors.Is(err, ErrNotFound):
			default:
				failures = errors.Join(failures, err)
				if ctx.Err() != nil {
					return failures
				}
			}
		}
		return failures
	}

	// The load runs with the caller's cancellation stripped and the
	// namespace's timeout as its deadline, as a single-key load does: a
	// caller that gives up stops waiting, and the load still stores what it
	// found for whoever asks next.
	type result struct {
		found map[string][]byte
		err   error
	}
	done := make(chan result, 1)
	go func() {
		loadCtx, cancel := clock.WithTimeout(context.WithoutCancel(ctx), n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()
		found, err := n.runBatchLoader(loadCtx, missing)
		done <- result{found, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			return fmt.Errorf("namespace %q: batch load of %d key(s): %w", n.name, len(missing), r.err)
		}
		for k, v := range r.found {
			out[k] = v
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runBatchLoader calls the batch loader and stores what it returned, each key
// with SetIfAbsent: a write that landed while the loader ran is newer than
// what the loader read, and is what the caller gets. It returns the value each
// requested key now has. Keys the loader returned that were not asked for are
// ignored. A loader that fails stores nothing, whatever it also returned.
func (n *namespace) runBatchLoader(ctx context.Context, keys []string) (found map[string][]byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			found, err = nil, fmt.Errorf("%w: %v", ErrLoaderPanic, r)
		}
	}()
	ctx, span := n.cache.traceLoad(ctx, n.name, len(keys))
	vals, ttl, err := n.opts.BatchLoader(ctx, keys)
	span.End(err)
	if err != nil {
		return nil, err
	}
	found = make(map[string][]byte, len(vals))
	for _, key := range keys {
		val, ok := vals[key]
		if !ok {
			// It does not exist: remembered, in a namespace that
			// remembers absences, as a per-key load's ErrNotFound is.
			if n.opts.NegativeTTL > 0 {
				n.store.MarkAbsent(key, n.opts.NegativeTTL)
			}
			continue
		}
		resident, stored, held := n.store.SetIfAbsentGuarded(key, n.encode(val), ttl, n.shedHeld)
		if held {
			// Shed while the loader ran: storage holds something newer
			// than the load. Left out, the batch would report it absent;
			// the batch is refused, as a single load is.
			return nil, n.shedSinceRestore(key)
		}
		if !stored {
			current, err := n.decode(resident)
			if err != nil {
				return nil, err
			}
			found[key] = current
			continue
		}
		n.cache.pressure.noteWrite(len(val))
		found[key] = val
	}
	return found, nil
}

// remoteBatchGetOrLoad asks one owner for its share with load set. Keys the
// owner reports it does not own — ownership moved while the request was in
// flight — are asked for again one at a time, wherever they live now, so a
// moved key is not mistaken for one that does not exist.
func (n *namespace) remoteBatchGetOrLoad(ctx context.Context, batch *ownerBatch, out map[string][]byte, reask bool) error {
	wanted := batch.keys
	if n.l1 != nil {
		wanted = wanted[:0:0]
		for _, key := range batch.keys {
			if val, _, ok := n.l1.Get(key); ok {
				out[key] = val // plain; see remoteGet
				continue
			}
			wanted = append(wanted, key)
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	entries, misrouted, err := n.cache.route.remoteBatchGetOrLoad(ctx, batch.placement, n.name, wanted)
	if err != nil {
		return err
	}
	for _, e := range entries {
		out[e.Key] = e.Value // plain; see remoteGet
		if n.l1 != nil {
			n.l1.Put(e.Key, e.Value, e.Stamp, l1Lifetime(n.opts.L1TTL, time.Duration(e.TTLRemainingNanos)))
		}
	}
	if len(misrouted) == 0 {
		return nil
	}
	if !reask {
		return fmt.Errorf("peercache: %d key(s) moved again while being loaded: %w", len(misrouted), ErrNotOwner)
	}
	// Asked for again as a batch, not one key at a time through GetOrLoad:
	// a namespace with only a batch loader has no per-key loader, and every
	// moved key failed the whole call with ErrNoLoader.
	return n.batchGetOrLoadInto(ctx, misrouted, out, false)
}
