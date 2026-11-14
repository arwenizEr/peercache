package peercache

import (
	"context"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// maybeRefresh starts a background reload of key if its remaining lifetime,
// left (zero for a key that does not expire), is under the namespace's refresh
// window and none is in flight. See [WithRefreshAhead] and
// docs/decisions/0036-refresh-ahead.md.
func (n *namespace) maybeRefresh(key string, version uint64, left time.Duration) {
	if left == 0 || left >= n.opts.RefreshAhead {
		return
	}
	if _, busy := n.refreshing.LoadOrStore(key, struct{}{}); busy {
		return
	}
	n.refreshMu.Lock()
	if n.refreshClosed {
		n.refreshMu.Unlock()
		n.refreshing.Delete(key)
		return
	}
	n.refreshWG.Add(1)
	n.refreshMu.Unlock()
	go func() {
		defer n.refreshWG.Done()
		defer n.refreshing.Delete(key)
		n.refresh(key, version)
	}()
}

// refresh reloads key in the key's single-flight — a caller missing the key,
// because it expired meanwhile, joins this load rather than starting another —
// and stores the result only if the key still holds version. Joiners get the
// reloaded value whether or not it was stored: it is what the source holds.
func (n *namespace) refresh(key string, version uint64) {
	// No caller is waiting, so no caller's context bounds it: the
	// namespace's loader timeout does, inside, as for every load.
	result := n.flight.Do(context.Background(), key, func(loadCtx context.Context) ([]byte, time.Duration, error) {
		loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()
		loadCtx, span := n.cache.traceLoad(loadCtx, n.name, 1)
		val, ttl, err := n.opts.Loader(loadCtx, key)
		span.End(err)
		if err != nil {
			return nil, 0, err
		}
		if n.store.PutIfVersion(key, n.encode(val), ttl, version) {
			n.cache.pressure.noteWrite(len(val))
		}
		return val, ttl, nil
	})
	if result.Err != nil {
		n.refreshFailures.Add(1)
		return
	}
	n.refreshes.Add(1)
}

// closeRefreshes stops new refreshes and waits for the ones in flight.
func (n *namespace) closeRefreshes() {
	n.refreshMu.Lock()
	n.refreshClosed = true
	n.refreshMu.Unlock()
	n.refreshWG.Wait()
}
