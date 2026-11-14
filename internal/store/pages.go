package store

import "github.com/arwenizEr/peercache/internal/pager"

// SnapshotFrom is [Store.Snapshot] for a pass that pages through the whole
// store: catch-up and hand-off. The pass's sorted key list is built once, when
// after is empty, and kept by p under id, so each page costs its own size and a
// binary search instead of a scan and sort of everything above the cursor. id
// names the pass — one per requester, namespace and kind. want is applied when
// the list is built and again to every key read, so a key the asker stopped
// being entitled to mid-pass is not sent.
//
// Keys removed since the list was built are skipped; a page they empty is
// skipped too, so a page is empty only at the end of the pass — the caller
// stops on an empty page.
func (s *Store) SnapshotFrom(p *pager.Pager, id, after string, limit int, want func(key string) bool) ([]SnapshotEntry, bool) {
	build := func() []string {
		var keys []string
		now := s.clk.Now().UnixNano()
		for _, sh := range s.shards {
			sh.mu.RLock()
			for key, e := range sh.entries {
				if e.expiresAt != 0 && now >= e.expiresAt {
					continue
				}
				if want != nil && !want(key) {
					continue
				}
				keys = append(keys, key)
			}
			sh.mu.RUnlock()
		}
		return keys
	}
	for {
		keys, more := p.Page(id, after, limit, build)
		page := make([]SnapshotEntry, 0, len(keys))
		now := s.clk.Now().UnixNano()
		for _, key := range keys {
			sh := s.shardFor(key)
			sh.mu.RLock()
			e, ok := sh.entries[key]
			if ok && (e.expiresAt == 0 || now < e.expiresAt) && (want == nil || want(key)) {
				val := make([]byte, len(e.val))
				copy(val, e.val)
				page = append(page, SnapshotEntry{Key: key, Value: val, Version: e.version, ExpiresAt: e.expiresAt, Order: e.order})
			}
			sh.mu.RUnlock()
		}
		if len(page) > 0 || !more {
			return page, more
		}
		after = keys[len(keys)-1]
	}
}
