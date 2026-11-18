package store

// TakeWhere removes and returns every live entry whose key take accepts. It is
// how a node gives up keys it stopped owning: the caller files them as replica
// copies, where the replica store's own rules keep, hand off or drop them. It
// writes no tombstone — the key was not deleted, only no longer owned here.
//
// Each shard is scanned under its read lock and write-locked only if it has
// something to give up, so a pass that finds nothing — the usual case — does
// not stall writers. take may be called twice for a key.
func (s *Store) TakeWhere(take func(key string) bool) []SnapshotEntry {
	var out []SnapshotEntry
	for _, sh := range s.shards {
		var keys []string
		sh.mu.RLock()
		for key := range sh.entries {
			if take(key) {
				keys = append(keys, key)
			}
		}
		sh.mu.RUnlock()
		if len(keys) == 0 {
			continue
		}
		now := s.clk.Now().UnixNano()
		sh.mu.Lock()
		for _, key := range keys {
			e, ok := sh.entries[key]
			if !ok || !take(key) {
				continue
			}
			if e.expiresAt == 0 || now < e.expiresAt {
				out = append(out, SnapshotEntry{Key: key, Value: e.val, Version: e.version, ExpiresAt: e.expiresAt, Order: e.order})
			}
			sh.bytes.Add(-size(key, e.val))
			s.deleteEntry(sh, key)
		}
		sh.shrink(false)
		sh.mu.Unlock()
	}
	return out
}
