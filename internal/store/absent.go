package store

import "time"

// MaxAbsentPerShard bounds a shard's remembered absences. A full shard drops
// expired ones first, then an arbitrary one: forgetting an absence costs one
// load, never a wrong answer, so the bound can be small.
const MaxAbsentPerShard = 256

// MarkAbsent remembers, for ttl, that key's loader found nothing, and reports
// whether it did.
//
// It records nothing if the store holds a live entry for key: a write that
// landed while the loader ran is newer than what the loader saw, the same rule
// [Store.SetIfAbsent] applies to a load that found a value. Every later
// mutation of key forgets the absence, so it never outlives a write. It is not
// an entry: [Store.Get], [Store.Exists], snapshots and the byte accounting do
// not see it. See docs/decisions/0034-negative-caching.md.
func (s *Store) MarkAbsent(key string, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	now := s.clk.Now()
	deadline := deadlineNanos(now, ttl)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if e, ok := sh.entries[key]; ok && (e.expiresAt == 0 || now.UnixNano() < e.expiresAt) {
		return false
	}
	if sh.absent == nil {
		sh.absent = make(map[string]int64)
	}
	if _, ok := sh.absent[key]; !ok && len(sh.absent) >= MaxAbsentPerShard {
		sh.dropExpiredAbsences(now.UnixNano())
		if len(sh.absent) >= MaxAbsentPerShard {
			for k := range sh.absent {
				delete(sh.absent, k)
				break
			}
		}
	}
	sh.absent[key] = deadline
	return true
}

// KnownAbsent reports whether key is remembered as absent: marked by
// [Store.MarkAbsent], not yet past its deadline, and not written since.
func (s *Store) KnownAbsent(key string) bool {
	sh := s.shardFor(key)
	sh.mu.RLock()
	deadline, ok := sh.absent[key]
	sh.mu.RUnlock()
	return ok && s.clk.Now().UnixNano() < deadline
}

// Absences returns the number of remembered absences, expired ones not yet
// swept included.
func (s *Store) Absences() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.absent)
		sh.mu.RUnlock()
	}
	return n
}

// dropExpiredAbsences removes absences past their deadline. Callers hold mu
// for writing.
func (sh *shard) dropExpiredAbsences(now int64) {
	for k, deadline := range sh.absent {
		if now >= deadline {
			delete(sh.absent, k)
		}
	}
}
