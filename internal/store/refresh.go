package store

import "time"

// Lookup is [Store.Get] that also returns how long the entry has left, zero
// for an entry that does not expire. A refresh needs to know; computing it
// from the clock read the expiry check already makes saves a second read on
// every hit.
func (s *Store) Lookup(key string) (val []byte, version uint64, left time.Duration, ok bool) {
	val, version, left, ok, _ = s.LookupGuarded(key, nil)
	return val, version, left, ok
}

// PutIfVersion stores val under key for ttl only if the key is live and still
// at version, and reports whether it did. It is what a refresh stores with: a
// reload read its source while the key held version, and a write that landed
// since — a Set, a Delete, an atomic operation — is newer than what it read.
// A key that expired meanwhile is not stored either.
func (s *Store) PutIfVersion(key string, val []byte, ttl time.Duration, version uint64) bool {
	now := s.clk.Now()
	var expiresAt int64
	if ttl > 0 {
		expiresAt = deadlineNanos(now, ttl)
	}
	stored := make([]byte, len(val))
	copy(stored, val)

	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	old, had := sh.entries[key]
	if !had || old.version != version || (old.expiresAt != 0 && now.UnixNano() >= old.expiresAt) {
		return false
	}
	sh.forget(key)
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: version + 1, order: s.nextOrder(), prev: old.order | unrecordedBit, writer: s.self})
	sh.bytes.Add(size(key, stored) - size(key, old.val))
	sh.sets.Add(1)
	return true
}

// Remaining is how long key has left, zero for a key that does not expire, and
// whether it is live — [Store.Lookup] without the value and without counting a
// hit or a miss. An owner answering a peer reads it to tell the peer how long
// its copy may live.
func (s *Store) Remaining(key string) (time.Duration, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, found := sh.entries[key]
	sh.mu.RUnlock()
	if !found {
		return 0, false
	}
	if e.expiresAt == 0 {
		return 0, true
	}
	left := time.Duration(e.expiresAt - s.clk.Now().UnixNano())
	if left <= 0 {
		return 0, false
	}
	return left, true
}
