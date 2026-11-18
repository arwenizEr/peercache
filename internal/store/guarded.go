package store

import "time"

// LookupGuarded is [Store.Lookup] for a store whose absent keys may be held
// elsewhere — a persistent namespace's owned store, whose memory budget sheds
// keys it can read back. On a miss, and only then, it calls held under the
// shard's lock and reports what it said: true means the key is not in memory
// but exists, so the miss must not be answered as an absence.
//
// Called under the lock because that is where [Store.ShedMarked] marks a key
// before removing it: a miss followed by a separate check could see the key
// shed between the two and report it absent. held must not call into this
// store. A nil held asks nothing: this is then [Store.Lookup].
func (s *Store) LookupGuarded(key string, held func(key string) bool) (val []byte, version uint64, left time.Duration, ok, isHeld bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	e, found := sh.entries[key]
	if !found && held != nil {
		isHeld = held(key)
	}
	sh.mu.RUnlock()
	if !found {
		sh.misses.Add(1)
		return nil, 0, 0, false, isHeld
	}
	if e.expiresAt != 0 {
		left = time.Duration(e.expiresAt - s.clk.Now().UnixNano())
		if left <= 0 {
			sh.misses.Add(1)
			return nil, 0, 0, false, false
		}
	}
	sh.hits.Add(1)
	return e.val, e.version, left, true, false
}

// SetIfAbsentGuarded is [Store.SetIfAbsent] that, when the key is absent,
// first asks held under the shard's lock; true stores nothing and reports
// isHeld. A value stored over a key held elsewhere would outrank it. See
// [Store.LookupGuarded].
func (s *Store) SetIfAbsentGuarded(key string, val []byte, ttl time.Duration, held func(key string) bool) (resident []byte, stored, isHeld bool) {
	var expiresAt int64
	now := s.clk.Now()
	if ttl > 0 {
		expiresAt = s.deadline(key, now, ttl)
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	old, had := sh.entries[key]
	if had && (old.expiresAt == 0 || now.UnixNano() < old.expiresAt) {
		return old.val, false, false
	}
	if !had && held != nil && held(key) {
		return nil, false, true
	}
	sh.forget(key)
	copied := make([]byte, len(val))
	copy(copied, val)
	version := old.version + 1
	if !had && version <= sh.floor {
		version = sh.floor + 1
	}
	s.putEntry(sh, key, entry{val: copied, expiresAt: expiresAt, version: version, order: s.nextOrder(), prev: old.order | unrecordedBit, writer: s.writer})
	delta := size(key, copied)
	if had {
		delta -= size(key, old.val)
	}
	sh.bytes.Add(delta)
	sh.sets.Add(1)
	return copied, true, false
}
