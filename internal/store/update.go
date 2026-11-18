package store

import "time"

// Mutation is what an [Store.Update] callback asks to store.
type Mutation struct {
	// Value is the new stored value. It is copied.
	Value []byte
	// TTL is the new lifetime, with Set's meaning: non-positive means no
	// deadline. Ignored when KeepDeadline is set.
	TTL time.Duration
	// KeepDeadline keeps the current entry's deadline, or none if the key
	// is absent. An increment keeps its counter's expiry; a touch does not.
	KeepDeadline bool
}

// UpdateResult reports what [Store.Update] did.
type UpdateResult struct {
	// Wrote reports whether a mutation was stored.
	Wrote bool
	// Version is the stored entry's version after the call: the new one if
	// Wrote, the current one otherwise, zero if the key is absent.
	Version uint64
	// ExpiresAt is the stored entry's deadline in Unix nanoseconds, zero
	// for none, on the same terms as Version.
	ExpiresAt int64
	// Order is the write's persistence order, or zero without
	// [Options.Order] or when nothing was written.
	Order uint64
	// Prev is the order of the value the write replaced; see [Write.Prev].
	Prev uint64
}

// Update is the store's one read-modify-write: it calls fn with key's live
// value under the shard's lock and stores the mutation fn returns, so nothing
// can write the key between the read and the write.
//
// fn sees ok=false for an absent or expired key, exactly as [Store.Get] would.
// If it returns write=false or an error, nothing changes and the error is
// returned as it is. cur is the stored slice and must not be retained or
// modified. fn runs with the shard locked: it must be quick, must not block,
// and must not call back into the store.
//
// Versions and the floor follow [Store.Set]: an existing key's version
// increases by one, and a key recreated after a delete or a shed starts above
// the shard's floor.
func (s *Store) Update(key string, fn func(cur []byte, ok bool) (m Mutation, write bool, err error)) (UpdateResult, error) {
	now := s.clk.Now()
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.forget(key)

	old, had := sh.entries[key]
	live := had && (old.expiresAt == 0 || now.UnixNano() < old.expiresAt)
	var cur []byte
	if live {
		cur = old.val
	}
	m, write, err := fn(cur, live)
	if err != nil || !write {
		if live {
			return UpdateResult{Version: old.version, ExpiresAt: old.expiresAt}, err
		}
		return UpdateResult{}, err
	}

	var expiresAt int64
	switch {
	case m.KeepDeadline:
		if live {
			expiresAt = old.expiresAt
		}
	case m.TTL > 0:
		expiresAt = s.deadline(key, now, m.TTL)
	}

	stored := make([]byte, len(m.Value))
	copy(stored, m.Value)

	// An expired entry still in the map is a predecessor like any other: the
	// version continues from it, as Set's does.
	version := old.version + 1
	if !had && version <= sh.floor {
		version = sh.floor + 1
	}
	order := s.nextOrder()
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: version, order: order, prev: old.order, writer: s.writer})
	delta := size(key, stored)
	if had {
		delta -= size(key, old.val)
	}
	sh.bytes.Add(delta)
	sh.sets.Add(1)
	return UpdateResult{Wrote: true, Version: version, ExpiresAt: expiresAt, Order: order, Prev: old.order}, nil
}
