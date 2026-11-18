package store

import "github.com/arwenizEr/peercache/internal/digest"

// putEntry stores e under key and keeps the digest in step: the entry it
// replaces is removed from it, e added. Every write to entries goes through
// here, so no path can change what the store holds without the digest seeing
// it. Callers hold sh.mu for writing.
func (s *Store) putEntry(sh *shard, key string, e entry) {
	k := digest.KeyOf(key)
	if old, had := sh.entries[key]; had {
		s.digest.Replace(k, old.version, e.version)
	} else {
		s.digest.Add(k, e.version)
		if len(sh.entries) < sh.high {
			// Into a slot a removal left, which was charged as one: the
			// caller charges the entry in full, so that charge goes.
			sh.bytes.Add(-EntryOverhead)
		}
	}
	sh.entries[key] = e
	sh.high = max(sh.high, len(sh.entries))
}

// deleteEntry removes key and its digest contribution. Callers hold sh.mu for
// writing; it is safe inside a range over sh.entries.
func (s *Store) deleteEntry(sh *shard, key string) {
	if old, had := sh.entries[key]; had {
		s.digest.Remove(digest.KeyOf(key), old.version)
		delete(sh.entries, key)
		// The caller takes the entry's charge off in full; its slot stays
		// in the map until a rebuild, and is charged until then.
		sh.bytes.Add(EntryOverhead)
	}
}

// shrink rebuilds the shard's map once it holds under three quarters of the
// entries it has held, or whenever it holds fewer if force is set, and gives
// back the slots' charge. Callers hold mu, and call it after removing rather
// than from deleteEntry, which runs inside ranges over the map.
//
// A Go map never gives back the slots of what is deleted from it; see the
// replica store's shrink, which found it. A shard is a 256th of the store,
// so the copy is a few thousand entries.
func (sh *shard) shrink(force bool) {
	n := len(sh.entries)
	if n == sh.high || (!force && n*4 >= sh.high*3) {
		return
	}
	fresh := make(map[string]entry, n)
	for key, e := range sh.entries {
		fresh[key] = e
	}
	sh.bytes.Add(-int64(sh.high-n) * EntryOverhead)
	sh.entries, sh.high = fresh, n
}

// Digest returns the running digest of every entry the store holds, expired
// ones not yet swept included, without walking it. See
// docs/decisions/0039-cluster-view.md.
func (s *Store) Digest() [digest.Parts]digest.Pair { return s.digest.Snapshot() }
