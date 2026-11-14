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
	}
	sh.entries[key] = e
}

// deleteEntry removes key and its digest contribution. Callers hold sh.mu for
// writing; it is safe inside a range over sh.entries.
func (s *Store) deleteEntry(sh *shard, key string) {
	if old, had := sh.entries[key]; had {
		s.digest.Remove(digest.KeyOf(key), old.version)
		delete(sh.entries, key)
	}
}

// Digest returns the running digest of every entry the store holds, expired
// ones not yet swept included, without walking it. See
// docs/decisions/0039-cluster-view.md.
func (s *Store) Digest() [digest.Parts]digest.Pair { return s.digest.Snapshot() }
