package digest

import (
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// Parts is the number of partitions. A mismatch is localised to one of them;
// more would localise better and cost more on every comparison.
const Parts = 64

// Pair is one partition's count of keys and wrapping sum of their hashes.
type Pair struct {
	Count uint64
	Sum   uint64
}

// Set is a running digest. Its methods are safe for concurrent use; a store
// calls Add and Remove under the lock that guards the entry they describe.
type Set struct {
	parts [Parts]struct {
		count atomic.Uint64
		sum   atomic.Uint64
	}
}

// Key is a key's hash, computed once per write and used for both its partition
// and its contribution.
type Key uint64

// KeyOf hashes key.
func KeyOf(key string) Key { return Key(xxhash.Sum64String(key)) }

// Add records the key k hashes held at version.
func (s *Set) Add(k Key, version uint64) {
	p := &s.parts[k.part()]
	p.count.Add(1)
	p.sum.Add(k.contribution(version))
}

// Remove forgets the key k hashes held at version. It must match an earlier
// Add.
func (s *Set) Remove(k Key, version uint64) {
	p := &s.parts[k.part()]
	p.count.Add(^uint64(0))
	p.sum.Add(-k.contribution(version))
}

// Replace is Remove of from and Add of to for one key, one partition: an
// overwrite, with two atomic adds instead of four.
func (s *Set) Replace(k Key, from, to uint64) {
	s.parts[k.part()].sum.Add(k.contribution(to) - k.contribution(from))
}

// Snapshot returns every partition's pair. Partitions are read one at a time,
// so a snapshot taken during writes may mix moments; compare twice to tell
// writes in flight from a real difference.
func (s *Set) Snapshot() [Parts]Pair {
	var out [Parts]Pair
	for i := range s.parts {
		out[i] = Pair{Count: s.parts[i].count.Load(), Sum: s.parts[i].sum.Load()}
	}
	return out
}

// Part is the partition key falls in.
func Part(key string) int { return KeyOf(key).part() }

func (k Key) part() int { return int(uint64(k) >> 58) }

// contribution is what the key at version adds to its partition's sum.
func (k Key) contribution(version uint64) uint64 {
	h := uint64(k) ^ (version * 0x9E3779B97F4A7C15)
	// A finalising mix (splitmix64), so that versions differing in low
	// bits change every bit of the contribution.
	h ^= h >> 30
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	h ^= h >> 31
	return h
}
