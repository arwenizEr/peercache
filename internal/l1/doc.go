// Package l1 caches copies of keys this node does not own.
//
// Two rules govern every entry, and they are correctness rules rather than
// tuning:
//
//  1. Entries are TTL bounded, always. The TTL is the upper bound on how stale
//     a read can be, and it is the only such bound that exists. There is no way
//     to store an entry here without one.
//  2. Entries are version stamped by the owner, so a stale copy can be
//     recognised rather than merely aged out, and so an out-of-order update
//     cannot resurrect data the owner has already replaced.
//
// Invalidation broadcasts are a latency optimisation. They are best effort,
// they will be lost under partition, and correctness must never depend on one
// arriving. If a change here would make an invalidation load-bearing, the
// change is wrong.
//
// # Why the TTL is not delegated to the cache library
//
// Every candidate library implements expiry against the wall clock. That is
// untestable under a fake clock and unreachable by any clock this project
// injects, so the library provides capacity-bounded eviction and nothing else.
// Expiry is computed here, from the injected clock, and enforced on read. The
// library never sees a TTL.
//
// # Choice of library
//
// github.com/maypok86/otter v1, selected by measurement over hashicorp/golang-lru,
// otter v2 and dgraph-io/ristretto. The evaluation harness is in ./eval and the
// numbers and reasoning are in docs/decisions/0005-l1-cache-library.md. The
// short version: reads dominate this tier and otter serves them about six times
// faster than an exact LRU behind a single mutex, ristretto is disqualified
// because its writes are not visible to the next read, and otter v2 and
// ristretto both require Go 1.24 while this project's floor is 1.23.
package l1
