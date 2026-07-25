// Package l1 caches copies of keys this node does not own.
//
// Two rules govern every entry, and they are correctness rules rather than
// tuning (CLAUDE.md, decision 6):
//
//  1. Entries are TTL bounded. The TTL is the upper bound on how stale a read
//     can be, and it is the only such bound that exists.
//  2. Entries are version stamped by the owner, so a stale copy can be
//     recognised rather than merely aged out.
//
// Invalidation broadcasts are a latency optimisation. They are best effort,
// they will be lost under partition, and correctness must never depend on one
// arriving. If a change here would make an invalidation load-bearing, the
// change is wrong.
//
// Milestone: M1. This package is empty until then.
package l1
