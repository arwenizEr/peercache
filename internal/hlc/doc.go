// Package hlc is the hybrid logical clock that orders writes to persistent
// namespaces.
//
// An [Order] is 64 bits: the high 48 are milliseconds of a node's wall clock,
// the low 16 a counter. A node issues orders with [Clock.Next], which never
// repeats or goes back, and advances its clock with [Clock.Observe] whenever it
// learns of an order from elsewhere — a replicated write, a catch-up page, a
// record read back from storage.
//
// # Why not the in-memory stamp
//
// A persisted record outlives every node that wrote it, so its order must be
// comparable across owners and across time. The in-memory core.Stamp compares
// per-view epochs first, and a key's new owner can hold a lower epoch than its
// old one. The wall-clock half of an Order is what makes a write by a node that
// never saw a key's history still outrank that history; the logical half and
// Observe are what make a node that did see it outrank it whatever its own
// clock says. Two writes to one key, by owners that never exchanged an order,
// closer together than their clocks' skew, may be ordered either way. That is
// the stated limit; see docs/decisions/0029-persistence-as-a-segment-log.md.
//
// # Drift
//
// An observed order more than [MaxDrift] ahead of this node's wall clock is
// refused and counted rather than adopted, so one node with a wrong clock cannot
// drag every node's orders into the future.
package hlc
