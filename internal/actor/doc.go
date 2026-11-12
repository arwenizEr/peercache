// Package actor runs groups as virtual actors.
//
// A group is addressable at all times but activated lazily and deactivated
// after an idle timeout. While activated it owns exactly
// one goroutine, and every operation on it is fed through a bounded mailbox and
// executed in turn. That single goroutine is the entire serialization
// mechanism, which is why the public API has no lock.
//
// # Records and activations
//
// The two are deliberately separate:
//
//   - A [record] holds a group's identity, its state, and its timer schedule.
//     It survives deactivation, which is what makes "a group that deactivates
//     and reactivates preserves state and pending timers" possible at all.
//   - An activation holds the mailbox, the goroutine and the idle timer. It is
//     created on demand and torn down when the group goes idle or loses an LRU
//     eviction.
//
// So an addressable group costs a record; an active one costs a goroutine on
// top. The distinction is the whole memory story: a million addressable groups
// and twenty thousand active ones are very different numbers, and only the
// second involves a scheduler.
//
// # Consequences that shape the implementation
//
//   - A full mailbox returns ErrGroupOverloaded rather than blocking, because
//     an unbounded queue converts one slow group into a cluster-wide stall.
//     Rejection is immediate: waiting for room would mean allocating a timer
//     per rejected call exactly when the system is already overloaded.
//   - The caller's context deadline is carried into the turn. A group function
//     that ignores it holds the group's only goroutine hostage while every
//     other request queues behind it.
//   - Timers belong to the group, run on the injected clock, and are re-armed
//     on activation from a schedule that outlives deactivation. A group with
//     pending timers is exempt from idle deactivation — otherwise nothing would
//     ever wake it and its timer would silently never fire.
//   - A [State] handed to a group function is valid for that turn only. Using
//     it afterwards does nothing and is counted; it does not panic, and it
//     cannot race with a later turn.
//
// # Why this package does not import the root package
//
// Same reason as every other internal package: the root package's constructor
// imports this one. See docs/decisions/0001-core-types-location.md.
package actor
