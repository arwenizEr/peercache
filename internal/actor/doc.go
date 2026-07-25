// Package actor runs groups as virtual actors.
//
// A group is addressable at all times but activated lazily on its owner and
// deactivated after an idle timeout (CLAUDE.md, decision 2). While activated it
// owns exactly one goroutine, and every operation on it is fed through a
// bounded mailbox and executed in turn. That single goroutine is the entire
// serialization mechanism, which is why the public API has no lock.
//
// Consequences that shape the implementation:
//
//   - A full mailbox returns ErrGroupOverloaded rather than blocking, because
//     an unbounded queue converts one slow group into a cluster-wide stall.
//   - The caller's context deadline is carried into the turn. A group function
//     that ignores it holds the group's only goroutine hostage.
//   - Timers belong to the group, run on the injected clock, are cancelled on
//     deactivation and re-registered on activation. A timer that outlives its
//     activation is a leak and a correctness bug at once.
//   - A GroupState handed to a group function is valid for that turn only. The
//     generation stamp that enforces this must fail closed, and without a
//     panic.
//
// Milestone: M2. This package is empty until then.
package actor
