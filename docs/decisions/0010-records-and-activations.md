# 0010 — A group is a record plus an optional activation

**Status:** accepted, M2
**Affects:** `internal/actor`, `Group`

## The forcing constraint

SPEC.md section 4 (M2) requires two things that pull in opposite directions:

- a group that deactivates and reactivates **preserves state and pending
  timers**;
- memory for **1M addressable / 20k active** groups stays inside budget.

If a group's state lived in its goroutine, deactivation would destroy it and the
first requirement would be unsatisfiable. If every addressable group held a
goroutine, the second would be.

## The decision

Split the group in two:

| | Holds | Lifetime |
|---|---|---|
| **record** | id, state map, timer schedule | created on first touch, never destroyed |
| **activation** | mailbox, goroutine, idle timer, LRU position | created on demand, released when idle or evicted |

Deactivation drops the activation and keeps the record. Reactivation attaches a
new one and re-arms the surviving timer schedule from its absolute deadlines.

Measured on Machine A: a record costs **167 B**, an activation **~1.1 KiB of
heap plus ~8 KiB of goroutine stack**. Fifty-fold in heap, and the stack is the
real term — a million active groups would want roughly 9 GiB of stack, which is
why `MaxActiveGroups` exists and defaults to 20,000.

## What this makes safe, and why it needs no lock on state

State is touched by exactly one goroutine at a time, and the mechanism is
structural rather than a lock:

- While active, only the turn goroutine touches state.
- Activation happens under the record's mutex **before** the goroutine starts.
- Deactivation is performed **by the turn goroutine itself**, as its last act.

So there is never a moment when two goroutines could both consider themselves
entitled to the state. The record's mutex guards the *activation pointer*, not
the state — which is why enqueueing holds it for the duration of a non-blocking
channel send. Without that, a caller could observe a live activation and post to
a mailbox whose goroutine had already decided to exit, then wait forever for a
reply nobody would send.

## Consequences that had to be chosen deliberately

**A group with pending timers is exempt from idle deactivation.** SPEC.md says
timers are cancelled on deactivation and re-registered on activation — but
nothing would ever reactivate an idle group, so its timer would silently never
fire. Exempting it is the smaller surprise. The LRU cap can still evict such a
group, and then its timers lie dormant until it is next called; both facts are
documented rather than hidden.

The tidier alternative is a single wake-up timer per dormant group, re-arming at
its earliest deadline. That decouples "scheduled" from "active" properly and is
the right answer if M6 finds groups pinned open by long timers. It is more
machinery than M2 should carry.

**The active cap is a settled bound, not an instantaneous one.** Eviction is
signalled after the call that tripped the cap is already enqueued, and a victim
exits when its current turn ends. Making the caller wait for someone else's turn
would couple unrelated requests' latency. So the count can sit slightly above
the cap momentarily, and the tests assert it *settles* within the cap.

**Eviction is least-recently-*activated*, not least-recently-used.** Updating an
LRU position on every turn would put a process-wide lock on the hot path of
every group operation. Recorded here because it is a real approximation: a group
activated long ago and busy ever since is evicted before one activated recently
and idle. M6 can refine it with a per-record atomic timestamp if the
approximation proves costly.

## Reversal

The two types are internal. Merging them again would mean giving up either the
preservation guarantee or the memory ratio, so this is unlikely to be revisited
except to add the wake-up timer described above.
