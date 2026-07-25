# 0012 — The activation guard defers newly acquired groups

**Status:** accepted
**Milestone:** M3
**Supersedes:** nothing

## Context

M2 made a group a virtual actor: at most one live goroutine per group, on one
node, and every operation on it runs as a turn on that goroutine. That is the
whole safety story. It holds because the runtime keeps a single record per group
key and activates it once.

M3 makes ownership a cluster-wide question, and the runtime's guarantee stops
being local. Two nodes each keep their own record map. If both believe they own
`orders/42`, both will happily activate it, and each will run turns against its
own copy of the state. Nothing in M2 detects that — the mailbox is per-record,
and neither record can see the other.

Membership is gossip based and therefore AP, per SPEC.md §3.5. There is no
moment at which the cluster agrees on who owns what. A node that has just
learned of a departure believes it owns keys that the departing node may still
be serving, because the departing node has not necessarily noticed anything at
all.

So the question is not "how do we prevent double activation" — with AP
membership we cannot, without consensus we are not building until M5 at the
earliest. The question is how narrow we can make the window and what we tell the
caller while it is open.

## Decision

Ownership is answered by rendezvous hashing over the current view, and every
answer is stamped with the epoch it was computed under. On top of that, a
`Guard` sits in front of activation and answers one of three things:

- **nil** — activate.
- **`ErrNotOwner`** — this node does not own the key. Terminal here; the caller
  should re-resolve and go elsewhere. From M4 the router does that for them.
- **`ErrActivationPending`** — this node owns the key but acquired it within the
  last `ActivationDelay`. Retryable *here*, shortly.
- **`ErrStaleEpoch`** — the caller resolved ownership under a view this node has
  already replaced. Retryable elsewhere, after re-resolving.

The delay defaults to 3 seconds and is a deliberate availability trade. It is a
guess about how long the previous owner takes to notice it lost the key. Too
short and both nodes serve the group; too long and every membership change is a
brownout.

The epoch check is the other half of SPEC.md §4's requirement and it is
deliberately asymmetric. A caller whose epoch is *older* than ours reached its
conclusion from facts we have since discarded, and gets `ErrStaleEpoch`. A
caller whose epoch is *newer* is not wrong — we are behind, and cannot confirm
ownership under a view we have not seen — so it gets `ErrActivationPending` and
is told to ask again here. One error for both would send a caller away from the
node that is about to be able to serve it.

Ownership is checked before the epoch. When both are wrong, "go elsewhere" is
the action the caller would take after re-resolving anyway, delivered a round
trip sooner.

Nothing carries an epoch across a boundary until M4. `CanActivateAt` exists and
is tested now rather than left as a hole for the transport to discover.

Two refinements matter more than the delay itself.

**Only newly acquired groups are deferred.** A node that has held a group all
along cannot be running it anywhere else, so blocking it would turn a membership
change into an outage for the entire keyspace rather than for the part that
actually moved. The guard therefore compares the current owner against the owner
under an earlier view, and returns nil immediately if this node owned the key
before as well.

**That earlier view is the last *settled* view, not the immediately previous
one.** This is the part that was wrong in the first implementation, and the
failing test that caught it is `TestPartitionedMinorityStopsActivating`.

A partition does not arrive as one membership change. Each peer is detected
separately, so a five-node cluster splitting 3/2 produces a burst of updates on
each side — one per peer that vanished. Comparing against the previous update
means comparing one post-partition view against another post-partition view. In
that comparison nothing looks newly acquired, so nothing is deferred, at
precisely the moment the deferral exists for. Holding the last view that
survived a whole activation delay without being replaced treats the burst as the
single transition it really is.

## Consequences

**A brief, bounded, visible unavailability after every membership change.** It is
confined to the keys that moved, it is reported as a distinct retryable error
rather than as a timeout or a wrong answer, and `Stats().DeferredActivations`
counts it. A spike after a change that returns to zero is healthy; a sustained
non-zero value is a bug.

**The window is narrowed, not closed.** Two nodes can still activate the same
group if the previous owner is slower to notice than `ActivationDelay`. This is
inherent to AP membership and is stated in the package documentation rather than
hidden. M5's replication and M6's handoff reduce the consequences; neither
eliminates the window.

**A node that *loses* a group keeps serving it until it notices.** The guard runs
on activation, not on every turn, so an already-active group is not stopped when
ownership moves away. Deactivation on ownership loss is M6 work; it is recorded
in `docs/decisions/open.md`.

**A node that has not yet learned any membership owns nothing.** The alternative
— treating an empty view as "I am alone, so I own everything" — would let a
starting pod claim the entire keyspace for as long as it took to hear from
anyone. Returning `ErrNotOwner` is the safe direction to be wrong in.

**A negative delay disables deferral entirely.** Only sane on a single node, and
documented as such. It exists because the single-node case is real and paying
three seconds for a cluster of one is absurd.

## Alternatives rejected

**A lease per group, renewed against a quorum.** Correct, and it closes the
window rather than narrowing it. It also requires consensus, which SPEC.md §5
defers, and it puts a quorum round trip in front of every activation. Rejected
for M3; the interface does not preclude it.

**Deferring every activation after a change, not just acquired ones.** Simpler,
and strictly safer. Rejected because it converts every rolling deploy into a
cluster-wide brownout of `ActivationDelay`, which is a much worse failure than
the one it prevents.

**Blocking inside `CanActivate` until the window closes.** Superficially nicer:
the caller just waits. Rejected because it consumes the caller's deadline budget
invisibly and turns a fast retryable error into a slow one. CLAUDE.md's rule
that nothing blocks past its context deadline is easier to keep when the library
does not block at all.

**Using the epoch alone, without a wall-clock window.** Epochs are per-view and
never agreed, so "my epoch is higher" establishes staleness, not authority. A
node returning from a partition can hold a higher epoch than the majority that
kept serving. The epoch is recorded with every answer for exactly that reason,
but it cannot be the safety mechanism.
