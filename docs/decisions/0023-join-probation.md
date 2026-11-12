# 0023 — Join probation, by age, in a wrapper around the authority

**Status:** accepted
**Milestone:** M9
**Adds:** `WithJoinProbation`, `core.MetaStartedKey`, `Stats().MembersOnProbation`,
one metric series. The gossip node metadata gains a third field. No new
dependency, no new RPC.

## Context

Ownership is rendezvous hashing over the membership view, and a node enters the
view the moment gossip admits it. A pod in CrashLoopBackOff therefore rejoins on
every restart: every peer ranks it, a fifth of a five-node keyspace moves to a
node holding nothing, reads of that share report `ErrNotFound` for data four
healthy nodes hold, and the share moves back when it dies. `Ready` keeps traffic
off the pod itself; it does nothing about its peers routing to it.

## Options considered

1. **Probation by age** — chosen. A node is ranked once it has been up for the
   probation, measured from a start time its authority publishes.
2. **Self-declared rankability.** A node advertises that it is ready to be
   ranked. More exact, but catch-up pulls the keys a node ranks for, computed by
   owners from a ranking that would not yet include it — a chicken and egg that
   needs the catch-up protocol changed to reason about a hypothetical ranking.
3. **Do nothing**, and document.

The user chose 1.

## The rule

Ranked if up for at least the probation; or if it publishes no start time (an
older build, so a rolling upgrade is not held up); or if no member has lasted
that long (a cold start — excluding everyone leaves nothing to own anything). A
pure function of the member list and the time, so nodes that see the same
members at the same moment rank the same ones. A young node applies it to
itself, so it never acts as owner while its peers route elsewhere.

## Why a wrapper

The guard, catch-up, drain and readiness all read membership through
`core.MembershipAuthority`. Wrapping it filters what all of them see at once and
changes none of them, and CLAUDE.md decision 1's one ownership file is
untouched. A probation ending changes the ranked view with no event from the
underlying authority, so the wrapper arms a timer and reports an epoch of the
authority's plus the number of such changes: monotonic, as the guard needs.

## Why off by default

Probation means a new pod carries no load for its first period. That is a real
cost to an operator who never sees crashloops, and most of the existing suite
revives nodes and expects them ranked. Reversible: the default can be raised
later without changing the option.

## Costs and limits

Clocks must be roughly synchronised; skew moves the moment a node is ranked by
that much on some peers, inside what the activation delay already absorbs. A
pod that crashes *after* outliving its probation is ranked and then lost like
any other failure — the defence is aimed at fast crashloops, which are the
common kind. At cold start, nodes coming up staggered across more than one
probation period can see a node leave and re-enter the ranking once, as the
first nodes cross the threshold before the later ones; it settles within one
period.
