# 0011 — A full mailbox rejects immediately

**Status:** accepted, M2
**Affects:** `internal/actor`, `Group.Call`

## The two sentences that had to be reconciled

docs/design.md section 4 (M2) says backpressure means:

> full mailbox returns `ErrGroupOverloaded` rather than blocking indefinitely;
> caller deadline is respected.

Read together these are ambiguous. If a caller waits for room until its deadline
expires, it gets `DeadlineExceeded` and `ErrGroupOverloaded` is never returned —
the error is dead code. If it never waits, the deadline plays no part in
enqueueing at all.

## The decision

**Enqueueing is non-blocking.** A full mailbox is `ErrGroupOverloaded`,
immediately. The caller's deadline governs waiting for the *result*, which is
where waiting is legitimate: the turn is running and an answer is coming.

The middle option — wait briefly for room, then give up — was rejected. It needs
a timer per rejected enqueue, allocated precisely when the system is already
overloaded. Spending more per request the more overloaded you are is how a busy
system becomes a dead one.

## Why the queue is shallow

The default mailbox is 64. A deep queue does not make a slow group faster; it
delays the moment the caller finds out, while converting one slow group into
unbounded memory. Sixty-four is enough to absorb a burst and small enough that
saturation is visible quickly.

## What callers must do

`ErrGroupOverloaded` is **retryable**, and that is the contract: a caller that
receives it should back off and try again, exactly as `examples/groups` does. This
is not a failure mode, it is flow control. The counter behind
`Stats().Rejections` rising is a signal to add capacity or shard the hot group,
not a bug report.

The interesting consequence, visible in the M2 example: a thousand concurrent
callers against a mailbox of sixty-four *will* see rejections, and the total is
still exactly right once they retry. Backpressure and correctness are
independent.

## The hot-group limitation this exposes

A single group is a serialization point, and no amount of mailbox tuning changes
that: one goroutine, one turn at a time. Measured at roughly 1.2 µs per call,
that is about 800k calls per second for one group, forever, however many cores
are added.

That is a real ceiling and it is already in the README's honest-guarantees
section: *throughput degrades to single-pod capacity for a single hot group*.
docs/design.md section 5 defers group sharding for hot-key mitigation to after v1, so
the answer for now is to choose a partitioner that does not funnel a workload
into one group.

## Reversal

Adding an opt-in blocking enqueue later would be additive. Making it the default
would not be: it changes a fast, visible failure into a slow, invisible one.
