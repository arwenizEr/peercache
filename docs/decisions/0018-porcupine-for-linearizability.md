# 0018 — Porcupine for linearizability checking

**Status:** accepted
**Milestone:** M8
**Adds:** `github.com/anishathalye/porcupine` v1.3.0, test-only

## Context

design principle 2 (docs/design.md) says all operations on an activated group run on a
single goroutine through a bounded mailbox, and non-negotiable 5 says the
consistency ceiling is per-group serializability. Until now that was asserted by
construction — there is one goroutine, therefore operations are serialized — and
tested by example: `TestConcurrentCallsToOneGroupAreSerialized` and
`TestTenThousandCallsAcrossOneThousandGroups` check that concurrent callers
produce correct counts.

Correct counts are weaker than they look. A cache that lost an update and then
gained a duplicate produces the same total. What those tests establish is that
nothing is obviously broken, not that every observed history could have happened
under some sequential order.

docs/design.md's dependency budget permits test dependencies. This note exists
because the budget also asks for one before anything is added.

## Decision

Record real operation histories against a live cluster under fault injection and
check them with Porcupine against a sequential model.

Porcupine is the standard Go implementation of the Wing–Gong linearizability
checker with the Lowe optimisations. Writing one is a known-hard exercise in
exactly the way that produces a checker which agrees with itself and nothing
else; the whole point of this milestone is evidence that does not depend on my
own reasoning being right.

### Linearizability, not serializability

SPEC and docs/design.md say "serializability". This checks linearizability, which is
strictly stronger: it additionally requires that the sequential order respect
real-time precedence, so an operation that returned before another began must be
ordered before it.

That is deliberate. Linearizability is what a single-goroutine activation
actually provides, it is what callers assume whether or not the documentation
promises it, and checking the stronger property means a violation is a real
finding rather than an argument about definitions. Where the two differ, this
proves more than was claimed.

### Timestamps come from a counter, not a clock

docs/design.md forbids `time.Now` outside `internal/clock`. Porcupine only needs
totally ordered integers for call and return points, so the harness stamps them
from an atomic counter. This is not a workaround: a counter is *more* correct
here than a clock, because it cannot be perturbed by NTP or by the coarse ~500 µs
monotonic resolution measured on the test machine, which is larger than most
operations being ordered.

### Partition is excluded, and that is the interesting part

design principle 5 (docs/design.md) is explicit that membership is AP and a partition can
produce two activations of one group. Two activations mean two serialization
points, and a history spanning them is *not* linearizable — correctly so.

So the checker runs against fault schedules that drop, delay and reorder
messages but do not partition the cluster. Those faults exercise the retry and
routing paths without violating the precondition the guarantee is stated under.

Running the checker under partition and calling the result a pass would require
weakening the model until it accepted anything. The honest alternative is to
check the guarantee where it is claimed to hold, and to say plainly that the
partition case is a documented exception rather than a tested one. A separate
test asserts the split-brain *detector* fires in that window, which is what the
design actually promises there.

## Consequences

**A failing check prints a replayable history.** The seed and the operation log
are both in the failure output, which matters because a linearizability
violation is otherwise nearly impossible to reason about after the fact.

**The model is small on purpose.** Four keys per group, values written as
distinct strings. A larger key space makes the search exponentially more
expensive and finds nothing extra: linearizability is compositional per key
under this model, and the interesting interleavings are per group, not per key.

**This does not check across groups.** There is no cross-group guarantee to
check — docs/design.md is explicit that no cross-group transaction exists — so a
checker spanning groups would be testing a property the system deliberately does
not have.

## Alternatives rejected

**Writing the checker.** Rejected on the grounds above: a hand-written checker
is a second thing that can be wrong, and its agreement with my model of the
system is not evidence.

**Elle or Jepsen.** Both are excellent and both assume a real cluster, an
external harness, and a JVM or Clojure toolchain. This project's whole testing
premise is an in-process deterministic harness with a fake clock; adopting
Jepsen would mean maintaining a second, slower, non-deterministic way to run the
same cluster. Worth revisiting if PeerCache ever ships a standalone binary.

**Asserting serializability only.** Weaker, and no cheaper to check.
