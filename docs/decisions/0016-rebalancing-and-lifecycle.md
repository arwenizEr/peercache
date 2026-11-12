# 0016 — Rebalancing, readiness, and the shape of a graceful shutdown

**Status:** accepted
**Milestone:** M6
**Resolves:** open question 30 (a restarted node owns keys it does not have)

## Context

M5 left a cluster that survived failures and a documented trap: a restarted node
becomes an owner the instant it joins, because every peer recomputes the ranking
from the new membership, but it holds nothing until `CatchUp` runs. In between
it answers `ErrNotFound` — confidently, and indistinguishably from a genuine
miss — for data sitting safely on the node that covered for it.

docs/design.md §4 asks M6 for move planning that touches only affected partitions,
streaming transfer with backpressure and resumability, a PreStop hook, a
readiness gate, and manifests.

## Decisions

### Readiness is a separate question from membership

`Cache.Ready()` is false until the node has joined, its view has been stable for
longer than the activation delay, and catch-up has completed at least once. It
becomes permanently false once `Drain` begins.

Kubernetes already has the vocabulary: a pod that is running but not ready
receives no traffic. Exposing the three conditions through a probe is what
closes the M5 window, and it closes it at the right layer — the library cannot
stop peers routing to a node that is a legitimate cluster member, but the
service can stop *clients* reaching it.

`Ready()` returns a struct rather than a bool because "not ready" has several
causes with different remedies. A node waiting for gossip needs patience; one
that has never caught up needs a `CatchUp` call; a draining node needs to be
left alone. An operator reading a probe failure that says only "not ready"
learns nothing.

### Drain's ordering is the design

Mark unready → announce departure → hand off owned keys → deactivate groups.

Every other order leaves a window in which requests arrive at a node that has
already given away the ability to serve them. Announcing *before* transferring
is the subtle one: the receiving nodes have to already be the new owners when
the keys arrive, or a transfer lands on a node that files it as a replica.

### Planning compares replica sets, not slots

This was a bug before it was a decision, and it doubled every figure.

Removing a node shifts every remaining node up one position in the ranking, so a
slot-by-slot comparison reports `rf` moves per affected key. But a node promoted
from replica to owner already holds the data; promotion costs a map drain, not a
network transfer. The only genuine move is a node entering a replica set it was
not in.

Corrected, a 9-node cluster losing one member at RF=3 moves 6,692 of 20,000
placements against a derived expectation of 6,667 — 0.4% off, where the slot
comparison had said 13,377.

### Paged unary transfer, not streaming

Open question 1 predicted M6 would want `InvokeStream`. It does not, and the
evidence was already in the tree: M5's catch-up is a resumable bulk transfer
built on paged unary requests, and it resumes naturally because the cursor *is*
the resume token. A broken stream has to reconstruct that state anyway.

A page is also a unit of backpressure — the receiver controls the rate by when
it asks for the next one — so streaming would add a method to `core.Transport`
in exchange for nothing. Both the gRPC and in-process implementations satisfy
the interface unchanged. Question 1 is now closed rather than open.

### Hand-off reuses replication

A handed-off key and a replicated key are the same thing: a value plus the stamp
that orders it. The receiver already knows how to refuse one older than what it
holds. A separate transfer protocol would be a second implementation of the same
decision, which is how two implementations come to disagree.

One addition was necessary. A receiver that *already owns* the key takes it
straight into the owned store rather than filing it as a replica, because the
departure was announced first and the ranking already names it owner. Without
that the key waits for the next membership change to be reconciled — and at
replication factor 1 there is no next change and no replica set to belong to, so
it is evicted instead. A graceful shutdown lost exactly the data it was trying
to save.

## Consequences

**A restarted pod must still call `CatchUp`.** Readiness does not do it — that
would make a starting node's first act a bulk transfer competing with the
traffic it was started to serve, which M5 rejected. What readiness adds is that
skipping the call is now *visible*: the pod never becomes ready and never
receives traffic, instead of silently serving misses.

**`terminationGracePeriodSeconds` must exceed the drain.** A grace period
shorter than the hand-off turns a graceful shutdown into a kill halfway through
one. The manifests set 60 and say to measure.

**A Deployment costs two rebalances per pod replacement**, because a random pod
name is a new node ID: rendezvous hashing sees a departure and an unrelated
arrival. A StatefulSet's stable name hashes to the same place. Both manifests
are provided with the trade stated.

**Replication, not drain, is what makes a rolling upgrade safe.** This was
discovered by mutation: deleting the entire hand-off left the 9-node rolling
upgrade passing, because at RF=3 the keys are already elsewhere and promotion
plus catch-up cover the rest. Drain is load-bearing at RF=1 and a latency
optimisation above it — and `Leave` specifically is *only* ever a latency
optimisation, since failure detection covers the same ground more slowly.

That is worth knowing rather than hiding: it means an operator running RF≥2 who
loses a PreStop hook has a slower upgrade, not a data loss.

## Alternatives rejected

**Automatic catch-up on readiness.** Would make the gate self-clearing and
remove a required manual step. Rejected for the reason M5 rejected automatic
catch-up on join: the application knows when it can afford a bulk transfer and
the library does not.

**Blocking `Drain` until every peer acknowledges the departure.** Would close
the routing window entirely rather than narrowing it. Rejected because it makes
shutdown depend on the health of every peer, and a pod that cannot reach a dead
node would hang until its grace period expired — trading a brief window of
retryable errors for a guaranteed hard kill.

**A dedicated transfer RPC with its own flow control.** More conventional, and
it would have satisfied "streaming transfer with backpressure" literally.
Rejected because paging already provides both, and the second protocol would
need its own version-ordering rules that must agree with replication's forever.
