# 0043 — A key's history, kept on every node that touched it

**Status:** accepted (2026-11-18).
**Bound by:** decision 0042 (additive API, additive debug JSON), the dependency
budget (nothing new).

## Context

The kind partition run of 2026-11-18 ended with four stock counters hundreds
of decrements above the order ledger. Every aggregate the cluster exports was
there — metrics per pod, logs, Prometheus — and none of it could say where
those decrements went. The question was about one key: which node held it when,
which value each installed and from where (a write, a replica, a promotion, a
restore, a read-back after a shed, a catch-up), at what order and by which
writer. The answer was finally reached by reading code (open questions 61 and
62), not by looking.

Aggregates count events; a correctness failure on kind is a sequence of them
on one key across several nodes. That sequence is lineage, in the sense data
pipelines use the word: where a value came from.

## Decision

1. **An opt-in per-key trace.** `WithKeyTrace(namespace, prefix)` names the keys
   a node records the history of. Without it nothing is recorded and each hook
   costs one predictable branch.
2. **What is recorded** is every change to what a node holds for a traced key,
   with its source: a write applied by the owner, a replica accepted or refused
   (and why: older, a fork), a promotion, a demotion or hand-off out, a restore
   from storage, a shed and its read-back, a catch-up install. Each event
   carries the node's clock time, the write's order and writer, the stored
   version, and the value's length and a 64-bit hash: enough to tell two values
   apart without keeping them.
3. **Bounded.** A ring of the newest 64 events per key, at most 4,096 keys per
   node; keys beyond that are counted, not traced. A trace is a diagnostic
   instrument for a handful of keys, not an audit log.
4. **Read by key, from every node.** `Cache.KeyHistory(namespace, key)` returns
   this node's events, and the debug handler serves them at
   `/debug/peercache/keys?namespace=…&key=…`. A key's full history is the union
   over the nodes, merged by time; the shop's checker does that for every key
   it finds wrong.

## Consequences

- A wrong counter on kind comes with its timeline: the event that lost the
  decrements is in it, on whichever node it happened.
- Clock times from different nodes are compared when the timelines are merged.
  Within one cluster they are close, not equal; the order and writer on each
  event are what decide which value is newer, and the time only arranges the
  reading.
- The hooks sit on paths that change ownership state, not on reads: a read
  changes nothing a trace records.
