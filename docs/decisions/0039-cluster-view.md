# 0039 — A cluster-wide view, and checking replicas against owners

**Status:** accepted.
**Bound by:** decision 5 (inconsistency is detected, never repaired by a
consensus step), decision 0031 (introspection holds counts, options and states,
never a value, and needs no authentication because it reveals none), and the
no-cross-group-transaction rule.

## Context

`Describe` and `DescribeKey` are one node's belief. The questions an operator
asks during an incident are about the cluster: *do all nodes agree who is in
it?* and *do the replicas hold what the owners hold?* `split_brain_detections`
and `version_conflicts` count symptoms but answer neither. Akka exposes a
cluster-wide sharding-state query for the first; Hazelcast reconciles
near-cache sequence numbers against members for the second.

## Decision

**`Cache.DescribeCluster(ctx, digests bool) ClusterDescription`**, and the
debug handler's `/cluster` route (`?digests=1`). The node asks every member of
its view for its `Describe` over a new peer method, `Describe`, and reports:

- each node's description, or the error reaching it (a node on an earlier
  release answers `ErrNotImplemented`, reported as such);
- whether every reachable node sees the same member set, and which differ.

With digests, each node also returns its **running digests**: for every
namespace with replicas, 64 partitions of the keyspace (by key hash), each a
count of keys and the wrapping sum of a hash of every key with its version —
one set over its owned store, one over the replica copies it holds.

**Kept on every write, read for free.** Every mutation of either store already
runs under its shard lock and adjusts the byte accounting; at the same point it
adds and removes the entry's contribution: one key hash and two atomic adds.
All writes go through two helpers per store, so no path can change what a store
holds without its digest seeing it, and a property test drives every write path
and compares the running digest with one recomputed from scratch. Reading the
digests is 64 pairs per store, whatever the store holds — no walk, nothing
proportional to the keyspace, on the node or on the wire.

**The check is per partition, cluster-wide.** In step, every owned key has
`copies - 1` replica copies at its owner's version, where copies is the
replication factor capped at the number of members. So for each namespace and
partition, the held count and sum across all nodes equal the owned count and
sum times `copies - 1`, sums wrapping alike. A partition that does not is
reported with every node's owned and held counts, which shows the node that is
short. Every member must answer, or nothing is concluded.

**Loaded keys are expected short.** A loaded value is stored on its owner
alone — loads are reconstructible and not replicated — so a namespace with a
loader holds fewer copies than its owned keys imply. Its out-of-step partitions
carry `Loaded`, and only copies above the expectation point at a fault there. A
per-entry flag excluding loaded values from the digest was rejected: eight
bytes on every entry of every namespace, and catch-up sends loaded values to
replicas anyway, so the flag would not make the count exact.

**Detection only.** Nothing is repaired: catch-up and reconciliation already
do that, and a debug read that wrote would be a second, untested repair path.
Replication in flight shows as a transient mismatch, and so can keys with a TTL
around their expiry, since each node sweeps on its own; reading twice tells lag
from loss.

**No keys, no values.** Counts and hash sums only, so the route stays as safe
to expose internally as the rest of the debug handler. Each member's answer is
bounded by `DescribeTimeout` on the cache's clock.

## Rejected

- **Per-key comparison:** proportional to the keyspace on the wire, and it
  would put keys in the response.
- **Digests computed on request by walking the stores:** proportional to the
  keyspace on every node for every request — the first version of this, and
  rejected for exactly that.
- **Repairing mismatches:** see above.
- **Gossiping descriptions:** gossip metadata is small by design, and a view
  that is itself eventually consistent answers "do we agree" badly.

## Acceptance

- Three nodes, RF 2, synchronous: no partition out of step.
- A replica that missed writes is reported: its partitions short of copies.
- An unreachable member is reported with its error, no agreement and no
  digest conclusion; one that never answers costs only the timeout.
- The running digest of each store equals one recomputed from its entries
  after a random mix of every write path.
- The response holds no key and no value.
