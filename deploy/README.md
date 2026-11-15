# Deploying PeerCache

PeerCache is an embedded library, so there is nothing here to deploy on its own.
These manifests describe how to run *your* application when it embeds PeerCache,
and the parts that matter are the ones most easily got wrong: peer discovery,
the readiness gate, and the PreStop hook.

Two shapes are provided. Pick by whether your pods need stable identities.

| | `deployment.yaml` | `statefulset.yaml` |
|---|---|---|
| Pod identity | Random, changes every restart | Stable: `app-0`, `app-1`, … |
| Node ID | Pod name (random) | Pod name (stable) |
| Ownership after a restart | The pod is a *new* node; its share is redistributed twice | The pod reclaims the same share |
| Use when | The namespace is reconstructible from a loader | Losing and re-earning a share is expensive |

**Prefer the StatefulSet if your keyspace is large.** A Deployment gives each
restarted pod a new identity, which rendezvous hashing treats as a departure
plus an unrelated arrival — two rebalances instead of none. With stable names a
restarted pod hashes to the same place and reclaims exactly what it had.

## The five things that must be right

### 1. Open every namespace on every node, before serving

```go
// On every pod, at startup, with identical options.
cache.Namespace("orders", peercache.WithReplicationFactor(3), peercache.WithSyncReplication())
```

**A namespace not opened locally is created for you on the first routed write —
with default options, meaning replication factor 1.** Options are fixed at
creation, so that node then replicates nothing for that namespace, permanently,
however you configure it afterwards. Losing the node loses its entire share at
what you believe is RF=3.

In testing, a cluster where one of five nodes had opened the namespace placed
17% of the copies RF=3 implies, and every write reported success.

`Stats().ImplicitNamespaces` counts this. **Alert on it being non-zero** — it
means a node is running with configuration your application never chose.

### 2. Alert on the two counters that mean "your writes are not what you think"

`peercache_implicit_namespaces_total` is one. The other is
`peercache_degraded_writes_total`, and it is the sharper of the two because the
misconfiguration above is at least static, while this one appears and disappears
with the network.

**It counts synchronous writes acknowledged holding fewer copies than the
namespace's replication factor asked for.** Replication targets come from the
ranking over *that node's own membership view*, so a pod cut off from the cluster
eventually detects every peer as failed, ranks itself the owner of everything,
places the one copy it can reach, and returns success. A caller that asked for
RF=3 got one copy and cannot tell. The window lasts as long as the partition.

```
- alert: PeerCacheDegradedWrites
  expr: increase(peercache_degraded_writes_total[5m]) > 0
  annotations:
    summary: "{{ $labels.pod }} acknowledged writes below its replication factor"

- alert: PeerCacheImplicitNamespace
  expr: peercache_implicit_namespaces_total > 0
  annotations:
    summary: "{{ $labels.pod }} is running a namespace its application never configured"
```

Worth watching but not paging on: `peercache_replicas_retained_total`, which
counts replica copies a pod could not hand to the nodes that now rank for them
and kept instead. That is memory rather than data loss — the safe direction —
but rising under stable membership it means hand-offs are failing and the
cluster is not converging.

No `PrometheusRule` ships with these manifests. Alerting thresholds belong to
whoever runs the cluster, and the two above are the ones worth having before any
of the throughput or latency series.

### 3. Peer discovery needs a headless service

Both manifests include a headless service (`clusterIP: None`). It exists so DNS
returns every pod's address rather than one load-balanced address, which is what
gossip seeding requires: a node joining needs to reach a *specific* peer, not
whichever one a service IP happens to pick.

`SEEDS` is set to the headless service name. Resolve it at startup and pass the
addresses to `Join`.

### 4. Two ports, and they are not interchangeable

- **7946** — gossip (SWIM). memberlist binds this itself.
- **7947** — data (gRPC). The peer transport binds this.

They are different sockets. A node advertises its *data* address through gossip
metadata; assuming the two are the same makes every peer dial memberlist
expecting gRPC, which fails with `error reading server preface: EOF`. This is
not hypothetical — it is how the first real-network run failed.

### 5. The readiness probe and PreStop hook are not optional

```
readinessProbe -> GET /readyz -> cache.Ready()
preStop        -> POST /drain -> cache.Drain(ctx)
```

**Readiness** keeps traffic away from a pod that has joined the cluster but does
not yet hold what it owns. A node becomes an owner the instant it joins, because
every peer recomputes the ranking — but it holds nothing until `CatchUp` runs.
In between it answers `ErrNotFound`, confidently, for data that exists
elsewhere. Readiness keeps clients away in that window, not peers, which forward
writes to the new owner at once; open the cache with `WithCatchUpGate()` so they
are held until catch-up completes (docs/api.md, "WithCatchUpGate"). Without the gate, a rolling upgrade serves misses for a fraction of
the keyspace on every pod replacement.

**PreStop** drains: mark unready, announce departure, hand off owned keys,
deactivate groups. Without it a graceful shutdown becomes an ungraceful one —
peers wait out the failure timeout instead of learning immediately, and at
replication factor 1 the pod's entire share is lost.

`terminationGracePeriodSeconds` must exceed the time a drain takes. It is set to
60 here; measure yours. A grace period shorter than the drain turns a graceful
shutdown into a kill halfway through a hand-off.

**Leave a gap between pod replacements.** Readiness tells you a pod can serve; it
does not tell you the cluster is back at its replication factor. Any membership
change moves keys, and until the copies are placed a key can be readable while
fewer than RF nodes hold it — one node answering is enough for the first and not
for the second.

That is why a rolling update should not replace the next pod the instant the
previous one is ready. `maxUnavailable: 1` and a `minReadySeconds` long enough
for catch-up to finish is the shape you want, and `peercache_replicas_current`
reading 1 on every pod is the signal it has. It is the gauge form of
`Ready().ReplicasCurrent`, which a node reports only after a `CatchUp` that began
under its current membership view returned `nil`. Replacing pods faster than the cluster restores copies is
how a rolling upgrade at RF=3 ends up, briefly, at RF=1.

## What these manifests deliberately do not include

**Resource limits.** Memory is the interesting one and it depends entirely on
your keyspace. PeerCache holds owned data, replicas, and a bounded non-owned
cache; `Stats()` reports all three. Measure, then set limits with headroom for
the replicas a node inherits when a peer fails — a pod sized for its steady
state will be OOM-killed by its first failover.

**CPU.** Prefer a request without a limit, or a generous limit. A throttled pod
answers late, and the failure detector reads lateness: memberlist probes with a
500 ms timeout. Go 1.25 already sizes `GOMAXPROCS` to the CPU limit, so do not
set it by hand.

Use `WithJoinProbation(30 * time.Second)` as well. A pod in CrashLoopBackOff
otherwise rejoins on every restart, takes a share of the keyspace it never
holds, and makes reads of that share report not-found until it dies again;
with a probation longer than it survives, its restarts move nothing.
`peercache_members_on_probation` shows it waiting.

Then set `GOMEMLIMIT` to about nine tenths of that limit and `WithMemoryBudget`
to about half of it. The budget counts the data tiers, not the heap around them:
on kind, at a million orders, the heap in use was about 1.55 times what it
counted, and without `GOMEMLIMIT` the Go runtime lets the heap reach twice its
live size before collecting. A pod budgeted at three quarters of its limit, or
at half of it with no `GOMEMLIMIT`, is OOM-killed under the load the budget was
meant to absorb (`docs/decisions/open.md`, question 55). The budget is what
makes a failover cascade stop:
a survivor over its budget sheds L1, then replicas, then owned data, instead of
being killed and passing its share to the next. `peercache_memory_used_bytes`
against `peercache_memory_budget_bytes` is the margin; a rising
`peercache_shed_owned_total` means the budget, or the cluster, is too small for
the working set.

**A PodDisruptionBudget.** You want one. `maxUnavailable: 1` is right for
replication factor 2, and 2 for factor 3, but the correct value depends on
whether you can tolerate the loss window, so it is a decision rather than a
default.

**Autoscaling.** Every scale event is a rebalance. That is cheap here —
rendezvous hashing moves only the affected share — but it is not free, and an
HPA that flaps will rebalance continuously.

## Wiring it up

The HTTP endpoints the probes use are yours to write; `examples/rolling-upgrade` has a
complete one. The shape is:

```go
mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
    st := cache.Ready()
    if !st.Ready {
        http.Error(w, st.String(), http.StatusServiceUnavailable)
        return
    }
    fmt.Fprintln(w, st)
})

mux.HandleFunc("/drain", func(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 50*time.Second)
    defer cancel()
    if err := cache.Drain(ctx); err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    fmt.Fprintln(w, "drained")
})
```

`ReadyState.String()` is written for exactly this: an operator reading a probe
failure sees which condition is unmet, because the remedies differ.
