# PeerCache

An embedded distributed cache and stateful aggregation engine for Go services
deployed as N replicas.

Every pod embeds the library; there is no separate cache tier. Each key belongs
to exactly one pod, chosen by rendezvous hashing over the live membership. Reads
hit local memory when the data is owned locally or recently cached, and route to
the owner otherwise. Mutations and registered computations execute on the owner,
so a working set is never shipped to the caller.

```go
cache, err := peercache.New(
    peercache.WithNodeID(os.Getenv("POD_NAME")),
    peercache.WithBindAddr(os.Getenv("POD_IP") + ":7946"),
)
defer cache.Close(ctx)

// Computation moves to the data, not the other way round.
cache.RegisterGroupFunc("apply", func(ctx context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
    current, _ := s.Get("total")
    s.Set("total", add(current, arg))
    return nil, nil
})

_, err = cache.Group("tenant:acme").Call(ctx, "apply", payload)
```

---

## Guarantees

This section is the most useful documentation in the project, so it comes before
the feature list rather than after it.

- **Operations on a single group are serialized while that group has one
  activation.** Serialization is structural — one goroutine per activated group,
  fed by a bounded mailbox — which is why there is no lock API.
- **Membership is eventually consistent.** It is gossip based and therefore AP.
  A network partition can briefly produce two activations of the same group.
  Conflicting writes are *detected and counted*, not automatically merged. The
  window is narrowed by activation delay and detected via epochs. It is not
  eliminated, and no configuration setting will eliminate it.
- **State is not durable.** Ungraceful pod termination loses that pod's owned
  data and any replicas not yet caught up. Graceful shutdown transfers ownership
  and streams state. Every namespace must therefore either be reconstructible
  from a loader or tolerate loss.
- **Async replication has a data-loss window** averaging **≈0.1 ms**, p99 under
  1.5 ms, measured at RF=3 over gRPC (see `docs/benchmarks.md`). A write
  acknowledged inside it exists on one node only. `WithSyncReplication()` closes
  the window while the writer can see its replicas, at the cost of bounding a
  group's write rate by its slowest replica.
- **A synchronous write can still be acknowledged below its replication factor.**
  Replication targets come from the ranking over the *writer's own membership
  view*, so a node cut off from the cluster eventually detects every peer as
  failed, ranks itself the owner of everything, places the one copy it can reach
  and returns `nil`. The caller asked for three copies, got one, and cannot tell.
  The window lasts as long as the partition. `Stats().DegradedWrites` and
  `peercache_degraded_writes_total` count it and are the only signal that
  separates it from a healthy write — **alert on any non-zero value**. The
  neighbouring case is correct and worth knowing: while the writer still *sees*
  its replicas but cannot reach them, the write fails rather than being
  acknowledged. A namespace opened with `WithRequireReplicationFactor()` refuses
  these writes with `ErrUnderReplicated` instead — trading availability during a
  partition, and on any cluster smaller than the factor, for a `nil` that always
  means the factor was reached. Off by default.
- **With a memory budget, a node over it sheds data rather than being
  OOM-killed.** `WithMemoryBudget` caps the owned, replica and L1 tiers. Over it,
  the node sheds to nine tenths: L1 first, then replicas, then owned data — the
  last of which is data loss, by design, counted in `Stats().ShedOwned`. It is
  what stops a failover cascade, where survivors inherit a dead node's share,
  exceed their limits and die in turn. A shed key that is written again is never
  served at its older value after a failover; that holds whether or not the
  removal sent to its replicas arrives. The trade is data for liveness: in the
  cascade test, three deaths of five at RF=2 left two survivors under budget
  and 1152 of 1500 keys readable, where without a budget all 1500 survived on
  nodes holding 1.9× the budget — which in a real pod is three OOM kills. No budget is the default, and then
  nothing is shed.
- **With join probation, a crashlooping pod moves no keys.** `WithJoinProbation`
  keeps a node out of the ownership ranking until it has been up for the given
  time, so a pod that restarts every few seconds is never ranked and its
  restarts cause no rebalancing and no "not found" for data the healthy nodes
  hold. A new pod carries no share for that long; a cold start, where every node
  is young, ranks them all. Off by default.
- **Every namespace must be opened on every node, with the same options, before
  serving.** A namespace not opened locally is created on the first routed write
  with *default* options — replication factor 1 — and options cannot be changed
  afterwards. That node then silently replicates nothing for it. A five-node
  cluster where one node had opened the namespace placed 17% of the copies RF=3
  implies, with every write reporting success. `Stats().ImplicitNamespaces`
  counts the misconfiguration; any non-zero value deserves an alert.
- **A restarted node must call `CatchUp` before it serves traffic.** This is the
  sharpest edge in the system. Replication is a stream of writes, not a state
  transfer, so a node that restarts receives only the keys written *after* it
  came back — while the ownership ranking hands it back its keys immediately. It
  will answer "not found", with total confidence, for data sitting safely on the
  node that covered for it. In the chaos test that presented as 558 of 600 keys
  lost, none of them actually gone. Call `CatchUp` after readiness and before
  serving.
- **Reads of non-owned keys may be stale** by up to the configured L1 TTL.
  Invalidation broadcasts are a latency optimisation and may be lost;
  correctness never depends on one arriving.
- **Replication survives the loss of RF−1 nodes, and no more.** At RF=3, a
  chaos run of 30 kills and 30 restarts over 60 simulated seconds lost no
  synchronously written key. At RF=2, killing both holders of a key loses it —
  that is what RF=2 means, and it is asserted as a test rather than left as a
  caveat. Survivors are always correct: losing data is possible, corrupting it
  is not.
- **That guarantee describes a cluster at rest, not one still settling.** A
  membership change — a node joining, a partition healing — moves keys, and until
  the copies have been restored a key can be *readable* while fewer than RF nodes
  hold it. Readable and replicated are not the same condition: one node answering
  is enough for the first and not for the second. Losing RF−1 nodes during that
  window can therefore lose a key that would have survived once the cluster had
  settled.

  `CatchUp` is what closes the window, which is why it is an explicit call. The
  operational rule is the same one restarts already have: after a membership
  change, catch up before you rely on the replication factor again.
  `Ready().ReplicasCurrent`, exported as `peercache_replicas_current`, says when
  a node has done so under the view it holds now. It sits beside readiness rather
  than inside it, because a node can serve correctly long before the cluster is
  back at its factor. A test that
  skipped this reported the loss as a failure of RF=3, roughly one run in seven,
  and the cache was behaving exactly as documented.
- **A membership change moves replicas rather than discarding them.** When the
  ranking reassigns a key, the node holding a copy hands it to the nodes that now
  rank for it before dropping its own; if it cannot place it anywhere, it keeps
  it. Dropping on the local ranking alone lost keys whenever a partition healed,
  because the copies had been placed at positions computed without the absent
  node — 20 of 94 acknowledged writes in the reduced case. `ReplicasRehomed` and
  `ReplicasRetained` account for every moved copy.
- **Failure detection must be faster than your failure rate.** A cluster losing
  a node every two seconds while taking three to notice never converges. This is
  a deployment constraint the library cannot fix.
- **Throughput degrades to single-pod capacity for a single hot group.** Group
  sharding for hot-key mitigation is deliberately not in v1.

The words "strong" and "quorum" do not appear in this API, deliberately.

### What this is not

- Not a database.
- Not a general distributed transaction system. There are no cross-group
  transactions, ever.
- Not a Redis replacement for pub/sub, streams, or Lua scripting.
- Not multi-region. Single Kubernetes cluster, single failure domain.

---

## Status

Built milestone by milestone. Each is complete, tested and benchmarked before
the next begins.

| | Milestone | Status |
|---|---|---|
| M0 | Test harness and skeleton | **complete** |
| M1 | Single-node core | **complete** |
| M2 | Group actor runtime | **complete** |
| M3 | Membership and ownership | **complete** |
| M4 | Transport and remote routing | **complete** |
| M5 | Replication and failure handling | **complete** |
| M6 | Rebalancing and Kubernetes lifecycle | **complete** |
| M7 | Observability and hardening | **complete** |
| M8 | Adversarial verification | **complete** |

All seven SPEC milestones are done, and M8 — linearizability checking, a
deterministic fault simulation and exhaustive protocol fuzzing — went looking for
what they had missed. It found three things worth naming: a five-byte frame that
could allocate 4 GiB on any node that received it, a partition heal that
discarded replicas instead of moving them, and a synchronous write that reports
success while holding one copy of three. The first two are fixed; the third is
counted by default and refused on request (`WithRequireReplicationFactor`),
because refusing it everywhere is an availability decision rather than a bug
fix. `docs/verification.md` has the full account.

`ErrNotImplemented` no longer marks unbuilt scaffolding — nothing returns it for
that reason any more. It remains reachable in two places
that are not about milestones at all: an RPC method this binary does not
recognise, which is what a rolling upgrade to a newer peer looks like, and a
cache configured without a transport being asked to reach another node.

---

## Repository layout

The root directory is the public API. Go does not allow one package to span
directories, so everything a caller imports lives here and everything else lives
under `internal/`.

```
doc.go            Package documentation
peercache.go      Cache construction, lifecycle
namespace.go      Namespace handle
group.go          Group handle
types.go          Shared public types
errors.go         Sentinel errors
options.go        Functional options and partitioners
context.go        Nil-context guard at every API boundary

router.go         Queue-message routing with owner batching     (M4)
remote.go         Owner lookup and the remote read/write path   (M4)
handler.go        Server side: dispatches an inbound RPC        (M4)
replicate.go      Replica fan-out, sync and async               (M5)
promote.go        Reconciles held state against membership      (M5)
catchup.go        Pulls the replicas this node should hold      (M5)
invalidate.go     Best-effort L1 invalidation broadcast         (M5)
drain.go          Hands work away without closing               (M6)
handoff.go        Moves replicas on a membership change rather
                  than dropping them                            (M8)
ready.go          Readiness, its three conditions, and whether
                  replicas are current for this view            (M6)
observe.go        Stats and the metric series they export       (M7)
pressure.go       The memory budget and the order it sheds in   (M9)
probation.go      Keeps young nodes out of the ranking          (M9)

internal/
  core/           Type definitions shared by the public and internal packages
  clock/          Real and fake clock. Nothing else calls time.Now.
  testkit/        In-process cluster harness with fault injection
  store/          Sharded owned store, TTL, versions          (M1)
  l1/             Non-owned cache, TTL and version discipline (M1)
  l1/eval/        Cache-library comparison (nested module)    (M1)
  codec/          Codec registry, LZ4 framing                 (M1)
  singleflight/   Collapses concurrent loads of one key       (M1)
  actor/          Group activation, mailbox, lifecycle        (M2)
                  Records persist state; activations hold the goroutine
  ownership/      Rendezvous hashing, ring epochs             (M3)
  membership/     Gossip membership over memberlist           (M3)
  transport/      gRPC client and server                      (M4)
  wire/           Protocol messages, bounded decoders, frame
                  validation. The only code here that interprets
                  bytes it did not write.                     (M4)
  replication/    Replica fan-out, promotion, catch-up        (M5)
  rebalance/      Move planning and streaming transfer        (M6)
  metrics/        Metric collection, text format, histograms  (M7)

proto/            gRPC service definitions                   (M4)
examples/         One runnable example per milestone, m0 through m8
deploy/           Kubernetes manifests and the operational guide (M6)
scripts/          mutation-audit.sh, clean/ (the `make clean` implementation)
docs/             api.md, benchmarks.md, verification.md, decisions/
.github/          ci.yml per commit, nightly.yml for the long runs
```

---

## Testing

Everything is deterministic. Time only moves when a test moves it, randomness
comes from a seed that is printed on failure, and no test sleeps.

```bash
make test          # fast feedback
make test-race     # the gate: -race -count=2
make vet lint      # go vet and staticcheck
make examples      # run every example to completion; they assert what they show
make bench         # benchmarks, 5 runs
make fuzz          # fuzz the frame parser, the decoder, the codecs, the partitioner
make bench-l1-eval # re-run the cache-library comparison (nested module)

bash scripts/mutation-audit.sh   # break each invariant, check its test notices
make ci            # exactly what the pipeline runs
```

The long-running verification is nightly rather than per-commit, in
`.github/workflows/nightly.yml`: two thousand simulation seeds, repeated
linearizability checks, an hour of soak, ten minutes per fuzz target, and the
mutation audit. A failure there prints the seed or commits the crasher, so it
reproduces with one command.

On Windows without GNU make, `mingw32-make` works, or run the `go` commands
directly.

To reproduce a failure:

```bash
PEERCACHE_SEED=1234 go test ./... -run TestThatFailed
```

### The harness

`internal/testkit` builds N in-process nodes over a fake network and a fake
clock, with fault injection for drop, delay, reorder, partition, isolation,
graceful kill, hard kill and revival. Teardown asserts three things a harness is
uniquely placed to check: no clock misuse, no timer left armed, and no leaked
goroutine.

Simulated time is free. The M0 acceptance scenario — five nodes, a 3/2
partition, an hour advanced on each side of a heal — runs in about **1.5 ms** of
wall clock against a 50 ms budget.

### Rules the tests enforce on themselves

`discipline_test.go` reads the repository as text and fails the build on:

- any `time.Sleep`, anywhere;
- any direct `time.Now`, `time.After`, `time.NewTimer`, `time.NewTicker` or
  `time.AfterFunc` outside `internal/clock`;
- any `context.WithTimeout` or `context.WithDeadline` outside `internal/clock`,
  because both read the wall clock and make deadline behaviour untestable;
- the `log` package;
- an `internal/` package importing the root package;
- a package without a `doc.go`;
- a leftover TODO marker.

---

## Documentation

- [`docs/api.md`](docs/api.md) — the public API, with runnable examples
- [`docs/benchmarks.md`](docs/benchmarks.md) — numbers, with the machine they
  were measured on
- [`docs/verification.md`](docs/verification.md) — which acceptance criteria
  were mutation-tested, and how each failed when the behaviour was broken on
  purpose
- [`docs/decisions/`](docs/decisions/) — one file per significant choice, plus
  `open.md` for everything still guessed at
- [`SPEC.md`](SPEC.md) — the build plan
- [`CLAUDE.md`](CLAUDE.md) — the non-negotiable constraints

---

## Requirements

Go 1.25 or later.

The floor was 1.23 until M3. Adding memberlist raised it: its transitive graph
(`x/net`, `x/sys`, `miekg/dns`) has moved past 1.23, and holding those back to
keep the older floor would mean pinning the modules that receive security
patches. [`docs/decisions/0013-go-floor-raised-to-1.25.md`](docs/decisions/0013-go-floor-raised-to-1.25.md)
records the trade.
