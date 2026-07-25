# Open questions

Things guessed at, in the order they will start to hurt. Each has a
provisional answer that is cheap to reverse; none of them are silent.

Format: what is unresolved, what was picked for now, and what would force a
change.

---

## 1. The shape of `PeerClient` and `Handler` — M0, decided provisionally

SPEC.md section 3.6 names `Transport.Dial(node) (PeerClient, error)` and
`Serve(l net.Listener, h Handler)` but never defines either type.

**Picked:** a byte-oriented request/response pair.

```go
type PeerClient interface {
    Invoke(ctx context.Context, method string, req []byte) ([]byte, error)
    Close() error
}

type Handler interface {
    Handle(ctx context.Context, from Node, method string, req []byte) ([]byte, error)
}
```

**Why:** gRPC implements it, an in-process fake implements it in a few hundred
lines, and adding an RPC in M4 or M5 does not widen the interface.

**What would force a change:** streaming. M6's resumable partition transfer
wants a stream, not a unary call, and M5's replication fan-out might too. The
likely resolution is an additional `InvokeStream` method rather than reshaping
`Invoke`, but that is not decided.

---

## 2. Whether `Serve` should take a `net.Listener` — M0, deferred

The signature is fixed by SPEC.md section 3.6 and it suits gRPC. It suits an
in-process transport badly: there is nothing to listen on.

**Picked:** the fake transport accepts a nil listener and ignores it.

**What would force a change:** nothing urgent. If a second non-socket transport
appears, replacing the parameter with an opaque `ListenSpec` would be tidier.

---

## 3. The contents of `Stats` — M0, provisional

SPEC.md section 3.1 returns `Stats` and never says what is in it.

**Picked:** a plain struct with self, member count, epoch, key and group counts,
and hit/miss counters. A struct, not an interface, so adding a field is not a
breaking change.

**What would force a change:** M7, which adds the correctness counters
(split-brain detections, stale-epoch rejections, version conflicts, mailbox
rejections, transfer failures). Those are additive.

---

## 4. Module path — M0, placeholder

**Picked:** `github.com/peercache/peercache`.

**What would force a change:** the repository actually being published
somewhere. This is a single `go mod edit -module` plus a find-and-replace, and
it should happen before the first tag, not after.

---

## 5. `Watch()` semantics under a slow consumer — M0, provisional

SPEC.md section 3.5 gives `Watch() <-chan MembershipChange` with no buffering
or fan-out rule.

**Picked:** one channel per call, buffered at 64, non-blocking send. A watcher
that stops reading loses changes rather than stalling membership.

**Why:** the alternative is letting a slow consumer block the failure detector,
which converts a slow watcher into a cluster-wide outage.

**What would force a change:** a consumer that genuinely cannot miss a change.
That consumer should be reading `Members()` after each wake-up rather than
treating the channel as a log, and if one appears the fix is to document that,
not to make the channel lossless.

---

## 6. Real-clock tests wait on real timers — M0, accepted

CLAUDE.md forbids `time.Sleep` in tests. `internal/clock/real_test.go` blocks on
a real 1 ms timer, which is a wait by another name.

**Picked:** allow it, only in the tests for the real clock itself, only on
timers of 1 ms, and nowhere else.

**Why:** there is no way to test that the real clock's timers fire without
letting one fire. The rule's purpose — that no test waits on a timeout that
belongs to the code under test — is intact everywhere else, and
`TestNoSleepAnywhere` enforces the literal rule across the whole tree.

---

## 7. Detection latency defaults — M0, arbitrary

`DefaultFailureTimeout` is 3 s and `DefaultJoinDelay` is 500 ms in the harness.
They are distinct so that a test asserting on one cannot pass by accidentally
satisfying the other.

**What would force a change:** M3, where the real gossip layer has its own
timings and the harness should probably default to matching them.

---

## 8. `activationDelay` interacts with join delay — M3, not yet decided

SPEC.md section 4 (M3) requires a node to wait `activationDelay` after a
membership change before activating groups it newly owns. The harness's
`JoinDelay` is a different thing — admission latency, not activation latency —
and the two will need to be reasoned about together.

Recording it now because getting them confused is the sort of mistake that
produces a test proving the wrong property.

---

## 9. `BlockUntil` counts every timer, not the interesting ones — M0, known sharp edge

`Fake.BlockUntil(n)` parks until *n* timers are armed on the clock. It cannot
distinguish yours from anyone else's. In a test where a component already holds
a timer, `BlockUntil(1)` returns immediately and synchronises with nothing —
the test then passes for the wrong reason, which is worse than failing.

**Picked:** document it, and keep the harness quiescent in the steady state so
the common case is safe. `testkit.Membership` arms timers only during
transitions, so a converged cluster has zero pending and `BlockUntil(1)` means
what it looks like it means.

**What would force a change:** M2's idle-deactivation sweep, which will hold a
timer per active group. At that point `BlockUntil` needs either a baseline
(`Pending()` before, then block on baseline+n) or a tagged variant.

---

## 10. Network change watchers accumulate — M0, bounded

`Network.OnChange` registrations cannot be removed, so each
`Cluster.Revive` adds a watcher that outlives the membership it belongs to. A
closed membership's `Refresh` returns immediately, so the cost is a slice entry
and a function call per revival.

**Picked:** leave it. Bounded by the length of one test.

**What would force a change:** M5's chaos test, which kills and revives a random
node every two seconds for sixty seconds. Thirty stale watchers is still
nothing, so this is being recorded rather than fixed — but if a soak test ever
revives thousands of times, `OnChange` needs to return a deregistration
function.

---

## 11. `goleak` and parallel tests — M0, constraint not yet violated

`Cluster.Close` runs `goleak.VerifyNone` with `IgnoreCurrent` captured at
construction. That correctly ignores goroutines that existed beforehand, but it
cannot distinguish another test's goroutines started afterwards.

**Picked:** no test in the repository calls `t.Parallel`, so this is currently
sound.

**What would force a change:** the first parallel test. The fix is to move the
leak check to `TestMain` for parallel packages, and it should happen before, not
after, someone adds `t.Parallel` and starts seeing phantom leaks.

---

## 12. Deliberately deferred, per SPEC.md section 5

Not open questions. Listed so that nobody reopens them by accident:

- Persistence: WAL, snapshots, object-storage backup.
- Cross-group transactions.
- Multi-region and multi-cluster.
- Raft-backed membership. The interface exists; the implementation does not.
- Group sharding for hot keys. The limitation is documented instead.
- Protobuf and CBOR codecs beyond the registry hook.
- Snappy and Zstd beyond the compression interface.
