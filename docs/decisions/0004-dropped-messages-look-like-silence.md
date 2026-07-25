# 0004 — A dropped message looks like silence, not like an error

**Status:** accepted, M0
**Affects:** `internal/testkit`, and every test that injects loss

## Decision

When `Network.SetDropRate` causes a message to be dropped, `PeerClient.Invoke`
does **not** return an error. It blocks until the caller's context expires, and
then returns `context.DeadlineExceeded`.

An unreachable peer — partitioned, isolated, or unregistered — is different: it
returns `ErrNodeUnreachable` immediately, because the fabric genuinely knows the
message cannot be delivered.

## Why

Returning an error immediately on a drop would be convenient and wrong. No real
network tells a sender that a packet was lost. If the harness did, then every
component built against it could quietly come to depend on prompt notification
of loss, and would break the first time it met a real network.

The distinction the harness needs to teach is:

- **No reply** is what loss looks like. The only bound on it is your deadline.
- **An immediate error** is what a *known* failure looks like, and only the
  transport layer is in a position to know.

## The asymmetry that matters more

Drop rules are directional, so a test can drop the reply while letting the
request through:

```go
c.Net.SetDropRate("n1", "n0", 1)   // requests flow, replies do not
```

The handler runs, its side effects happen, and the caller times out having
learned nothing. This is the case that breaks naive retry logic and the reason
`Namespace.Set` cannot be assumed idempotent-by-accident. It has a named test:
`TestNetworkLostResponseStillRanTheHandler`.

## Consequence for tests

A test that injects drops must give its calls a deadline, or it hangs. This is
deliberate. `clock.WithTimeout` makes that deadline cost nothing in wall clock,
so there is no excuse for omitting it, and `Network.Close` releases any call
still waiting so a forgotten deadline fails the test rather than the suite.

## Reversal

Adding an opt-in `SetDropMode(Immediate)` later would be easy. Nothing has asked
for it, and the default should remain the honest one.
