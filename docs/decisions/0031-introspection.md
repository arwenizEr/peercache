# 0031 — Introspection: what one node believes, on request

**Status:** accepted, being built in M15a.
**Milestone:** M15, first part. OpenTelemetry, as a separate module, is the
second.

## Context

What PeerCache exports is aggregate. `Stats()` and the metric series sum over
every namespace on a node; `Ready()` answers yes or no with one reason. That is
what dashboards and alerts want, and it is not what an operator wants when
something is already wrong. The questions then are specific:

- What does this node believe the cluster is — which members, at which epoch?
- Which namespace is the one misbehaving: pending persistence, failing
  restores, a misconfiguration?
- Who owns this key, who should hold its copies, and what does *this* node hold
  of it?

Each is answerable from state a node already has, and none is reachable today
without a debugger.

## Decision

Three methods on `Cache`:

```go
// Describe returns what this node currently believes: itself, its view of
// the membership, its readiness, and each namespace it has opened.
Describe() Description

// DescribeKey returns where key of namespace lives as this node sees it: its
// group key, owner and ranked holders, and what this node holds of it.
// The namespace must have been opened on this node.
DescribeKey(namespace, key string) (KeyPlacement, error)

// DebugHandler serves Describe and DescribeKey as JSON.
DebugHandler() http.Handler
```

`DebugHandler` answers `GET /` (the description), `GET /namespaces`, and
`GET /key?namespace=…&key=…`. Standard library only; nothing new in the
dependency budget.

### What it never shows

**Values.** No method returns one, and a key appears only when the caller names
it. A description lists counts, options and states — the things that are the
same for a cache full of secrets and a cache full of nothing.

### It has no authentication

It is a debug surface for an internal admin port, the way `net/http/pprof` is,
and the documentation says to mount it there. Adding a token check would invite
mounting it on the public port and trusting the check. What it reveals — member
addresses, namespace names, key placement on request — is internal topology, not
data.

### A snapshot, not a consistent one

Each field is read on its own; a description taken during a membership change
can show a view from one epoch and a namespace count from just after. It says
what it is: a node's belief at roughly one moment. Nothing in it is used by the
library itself.

## Alternatives rejected

- **More metric series, per namespace.** Labels per namespace multiply every
  series by the namespace count, and still could not answer "who owns this key".
- **Logging the same on demand.** A log line cannot be asked a question.
- **A gRPC introspection service on the peer port.** That port carries cluster
  traffic and is authenticated by mTLS between peers; operators reaching it
  would need peer credentials.

## Acceptance

- `Describe` reports this node, the members of its view and its epoch,
  readiness, and per namespace its options and counts, including persistence.
- `DescribeKey` names the same owner and holders every node computes, and says
  whether this node holds the key as owner, replica or L1 copy — on each node of
  a cluster.
- No description or placement contains a value, checked by writing a value and
  searching the JSON for it.
- `DebugHandler` serves all three routes as JSON, refuses methods other than
  GET, and reports a missing or unknown namespace as a 4xx, not a 500.
