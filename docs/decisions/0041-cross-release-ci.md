# 0041 — Every change runs beside the previous release

**Status:** accepted (2026-11-15).
**Bound by:** decision 3 (all pods run the same binary — true at rest, false
for the length of every rolling upgrade), decision 0014 (msgpack over gRPC,
no generated schema), decision 0035 (a new field an earlier peer ignores).

## Context

A rolling upgrade runs two releases in one cluster for as long as it takes.
Each wire change so far was argued compatible and given a test in
`internal/wire` that decodes the new message with a hand-written copy of the
earlier one. That copy is what the author believed the earlier release did.
Nothing ran the earlier release itself, and nothing covered the messages no
one thought to copy: a renamed msgpack tag, a reordered array, a changed
hand-off page, an RPC an earlier server does not route.

## Decision

A CI job, `scripts/crossversion.sh`, builds one node program
(`tests/crossversion`) against the latest release tag and against the change,
and runs both on localhost over real gossip and real gRPC. Two three-node
clusters: the earlier release as the minority, then the change as the
minority. In each, every node writes plain keys, a batch, and appends to
group state through a registered function; every node then reads every other
node's keys, singly and batched, and every group; then the minority node
drains and the rest read every key again, so the hand-off stream crosses in
both directions. A wrong value fails at once; a missing one is asked again
until the phase times out, because a view still settling is not a bug.

The node program uses only API present in the earlier release, so it is its
own module and the same source compiles against both. When a release adds
something worth crossing — a new RPC, a new message field with a fallback —
the program gains it after that release is the previous one.

Supported skew is one release: adjacent releases interoperate. Two releases
apart is not promised and not tested.

## Consequences

- A wire change that breaks the earlier release fails CI on the pull request,
  not on a cluster mid-upgrade. Renaming `GetRequest`'s `key` tag fails both
  clusters (every Get across releases times out or reaches the wrong owner).
- It checks interoperation, not the earlier release's correctness: the
  earlier release's own bugs (F10, F11) are in what it runs beside.
- It needs the tags: the job checks out with full history.
- About 15 seconds when green on the reference machine, builds included; a
  failure waits out a phase timeout (90s).
