# 0040 — Reporting hot keys, by name, when asked

**Status:** accepted. The project owner chose opt-in tracking that names keys
(2026-11-15), over always-on tracking that reports only key hashes.
**Bound by:** decision 0031 (introspection holds counts and options, never a
key or value — this is the one exception, and only where enabled), decision 2
(one owner per group: a hot key's load lands on one node, which is why it is
worth seeing), and design §5 (hot groups are documented, not split).

## Context

Coalesced misses and replica reads (decision 0028) soften a hot key, but
nothing shows which key is hot. `DescribeKey` answers only for a key an
operator already suspects. Caffeine keeps a frequency sketch for admission;
the same idea, on the owner, answers the incident question "which key is
melting this pod".

## Decision

`WithHotKeyTracking(n)`, per namespace, 1 ≤ n ≤ 100, off by default. The
namespace's owners count every operation they serve on a key — their own and
their peers' forwarded ones — at the one point every owner-side operation
passes (`awaitRestore`), and report the top n in `Describe`
(`NamespaceDescription.HotKeys`), so in the debug handler's JSON and in the
dashboard's hot-key table, merged across the cluster.

- **Space-Saving over a 1-in-16 sample**, four counters per reported key. A
  key above 1/(4n) of the sampled accesses is always tracked; counts are
  overestimated by at most the counter it replaced. Memory is fixed; one
  access in 16 takes the tracker's lock, the rest pay one atomic add.
- **Halved every minute**, lazily on the next access or report, so the list is
  about now. A key hot an hour ago and quiet since is gone.
- **It names keys.** Enabling it is a decision that this namespace's keys may
  be read by whoever reaches the debug port. The option's documentation says
  so; a namespace whose keys are personal data should not enable it.

## Rejected

- **Always on, hashes only:** keeps 0031 whole, but a hash names nothing an
  operator can recognise; it confirms a suspicion and discovers nothing.
- **Exact per-key counters:** memory proportional to the keyspace.
- **A metric per hot key:** unbounded label cardinality, which decision 0017
  forbids for exactly this reason.

## Acceptance

- One key read far more than a hundred others is reported first, its estimate
  within sampling error of its true count.
- A key hot an hour ago and untouched since is not reported.
- Reads forwarded by a peer are counted on the owner.
- Without the option nothing names a key; outside 0..100 is ErrInvalidOption.
- A cached read with tracking costs ~6 ns more and allocates nothing.
