package metrics

import (
	"sort"
	"strconv"
	"strings"

	"github.com/peercache/peercache/internal/core"
)

// Kind classifies a metric for an exporter that cares about the distinction.
type Kind int

const (
	// Counter only ever increases. A drop to zero means the process
	// restarted, and an exporter should let its backend infer that rather
	// than smoothing it away.
	Counter Kind = iota
	// Gauge moves in both directions.
	Gauge
)

// String implements [fmt.Stringer].
func (k Kind) String() string {
	if k == Counter {
		return "counter"
	}
	return "gauge"
}

// Metric is one exported series.
type Metric struct {
	// Name is in Prometheus form: lowercase, underscore-separated, prefixed
	// with the subsystem. It is part of the operator-facing contract and
	// changing one breaks dashboards, so names are fixed here rather than
	// derived from Go field names.
	Name string
	// Kind is Counter or Gauge.
	Kind Kind
	// Help is a single sentence. Several of these describe windows the
	// system is honest about having, and the help text is where an operator
	// finds out what a non-zero value means.
	Help  string
	Value float64
}

// Collect turns a [core.Stats] snapshot into the exported series.
//
// # Why this is a translation rather than reflection over the struct
//
// Metric names are a contract with dashboards and alerts. Deriving them from Go
// field names would mean a rename in the library silently breaks somebody's
// paging rule, and it would export every future field automatically whether or
// not it means anything to an operator. Writing them out is a few dozen lines
// that make each addition a deliberate act.
//
// # What is deliberately not here
//
// No per-namespace or per-key labels. A cardinality explosion in a metrics
// backend is a production incident of its own, and the aggregate is what an
// operator alerts on. A namespace-level breakdown belongs behind an explicit
// option if it is ever wanted.
func Collect(s core.Stats) []Metric {
	out := []Metric{
		// Membership.
		{"peercache_members", Gauge, "Nodes in this node's membership view, including itself.", float64(s.Members)},
		{"peercache_epoch", Gauge, "This node's membership epoch. Per-view, not globally agreed.", float64(s.Epoch)},

		// Storage.
		{"peercache_owned_keys", Gauge, "Keys this node owns and holds.", float64(s.OwnedKeys)},
		{"peercache_replica_keys", Gauge, "Keys this node holds as a replica for another owner.", float64(s.ReplicaKeys)},
		{"peercache_l1_entries", Gauge, "Non-owned cached entries, bounded by the namespace's L1 TTL.", float64(s.L1Entries)},

		// Groups.
		{"peercache_groups_addressable", Gauge, "Groups this node knows of. A group is addressable whether or not it is activated.", float64(s.AddressableGroups)},
		{"peercache_groups_active", Gauge, "Groups with a live actor goroutine on this node.", float64(s.ActiveGroups)},

		// Traffic.
		{"peercache_hits_total", Counter, "Namespace reads served from local memory.", float64(s.Hits)},
		{"peercache_misses_total", Counter, "Namespace reads not served locally. An expired key counts as a miss.", float64(s.Misses)},
		{"peercache_loads_total", Counter, "Loader invocations.", float64(s.Loads)},
		{"peercache_collapsed_loads_total", Counter, "GetOrLoad calls that joined a load already in flight instead of starting one.", float64(s.CollapsedLoads)},
		{"peercache_inflight_loads", Gauge, "Callers currently waiting on a loader.", float64(s.InflightLoads)},

		// Correctness. These are the ones that matter.
		{"peercache_split_brain_detections_total", Counter,
			"Writes a replica saw from two different nodes under one membership epoch. " +
				"One group has one owner, so any non-zero value means two nodes believed they owned the same key.",
			float64(s.SplitBrainDetections)},
		{"peercache_stale_epoch_refusals_total", Counter,
			"Requests refused because the caller resolved ownership under a view this node has replaced.",
			float64(s.StaleEpochRefusals)},
		{"peercache_epoch_skew_total", Counter,
			"Peer requests whose caller held a different epoch. Expected during churn; sustained means views are not settling.",
			float64(s.EpochSkew)},
		{"peercache_refused_activations_total", Counter,
			"Activations refused because this node does not own the key. Sustained under stable membership means callers disagree about ownership.",
			float64(s.RefusedActivations)},
		{"peercache_deferred_activations_total", Counter,
			"Activations postponed because the group was just acquired. Should spike after a membership change and return to zero.",
			float64(s.DeferredActivations)},
		{"peercache_implicit_namespaces_total", Counter,
			"Namespaces created by an incoming peer write rather than by this node's application. " +
				"Any non-zero value is a misconfiguration: such a namespace runs at replication factor 1 and cannot be reconfigured.",
			float64(s.ImplicitNamespaces)},
		{"peercache_degraded_writes_total", Counter,
			"Synchronous writes acknowledged holding fewer copies than the namespace's replication factor asked for. " +
				"Replication targets come from this node's own membership view, so a node cut off from the cluster ranks itself the owner of everything, places the one copy it can reach and returns success. Any non-zero value means a caller was told a write reached RF nodes when it did not.",
			float64(s.DegradedWrites)},
		{"peercache_state_escapes_total", Counter,
			"Uses of a group state outside the turn that issued it. " +
				"Any non-zero value is an application bug: a group function retained its state and used it later, and those calls silently did nothing. There is no error for this — the caller is not inside a Call — so this counter is the only way it is ever visible.",
			float64(s.StateEscapes)},
		{"peercache_mailbox_rejections_total", Counter,
			"Calls refused because a group's mailbox was full, reported as ErrGroupOverloaded. " +
				"A rising value is backpressure working, not a fault — but it is the first sign a group is saturated, and it arrives before the latency does.",
			float64(s.MailboxRejections)},
		{"peercache_version_conflicts_total", Counter,
			"Replicated copies rejected because a newer one was already held. " +
				"Distinct from split brain: this is one owner's writes arriving out of order, which async replication makes ordinary and the stamp ordering handles. A rising rate means reordering under load.",
			float64(s.VersionConflicts)},

		// Replication.
		{"peercache_replications_sent_total", Counter, "Replica copies successfully placed on a peer.", float64(s.ReplicationsSent)},
		{"peercache_replications_failed_total", Counter,
			"Replica copies a peer did not accept. Each one means a key is on fewer nodes than its replication factor claims.",
			float64(s.ReplicationsFailed)},
		{"peercache_replications_dropped_total", Counter,
			"Replica copies dropped because the asynchronous queue was full. A real reduction in redundancy, not a hint.",
			float64(s.ReplicationsDropped)},

		// Invalidation. Best effort by design, so losses are not alarming.
		{"peercache_invalidations_sent_total", Counter, "Invalidation hints delivered to a peer.", float64(s.InvalidationsSent)},
		{"peercache_invalidations_failed_total", Counter, "Invalidation hints a peer did not accept. A latency cost, not a correctness one.", float64(s.InvalidationsFailed)},
		{"peercache_invalidations_dropped_total", Counter, "Invalidation hints dropped because the queue was full.", float64(s.InvalidationsDropped)},

		// Rebalancing and lifecycle.
		{"peercache_keys_promoted_total", Counter, "Replicas this node took over as owner after a peer failed.", float64(s.KeysPromoted)},
		{"peercache_replicas_evicted_total", Counter, "Replica copies this node stopped ranking for because membership moved them elsewhere. Each one is then either rehomed or retained.", float64(s.ReplicasEvicted)},
		{"peercache_replicas_rehomed_total", Counter,
			"Moved replicas handed to the nodes that now rank for them before the local copy was dropped. " +
				"Dropping without handing off loses the key whenever the new holders have nothing, which is the ordinary case after a partition heals.",
			float64(s.ReplicasRehomed)},
		{"peercache_replicas_retained_total", Counter,
			"Moved replicas this node could not place and kept instead. " +
				"Memory rather than data loss, which is the safe direction — but a rising value under stable membership means hand-offs are failing and the cluster is not converging.",
			float64(s.ReplicasRetained)},
		{"peercache_promotion_rounds_total", Counter, "Reconciliations run. Movement under stable membership means the view is not settling.", float64(s.PromotionRounds)},
		{"peercache_replicas_pulled_total", Counter, "Keys obtained by catch-up after a restart.", float64(s.ReplicasPulled)},
		{"peercache_catchup_runs_total", Counter, "Catch-up invocations.", float64(s.CatchUpRuns)},
		{"peercache_catchup_failures_total", Counter, "Peers unreachable during a catch-up. The replica set is thinner than configured.", float64(s.CatchUpFailures)},
		{"peercache_keys_handed_off_total", Counter, "Keys pushed to a new owner during a graceful drain.", float64(s.KeysHandedOff)},
		{"peercache_keys_adopted_total", Counter, "Keys received from a draining peer that this node already owned.", float64(s.KeysAdopted)},
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Names returns the exported metric names, sorted.
//
// Used by the test that asserts every counter in [core.Stats] reaches an
// exporter: a field added to Stats and forgotten here is invisible to
// operators, which is the failure mode this whole package exists to prevent.
func Names() []string {
	metrics := Collect(core.Stats{})
	out := make([]string, 0, len(metrics))
	for _, m := range metrics {
		out = append(out, m.Name)
	}
	return out
}

// WriteText renders metrics in the Prometheus text exposition format.
//
// Hand-written rather than delegating to the Prometheus client library, so that
// nothing here forces that dependency on an application that does not want it.
// The format is small, stable, and specified; see
// docs/decisions/0017-metrics-without-a-client-library.md.
func WriteText(sb *strings.Builder, metrics []Metric) {
	for _, m := range metrics {
		sb.WriteString("# HELP ")
		sb.WriteString(m.Name)
		sb.WriteString(" ")
		sb.WriteString(m.Help)
		sb.WriteString("\n# TYPE ")
		sb.WriteString(m.Name)
		sb.WriteString(" ")
		sb.WriteString(m.Kind.String())
		sb.WriteString("\n")
		sb.WriteString(m.Name)
		sb.WriteString(" ")
		sb.WriteString(formatFloat(m.Value))
		sb.WriteString("\n")
	}
}

// formatFloat renders a value the way the exposition format expects.
//
// Counters and gauges here are all whole numbers held as float64, so an integer
// rendering is both shorter and easier to read than the general form. Values
// large enough to lose integer precision in a float64 would need scientific
// notation, which is why the fallback exists rather than being unreachable.
func formatFloat(v float64) string {
	if v == float64(int64(v)) && v < 1e15 && v > -1e15 {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
