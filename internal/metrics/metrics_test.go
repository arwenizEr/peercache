package metrics_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/metrics"
)

// TestEveryStatsFieldIsExported is the test this package exists for.
//
// A counter that is collected but never exported is worse than one that does
// not exist: the code carries the cost of maintaining it and an operator gets
// none of the benefit, while everyone assumes it is visible because the field
// is right there in Stats.
//
// The check is by reflection over [core.Stats] rather than by a hand-written
// list, because a hand-written list has exactly the same failure mode as the
// thing it is checking — somebody adds a field and forgets two places instead
// of one.
func TestEveryStatsFieldIsExported(t *testing.T) {
	// Fields that are deliberately not metrics, each with a reason. Adding
	// to this list is a decision; forgetting a field is not.
	exempt := map[string]string{
		"Self": "an identity, not a measurement; belongs in labels if anywhere",
	}

	typ := reflect.TypeOf(core.Stats{})
	exported := make(map[string]bool)
	for _, name := range metrics.Names() {
		exported[name] = true
	}

	var missing []string
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if _, ok := exempt[field.Name]; ok {
			continue
		}
		if !hasMetricFor(field.Name, exported) {
			missing = append(missing, field.Name)
		}
	}

	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("%d Stats field(s) have no exported metric: %v\n"+
			"A field nobody can scrape is a field nobody can alert on. Add it to "+
			"metrics.Collect, or to the exempt list in this test with a reason.",
			len(missing), missing)
	}
}

// hasMetricFor reports whether a Stats field name plausibly corresponds to one
// of the exported metric names.
//
// The mapping is CamelCase to snake_case with an optional _total suffix, which
// is what Collect does by hand. This does not verify the *value* is right —
// TestValuesReachTheOutput does that — only that the field is represented at
// all.
func hasMetricFor(field string, exported map[string]bool) bool {
	snake := toSnake(field)
	for _, candidate := range []string{
		"peercache_" + snake,
		"peercache_" + snake + "_total",
		// A few names read better without the pluralising suffix the field
		// carries, so accept the singular too.
		"peercache_" + strings.TrimSuffix(snake, "s"),
		"peercache_" + strings.TrimSuffix(snake, "s") + "_total",
	} {
		if exported[candidate] {
			return true
		}
	}
	// Names that were deliberately reworded for the operator rather than
	// transliterated. Each pair is a judgement call recorded here.
	aliases := map[string]string{
		"AddressableGroups": "peercache_groups_addressable",
		"ActiveGroups":      "peercache_groups_active",
		// "catchup" is one word to an operator, whatever Go's field name
		// says. This test caught the mismatch, which is what it is for.
		"CatchUpRuns":     "peercache_catchup_runs_total",
		"CatchUpFailures": "peercache_catchup_failures_total",
	}
	if alias, ok := aliases[field]; ok {
		return exported[alias]
	}
	return false
}

func toSnake(s string) string {
	var sb strings.Builder
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 {
				sb.WriteByte('_')
			}
			sb.WriteRune(r - 'A' + 'a')
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

// TestValuesReachTheOutput checks that a non-zero stat produces a non-zero
// series, which is the other half of "exported": a metric wired to the wrong
// field is as useless as a missing one.
func TestValuesReachTheOutput(t *testing.T) {
	s := core.Stats{
		Members:              5,
		OwnedKeys:            100,
		ReplicaKeys:          200,
		SplitBrainDetections: 7,
		ImplicitNamespaces:   3,
		ReplicationsFailed:   11,
		KeysHandedOff:        13,
		MailboxRejections:    17,
		VersionConflicts:     19,
	}

	got := make(map[string]float64)
	for _, m := range metrics.Collect(s) {
		got[m.Name] = m.Value
	}

	for name, want := range map[string]float64{
		"peercache_members":                      5,
		"peercache_owned_keys":                   100,
		"peercache_replica_keys":                 200,
		"peercache_split_brain_detections_total": 7,
		"peercache_implicit_namespaces_total":    3,
		"peercache_replications_failed_total":    11,
		"peercache_keys_handed_off_total":        13,
		"peercache_mailbox_rejections_total":     17,
		"peercache_version_conflicts_total":      19,
	} {
		if got[name] != want {
			t.Errorf("%s = %v, want %v", name, got[name], want)
		}
	}
}

// TestCorrectnessMetricsAreCounters pins the type of the series an operator
// alerts on.
//
// A counter that an exporter treats as a gauge loses rate() and increase(),
// which are exactly the queries these are for: "did split brain happen in the
// last hour" is a rate question, not a level question.
func TestCorrectnessMetricsAreCounters(t *testing.T) {
	kinds := make(map[string]metrics.Kind)
	for _, m := range metrics.Collect(core.Stats{}) {
		kinds[m.Name] = m.Kind
	}

	for _, name := range []string{
		"peercache_split_brain_detections_total",
		"peercache_stale_epoch_refusals_total",
		"peercache_implicit_namespaces_total",
		"peercache_replications_failed_total",
		"peercache_replications_dropped_total",
		"peercache_catchup_failures_total",
		"peercache_mailbox_rejections_total",
		"peercache_version_conflicts_total",
	} {
		if kinds[name] != metrics.Counter {
			t.Errorf("%s is a %v, want a counter; rate() and increase() are the "+
				"queries this metric exists for", name, kinds[name])
		}
	}

	// And the levels are gauges, for the same reason in reverse.
	for _, name := range []string{
		"peercache_members",
		"peercache_owned_keys",
		"peercache_replica_keys",
		"peercache_groups_active",
	} {
		if kinds[name] != metrics.Gauge {
			t.Errorf("%s is a %v, want a gauge", name, kinds[name])
		}
	}
}

// TestEveryMetricHasHelp guards the text an operator reads at 3am. Several of
// these describe windows the system is honest about having, and the help is
// where the meaning of a non-zero value lives.
func TestEveryMetricHasHelp(t *testing.T) {
	for _, m := range metrics.Collect(core.Stats{}) {
		if m.Help == "" {
			t.Errorf("%s has no help text", m.Name)
		}
		if !strings.HasSuffix(m.Help, ".") {
			t.Errorf("%s help does not end in a full stop: %q", m.Name, m.Help)
		}
	}
}

// TestNamesFollowPrometheusConvention keeps the operator-facing contract
// consistent. A name that breaks convention is one somebody has to special-case
// in a dashboard forever.
func TestNamesFollowPrometheusConvention(t *testing.T) {
	seen := make(map[string]bool)
	for _, m := range metrics.Collect(core.Stats{}) {
		if !strings.HasPrefix(m.Name, "peercache_") {
			t.Errorf("%s does not carry the subsystem prefix", m.Name)
		}
		if strings.ToLower(m.Name) != m.Name {
			t.Errorf("%s is not lowercase", m.Name)
		}
		if seen[m.Name] {
			t.Errorf("%s is exported twice", m.Name)
		}
		seen[m.Name] = true

		// Counters end in _total, gauges do not. Prometheus tooling keys
		// off this and a mismatch misleads every reader.
		if m.Kind == metrics.Counter && !strings.HasSuffix(m.Name, "_total") {
			t.Errorf("counter %s does not end in _total", m.Name)
		}
		if m.Kind == metrics.Gauge && strings.HasSuffix(m.Name, "_total") {
			t.Errorf("gauge %s ends in _total, which reads as a counter", m.Name)
		}
	}
}

// TestTextFormatIsParseable checks the exposition output against the shape a
// scraper expects.
func TestTextFormatIsParseable(t *testing.T) {
	var sb strings.Builder
	metrics.WriteText(&sb, metrics.Collect(core.Stats{Members: 3, OwnedKeys: 42}))
	out := sb.String()

	if !strings.Contains(out, "# HELP peercache_members ") {
		t.Error("no HELP line for peercache_members")
	}
	if !strings.Contains(out, "# TYPE peercache_members gauge\n") {
		t.Error("no TYPE line for peercache_members")
	}
	if !strings.Contains(out, "\npeercache_owned_keys 42\n") {
		t.Errorf("owned_keys not rendered as an integer:\n%s", out)
	}

	// Every non-comment line must be "name value".
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		if parts := strings.Fields(line); len(parts) != 2 {
			t.Errorf("malformed sample line: %q", line)
		}
	}
}
