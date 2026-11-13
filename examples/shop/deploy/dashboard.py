"""Generates examples/shop/deploy/peercache.json, the M16 Grafana dashboard.

Run: python dashboard.py <output path>. The panel list below is the source;
the JSON is regenerated from it rather than edited.
"""
import json
import sys

# (title, unit, [(legend, expr)]) -- one row of the dashboard per section.
SECTIONS = [
    ("Cluster totals", [
        ("Owned keys (sum)", "short", [("owned", "sum(peercache_owned_keys)")], "stat"),
        ("Replica keys (sum)", "short", [("replicas", "sum(peercache_replica_keys)")], "stat"),
        ("Replicas per owned key", "short",
         [("ratio", "sum(peercache_replica_keys) / clamp_min(sum(peercache_owned_keys), 1)")], "stat"),
        ("Members seen (min/max)", "short",
         [("min", "min(peercache_members)"), ("max", "max(peercache_members)")], "stat"),
    ]),
    ("Traffic", [
        ("Hits / misses per second", "ops", [
            ("hits", "sum(rate(peercache_hits_total[1m]))"),
            ("misses", "sum(rate(peercache_misses_total[1m]))"),
            ("loads", "sum(rate(peercache_loads_total[1m]))"),
        ], "timeseries"),
        ("Replication per second", "ops", [
            ("sent", "sum(rate(peercache_replications_sent_total[1m]))"),
            ("failed", "sum(rate(peercache_replications_failed_total[1m]))"),
            ("dropped", "sum(rate(peercache_replications_dropped_total[1m]))"),
            ("degraded writes", "sum(rate(peercache_degraded_writes_total[1m]))"),
        ], "timeseries"),
        ("Replica reads per second", "ops", [
            ("served", "sum(rate(peercache_replica_reads_served_total[1m]))"),
            ("referred", "sum(rate(peercache_replica_reads_referred_total[1m]))"),
        ], "timeseries"),
    ]),
    ("Membership and ownership", [
        ("Epoch per pod (per view: they need not agree)", "short",
         [("{{pod}}", "peercache_epoch")], "timeseries"),
        ("Owned keys per pod", "short", [("{{pod}}", "peercache_owned_keys")], "timeseries"),
        ("Replica keys per pod", "short", [("{{pod}}", "peercache_replica_keys")], "timeseries"),
        ("Ready to lose a node (replicas current)", "short",
         [("{{pod}}", "peercache_replicas_current")], "timeseries"),
    ]),
    ("Churn: what membership changes cost", [
        ("Replica hand-off per second", "ops", [
            ("evicted", "sum(rate(peercache_replicas_evicted_total[1m]))"),
            ("rehomed", "sum(rate(peercache_replicas_rehomed_total[1m]))"),
            ("retained", "sum(rate(peercache_replicas_retained_total[1m]))"),
        ], "timeseries"),
        ("Promotion and catch-up per second", "ops", [
            ("promoted", "sum(rate(peercache_keys_promoted_total[1m]))"),
            ("pulled", "sum(rate(peercache_replicas_pulled_total[1m]))"),
            ("handed off", "sum(rate(peercache_keys_handed_off_total[1m]))"),
            ("adopted", "sum(rate(peercache_keys_adopted_total[1m]))"),
        ], "timeseries"),
        ("Conflicts per second, per pod", "ops", [
            ("version {{pod}}", "rate(peercache_version_conflicts_total[1m])"),
            ("split brain {{pod}}", "rate(peercache_split_brain_detections_total[1m])"),
        ], "timeseries"),
        ("Refusals per second", "ops", [
            ("stale epoch", "sum(rate(peercache_stale_epoch_refusals_total[1m]))"),
            ("deferred activation", "sum(rate(peercache_deferred_activations_total[1m]))"),
            ("refused activation", "sum(rate(peercache_refused_activations_total[1m]))"),
            ("mailbox full", "sum(rate(peercache_mailbox_rejections_total[1m]))"),
        ], "timeseries"),
    ]),
    ("Persistence and memory", [
        ("Records waiting to flush, per pod", "short",
         [("{{pod}}", "peercache_persist_pending")], "timeseries"),
        ("Flushes and failures per second", "ops", [
            ("flushes", "sum(rate(peercache_persist_flushes_total[1m]))"),
            ("records", "sum(rate(peercache_persist_records_written_total[1m]))"),
            ("failures", "sum(rate(peercache_persist_failures_total[1m]))"),
        ], "timeseries"),
        ("Memory used vs budget, per pod", "bytes", [
            ("used {{pod}}", "peercache_memory_used_bytes"),
            ("budget {{pod}}", "peercache_memory_budget_bytes"),
        ], "timeseries"),
    ]),
]


def panel(pid, title, unit, targets, kind, x, y, w, h):
    p = {
        "id": pid,
        "type": kind,
        "title": title,
        "datasource": {"type": "prometheus", "uid": "prom"},
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "targets": [
            {"refId": chr(ord("A") + i), "expr": expr, "legendFormat": legend,
             "datasource": {"type": "prometheus", "uid": "prom"}}
            for i, (legend, expr) in enumerate(targets)
        ],
    }
    if kind == "stat":
        p["options"] = {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "none"}
    return p


def build():
    panels, pid, y = [], 1, 0
    for name, items in SECTIONS:
        panels.append({"id": pid, "type": "row", "title": name, "collapsed": False,
                       "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
        pid += 1
        y += 1
        stat = all(k == "stat" for *_, k in items)
        w = 24 // len(items) if stat else 12
        h = 4 if stat else 8
        x = 0
        for title, unit, targets, kind in items:
            if x + w > 24:
                x = 0
                y += h
            panels.append(panel(pid, title, unit, targets, kind, x, y, w, h))
            pid += 1
            x += w
        y += h
    return {
        "uid": "peercache-m16",
        "title": "PeerCache M16",
        "tags": ["peercache"],
        "timezone": "browser",
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "schemaVersion": 39,
        "panels": panels,
    }


if __name__ == "__main__":
    with open(sys.argv[1], "w", encoding="utf-8", newline="\n") as f:
        json.dump(build(), f, indent=2)
        f.write("\n")
