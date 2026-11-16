package core

// Differences names the fields of asked that differ from o, in field order, or
// returns nil when none do.
//
// Values are compared directly. Functions and interfaces — a loader, a
// partitioner, a blob store — cannot be compared meaningfully in Go, so only
// whether one is set is: asking for a loader on a namespace opened without one
// is a difference, passing a loader to one that has a loader is not.
//
// It is written out field by field rather than by reflection because Namespace
// is documented as cheap enough for a hot path, and a reflective walk cost a
// microsecond and three allocations per call. TestDifferencesCoversEveryField
// fails when a field is added to NamespaceOptions and not here.
func (o *NamespaceOptions) Differences(asked *NamespaceOptions) []string {
	var d []string
	add := func(differ bool, name string) {
		if differ {
			d = append(d, name)
		}
	}
	add((o.Loader == nil) != (asked.Loader == nil), "Loader")
	add((o.Partitioner == nil) != (asked.Partitioner == nil), "Partitioner")
	add(o.SyncReplication != asked.SyncReplication, "SyncReplication")
	add(o.RequireReplicationFactor != asked.RequireReplicationFactor, "RequireReplicationFactor")
	add(o.ReplicationFactor != asked.ReplicationFactor, "ReplicationFactor")
	add(o.Codec != asked.Codec, "Codec")
	add((o.BatchLoader == nil) != (asked.BatchLoader == nil), "BatchLoader")
	add((o.TypedLoader == nil) != (asked.TypedLoader == nil), "TypedLoader")
	add(o.L1TTL != asked.L1TTL, "L1TTL")
	add(o.L1MaxBytes != asked.L1MaxBytes, "L1MaxBytes")
	add(o.Compressor != asked.Compressor, "Compressor")
	add(o.CompressionThreshold != asked.CompressionThreshold, "CompressionThreshold")
	add(o.LoaderTimeout != asked.LoaderTimeout, "LoaderTimeout")
	add(o.NegativeTTL != asked.NegativeTTL, "NegativeTTL")
	add(o.HotKeys != asked.HotKeys, "HotKeys")
	add(o.TTLJitter != asked.TTLJitter, "TTLJitter")
	add(o.RefreshAhead != asked.RefreshAhead, "RefreshAhead")
	add(o.Shards != asked.Shards, "Shards")
	add(o.ReplicaReads != asked.ReplicaReads, "ReplicaReads")
	add((o.Persistence == nil) != (asked.Persistence == nil), "Persistence")
	add(o.PersistFlushInterval != asked.PersistFlushInterval, "PersistFlushInterval")
	return d
}
