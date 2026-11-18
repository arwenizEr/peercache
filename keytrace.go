package peercache

import (
	"strconv"

	"github.com/arwenizEr/peercache/internal/keytrace"
)

// KeyEvent is one change to what a node held for a traced key. See
// [WithKeyTrace].
type KeyEvent struct {
	// At is when it happened, by this node's clock, in Unix nanoseconds.
	At int64 `json:"at"`
	// Kind is what happened: write, delete, replica-accepted,
	// replica-refused, fork, promote, demote, hand-off, restore, shed,
	// replica-shed, read-back or catch-up.
	Kind string `json:"kind"`
	// Order and Writer identify the write the value came from: the order
	// decides which of two values is newer, the writer is the node that
	// wrote it.
	Order  uint64 `json:"order"`
	Writer string `json:"writer,omitempty"`
	// Version is the stored version, where the change stored one.
	Version uint64 `json:"version,omitempty"`
	// Len and Hash describe the value without carrying it: two events with
	// one hash held the same value. Hash is empty for a removal.
	Len  int    `json:"len"`
	Hash string `json:"hash,omitempty"`
	// Detail adds what the kind does not say: a peer, a reason.
	Detail string `json:"detail,omitempty"`
}

// keyHistoryResponse is the debug handler's answer for one key.
type keyHistoryResponse struct {
	Node   string     `json:"node"`
	Events []KeyEvent `json:"events"`
}

func (c *cache) KeyHistory(namespace, key string) []KeyEvent {
	events := c.trace.History(namespace, key)
	if events == nil {
		return nil
	}
	out := make([]KeyEvent, len(events))
	for i, e := range events {
		out[i] = KeyEvent{
			At: e.AtNanos, Kind: e.Kind, Order: e.Order, Writer: e.Writer,
			Version: e.Version, Len: e.Len, Detail: e.Detail,
		}
		if e.Hash != 0 {
			out[i].Hash = strconv.FormatUint(e.Hash, 16)
		}
	}
	return out
}

// Event kinds, as the hooks name them.
const (
	keytraceWrite           = keytrace.Write
	keytraceDelete          = keytrace.Delete
	keytraceReplicaAccepted = keytrace.ReplicaAccepted
	keytraceCatchUp         = keytrace.CatchUp
	keytraceReplicaRefused  = keytrace.ReplicaRefused
	keytracePromote         = keytrace.Promote
	keytraceDemote          = keytrace.Demote
	keytraceHandOff         = keytrace.HandOff
	keytraceRestore         = keytrace.Restore
	keytraceShed            = keytrace.Shed
	keytraceFork            = keytrace.Fork
	keytraceReplicaShed     = keytrace.ReplicaShed
	keytraceReadBack        = keytrace.ReadBack
)

// recordCopy records a replica copy's arrival: stored (as stored, which names
// how it came: replicated or caught up), refused as older, or in conflict with
// the copy held, which is a fork.
func (c *cache) recordCopy(namespace, key, stored string, accepted, conflict bool, order uint64, from string, version uint64, val []byte, detail string) {
	kind := keytrace.ReplicaRefused
	switch {
	case conflict:
		kind = keytrace.Fork
	case accepted:
		kind = stored
	}
	c.record(namespace, key, kind, order, from, version, val, detail)
}

// traced reports whether key of namespace is traced, so that a hook builds
// its event only then.
func (c *cache) traced(namespace, key string) bool { return c.trace.Traced(namespace, key) }

// record adds an event to key's history. Callers check traced first.
func (c *cache) record(namespace, key, kind string, order uint64, writer string, version uint64, val []byte, detail string) {
	c.trace.Record(namespace, key, keytrace.Event{
		Kind: kind, Order: order, Writer: writer, Version: version,
		Len: len(val), Hash: keytrace.Hash(val), Detail: detail,
	})
}
