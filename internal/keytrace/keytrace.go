package keytrace

import (
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"

	"github.com/arwenizEr/peercache/internal/clock"
)

// Bounds. A trace is a diagnostic instrument for a handful of keys.
const (
	// PerKey is how many of a key's newest events are kept.
	PerKey = 64
	// MaxKeys is how many keys one node traces; keys beyond it are counted.
	MaxKeys = 4096
)

// Event is one change to what a node holds for a traced key.
type Event struct {
	// AtNanos is the node's clock, in Unix nanoseconds.
	AtNanos int64
	// Kind names the change; see the Kind constants.
	Kind string
	// Order and Writer identify the write the value came from.
	Order  uint64
	Writer string
	// Version is the stored version, where the change stored one.
	Version uint64
	// Len and Hash describe the value without keeping it. Hash is zero for a
	// removal.
	Len  int
	Hash uint64
	// Detail says what the kind alone does not: a peer, a reason.
	Detail string
}

// Kinds of event.
const (
	Write           = "write"            // the owner applied a write
	Delete          = "delete"           // the owner applied a delete
	ReplicaAccepted = "replica-accepted" // a replica copy was stored here
	ReplicaRefused  = "replica-refused"  // a replica copy was refused here
	Fork            = "fork"             // a copy conflicted with this one: two writers
	Promote         = "promote"          // a replica copy became the owned value
	Demote          = "demote"           // the owned value left for a replica set
	HandOff         = "hand-off"         // a replica copy was placed elsewhere
	Restore         = "restore"          // storage's record was merged into memory
	Shed            = "shed"             // the memory budget dropped the owned value
	ReplicaShed     = "replica-shed"     // the memory budget dropped a replica copy
	ReadBack        = "read-back"        // a shed key was read back from storage
	CatchUp         = "catch-up"         // catch-up installed a copy
)

// Hash is the hash events carry for a value.
func Hash(val []byte) uint64 {
	if val == nil {
		return 0
	}
	return xxhash.Sum64(val)
}

// Tracer records the events of the keys it was configured with. A nil Tracer
// records nothing, and every method is safe on it.
type Tracer struct {
	clk      clock.Clock
	prefixes map[string][]string // namespace -> key prefixes; read-only

	mu      sync.Mutex
	rings   map[string]*ring // namespace + "\x00" + key
	dropped atomic.Int64
}

type ring struct {
	events [PerKey]Event
	next   int
	full   bool
}

// New returns a tracer for the keys of each namespace that start with one of
// its prefixes, or nil if prefixes is empty. An empty prefix traces every key.
func New(clk clock.Clock, prefixes map[string][]string) *Tracer {
	if len(prefixes) == 0 {
		return nil
	}
	cp := make(map[string][]string, len(prefixes))
	for ns, ps := range prefixes {
		cp[ns] = append([]string(nil), ps...)
	}
	return &Tracer{clk: clk, prefixes: cp, rings: map[string]*ring{}}
}

// Traced reports whether key in namespace is traced. Hooks call it before
// building an event, so an untraced key costs a map lookup and nothing more.
func (t *Tracer) Traced(namespace, key string) bool {
	if t == nil {
		return false
	}
	for _, p := range t.prefixes[namespace] {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Record appends e to key's history if key is traced, stamping its time.
func (t *Tracer) Record(namespace, key string, e Event) {
	if !t.Traced(namespace, key) {
		return
	}
	e.AtNanos = t.clk.Now().UnixNano()
	id := namespace + "\x00" + key
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.rings[id]
	if !ok {
		if len(t.rings) >= MaxKeys {
			t.dropped.Add(1)
			return
		}
		r = &ring{}
		t.rings[id] = r
	}
	r.events[r.next] = e
	r.next++
	if r.next == PerKey {
		r.next, r.full = 0, true
	}
}

// History returns key's events, oldest first.
func (t *Tracer) History(namespace, key string) []Event {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	r, ok := t.rings[namespace+"\x00"+key]
	if !ok {
		return nil
	}
	if !r.full {
		return append([]Event(nil), r.events[:r.next]...)
	}
	out := make([]Event, 0, PerKey)
	out = append(out, r.events[r.next:]...)
	return append(out, r.events[:r.next]...)
}

// Dropped counts events for keys beyond MaxKeys.
func (t *Tracer) Dropped() int64 {
	if t == nil {
		return 0
	}
	return t.dropped.Load()
}
