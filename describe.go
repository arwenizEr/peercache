package peercache

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/arwenizEr/peercache/internal/debugui"
)

// Description is what one node currently believes. See [Cache.Describe].
//
// It holds counts, options and states, never a value. Each field is read on its
// own, so a description taken while membership changes may mix two moments.
type Description struct {
	// Node is this node's ID and address.
	Node Member `json:"node"`
	// Epoch is the membership epoch of this node's view. Epochs are
	// per-view: two nodes that agree on the members may hold different ones.
	Epoch uint64 `json:"epoch"`
	// Members is this node's view of the cluster, sorted by ID. Without a
	// membership authority it is this node alone.
	Members []Member `json:"members"`
	// Ready is this node's readiness, as Cache.Ready reports it.
	Ready ReadyState `json:"ready"`
	// Namespaces is every namespace opened on this node, sorted by name.
	Namespaces []NamespaceDescription `json:"namespaces"`
}

// Member is one node of a membership view.
type Member struct {
	ID   string `json:"id"`
	Addr string `json:"addr,omitempty"`
}

// NamespaceDescription is one namespace's options and counts on this node.
type NamespaceDescription struct {
	Name string `json:"name"`
	// Error is the namespace's configuration error, if it has one; every
	// operation on it reports this.
	Error string `json:"error,omitempty"`

	ReplicationFactor int           `json:"replicationFactor"`
	SyncReplication   bool          `json:"syncReplication"`
	ReplicaReads      bool          `json:"replicaReads"`
	L1TTL             time.Duration `json:"l1TTL"`
	L1MaxBytes        int64         `json:"l1MaxBytes"`

	OwnedKeys   int64 `json:"ownedKeys"`
	ReplicaKeys int   `json:"replicaKeys"`
	L1Entries   int64 `json:"l1Entries"`
	// Absences is the number of keys this node remembers its loader found
	// missing, in a namespace opened with WithNegativeCaching.
	Absences int `json:"absences,omitempty"`
	// HotKeys is this node's estimate of the keys it serves most, highest
	// first, in a namespace opened with WithHotKeyTracking — and only then:
	// it names keys, which nothing else here does.
	HotKeys []HotKey `json:"hotKeys,omitempty"`

	// Persistence is set only for a namespace opened with WithPersistence.
	Persistence *PersistenceDescription `json:"persistence,omitempty"`
}

// HotKey is one key a node serves often: an estimate of its accesses over
// roughly the last minute or two, sampled one in 16, so a count is good to
// about that granularity.
type HotKey struct {
	Key      string `json:"key"`
	Accesses uint64 `json:"accesses"`
}

// PersistenceDescription is one persistent namespace's write-behind state.
type PersistenceDescription struct {
	FlushInterval   time.Duration `json:"flushInterval"`
	Pending         int           `json:"pending"`
	Flushes         int64         `json:"flushes"`
	RecordsWritten  int64         `json:"recordsWritten"`
	Failures        int64         `json:"failures"`
	Restored        int64         `json:"restored"`
	RestoreFailures int64         `json:"restoreFailures"`
	Compactions     int64         `json:"compactions"`
	Splits          int64         `json:"splits"`
}

// KeyPlacement is where one key lives, as this node sees it. See
// [Cache.DescribeKey].
type KeyPlacement struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	// GroupKey is what the namespace's partitioner made of Key, and what
	// ownership is decided by.
	GroupKey string `json:"groupKey"`
	// Owner is the node that owns the key under this node's view, and
	// Holders the owner followed by the replicas, as many as the
	// namespace's replication factor and the view allow.
	Owner   string   `json:"owner"`
	Holders []string `json:"holders"`
	// Role is this node's part in it: "owner", "replica" or "none".
	Role string `json:"role"`
	// Owned, ReplicaCopy and L1Copy report what this node holds of the key
	// right now: a live entry in its owned store, a replica copy, a
	// cached copy in its non-owned tier.
	Owned       bool `json:"owned"`
	ReplicaCopy bool `json:"replicaCopy"`
	L1Copy      bool `json:"l1Copy"`
}

func (c *cache) Describe() Description {
	d := Description{Node: Member{ID: c.selfID(), Addr: c.self().Addr}, Ready: c.Ready()}
	if c.guard != nil {
		table := c.guard.Table()
		d.Epoch = uint64(table.Epoch())
		for _, n := range table.Members() {
			d.Members = append(d.Members, Member{ID: n.ID, Addr: n.Addr})
		}
	} else {
		d.Members = []Member{d.Node}
	}
	sort.Slice(d.Members, func(i, j int) bool { return d.Members[i].ID < d.Members[j].ID })

	c.mu.RLock()
	namespaces := make([]*namespace, 0, len(c.namespaces))
	for _, ns := range c.namespaces {
		namespaces = append(namespaces, ns)
	}
	c.mu.RUnlock()
	sort.Slice(namespaces, func(i, j int) bool { return namespaces[i].name < namespaces[j].name })

	for _, ns := range namespaces {
		d.Namespaces = append(d.Namespaces, c.describeNamespace(ns))
	}
	return d
}

func (c *cache) describeNamespace(ns *namespace) NamespaceDescription {
	nd := NamespaceDescription{
		Name:              ns.name,
		ReplicationFactor: ns.opts.ReplicationFactor,
		SyncReplication:   ns.opts.SyncReplication,
		ReplicaReads:      ns.opts.ReplicaReads,
		L1TTL:             ns.opts.L1TTL,
		L1MaxBytes:        ns.opts.L1MaxBytes,
		ReplicaKeys:       len(c.replicas.Keys(ns.name)),
	}
	if ns.err != nil {
		nd.Error = ns.err.Error()
	}
	if ns.store != nil {
		nd.OwnedKeys = ns.store.Stats().Entries
		nd.Absences = ns.store.Absences()
	}
	if ns.hot != nil {
		for _, k := range ns.hot.Top() {
			nd.HotKeys = append(nd.HotKeys, HotKey{Key: k.Key, Accesses: k.Estimate})
		}
	}
	if ns.l1 != nil {
		nd.L1Entries = int64(ns.l1.Len())
	}
	if ns.persister != nil {
		pc := ns.persister.counters()
		interval := ns.opts.PersistFlushInterval
		if interval <= 0 {
			interval = persistFlushInterval
		}
		nd.Persistence = &PersistenceDescription{
			FlushInterval:   interval,
			Pending:         pc.pending,
			Flushes:         pc.flushes,
			RecordsWritten:  pc.written,
			Failures:        pc.failures,
			Restored:        pc.restored,
			RestoreFailures: pc.restoreFailures,
			Compactions:     pc.compactions,
			Splits:          pc.splits,
		}
	}
	return nd
}

func (c *cache) DescribeKey(namespace, key string) (KeyPlacement, error) {
	ns, ok := c.namespaceIfExists(namespace)
	if !ok {
		return KeyPlacement{}, fmt.Errorf("peercache: namespace %q is not open on this node: %w", namespace, ErrNotFound)
	}
	if ns.err != nil {
		return KeyPlacement{}, fmt.Errorf("namespace %q: %w", namespace, ns.err)
	}
	p := KeyPlacement{Namespace: namespace, Key: key, GroupKey: ns.groupKeyFor(key), Role: "none"}
	self := c.selfID()
	if c.guard == nil {
		p.Owner, p.Holders, p.Role = self, []string{self}, "owner"
	} else {
		holders, n, _ := c.guard.Table().Holders(p.GroupKey, ns.opts.ReplicationFactor)
		for i := 0; i < n; i++ {
			p.Holders = append(p.Holders, holders[i].ID)
			if holders[i].ID == self {
				p.Role = "replica"
				if i == 0 {
					p.Role = "owner"
				}
			}
		}
		if n > 0 {
			p.Owner = holders[0].ID
		}
	}
	if ns.store != nil {
		p.Owned = ns.store.Exists(key)
	}
	_, p.ReplicaCopy = c.replicas.Get(namespace, key)
	if ns.l1 != nil {
		// Get rather than a peek: an expired copy reads as absent, which is
		// the truth a reader would see. It counts one L1 lookup.
		_, _, p.L1Copy = ns.l1.Get(key)
	}
	return p, nil
}

func (c *cache) DebugHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, r, func() (any, int, error) { return c.Describe(), http.StatusOK, nil })
	})
	mux.HandleFunc("/ui", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		// Nothing but the page's own script runs, and nothing loads
		// from elsewhere: the dashboard reads this handler and only it.
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'")
		_, _ = w.Write(debugui.Page)
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, func() (any, int, error) { return c.Stats(), http.StatusOK, nil })
	})
	mux.HandleFunc("/namespaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, func() (any, int, error) { return c.Describe().Namespaces, http.StatusOK, nil })
	})
	mux.HandleFunc("/cluster", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, func() (any, int, error) {
			return c.DescribeCluster(r.Context(), r.URL.Query().Get("digests") == "1"), http.StatusOK, nil
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, func() (any, int, error) {
			q := r.URL.Query()
			namespace, key := q.Get("namespace"), q.Get("key")
			if namespace == "" || !q.Has("key") {
				return nil, http.StatusBadRequest, errors.New("namespace and key are required")
			}
			return keyHistoryResponse{Node: c.selfID(), Events: c.KeyHistory(namespace, key)}, http.StatusOK, nil
		})
	})
	mux.HandleFunc("/key", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, r, func() (any, int, error) {
			q := r.URL.Query()
			namespace, key := q.Get("namespace"), q.Get("key")
			if namespace == "" || !q.Has("key") {
				return nil, http.StatusBadRequest, errors.New("namespace and key are required")
			}
			p, err := c.DescribeKey(namespace, key)
			switch {
			case errors.Is(err, ErrNotFound):
				return nil, http.StatusNotFound, err
			case err != nil:
				return nil, http.StatusUnprocessableEntity, err
			}
			return p, http.StatusOK, nil
		})
	})
	return mux
}

// writeJSON answers GET and HEAD with what body returns, and anything else with
// 405. Errors are JSON too, so a script reading the endpoint has one format.
func writeJSON(w http.ResponseWriter, r *http.Request, body func() (any, int, error)) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	v, status, err := body()
	if err != nil {
		v = map[string]string{"error": err.Error()}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
