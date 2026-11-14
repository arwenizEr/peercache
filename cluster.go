package peercache

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/digest"
	"github.com/arwenizEr/peercache/internal/wire"
	"github.com/arwenizEr/peercache/transport"
)

// DescribeTimeout bounds how long DescribeCluster waits for one member.
const DescribeTimeout = 5 * time.Second

// ClusterDescription is what every member of one node's view reports, side by
// side. See [Cache.DescribeCluster] and docs/decisions/0039-cluster-view.md.
type ClusterDescription struct {
	// From is the node that gathered it, and whose view decided whom to ask.
	From string `json:"from"`
	// Nodes is one report per member of From's view, sorted by ID.
	Nodes []NodeReport `json:"nodes"`
	// MembersAgree reports whether every node that answered sees the same
	// member set. Disagreements names those that do not, and what they see.
	MembersAgree  bool     `json:"membersAgree"`
	Disagreements []string `json:"disagreements,omitempty"`
	// Digests is set when digests were asked for and every member answered.
	// OutOfStep lists the partitions whose replica copies do not match what
	// their owners hold, empty when all are in step. Replication still in
	// flight shows here too: ask again to tell lag from loss. Keys with a
	// TTL can differ for a moment around expiry, because each node sweeps
	// on its own.
	Digests   bool             `json:"digests"`
	OutOfStep []PartitionCheck `json:"outOfStep,omitempty"`
}

// NodeReport is one member's answer: its Description, or why there is none.
type NodeReport struct {
	ID          string       `json:"id"`
	Description *Description `json:"description,omitempty"`
	Error       string       `json:"error,omitempty"`
}

// PartitionCheck is one partition of one namespace whose replica copies are out
// of step with what its owners hold: how many keys the owners hold, how many
// copies there should be, how many there are, and each node's share. No key
// is named.
type PartitionCheck struct {
	Namespace  string      `json:"namespace"`
	Partition  int         `json:"partition"`
	Owned      uint64      `json:"owned"`
	WantCopies uint64      `json:"wantCopies"`
	HeldCopies uint64      `json:"heldCopies"`
	Nodes      []NodeShare `json:"nodes"`
}

// NodeShare is one node's part of a partition: keys it owns, copies it holds.
type NodeShare struct {
	Node  string `json:"node"`
	Owned uint64 `json:"owned"`
	Held  uint64 `json:"held"`
}

// DescribeCluster asks every member of this node's view for its description —
// and, with digests, for its replica digests — and reports them together with
// whether they agree. It reads, never repairs. With digests every node walks
// its stores once, so it costs in proportion to the keyspace: for incidents,
// not for scraping.
func (c *cache) DescribeCluster(ctx context.Context, digests bool) ClusterDescription {
	ctx = ctxOrBackground(ctx)
	self := c.self().ID
	out := ClusterDescription{From: self}
	members := []core.Node{c.self()}
	if c.guard != nil {
		members = c.guard.Table().Members()
	}

	byNode := map[string][]wire.NamespaceDigest{}
	for _, m := range members {
		report := NodeReport{ID: m.ID}
		if m.ID == self {
			d := c.Describe()
			report.Description = &d
			if digests {
				byNode[m.ID] = c.replicaDigests()
			}
		} else {
			var resp wire.DescribeResponse
			// Bounded per member, on the cache's clock: one member
			// that stopped answering must cost the report a field,
			// not the caller its whole budget — or, with no
			// deadline, forever.
			mctx, cancel := clock.WithTimeout(ctx, c.opts.Clock, DescribeTimeout)
			err := c.route.invoke(mctx, m, transport.MethodDescribe, &wire.DescribeRequest{Digests: digests}, &resp)
			cancel()
			if err == nil {
				var d Description
				if err = json.Unmarshal(resp.Description, &d); err == nil {
					report.Description = &d
					byNode[m.ID] = resp.Digests
				}
			}
			if err != nil {
				report.Error = err.Error()
			}
		}
		out.Nodes = append(out.Nodes, report)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })

	out.MembersAgree = true
	var want string
	for _, n := range out.Nodes {
		if n.Description == nil {
			continue
		}
		ids := make([]string, len(n.Description.Members))
		for i, m := range n.Description.Members {
			ids[i] = m.ID
		}
		seen := strings.Join(ids, ",")
		if n.ID == self {
			want = seen
		}
		out.Disagreements = append(out.Disagreements, n.ID+" sees ["+seen+"]")
	}
	agree := true
	for _, n := range out.Nodes {
		if n.Description == nil {
			agree = false
			continue
		}
		ids := make([]string, len(n.Description.Members))
		for i, m := range n.Description.Members {
			ids[i] = m.ID
		}
		if strings.Join(ids, ",") != want {
			agree = false
		}
	}
	out.MembersAgree = agree
	if agree {
		out.Disagreements = nil
	}
	if digests && len(byNode) == len(members) {
		rf := map[string]int{}
		c.mu.RLock()
		for name, ns := range c.namespaces {
			rf[name] = ns.opts.ReplicationFactor
		}
		c.mu.RUnlock()
		out.Digests = true
		out.OutOfStep = checkPartitions(byNode, rf, len(members))
	}
	return out
}

// replicaDigests is this node's running digests for every namespace with
// replicas. Reading them costs one pass over 64 partitions per store, whatever
// the store holds; the stores keep them up to date on every write.
func (c *cache) replicaDigests() []wire.NamespaceDigest {
	c.mu.RLock()
	names := append([]string(nil), c.order...)
	c.mu.RUnlock()
	sort.Strings(names)
	var out []wire.NamespaceDigest
	for _, name := range names {
		ns, ok := c.namespaceIfExists(name)
		if !ok || ns.err != nil || ns.store == nil || ns.opts.ReplicationFactor < 2 {
			continue
		}
		out = append(out, wire.NamespaceDigest{
			Namespace: name,
			Owned:     sparse(ns.store.Digest()),
			Held:      sparse(c.replicas.Digest(name)),
		})
	}
	return out
}

func sparse(parts [digest.Parts]digest.Pair) []wire.PartDigest {
	var out []wire.PartDigest
	for i, p := range parts {
		if p.Count != 0 || p.Sum != 0 {
			out = append(out, wire.PartDigest{Part: uint32(i), Count: p.Count, Sum: p.Sum})
		}
	}
	return out
}

// checkPartitions compares, per namespace and partition, the keys owners hold
// with the copies replicas hold. In step, every owned key has copies-1 replica
// copies at its owner's version, so the held count and sum are the owned ones
// times copies-1 — exactly, sums wrapping alike. Only partitions out of step
// are returned, with every node's share so the short node shows. It needs
// every member's digests; with one missing nothing is concluded.
func checkPartitions(byNode map[string][]wire.NamespaceDigest, rf map[string]int, members int) []PartitionCheck {
	type key struct {
		ns   string
		part uint32
	}
	type tally struct {
		owned, held digest.Pair
		nodes       map[string]*NodeShare
	}
	tallies := map[key]*tally{}
	get := func(k key) *tally {
		t := tallies[k]
		if t == nil {
			t = &tally{nodes: map[string]*NodeShare{}}
			tallies[k] = t
		}
		return t
	}
	share := func(t *tally, node string) *NodeShare {
		n := t.nodes[node]
		if n == nil {
			n = &NodeShare{Node: node}
			t.nodes[node] = n
		}
		return n
	}
	for node, list := range byNode {
		for _, nd := range list {
			for _, d := range nd.Owned {
				t := get(key{nd.Namespace, d.Part})
				t.owned.Count += d.Count
				t.owned.Sum += d.Sum
				share(t, node).Owned += d.Count
			}
			for _, d := range nd.Held {
				t := get(key{nd.Namespace, d.Part})
				t.held.Count += d.Count
				t.held.Sum += d.Sum
				share(t, node).Held += d.Count
			}
		}
	}
	var out []PartitionCheck
	for k, t := range tallies {
		copies := rf[k.ns]
		if copies > members {
			copies = members
		}
		m := uint64(copies - 1)
		if t.held.Count == t.owned.Count*m && t.held.Sum == t.owned.Sum*m {
			continue
		}
		pc := PartitionCheck{Namespace: k.ns, Partition: int(k.part), Owned: t.owned.Count,
			WantCopies: t.owned.Count * m, HeldCopies: t.held.Count}
		for _, n := range t.nodes {
			pc.Nodes = append(pc.Nodes, *n)
		}
		sort.Slice(pc.Nodes, func(i, j int) bool { return pc.Nodes[i].Node < pc.Nodes[j].Node })
		out = append(out, pc)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// describe answers a peer's Describe.
func (h *peerHandler) describe(_ context.Context, req []byte) ([]byte, error) {
	var in wire.DescribeRequest
	if err := wire.Decode(req, &in); err != nil {
		return nil, err
	}
	body, err := json.Marshal(h.cache.Describe())
	if err != nil {
		return nil, err
	}
	out := wire.DescribeResponse{Description: body}
	if in.Digests {
		out.Digests = h.cache.replicaDigests()
	}
	return wire.Encode(&out)
}
