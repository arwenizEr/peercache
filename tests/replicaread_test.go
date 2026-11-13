package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/transport"
)

// Acceptance tests for replica reads, docs/decisions/0028-hot-key-reads.md.
//
// Which holder a node reads a key from depends only on the node and the key,
// not on the namespace. So each test first finds the (key, reader, replica)
// triple it needs by reading in a "probe" namespace, and then runs its scenario
// in "hot", whose caches are still cold. Nothing here assumes the hash.

const replicaTTL = 2 * time.Second

// filteringTransport refuses chosen methods to chosen peers at once, as an
// unreachable peer does. A dropped message would look like silence instead,
// and silence stalls the serial invalidation and replication queues on the
// fake clock, which is not the failure these tests are about.
type filteringTransport struct {
	core.Transport
	mu     sync.Mutex
	refuse func(to, method string) error
}

func (t *filteringTransport) set(fn func(to, method string) error) {
	t.mu.Lock()
	t.refuse = fn
	t.mu.Unlock()
}

func (t *filteringTransport) Dial(node core.Node) (core.PeerClient, error) {
	inner, err := t.Transport.Dial(node)
	if err != nil {
		return nil, err
	}
	return &filteringClient{PeerClient: inner, to: node.ID, parent: t}, nil
}

// Ready forwards readiness; see countingTransport.Ready for why it must.
func (t *filteringTransport) Ready() <-chan struct{} {
	if r, ok := t.Transport.(interface{ Ready() <-chan struct{} }); ok {
		return r.Ready()
	}
	closed := make(chan struct{})
	close(closed)
	return closed
}

type filteringClient struct {
	core.PeerClient
	to     string
	parent *filteringTransport
}

func (c *filteringClient) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	c.parent.mu.Lock()
	refuse := c.parent.refuse
	c.parent.mu.Unlock()
	if refuse != nil {
		if err := refuse(c.to, method); err != nil {
			return nil, err
		}
	}
	return c.PeerClient.Invoke(ctx, method, req)
}

// replicaFabric is a counting fabric whose nodes all open "probe" and "hot"
// with replica reads.
type replicaFabric struct {
	*fabric
	rf      int
	filters map[string]*filteringTransport
	// writes counts Sets this fabric made, so that drainReplication knows
	// how many fan-outs to wait for.
	writes int
}

type replicaFabricConfig struct {
	nodes, rf int
	sync      bool
	// perNode overrides the replication factor on one node.
	perNode map[string]int
}

func newReplicaFabric(t *testing.T, cfg replicaFabricConfig) *replicaFabric {
	t.Helper()
	filters := map[string]*filteringTransport{}
	var mu sync.Mutex
	f := newFabricFull(t, cfg.nodes, true, testkit.MembershipConfig{}, -1,
		withWrappers(func(id string, tr core.Transport) core.Transport {
			ft := &filteringTransport{Transport: tr}
			mu.Lock()
			filters[id] = ft
			mu.Unlock()
			return ft
		}, nil))
	for _, id := range f.ids {
		rf := cfg.rf
		if v, ok := cfg.perNode[id]; ok {
			rf = v
		}
		opts := []peercache.NamespaceOption{
			peercache.WithReplicationFactor(rf),
			peercache.WithReplicaReads(),
			peercache.WithL1TTL(replicaTTL),
		}
		if cfg.sync {
			opts = append(opts, peercache.WithSyncReplication())
		}
		for _, name := range []string{"probe", "hot"} {
			if _, err := f.caches[id].Namespace(name, opts...).Exists(context.Background(), "x"); err != nil &&
				!errors.Is(err, peercache.ErrNotFound) {
				t.Fatalf("opening %s on %s: %v", name, id, err)
			}
		}
	}
	return &replicaFabric{fabric: f, rf: cfg.rf, filters: filters}
}

// holders returns key's owner and replicas, best first, as every node ranks
// them with the full membership.
func (r *replicaFabric) holders(t *testing.T, key string) []string {
	t.Helper()
	table := ownership.NewTable()
	nodes := make([]core.Node, 0, len(r.ids))
	for _, id := range r.ids {
		nodes = append(nodes, r.nodes[id])
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	table.Update(nodes, 1)
	ranked, _, err := table.Replicas(key, r.rf)
	if err != nil {
		t.Fatalf("Replicas(%q): %v", key, err)
	}
	out := make([]string, len(ranked))
	for i, n := range ranked {
		out[i] = n.ID
	}
	if out[0] != r.ownerOf(t, key) {
		t.Fatalf("the test's ranking of %q puts %s first, the cluster says %s", key, out[0], r.ownerOf(t, key))
	}
	return out
}

// readDelta is what one read cost, observed from outside.
type readDelta struct {
	val         []byte
	err         error
	ownerGets   int // read RPCs the reader sent to an owner
	replicaGets int // ReplicaGet RPCs the reader sent
	served      map[string]int64
	referred    map[string]int64
	floored     int64
}

func (r *replicaFabric) read(ns, reader, key string) readDelta {
	before := map[string]peercache.Stats{}
	for _, id := range r.ids {
		before[id] = r.caches[id].Stats()
	}
	gets := r.counters[reader].get(transport.MethodGet)
	rgets := r.counters[reader].get(transport.MethodReplicaGet)

	val, err := r.caches[reader].Namespace(ns).Get(context.Background(), key)

	d := readDelta{
		val:         val,
		err:         err,
		ownerGets:   r.counters[reader].get(transport.MethodGet) - gets,
		replicaGets: r.counters[reader].get(transport.MethodReplicaGet) - rgets,
		served:      map[string]int64{},
		referred:    map[string]int64{},
	}
	for _, id := range r.ids {
		s := r.caches[id].Stats()
		d.served[id] = s.ReplicaReadsServed - before[id].ReplicaReadsServed
		d.referred[id] = s.ReplicaReadsReferred - before[id].ReplicaReadsReferred
		if id == reader {
			d.floored = s.ReplicaReadsFloored - before[id].ReplicaReadsFloored
		}
	}
	return d
}

func (r *replicaFabric) set(t *testing.T, ns, key, val string) {
	t.Helper()
	owner := r.ownerOf(t, key)
	if err := r.caches[owner].Namespace(ns).Set(context.Background(), key, []byte(val), 0); err != nil {
		t.Fatalf("Set(%s, %q) on %s: %v", ns, key, owner, err)
	}
	r.writes++
}

// drainReplication waits until every write this fabric made has been fanned
// out to each of its replicas, successfully or not.
//
// Waiting for one sender's count to rise is not enough under asynchronous
// replication: a probe write made earlier can land first and satisfy it, which
// is exactly how the floor test once primed its reader from an empty replica.
func (r *replicaFabric) drainReplication(t *testing.T) {
	t.Helper()
	want := int64(r.writes * (r.rf - 1))
	waitFor(t, "every queued replication to be sent", func() bool {
		var done int64
		for _, id := range r.ids {
			s := r.caches[id].Stats()
			done += s.ReplicationsSent + s.ReplicationsFailed + s.ReplicationsDropped
		}
		return done >= want
	})
}

// setDegraded writes key on its owner while a test is refusing replication to
// some replica. A synchronous write then reports the refusal, having applied
// the value on the owner; that error, and only that one, is expected.
func (r *replicaFabric) setDegraded(t *testing.T, ns, key, val string) {
	t.Helper()
	owner := r.ownerOf(t, key)
	err := r.caches[owner].Namespace(ns).Set(context.Background(), key, []byte(val), 0)
	if err != nil && !errors.Is(err, core.ErrNodeUnreachable) {
		t.Fatalf("Set(%s, %q) on %s: %v", ns, key, owner, err)
	}
}

// pick is a key, a node reading it, and the replica that node reads it from.
type pick struct {
	key, reader, replica, owner string
	holders                     []string
}

// findPick searches keys for one where some node, not the owner, reads from a
// replica satisfying want. It reads in "probe" only.
//
// A read is attributed to a replica by that replica's own counters, so this
// finds what the code does rather than what the test expects it to do.
func (r *replicaFabric) findPick(t *testing.T, want func(p pick) bool) pick {
	t.Helper()
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("k%d", i)
		holders := r.holders(t, key)
		r.set(t, "probe", key, "probe")
		for _, reader := range r.ids {
			if reader == holders[0] {
				continue
			}
			d := r.read("probe", reader, key)
			if d.err != nil {
				t.Fatalf("probe read of %q from %s: %v", key, reader, d.err)
			}
			for _, h := range holders[1:] {
				if d.served[h]+d.referred[h] == 0 {
					continue
				}
				p := pick{key: key, reader: reader, replica: h, owner: holders[0], holders: holders}
				if want(p) {
					return p
				}
			}
		}
	}
	t.Fatal("no key found with the required reader and replica")
	return pick{}
}

func remoteReader(p pick) bool { return p.reader != p.replica }

// TestReplicaReadsSpreadAcrossHolders: reads of keys from every node divide
// among the keys' holders instead of all landing on the owners.
func TestReplicaReadsSpreadAcrossHolders(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 3, sync: true})

	const keys = 50
	var ownerGets, replicaServed, remoteReads int
	servedBy := map[string]int64{}
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("spread/%d", i)
		r.set(t, "hot", key, key)
		owner := r.ownerOf(t, key)
		for _, reader := range r.ids {
			if reader == owner {
				continue
			}
			remoteReads++
			d := r.read("hot", reader, key)
			if d.err != nil || string(d.val) != key {
				t.Fatalf("read of %q from %s = %q, %v", key, reader, d.val, d.err)
			}
			ownerGets += d.ownerGets
			for id, n := range d.served {
				replicaServed += int(n)
				servedBy[id] += n
			}
		}
	}
	t.Logf("%d remote reads: %d to owners, %d served by replicas %v",
		remoteReads, ownerGets, replicaServed, servedBy)
	if replicaServed == 0 {
		t.Fatal("no read was served by a replica")
	}
	// Three holders per key, so about a third of the reads should reach
	// an owner. Half is a loose ceiling that still fails if the replicas
	// are bypassed.
	if ownerGets*2 >= remoteReads {
		t.Fatalf("%d of %d remote reads went to an owner; replica reads are not spreading the load",
			ownerGets, remoteReads)
	}
	for _, id := range r.ids {
		if servedBy[id] == 0 {
			t.Errorf("%s served no replica read across %d keys", id, keys)
		}
	}
}

// TestAReplicaCopyOlderThanTheTTLGoesToTheOwner: a replica declines a copy it
// received an L1 TTL ago or more, and the read goes to the owner.
func TestAReplicaCopyOlderThanTheTTLGoesToTheOwner(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 3, sync: true})
	p := r.findPick(t, remoteReader)

	r.set(t, "hot", p.key, "v")
	r.clk.Advance(replicaTTL)

	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v" {
		t.Fatalf("read = %q, %v", d.val, d.err)
	}
	if d.served[p.replica] != 0 || d.referred[p.replica] != 1 || d.ownerGets != 1 {
		t.Fatalf("replica %s served %d, referred %d; owner reads %d. A copy one TTL old must be "+
			"declined and the read sent to the owner", p.replica, d.served[p.replica],
			d.referred[p.replica], d.ownerGets)
	}
}

// TestANodeThatNoLongerRanksRefersTheRead: a node holding a copy it no longer
// ranks for, under its own view, declines rather than answering from it.
//
// The node is made to rank out by opening the namespace on it with a lower
// replication factor than its peers: the owner still sends it the copy, and
// its own view says it is not a replica. That is the check membership churn
// relies on, exercised without the churn.
func TestANodeThatNoLongerRanksRefersTheRead(t *testing.T) {
	// Find the triple on an ordinary fabric, then rebuild with the
	// replica ranked out; the ranking depends only on the node IDs.
	probe := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 3, sync: true})
	p := probe.findPick(t, func(p pick) bool { return remoteReader(p) && p.replica == p.holders[2] })

	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 3, sync: true,
		perNode: map[string]int{p.replica: 2}})
	r.set(t, "hot", p.key, "v")
	if r.caches[p.replica].Stats().ReplicaKeys == 0 {
		t.Fatalf("%s holds no copy; the test would prove nothing", p.replica)
	}

	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v" {
		t.Fatalf("read = %q, %v", d.val, d.err)
	}
	if d.replicaGets != 1 || d.served[p.replica] != 0 || d.ownerGets != 1 {
		t.Fatalf("replica gets %d, %s served %d, owner reads %d; a node that does not rank as a "+
			"replica must refer the read", d.replicaGets, p.replica, d.served[p.replica], d.ownerGets)
	}
}

// TestAReplicaWithoutTheKeyIsNotAMiss: a replica that never received a key
// does not make the read report it absent; the owner answers.
func TestAReplicaWithoutTheKeyIsNotAMiss(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 2, sync: true})
	p := r.findPick(t, remoteReader)

	r.filters[p.owner].set(func(to, method string) error {
		if to == p.replica && method == transport.MethodReplicate {
			return fmt.Errorf("test refuses replication: %w", core.ErrNodeUnreachable)
		}
		return nil
	})
	r.setDegraded(t, "hot", p.key, "v")

	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v" {
		t.Fatalf("read = %q, %v; a replica lacking the key must not turn the read into a miss",
			d.val, d.err)
	}
	if d.replicaGets != 1 || d.ownerGets != 1 {
		t.Fatalf("replica gets %d, owner reads %d; want the replica asked, then the owner",
			d.replicaGets, d.ownerGets)
	}
}

// TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded: after an
// invalidation for stamp S, a replica's answer older than S is not used.
func TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded(t *testing.T) {
	// Asynchronous, because a synchronous write that cannot reach a replica
	// returns before it enqueues the invalidation (docs/decisions/open.md),
	// and the invalidation is what this test is about.
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 2})
	p := r.findPick(t, remoteReader)

	r.set(t, "hot", p.key, "v1")
	r.drainReplication(t)
	if d := r.read("hot", p.reader, p.key); d.err != nil || string(d.val) != "v1" || d.served[p.replica] != 1 {
		t.Fatalf("priming read = %q, %v, served by replica %d", d.val, d.err, d.served[p.replica])
	}

	// The replica misses the next write; the reader still hears of it.
	r.filters[p.owner].set(func(to, method string) error {
		if to == p.replica && (method == transport.MethodReplicate || method == transport.MethodInvalidate) {
			return fmt.Errorf("test refuses: %w", core.ErrNodeUnreachable)
		}
		return nil
	})
	sent := r.caches[p.owner].Stats().InvalidationsSent
	r.set(t, "hot", p.key, "v2")
	peers := int64(len(r.ids) - 2) // everyone but the owner and the cut-off replica
	waitFor(t, "the invalidation to reach the reader", func() bool {
		return r.caches[p.owner].Stats().InvalidationsSent >= sent+peers
	})

	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v2" {
		t.Fatalf("read after the invalidation = %q, %v; the replica's older copy was used", d.val, d.err)
	}
	if d.floored != 1 {
		t.Fatalf("floored %d, want 1", d.floored)
	}
}

// TestASelfReplicaServesWithoutAnRPC: a node that is itself a replica for a
// key, and is assigned to itself, reads its own copy without a round trip.
func TestASelfReplicaServesWithoutAnRPC(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 3, sync: true})
	p := r.findPick(t, func(p pick) bool { return p.reader == p.replica })

	r.set(t, "hot", p.key, "v")
	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v" {
		t.Fatalf("read = %q, %v", d.val, d.err)
	}
	if d.ownerGets != 0 || d.replicaGets != 0 || d.served[p.reader] != 1 {
		t.Fatalf("owner reads %d, replica gets %d, served locally %d; want 0, 0, 1",
			d.ownerGets, d.replicaGets, d.served[p.reader])
	}
}

// TestADroppedReplicationIsReadAfterOneTTL: a write whose replication never
// arrives, with every invalidation lost too, is read with its new value on
// every node once one L1 TTL has passed. Nothing but the TTL bounds it.
func TestADroppedReplicationIsReadAfterOneTTL(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 2, sync: true})
	p := r.findPick(t, remoteReader)

	r.set(t, "hot", p.key, "v1")
	for _, id := range r.ids {
		if d := r.read("hot", id, p.key); d.err != nil || string(d.val) != "v1" {
			t.Fatalf("priming read from %s = %q, %v", id, d.val, d.err)
		}
	}

	r.filters[p.owner].set(func(to, method string) error {
		if method == transport.MethodInvalidate || (to == p.replica && method == transport.MethodReplicate) {
			return fmt.Errorf("test refuses: %w", core.ErrNodeUnreachable)
		}
		return nil
	})
	r.setDegraded(t, "hot", p.key, "v2")
	r.clk.Advance(replicaTTL)

	for _, id := range r.ids {
		d := r.read("hot", id, p.key)
		if d.err != nil || string(d.val) != "v2" {
			t.Fatalf("read from %s one TTL after the write = %q, %v; staleness outlived its bound",
				id, d.val, d.err)
		}
	}
}

// TestAReplicaCopyIsCachedForWhatIsLeftOfItsTTL: a reader caches a replica's
// copy until one TTL after the replica received it, not one TTL from the read.
func TestAReplicaCopyIsCachedForWhatIsLeftOfItsTTL(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 2, sync: true})
	p := r.findPick(t, remoteReader)

	r.set(t, "hot", p.key, "v1")
	r.clk.Advance(replicaTTL / 2)
	if d := r.read("hot", p.reader, p.key); d.err != nil || string(d.val) != "v1" || d.served[p.replica] != 1 {
		t.Fatalf("read at half the TTL = %q, %v, served by replica %d", d.val, d.err, d.served[p.replica])
	}

	r.filters[p.owner].set(func(to, method string) error {
		if method == transport.MethodInvalidate || (to == p.replica && method == transport.MethodReplicate) {
			return fmt.Errorf("test refuses: %w", core.ErrNodeUnreachable)
		}
		return nil
	})
	r.setDegraded(t, "hot", p.key, "v2")
	r.clk.Advance(replicaTTL / 2)

	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v2" {
		t.Fatalf("read one TTL after the replica received v1 = %q, %v; the reader kept the copy "+
			"for a full TTL from its own read", d.val, d.err)
	}
}

// TestAPeerWithoutReplicaGetFallsBackToTheOwner: a replica answering
// ErrNotImplemented, as a peer on an older build does, is treated as a
// decline and the owner answers.
func TestAPeerWithoutReplicaGetFallsBackToTheOwner(t *testing.T) {
	r := newReplicaFabric(t, replicaFabricConfig{nodes: 5, rf: 2, sync: true})
	p := r.findPick(t, remoteReader)

	r.filters[p.reader].set(func(to, method string) error {
		if method == transport.MethodReplicaGet {
			return fmt.Errorf("older build: %w", peercache.ErrNotImplemented)
		}
		return nil
	})
	r.set(t, "hot", p.key, "v")
	d := r.read("hot", p.reader, p.key)
	if d.err != nil || string(d.val) != "v" {
		t.Fatalf("read = %q, %v; a peer without the method must not fail the read", d.val, d.err)
	}
	if d.ownerGets != 1 {
		t.Fatalf("owner reads %d, want 1", d.ownerGets)
	}
}

// TestReplicaReadsNeedReplicasAndAnL1 covers the two configurations replica
// reads cannot honour.
func TestReplicaReadsNeedReplicasAndAnL1(t *testing.T) {
	c, _ := newCache(t)
	cases := map[string][]peercache.NamespaceOption{
		"replication factor 1": {peercache.WithReplicaReads()},
		"L1 disabled": {peercache.WithReplicaReads(), peercache.WithReplicationFactor(2),
			peercache.WithL1TTL(-1)},
	}
	for name, opts := range cases {
		t.Run(name, func(t *testing.T) {
			ns := c.Namespace("bad-replica-reads-"+name, opts...)
			if _, err := ns.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrInvalidOption) {
				t.Fatalf("Get = %v, want ErrInvalidOption", err)
			}
		})
	}
	ok := c.Namespace("good-replica-reads", peercache.WithReplicaReads(), peercache.WithReplicationFactor(2))
	if _, err := ok.Get(context.Background(), "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("Get on a valid replica-read namespace = %v, want ErrNotFound", err)
	}
}
