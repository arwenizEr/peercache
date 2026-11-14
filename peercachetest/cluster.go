package peercachetest

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/testkit"
)

// Clock is the cluster's fake clock. Nothing in a [Cluster] moves until a test
// advances it: TTLs, failure detection, loader timeouts and every other timer
// in the library wait on it.
type Clock = clock.Fake

// Epoch is the instant every cluster's clock starts at.
var Epoch = clock.Epoch

// Option configures a [Cluster].
type Option func(*config)

type config struct {
	all     []peercache.Option
	perNode map[int][]peercache.Option
}

// WithCacheOptions applies opts to every node's cache, restarted ones
// included — the options an application passes to peercache.New in
// production. The harness supplies the node ID, clock, membership, transport
// and activation delay itself.
func WithCacheOptions(opts ...peercache.Option) Option {
	return func(c *config) { c.all = append(c.all, opts...) }
}

// WithNodeOptions applies opts to node i only, restarts included.
func WithNodeOptions(i int, opts ...peercache.Option) Option {
	return func(c *config) {
		if c.perNode == nil {
			c.perNode = map[int][]peercache.Option{}
		}
		c.perNode[i] = append(c.perNode[i], opts...)
	}
}

// Cluster is n caches in one process, connected by a simulated network and a
// simulated membership, all on one fake clock. Nodes are numbered 0 to n-1.
type Cluster struct {
	tb      testing.TB
	cfg     config
	clk     *clock.Fake
	net     *testkit.Network
	reg     *testkit.Registry
	mcfg    testkit.MembershipConfig
	infos   []core.Node
	members []*testkit.Membership
	caches  []peercache.Cache
	dead    []bool
	groups  [][]int // the current partition, nil when healed
}

// New starts a cluster of n nodes and waits until every node sees all of them.
// It is closed when the test ends.
func New(tb testing.TB, n int, opts ...Option) *Cluster {
	tb.Helper()
	if n < 1 {
		tb.Fatalf("peercachetest: a cluster needs at least one node, got %d", n)
	}
	c := &Cluster{tb: tb}
	for _, o := range opts {
		o(&c.cfg)
	}
	c.clk = clock.NewFake(clock.Epoch)
	c.clk.SetMisuseHandler(func(msg string) { tb.Errorf("peercachetest: clock misuse: %s", msg) })
	c.net = testkit.NewNetwork(c.clk, testkit.Seed(tb))
	c.reg = testkit.NewRegistry()
	c.mcfg = testkit.MembershipConfig{
		FailureTimeout: testkit.DefaultFailureTimeout,
		JoinDelay:      testkit.DefaultJoinDelay,
	}
	c.infos = make([]core.Node, n)
	c.members = make([]*testkit.Membership, n)
	c.caches = make([]peercache.Cache, n)
	c.dead = make([]bool, n)
	for i := range c.infos {
		id := fmt.Sprintf("node-%d", i)
		c.infos[i] = core.Node{ID: id, Addr: id + ":7946"}
		c.members[i] = testkit.NewMembership(c.infos[i], c.net, c.clk, c.reg, c.mcfg)
	}
	for i := range c.infos {
		c.caches[i] = c.start(i)
	}
	tb.Cleanup(c.close)
	// Converge only once every cache is serving: a node joins the
	// simulated network when its handler registers, inside peercache.New.
	c.clk.Advance(2 * c.mcfg.JoinDelay)
	c.Settle()
	return c
}

func (c *Cluster) start(i int) peercache.Cache {
	c.tb.Helper()
	base := []peercache.Option{
		peercache.WithNodeID(c.infos[i].ID),
		peercache.WithClock(c.clk),
		peercache.WithMembership(c.members[i]),
		peercache.WithTransport(c.net.Transport(c.infos[i])),
		// Ownership moves the moment membership does. The deferral
		// exists for real gossip, whose views disagree for a while; the
		// simulated one agrees as soon as the clock delivers it.
		peercache.WithActivationDelay(-1),
	}
	opts := append(append(base, c.cfg.all...), c.cfg.perNode[i]...)
	cache, err := peercache.New(opts...)
	if err != nil {
		c.tb.Fatalf("peercachetest: starting node %d: %v", i, err)
	}
	return cache
}

func (c *Cluster) close() {
	for i, cache := range c.caches {
		if c.dead[i] {
			continue
		}
		if err := cache.Close(context.Background()); err != nil {
			c.tb.Errorf("peercachetest: closing node %d: %v", i, err)
		}
	}
	c.net.Close()
}

// Size is the number of nodes, dead or alive.
func (c *Cluster) Size() int { return len(c.caches) }

// Cache returns node i's cache. It fails the test if node i is dead.
func (c *Cluster) Cache(i int) peercache.Cache {
	c.tb.Helper()
	c.check(i)
	if c.dead[i] {
		c.tb.Fatalf("peercachetest: node %d is dead; Restart it first", i)
	}
	return c.caches[i]
}

// ID returns node i's node ID, as Describe and DescribeKey report it.
func (c *Cluster) ID(i int) string {
	c.check(i)
	return c.infos[i].ID
}

// Clock returns the cluster's fake clock.
func (c *Cluster) Clock() *Clock { return c.clk }

// Advance moves the clock forward by d.
func (c *Cluster) Advance(d time.Duration) { c.clk.Advance(d) }

// Kill stops node i the way a crash does — no hand-off, no announcement, its
// memory lost — then advances the clock past failure detection and waits
// until the survivors have noticed and repaired what they can. What the
// application sees afterwards is what it would see after a pod was killed.
func (c *Cluster) Kill(i int) {
	c.tb.Helper()
	c.check(i)
	if c.dead[i] {
		return
	}
	c.dead[i] = true
	if err := c.caches[i].Close(context.Background()); err != nil {
		c.tb.Fatalf("peercachetest: killing node %d: %v", i, err)
	}
	c.members[i].Close()
	c.net.Unregister(c.infos[i].ID)
	c.clk.Advance(2 * c.mcfg.FailureTimeout)
	c.Settle()
}

// Restart brings dead node i back with empty memory and the same identity,
// as a restarted pod returns, and waits until every node sees it. It does not
// call CatchUp: an application decides when to, and a test of that decision
// is the point. Restarting a live node kills it first.
func (c *Cluster) Restart(i int) {
	c.tb.Helper()
	c.check(i)
	if !c.dead[i] {
		c.Kill(i)
	}
	c.members[i] = testkit.NewMembership(c.infos[i], c.net, c.clk, c.reg, c.mcfg)
	if err := c.members[i].Join(nil); err != nil {
		c.tb.Fatalf("peercachetest: node %d rejoining: %v", i, err)
	}
	c.caches[i] = c.start(i)
	c.dead[i] = false
	c.clk.Advance(2 * c.mcfg.JoinDelay)
	c.Settle()
}

// Partition splits the network into the given groups of node indexes: nodes
// in one group reach each other and no one else. Nodes in no group are cut off
// from everyone. It advances the clock past failure detection and waits until
// each side has declared the other dead — the moment two owners of one key are
// possible, which decision 5 of the design says is detected, not prevented.
func (c *Cluster) Partition(groups ...[]int) {
	c.tb.Helper()
	ids := make([][]string, len(groups))
	for g, group := range groups {
		for _, i := range group {
			c.check(i)
			ids[g] = append(ids[g], c.infos[i].ID)
		}
	}
	c.groups = groups
	c.net.Partition(ids...)
	c.clk.Advance(2 * c.mcfg.FailureTimeout)
	c.Settle()
}

// Heal removes the partition, advances the clock until the sides have
// rediscovered each other, and waits until every live node sees every other.
func (c *Cluster) Heal() {
	c.tb.Helper()
	c.groups = nil
	c.net.Heal()
	c.clk.Advance(2 * c.mcfg.JoinDelay)
	c.clk.Advance(2 * c.mcfg.FailureTimeout)
	c.Settle()
}

// Settle waits, without moving the clock, until every live node sees the
// membership it should — all live nodes, or its side of a partition — and
// replication has stopped reacting to the last change. Call it after
// advancing the clock across something the cluster has to act on.
func (c *Cluster) Settle() {
	c.tb.Helper()
	const maxYields = 10_000_000
	for i := 0; ; i++ {
		ok := true
		for n := range c.caches {
			if !c.dead[n] && c.caches[n].Stats().Members != c.expectedMembers(n) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		if i > maxYields {
			var seen []string
			for n := range c.caches {
				if !c.dead[n] {
					seen = append(seen, fmt.Sprintf("node-%d=%d/%d", n, c.caches[n].Stats().Members, c.expectedMembers(n)))
				}
			}
			c.tb.Fatalf("peercachetest: membership never settled: %v", seen)
		}
		runtime.Gosched()
	}
	c.quiesce()
}

// expectedMembers is how many members node n should see: every live node, or
// the live nodes of its side of the partition.
func (c *Cluster) expectedMembers(n int) int {
	count := 0
	if c.groups == nil {
		for i := range c.caches {
			if !c.dead[i] {
				count++
			}
		}
		return count
	}
	for _, g := range c.groups {
		in := false
		for _, i := range g {
			if i == n {
				in = true
			}
		}
		if !in {
			continue
		}
		for _, i := range g {
			if !c.dead[i] {
				count++
			}
		}
		return count
	}
	return 1 // in no group: alone
}

// quiesce waits until no live node has started a reconciliation round for a
// while. Seeing a membership change and acting on it are different moments.
func (c *Cluster) quiesce() {
	const maxYields, quietFor = 5_000_000, 2000
	seen := make([]int64, len(c.caches))
	for n := range c.caches {
		if !c.dead[n] {
			seen[n] = c.caches[n].Stats().PromotionRounds
		}
	}
	quiet := 0
	for i := 0; i < maxYields; i++ {
		moved := false
		for n := range c.caches {
			if c.dead[n] {
				continue
			}
			if r := c.caches[n].Stats().PromotionRounds; r != seen[n] {
				seen[n], moved = r, true
			}
		}
		if moved {
			quiet = 0
		} else if quiet++; quiet >= quietFor {
			return
		}
		runtime.Gosched()
	}
	c.tb.Fatal("peercachetest: reconciliation never quiesced")
}

// Owner returns the index of the node that owns key in namespace, as the first
// live node sees it. The namespace must be open on that node.
func (c *Cluster) Owner(namespace, key string) int {
	c.tb.Helper()
	for n := range c.caches {
		if c.dead[n] {
			continue
		}
		p, err := c.caches[n].DescribeKey(namespace, key)
		if err != nil {
			c.tb.Fatalf("peercachetest: DescribeKey(%q, %q) on node %d: %v", namespace, key, n, err)
		}
		for i, info := range c.infos {
			if info.ID == p.Owner {
				return i
			}
		}
		c.tb.Fatalf("peercachetest: %q is owned by %q, which is not in this cluster", key, p.Owner)
	}
	c.tb.Fatal("peercachetest: every node is dead")
	return -1
}

func (c *Cluster) check(i int) {
	c.tb.Helper()
	if i < 0 || i >= len(c.caches) {
		c.tb.Fatalf("peercachetest: node %d out of range 0..%d", i, len(c.caches)-1)
	}
}
