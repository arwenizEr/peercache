// Command m5 demonstrates replication and failure handling, completed by
// milestone M5.
//
// M4 left a cluster where every key had exactly one home: losing a pod lost its
// data. This one keeps copies, promotes them when an owner dies, and shows the
// two things an operator most needs to know before running it.
//
// It shows five things:
//
//   - Writes are copied to the runners-up in the same ranking that chose the
//     owner, so promotion needs no election.
//   - Killing an owner ungracefully promotes a replica and service continues.
//   - Asynchronous replication — the default — has a window in which an
//     acknowledged write exists on one node only, and killing that node inside
//     it loses the write.
//   - Synchronous replication closes that window, at a cost.
//   - A restarted node must call CatchUp before serving, or it will report keys
//     as missing when they are not. This is the sharpest edge in the system.
//
// It runs on the in-process fabric with a fake clock, so failures happen at
// exactly reproducible instants and sixty seconds of chaos costs milliseconds.
//
// Run it with:
//
//	go run ./examples/m5
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/testkit"
)

// Detection must be faster than the failure rate, or a cluster never converges
// between failures. That is a deployment constraint, not something the library
// can paper over, so it is stated here as configuration.
var detection = testkit.MembershipConfig{
	FailureTimeout: 300 * time.Millisecond,
	JoinDelay:      200 * time.Millisecond,
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// cluster is a set of caches sharing one simulated network and clock.
type cluster struct {
	clk    *clock.Fake
	net    *testkit.Network
	reg    *testkit.Registry
	ids    []string
	nodes  map[string]core.Node
	caches map[string]peercache.Cache
	dead   map[string]bool
}

func run() error {
	c, err := newCluster(5)
	if err != nil {
		return err
	}
	defer c.close()

	for _, step := range []func(*cluster) error{
		replicasFollowTheRanking,
		killingAnOwnerPromotesAReplica,
		theAsyncWindowIsReal,
		syncClosesTheWindow,
		aRestartedNodeMustCatchUp,
	} {
		if err := step(c); err != nil {
			return err
		}
	}

	fmt.Println("\nReplication survives the loss of RF-1 nodes and no more.")
	fmt.Println("M6 adds planned rebalancing and a graceful drain on shutdown, so a")
	fmt.Println("deliberate restart stops looking like a failure.")
	return nil
}

func newCluster(n int) (*cluster, error) {
	clk := clock.NewFake(clock.Epoch)
	c := &cluster{
		clk:    clk,
		net:    testkit.NewNetwork(clk, 1),
		reg:    testkit.NewRegistry(),
		nodes:  make(map[string]core.Node, n),
		caches: make(map[string]peercache.Cache, n),
		dead:   make(map[string]bool, n),
	}

	for i := 0; i < n; i++ {
		id := fmt.Sprintf("pod-%d", i)
		c.ids = append(c.ids, id)
		c.nodes[id] = core.Node{ID: id, Addr: id + ":7946"}
		if err := c.start(id); err != nil {
			return nil, err
		}
	}
	c.clk.Advance(2 * detection.JoinDelay)
	if err := c.converge(n); err != nil {
		return nil, err
	}

	fmt.Printf("cluster of %d formed\n", n)
	return c, nil
}

// start brings one node up, whether for the first time or after a kill.
func (c *cluster) start(id string) error {
	m := testkit.NewMembership(c.nodes[id], c.net, c.clk, c.reg, detection)
	cache, err := peercache.New(
		peercache.WithNodeID(id),
		peercache.WithClock(c.clk),
		peercache.WithMembership(m),
		peercache.WithTransport(c.net.Transport(c.nodes[id])),
		peercache.WithActivationDelay(-1),
	)
	if err != nil {
		return fmt.Errorf("starting %s: %w", id, err)
	}
	c.caches[id] = cache
	c.dead[id] = false
	return nil
}

// kill removes a node the way an ungraceful termination does: no announcement,
// no handover, and its memory goes with it.
func (c *cluster) kill(id string) error {
	if c.dead[id] {
		return nil
	}
	c.dead[id] = true
	if err := c.caches[id].Close(context.Background()); err != nil {
		return fmt.Errorf("killing %s: %w", id, err)
	}
	c.net.Unregister(id)
	c.clk.Advance(2 * detection.FailureTimeout)
	return c.converge(len(c.live()))
}

// revive restarts a killed node with empty memory, the way a rescheduled pod
// comes back.
func (c *cluster) revive(id string) error {
	if err := c.start(id); err != nil {
		return err
	}
	c.clk.Advance(2 * detection.JoinDelay)
	return c.converge(len(c.live()))
}

func (c *cluster) live() []string {
	out := make([]string, 0, len(c.ids))
	for _, id := range c.ids {
		if !c.dead[id] {
			out = append(out, id)
		}
	}
	return out
}

// converge waits for every live node to see the same cluster size and to have
// acted on it. Observing a change and reconciling it are different moments.
func (c *cluster) converge(want int) error {
	const maxYields = 10_000_000
	for i := 0; i < maxYields; i++ {
		agreed := true
		for _, id := range c.live() {
			if c.caches[id].Stats().Members != want {
				agreed = false
				break
			}
		}
		if agreed {
			return c.settle()
		}
		runtime.Gosched()
	}
	return fmt.Errorf("cluster did not converge to %d members", want)
}

// settle waits for reconciliation to quiesce on every live node.
func (c *cluster) settle() error {
	const maxYields = 5_000_000
	seen := make(map[string]int64, len(c.ids))
	for _, id := range c.live() {
		seen[id] = c.caches[id].Stats().PromotionRounds
	}
	quiet := 0
	for i := 0; i < maxYields; i++ {
		moved := false
		for _, id := range c.live() {
			if n := c.caches[id].Stats().PromotionRounds; n != seen[id] {
				seen[id], moved = n, true
			}
		}
		if moved {
			quiet = 0
		} else if quiet++; quiet >= 2000 {
			return nil
		}
		runtime.Gosched()
	}
	return errors.New("reconciliation never quiesced")
}

func (c *cluster) close() {
	for _, id := range c.live() {
		_ = c.caches[id].Close(context.Background())
	}
	_ = c.net.Close()
}

// ownerOf finds which pod owns a key, by asking each one to do something only
// its owner can do.
func (c *cluster) ownerOf(key string) (string, error) {
	for _, id := range c.live() {
		_, err := c.caches[id].Group(key).Keys(context.Background())
		switch {
		case err == nil:
			return id, nil
		case errors.Is(err, peercache.ErrNotOwner):
		default:
			return "", fmt.Errorf("probing %s: %w", id, err)
		}
	}
	return "", fmt.Errorf("no live pod owns %q", key)
}

func replicasFollowTheRanking(c *cluster) error {
	fmt.Println("\n--- copies go to the runners-up in the ownership ranking ---")

	const rf = 3
	for _, id := range c.live() {
		c.caches[id].Namespace("orders",
			peercache.WithReplicationFactor(rf),
			peercache.WithSyncReplication())
	}

	const key = "orders/42"
	owner, err := c.ownerOf(key)
	if err != nil {
		return err
	}
	if err := c.caches[owner].Namespace("orders").Set(
		context.Background(), key, []byte("one large coffee"), 0); err != nil {
		return fmt.Errorf("set: %w", err)
	}

	fmt.Printf("  %q is owned by %s\n", key, owner)
	holders := 0
	for _, id := range c.live() {
		if id != owner && c.caches[id].Stats().ReplicaKeys > 0 {
			fmt.Printf("  %s holds a copy\n", id)
			holders++
		}
	}
	fmt.Printf("  %d copies at RF=%d, placed with no election and no messages about placement\n",
		holders, rf)
	if holders != rf-1 {
		return fmt.Errorf("expected %d copies, found %d", rf-1, holders)
	}
	return nil
}

func killingAnOwnerPromotesAReplica(c *cluster) error {
	fmt.Println("\n--- killing an owner promotes a replica ---")

	const key = "orders/42"
	owner, err := c.ownerOf(key)
	if err != nil {
		return err
	}
	fmt.Printf("  killing %s, which owns %q\n", owner, key)
	if err := c.kill(owner); err != nil {
		return err
	}

	reader := c.live()[0]
	got, err := c.caches[reader].Namespace("orders").Get(context.Background(), key)
	if err != nil {
		return fmt.Errorf("the key was lost: %w", err)
	}
	newOwner, err := c.ownerOf(key)
	if err != nil {
		return err
	}
	fmt.Printf("  %s now owns it and still reads %q\n", newOwner, got)

	promoted := int64(0)
	for _, id := range c.live() {
		promoted += c.caches[id].Stats().KeysPromoted
	}
	fmt.Printf("  %d keys promoted across the survivors, with no handover protocol\n", promoted)

	return c.revive(owner)
}

func theAsyncWindowIsReal(c *cluster) error {
	fmt.Println("\n--- the asynchronous window, and what it costs ---")

	// Asynchronous is the default: no WithSyncReplication here.
	for _, id := range c.live() {
		c.caches[id].Namespace("async", peercache.WithReplicationFactor(3))
	}

	writer := c.live()[0]
	var key string
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("async/%d", i)
		if owner, err := c.ownerOf(k); err == nil && owner == writer {
			key = k
			break
		}
	}
	if key == "" {
		return errors.New("no key resolves to the writer")
	}

	// Silence the writer's outbound traffic so its fan-out cannot land.
	for _, id := range c.live() {
		if id != writer {
			c.net.SetDropRate(writer, id, 1.0)
		}
	}

	if err := c.caches[writer].Namespace("async").Set(
		context.Background(), key, []byte("acknowledged, not yet safe"), 0); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	fmt.Printf("  %s acknowledged a write with every replica unreachable\n", writer)
	fmt.Println("  the value now exists on exactly one node; this is the window")

	if err := c.kill(writer); err != nil {
		return err
	}
	_, err := c.caches[c.live()[0]].Namespace("async").Get(context.Background(), key)
	if err == nil {
		return errors.New("the write survived; the window does not exist")
	}
	fmt.Printf("  killing it inside the window lost the write: %v\n", err)
	fmt.Println("  measured at roughly 0.1 ms on a real network — see docs/benchmarks.md")

	for _, id := range c.ids {
		c.net.SetDropRate(writer, id, -1)
	}
	return c.revive(writer)
}

func syncClosesTheWindow(c *cluster) error {
	fmt.Println("\n--- synchronous replication closes it ---")

	for _, id := range c.live() {
		c.caches[id].Namespace("safe",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const key = "safe/1"
	owner, err := c.ownerOf(key)
	if err != nil {
		return err
	}
	before := replicaTotal(c)
	if err := c.caches[owner].Namespace("safe").Set(
		context.Background(), key, []byte("safe on return"), 0); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	after := replicaTotal(c)

	fmt.Printf("  Set returned with %d copies already placed (was %d)\n", after-before, before)
	fmt.Println("  no window at all — and the group's write rate is now its slowest replica's")
	if after <= before {
		return errors.New("a synchronous write returned before its copies existed")
	}
	return nil
}

func aRestartedNodeMustCatchUp(c *cluster) error {
	fmt.Println("\n--- a restarted node must catch up before it serves ---")

	for _, id := range c.live() {
		c.caches[id].Namespace("cold",
			peercache.WithReplicationFactor(3),
			peercache.WithSyncReplication())
	}

	const keys = 200
	writer := c.live()[0]
	for i := 0; i < keys; i++ {
		if err := c.caches[writer].Namespace("cold").Set(
			context.Background(), fmt.Sprintf("cold/%d", i), []byte("v"), 0); err != nil {
			return fmt.Errorf("set: %w", err)
		}
	}

	victim := c.live()[1]
	fmt.Printf("  killing %s and bringing it back with empty memory\n", victim)
	if err := c.kill(victim); err != nil {
		return err
	}
	if err := c.revive(victim); err != nil {
		return err
	}
	c.caches[victim].Namespace("cold",
		peercache.WithReplicationFactor(3),
		peercache.WithSyncReplication())

	// Before catching up: it owns keys it does not have.
	missing := 0
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("cold/%d", i)
		if _, err := c.caches[victim].Namespace("cold").Get(context.Background(), k); err != nil {
			missing++
		}
	}
	fmt.Printf("  before CatchUp: %d of %d keys report as missing\n", missing, keys)
	fmt.Println("  none of them are actually gone — they are on the pods that covered for it")

	if err := c.caches[victim].CatchUp(context.Background()); err != nil {
		return fmt.Errorf("catch-up: %w", err)
	}
	if err := c.settle(); err != nil {
		return err
	}

	stillMissing := 0
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("cold/%d", i)
		if _, err := c.caches[victim].Namespace("cold").Get(context.Background(), k); err != nil {
			stillMissing++
		}
	}
	fmt.Printf("  after CatchUp: %d of %d keys missing\n", stillMissing, keys)
	if stillMissing != 0 {
		return fmt.Errorf("%d keys still missing after catch-up", stillMissing)
	}
	fmt.Println("  this is why CatchUp is part of the restart procedure, not an optimisation")
	return nil
}

func replicaTotal(c *cluster) int64 {
	var total int64
	for _, id := range c.live() {
		total += c.caches[id].Stats().ReplicaKeys
	}
	return total
}
