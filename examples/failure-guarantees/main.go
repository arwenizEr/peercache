// Command failure-guarantees demonstrates two things about what this library
// promises under failure.
//
// Both were found by verification rather than by features — a deterministic
// simulation, a linearizability checker, an exhaustive protocol fuzzer:
//
//  1. A partition heal used to lose data. A node that stopped ranking for a key
//     it held as a replica evicted its copy, so recomputing ownership across a
//     healed cluster turned a rebalance into a loss. Replicas are now handed to
//     the nodes that do rank for them, and the local copy is kept until the
//     hand-off lands.
//
//  2. A synchronous write can still be acknowledged below its replication
//     factor, and that is deliberate. This shows the counter that makes it
//     visible, because the alternative — refusing the write — trades an
//     available system for an unavailable one during a partition.
//
// Run it with:
//
//	go run ./examples/failure-guarantees
//
// Everything runs in one process against a simulated network and a fake clock,
// so it is deterministic and takes milliseconds. The numbers it prints are
// counts, which are real; it prints no latencies, which would not be.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
	"github.com/arwenizEr/peercache/internal/core"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
)

var detection = testkit.MembershipConfig{
	FailureTimeout: 300 * time.Millisecond,
	JoinDelay:      200 * time.Millisecond,
}

// replicationFactor is 3 throughout, because both demonstrations are about the
// difference between holding RF copies and holding fewer, and RF=2 makes "fewer"
// and "none but one" the same number.
const replicationFactor = 3

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	c, err := newCluster(5)
	if err != nil {
		return err
	}
	defer c.close()

	for _, step := range []func(*cluster) error{
		handOffOnAHeal,
		degradedWriteIsCounted,
	} {
		if err := step(c); err != nil {
			return err
		}
	}

	printClosing()
	return nil
}

type cluster struct {
	clk    *clock.Fake
	net    *testkit.Network
	reg    *testkit.Registry
	ids    []string
	nodes  map[string]core.Node
	caches map[string]peercache.Cache
}

func newCluster(n int) (*cluster, error) {
	clk := clock.NewFake(clock.Epoch)
	c := &cluster{
		clk:    clk,
		net:    testkit.NewNetwork(clk, 1),
		reg:    testkit.NewRegistry(),
		nodes:  make(map[string]core.Node, n),
		caches: make(map[string]peercache.Cache, n),
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("pod-%d", i)
		c.ids = append(c.ids, id)
		c.nodes[id] = core.Node{ID: id, Addr: id + ":7947"}
		if err := c.start(id); err != nil {
			return nil, err
		}
	}
	c.clk.Advance(2 * detection.JoinDelay)
	if err := c.convergeTo(n); err != nil {
		return nil, err
	}
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	fmt.Printf("cluster of %d formed, replication factor %d\n", n, replicationFactor)
	return c, nil
}

func (c *cluster) start(id string) error {
	m := testkit.NewMembership(c.nodes[id], c.net, c.clk, c.reg, detection)
	if err := m.Join(nil); err != nil {
		return err
	}
	cache, err := peercache.New(
		peercache.WithNodeID(id),
		peercache.WithClock(c.clk),
		peercache.WithMembership(m),
		peercache.WithTransport(c.net.Transport(c.nodes[id])),
	)
	if err != nil {
		return err
	}
	c.caches[id] = cache
	return nil
}

func (c *cluster) close() {
	for _, id := range c.ids {
		_ = c.caches[id].Close(context.Background())
	}
	_ = c.net.Close()
}

// openAll opens a namespace on every node.
//
// This is a correctness requirement rather than a convenience: a namespace a node has not opened is created on the first routed
// write with default options, which means replication factor 1.
func (c *cluster) openAll(name string, opts ...peercache.NamespaceOption) {
	for _, id := range c.ids {
		c.caches[id].Namespace(name, opts...)
	}
}

// convergeTo waits until every node agrees the cluster has want members.
func (c *cluster) convergeTo(want int) error {
	return c.waitFor(fmt.Sprintf("every node to see %d members", want), func() bool {
		for _, id := range c.ids {
			if c.caches[id].Stats().Members != want {
				return false
			}
		}
		return true
	})
}

// waitFor yields until cond holds. Nothing here sleeps: the clock is fake and
// the network is in-process, so the only thing to wait on is other goroutines
// making progress.
func (c *cluster) waitFor(what string, cond func() bool) error {
	const maxYields = 10_000_000
	for i := 0; i < maxYields; i++ {
		if cond() {
			return nil
		}
		runtime.Gosched()
	}
	return errors.New("timed out waiting for " + what)
}

// waitMembers waits until each of ids sees want members. During a partition the
// two sides disagree, so this takes the side it is asked about rather than the
// whole cluster.
func (c *cluster) waitMembers(ids []string, want int) error {
	return c.waitFor(fmt.Sprintf("%v to see %d members", ids, want), func() bool {
		for _, id := range ids {
			if c.caches[id].Stats().Members != want {
				return false
			}
		}
		return true
	})
}

// settle waits until reconciliation has stopped moving.
//
// A membership change does not finish when the membership counts agree. Each
// node then reconciles what it holds against what it should hold, and the
// activation guard defers activating a group until its ownership has been stable
// for the activation delay. Writing before that is answered with
// ErrActivationDeferred, which is the guard working rather than a failure --
// this example waits for quiet instead of retrying, so that what it prints is a
// settled cluster rather than a race.
func (c *cluster) settle() error {
	const quietFor = 2000
	seen := make(map[string]int64, len(c.ids))
	for _, id := range c.ids {
		seen[id] = c.caches[id].Stats().PromotionRounds
	}
	quiet := 0
	return c.waitFor("reconciliation to go quiet", func() bool {
		moved := false
		for _, id := range c.ids {
			if n := c.caches[id].Stats().PromotionRounds; n != seen[id] {
				seen[id] = n
				moved = true
			}
		}
		if moved {
			quiet = 0
			return false
		}
		quiet++
		return quiet >= quietFor
	})
}

// totals sums a counter across the whole cluster. Every figure this example
// prints is a delta of one of these, never a running total: the counters are
// cumulative across namespaces, and quoting a total beside a scenario's key
// count would attribute the other scenario's traffic to it.
func (c *cluster) totals() (rehomed, retained, degraded int64) {
	for _, id := range c.ids {
		s := c.caches[id].Stats()
		rehomed += s.ReplicasRehomed
		retained += s.ReplicasRetained
		degraded += s.DegradedWrites
	}
	return rehomed, retained, degraded
}

func handOffOnAHeal(c *cluster) error {
	fmt.Println("\n--- a partition heal moves replicas rather than dropping them ---")

	const namespace = "orders"
	const keys = 60
	ctx := context.Background()

	c.openAll(namespace,
		peercache.WithReplicationFactor(replicationFactor),
		peercache.WithSyncReplication())
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	if err := c.convergeTo(len(c.ids)); err != nil {
		return err
	}
	if err := c.settle(); err != nil {
		return err
	}

	isolated := c.ids[0]
	majority := append([]string(nil), c.ids[1:]...)

	c.net.Partition([]string{isolated}, majority)
	c.clk.Advance(4 * detection.FailureTimeout)
	if err := c.waitMembers(majority, len(majority)); err != nil {
		return err
	}
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	if err := c.settle(); err != nil {
		return err
	}
	fmt.Printf("  partitioned %s away from the other %d\n", isolated, len(majority))

	// Written on the majority side, which still has enough nodes to place
	// every copy the namespace asks for.
	writer := majority[0]
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("%s/%d", namespace, i)
		if err := c.caches[writer].Namespace(namespace).Set(ctx, key, []byte("v"), 0); err != nil {
			return fmt.Errorf("set %s: %w", key, err)
		}
	}
	fmt.Printf("  wrote %d keys on the majority side\n", keys)

	rehomedBefore, retainedBefore, _ := c.totals()

	// Healing is the interesting part. Every node recomputes ownership over
	// the full membership, and keys move.
	c.net.Heal()
	c.clk.Advance(4 * detection.FailureTimeout)
	if err := c.convergeTo(len(c.ids)); err != nil {
		return err
	}
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	if err := c.settle(); err != nil {
		return err
	}

	// CatchUp is what restores the replication factor after any membership
	// change, not only after a restart. Until it has run, a key can be
	// readable while fewer than RF nodes hold it.
	for _, id := range c.ids {
		if err := c.caches[id].CatchUp(ctx); err != nil {
			return fmt.Errorf("catch up %s: %w", id, err)
		}
	}

	rehomedAfter, retainedAfter, _ := c.totals()
	rehomed := rehomedAfter - rehomedBefore
	retained := retainedAfter - retainedBefore

	readable := 0
	for i := 0; i < keys; i++ {
		if _, err := c.caches[isolated].Namespace(namespace).
			Get(ctx, fmt.Sprintf("%s/%d", namespace, i)); err == nil {
			readable++
		}
	}

	fmt.Println("\n  healed, ownership recomputed, catch-up run:")
	fmt.Printf("    peercache_replicas_rehomed_total    %d\n", rehomed)
	fmt.Printf("    peercache_replicas_retained_total   %d\n", retained)
	fmt.Printf("    keys still readable                 %d of %d\n", readable, keys)

	if readable != keys {
		return fmt.Errorf("a heal lost %d of %d keys; this example asserts what it "+
			"shows, and the property it shows has regressed", keys-readable, keys)
	}
	// The readability check above is necessary and nowhere near sufficient,
	// which is worth stating because the first version of this example
	// stopped there and was vacuous.
	//
	// Deleting the hand-off entirely -- reverting to the eviction it
	// replaced -- still leaves all 60 keys readable here, because
	// CatchUp then pulls the copies back. The loss only becomes visible when
	// nodes are destroyed inside the settling window, which is what
	// TestMajoritySideWritesSurviveAHealAndTwoDestructions does.
	//
	// So the load-bearing assertion is that the replicas were *moved* rather
	// than dropped and re-fetched. With the hand-off removed this counter is
	// zero and the example fails, which was confirmed by removing it.
	if rehomed == 0 {
		return errors.New("no replica was handed off across the heal; keys are still " +
			"readable because catch-up refetched them, which is the old eviction " +
			"behaviour rather than the hand-off")
	}

	fmt.Println("\n  Without the hand-off the rehomed count is zero and those replicas are")
	fmt.Println("  evicted. TestMajoritySideWritesSurviveAHealAndTwoDestructions lost")
	fmt.Println("  20 keys of 94 with the old behaviour.")
	fmt.Println()
	fmt.Println("  Note which of those two numbers is the evidence. Delete the hand-off")
	fmt.Println("  and all 60 keys are still readable, because catch-up refetches them --")
	fmt.Println("  the loss needs nodes destroyed inside the settling window before it")
	fmt.Println("  shows. This example asserts the rehomed count for that reason; the")
	fmt.Println("  first version asserted readability alone and passed with the feature")
	fmt.Println("  removed.")
	fmt.Println()
	fmt.Println("  Retained is zero here, and that is the healthy case rather than a")
	fmt.Println("  counter that does not work: every hand-off found a node to take it.")
	fmt.Println("  It counts the other half of the fix -- a hand-off that cannot be")
	fmt.Println("  placed keeps its local copy instead of trading one loss for another,")
	fmt.Println("  which needs a cluster with nowhere to put the replica.")
	fmt.Println("  TestAHandOffThatCannotBePlacedKeepsTheCopy builds one.")
	return nil
}

func degradedWriteIsCounted(c *cluster) error {
	fmt.Println("\n--- a synchronous write acknowledged below its replication factor ---")

	const namespace = "ledger"
	ctx := context.Background()

	c.openAll(namespace,
		peercache.WithReplicationFactor(replicationFactor),
		peercache.WithSyncReplication())
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	if err := c.convergeTo(len(c.ids)); err != nil {
		return err
	}
	if err := c.settle(); err != nil {
		return err
	}

	isolated := c.ids[0]
	rest := append([]string(nil), c.ids[1:]...)

	c.net.Partition([]string{isolated}, rest)
	// Long enough for the isolated node to detect every peer as failed and
	// for the activation guard's delay to expire, after which it ranks itself
	// the owner of everything it can see -- which is everything.
	c.clk.Advance(4 * detection.FailureTimeout)
	if err := c.waitMembers([]string{isolated}, 1); err != nil {
		return err
	}
	c.clk.Advance(2 * ownership.DefaultActivationDelay)
	if err := c.settle(); err != nil {
		return err
	}

	_, _, degradedBefore := c.totals()

	const key = namespace + "/only-copy"
	err := c.caches[isolated].Namespace(namespace).Set(ctx, key, []byte("acknowledged"), 0)

	_, _, degradedAfter := c.totals()
	degraded := degradedAfter - degradedBefore

	fmt.Printf("  %s now sees %d member(s) and was asked for %d copies\n",
		isolated, c.caches[isolated].Stats().Members, replicationFactor)
	fmt.Printf("  Set returned %v\n", err)
	fmt.Printf("    peercache_degraded_writes_total     %d\n", degraded)

	c.net.Heal()
	c.clk.Advance(4 * detection.FailureTimeout)
	if err := c.convergeTo(len(c.ids)); err != nil {
		return err
	}

	if err == nil && degraded == 0 {
		return errors.New("a write was acknowledged below its replication factor and " +
			"nothing counted it, which is the exact condition open question 37 exists " +
			"to keep visible")
	}

	fmt.Println("\n  The caller asked for three copies, got one, and the return value")
	fmt.Println("  cannot tell it apart from a healthy write. That counter is the only")
	fmt.Println("  thing that can -- alert on any non-zero value.")
	fmt.Println()
	fmt.Println("  This is counted rather than refused on purpose. Refusing converts an")
	fmt.Println("  available system into an unavailable one during a partition, which is")
	fmt.Println("  an AP/CP decision that should be taken deliberately and on evidence.")
	fmt.Println("  Open question 37 carries it.")
	fmt.Println()
	fmt.Println("  The neighbouring case is correct and worth knowing: while the writer")
	fmt.Println("  still sees its replicas but cannot reach them, the write fails rather")
	fmt.Println("  than being acknowledged.")
	return nil
}

func printClosing() {
	fmt.Println("\n--- what the promises are ---")
	fmt.Println()
	fmt.Println("  Readable is not replicated. One node answering is enough for the")
	fmt.Println("  first and not for the second, so a key can be served while fewer")
	fmt.Println("  than RF nodes hold it -- during any membership change, not only a")
	fmt.Println("  restart. CatchUp is what closes that window, which is why it is an")
	fmt.Println("  explicit call and why this example makes it one.")
	fmt.Println()
	fmt.Println("  Readiness does not imply the replication factor is restored. A node")
	fmt.Println("  inside the settling window serves correctly; draining the next pod")
	fmt.Println("  because it went ready is how an RF=3 cluster walks down to RF=1.")
	fmt.Println("  Ready().ReplicasCurrent reports whether it is.")
}
