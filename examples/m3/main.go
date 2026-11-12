// Command m3 demonstrates membership and ownership, completed by milestone M3.
//
// M2 left a single node that owned everything. This one forms a real three-node
// cluster over loopback gossip and shows the three things M3 adds:
//
//   - Every node independently computes the same owner for a key, from nothing
//     but the membership list. There is no coordinator and no lookup table.
//   - Removing a node moves only that node's keys. The survivors do not trade
//     keys with each other, which is the whole reason for rendezvous hashing
//     rather than a modulo.
//   - A node that has just acquired a group refuses to activate it for a
//     moment, and says so with a distinct retryable error rather than by
//     hanging or by quietly running a second copy.
//
// There is still no networking between the caches themselves — a call to a
// group this node does not own returns ErrNotOwner rather than being forwarded.
// M4 adds the transport and the router that makes that hop invisible.
//
// Run it with:
//
//	go run ./examples/m3
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/membership"
)

// activationDelay is how long a node defers a group it has just acquired. The
// default is three seconds, which makes for a slow example; a second is still
// comfortably longer than loopback gossip takes to converge.
const activationDelay = time.Second

// budget bounds every wait in this program, so a broken example reports rather
// than hangs. Loopback gossip converges in milliseconds.
const budget = 30 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type node struct {
	id     string
	gossip *membership.Gossip
	cache  peercache.Cache
}

func (n *node) close() {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	_ = n.cache.Close(ctx)
	_ = n.gossip.Close()
}

func run() error {
	nodes, err := startCluster("pod-0", "pod-1", "pod-2")
	if err != nil {
		return err
	}
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	if err := everyNodeAgrees(nodes); err != nil {
		return err
	}
	if err := departureMovesOnlyItsKeys(nodes); err != nil {
		return err
	}
	if err := acquiredGroupsAreDeferred(nodes); err != nil {
		return err
	}
	return nil
}

// startCluster builds n caches, each with its own gossip authority, and joins
// them into one cluster.
func startCluster(ids ...string) ([]*node, error) {
	return startClusterQuiet(false, ids...)
}

func startClusterQuiet(quiet bool, ids ...string) ([]*node, error) {
	nodes := make([]*node, 0, len(ids))
	for _, id := range ids {
		cfg := memberlist.DefaultLocalConfig()
		cfg.Name = id
		cfg.BindAddr = "127.0.0.1"
		cfg.BindPort = 0 // ask the OS for a port
		cfg.AdvertisePort = 0

		g, err := membership.New(membership.Options{Self: core.Node{ID: id}, Config: cfg})
		if err != nil {
			return nil, fmt.Errorf("gossip for %s: %w", id, err)
		}

		// The clock is real here because gossip converges on real time and
		// nothing injected reaches memberlist's own timers. Everywhere
		// else in this repository, time is fake and moves only when a test
		// moves it.
		c, err := peercache.New(
			peercache.WithNodeID(id),
			peercache.WithMembership(g),
			peercache.WithClock(clock.Real()),
			peercache.WithActivationDelay(activationDelay),
		)
		if err != nil {
			return nil, fmt.Errorf("cache for %s: %w", id, err)
		}

		if len(nodes) > 0 {
			if err := g.Join([]string{nodes[0].gossip.LocalAddr()}); err != nil {
				return nil, fmt.Errorf("%s joining: %w", id, err)
			}
		}
		nodes = append(nodes, &node{id: id, gossip: g, cache: c})
	}

	if err := await("every node to see the full cluster", func() bool {
		for _, n := range nodes {
			if n.cache.Stats().Members != len(nodes) {
				return false
			}
		}
		return true
	}); err != nil {
		return nil, err
	}

	if !quiet {
		fmt.Printf("cluster of %d formed over loopback gossip\n", len(nodes))
		for _, n := range nodes {
			s := n.cache.Stats()
			fmt.Printf("  %s sees %d members at epoch %d\n", n.id, s.Members, s.Epoch)
		}
	}
	return nodes, nil
}

// everyNodeAgrees shows that ownership is computed, not coordinated: each node
// reaches the same conclusion from the same membership list, and every key has
// exactly one owner.
func everyNodeAgrees(nodes []*node) error {
	fmt.Println("\n--- every node computes the same owner ---")

	if err := await("the activation window to close", func() bool {
		for _, n := range nodes {
			if !acceptsAny(n) {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	}

	const keys = 3000
	owners := make(map[string]int, len(nodes)) // node id -> keys owned
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("orders/%d", i)
		var claimed []string
		for _, n := range nodes {
			ok, err := owns(n, key)
			if err != nil {
				return err
			}
			if ok {
				claimed = append(claimed, n.id)
			}
		}
		if len(claimed) != 1 {
			return fmt.Errorf("key %q was claimed by %v; exactly one node must own it", key, claimed)
		}
		owners[claimed[0]]++
	}

	ideal := float64(keys) / float64(len(nodes))
	for _, n := range nodes {
		got := owners[n.id]
		skew := (float64(got) - ideal) / ideal * 100
		fmt.Printf("  %s owns %4d of %d keys (%+.1f%% from even)\n", n.id, got, keys, skew)
	}
	fmt.Println("no key was claimed twice and none was claimed by nobody")
	return nil
}

// departureMovesOnlyItsKeys is the property a modulo hash does not have. When a
// node leaves, its share is redistributed and nothing else moves — the
// survivors keep every key they already had.
func departureMovesOnlyItsKeys(nodes []*node) error {
	fmt.Println("\n--- a departure moves only the departed node's keys ---")

	const keys = 3000
	survivors := nodes[:len(nodes)-1]
	leaving := nodes[len(nodes)-1]

	before := make(map[string]string, keys) // key -> owner id
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("orders/%d", i)
		for _, n := range nodes {
			ok, err := owns(n, key)
			if err != nil {
				return err
			}
			if ok {
				before[key] = n.id
				break
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	if err := leaving.gossip.Leave(ctx); err != nil {
		return fmt.Errorf("%s leaving: %w", leaving.id, err)
	}
	if err := await("the survivors to notice the departure", func() bool {
		for _, n := range survivors {
			if n.cache.Stats().Members != len(survivors) {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	}
	fmt.Printf("  %s left; %d nodes remain\n", leaving.id, len(survivors))

	// Wait out the deferral window before recounting, so that "who owns this
	// now" is not confused with "who will serve this in a moment".
	if err := await("the survivors to resume activating", func() bool {
		for _, n := range survivors {
			if !acceptsAny(n) {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	}

	var moved, tradedBetweenSurvivors int
	for key, owner := range before {
		for _, n := range survivors {
			ok, err := owns(n, key)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			if n.id != owner {
				moved++
				if owner != leaving.id {
					tradedBetweenSurvivors++
				}
			}
			break
		}
	}

	fmt.Printf("  %d of %d keys changed owner (ideal: the %d that %s held)\n",
		moved, keys, countOwned(before, leaving.id), leaving.id)
	if tradedBetweenSurvivors != 0 {
		return fmt.Errorf("%d keys moved between two surviving nodes; rendezvous hashing must not "+
			"reshuffle keys that did not belong to the departed node", tradedBetweenSurvivors)
	}
	fmt.Println("  0 keys traded between surviving nodes")
	return nil
}

// acquiredGroupsAreDeferred shows the third M3 behaviour: a node that has just
// taken over a group waits before activating it, and reports that wait as a
// retryable error rather than as a hang or a second live copy.
func acquiredGroupsAreDeferred(nodes []*node) error {
	fmt.Println("\n--- a newly acquired group is briefly deferred ---")

	// The cluster has just lost a node, so the survivors have acquired its
	// share. Rejoin a node to force another acquisition and probe during
	// the window.
	fresh, err := startClusterQuiet(true, "pod-3")
	if err != nil {
		return err
	}
	defer fresh[0].close()

	survivors := nodes[:len(nodes)-1]
	if err := fresh[0].gossip.Join([]string{survivors[0].gossip.LocalAddr()}); err != nil {
		return fmt.Errorf("pod-3 joining: %w", err)
	}
	if err := await("everyone to see the new node", func() bool {
		want := len(survivors) + 1
		if fresh[0].cache.Stats().Members != want {
			return false
		}
		for _, n := range survivors {
			if n.cache.Stats().Members != want {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	}

	all := append(append([]*node{}, survivors...), fresh[0])

	// Probe a fixed sample of keys once per node and classify the answers.
	// Counting one full pass, rather than polling until something is
	// deferred, gives a number that means something: the share of the
	// keyspace the cluster is briefly declining to serve.
	const sample = 300
	var pending, served, notOwner int
	deadline := time.Now().Add(budget)
	for {
		pending, served, notOwner = 0, 0, 0
		for i := 0; i < sample; i++ {
			for _, n := range all {
				_, err := n.cache.Group(fmt.Sprintf("moved/%d", i)).Keys(context.Background())
				switch {
				case err == nil:
					served++
				case errors.Is(err, peercache.ErrActivationPending):
					pending++
				case errors.Is(err, peercache.ErrNotOwner):
					notOwner++
				default:
					return fmt.Errorf("unexpected error from %s: %w", n.id, err)
				}
			}
		}
		if pending > 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no activation was deferred within %v of a join", budget)
		}
		runtime.Gosched()
	}
	fmt.Printf("  of %d probes across %d nodes: %d served, %d deferred, %d not-owner\n",
		sample*len(all), len(all), served, pending, notOwner)

	if err := await("the window to close and activations to resume", func() bool {
		for _, n := range all {
			if !acceptsAny(n) {
				return false
			}
		}
		return true
	}); err != nil {
		return err
	}

	// Deliberately not printing Stats().DeferredActivations. It counts events,
	// not deferred groups, so the polling above inflates it into the millions:
	// the number would say more about how hard this program retried than about
	// the cluster. Recorded as an open question in docs/decisions/open.md.
	fmt.Println("  the window closed and every node resumed serving what it owns")
	fmt.Println("\nErrActivationPending is retryable here; ErrNotOwner means go elsewhere.")
	fmt.Println("M4 does the going-elsewhere for you.")
	return nil
}

// owns reports whether n will activate key. A deferred answer is retried,
// because the question is about ownership rather than about timing.
func owns(n *node, key string) (bool, error) {
	deadline := time.Now().Add(budget)
	for {
		_, err := n.cache.Group(key).Keys(context.Background())
		switch {
		case err == nil:
			return true, nil
		case errors.Is(err, peercache.ErrNotOwner):
			return false, nil
		case errors.Is(err, peercache.ErrActivationPending):
			if time.Now().After(deadline) {
				return false, fmt.Errorf("%s deferred %q for %v", n.id, key, budget)
			}
			runtime.Gosched()
		default:
			return false, fmt.Errorf("%s Keys(%q): %w", n.id, key, err)
		}
	}
}

// acceptsAny reports whether n activates at least one of a few probe keys
// without deferring, which is the cheapest evidence that its window has closed.
func acceptsAny(n *node) bool {
	for i := 0; i < 16; i++ {
		if _, err := n.cache.Group(fmt.Sprintf("probe/%s/%d", n.id, i)).Keys(context.Background()); err == nil {
			return true
		}
	}
	return false
}

func countOwned(owners map[string]string, id string) int {
	count := 0
	for _, owner := range owners {
		if owner == id {
			count++
		}
	}
	return count
}

// await polls until cond holds or the budget expires. It yields rather than
// sleeping; this repository does not wait on wall time when it can avoid it.
func await(what string, cond func() bool) error {
	deadline := time.Now().Add(budget)
	for !cond() {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out after %v waiting for %s", budget, what)
		}
		runtime.Gosched()
	}
	return nil
}
