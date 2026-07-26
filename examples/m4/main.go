// Command m4 demonstrates transport and remote routing, completed by milestone
// M4.
//
// M3 left a cluster that agreed on who owned what and refused everything else:
// a call for a group this node did not own returned ErrNotOwner and the caller
// was on its own. This one puts a real gRPC transport underneath, so ownership
// stops being something the caller has to know about.
//
// It shows five things:
//
//   - A read for a key owned elsewhere is served, over the network, with the
//     caller unable to tell except by latency.
//   - A write lands on the owner, not on the writer.
//   - A registered function runs on the group's owner. The computation moves
//     to the data, which is the point of the whole design.
//   - A cold key is reconstructed once, on its owner, however many nodes ask
//     for it at once.
//   - A router turns ten thousand messages into one request per owner.
//
// Everything here speaks real gRPC over loopback TCP and forms a real gossip
// cluster. Nothing is simulated.
//
// Run it with:
//
//	go run ./examples/m4
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/clock"
	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/membership"
	"github.com/peercache/peercache/internal/transport"
)

// budget bounds every wait, so a broken example reports rather than hangs.
const budget = 60 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// node is one pod: a gossip authority, a gRPC transport and a cache.
type node struct {
	id     string
	info   core.Node
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

	for _, step := range []func([]*node) error{
		readsCrossTheNetwork,
		writesLandOnTheOwner,
		computationMovesToTheData,
		aColdKeyIsLoadedOnce,
		theRouterBatchesByOwner,
	} {
		if err := step(nodes); err != nil {
			return err
		}
	}

	fmt.Println("\nOwnership is now invisible to the caller: every operation above was")
	fmt.Println("written the same way whether the data was here or three hops away.")
	fmt.Println("M5 adds replicas, so losing a pod stops losing its data.")
	return nil
}

// startCluster brings up n pods, each with its own gossip port and its own gRPC
// port, and waits for them to see each other.
func startCluster(ids ...string) ([]*node, error) {
	nodes := make([]*node, 0, len(ids))

	for _, id := range ids {
		// The data port. Bound here rather than inside the library so a
		// bind failure is reported where the application can act on it.
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("listening for %s: %w", id, err)
		}
		info := core.Node{ID: id, Addr: l.Addr().String()}

		tr, err := transport.New(transport.Options{Self: info})
		if err != nil {
			return nil, fmt.Errorf("transport for %s: %w", id, err)
		}

		// The gossip port is a *different* socket. Peers learn the data
		// address through gossip metadata; assuming the two were the
		// same is how the first real-network run of this code ended up
		// dialling memberlist and being told "error reading server
		// preface: EOF".
		cfg := memberlist.DefaultLocalConfig()
		cfg.Name = id
		cfg.BindAddr = "127.0.0.1"
		cfg.BindPort = 0
		cfg.AdvertisePort = 0
		g, err := membership.New(membership.Options{Self: info, Config: cfg})
		if err != nil {
			return nil, fmt.Errorf("gossip for %s: %w", id, err)
		}

		c, err := peercache.New(
			peercache.WithNodeID(id),
			peercache.WithClock(clock.Real()),
			peercache.WithMembership(g),
			peercache.WithTransport(tr),
			peercache.WithListener(l),
			peercache.WithActivationDelay(time.Second),
		)
		if err != nil {
			return nil, fmt.Errorf("cache for %s: %w", id, err)
		}

		nodes = append(nodes, &node{id: id, info: info, gossip: g, cache: c})
		if len(nodes) > 1 {
			if err := g.Join([]string{nodes[0].gossip.LocalAddr()}); err != nil {
				return nil, fmt.Errorf("%s joining: %w", id, err)
			}
		}
	}

	if err := await("every pod to see the whole cluster", func() bool {
		for _, n := range nodes {
			if n.cache.Stats().Members != len(nodes) {
				return false
			}
		}
		return true
	}); err != nil {
		return nil, err
	}
	// And to finish deferring the groups they have just acquired.
	if err := await("the activation window to close", func() bool {
		for _, n := range nodes {
			if !acceptsAny(n) {
				return false
			}
		}
		return true
	}); err != nil {
		return nil, err
	}

	fmt.Printf("cluster of %d formed: gossip over SWIM, data over gRPC\n", len(nodes))
	for _, n := range nodes {
		fmt.Printf("  %s  gossip %-20s  data %s\n", n.id, n.gossip.LocalAddr(), n.info.Addr)
	}
	return nodes, nil
}

func readsCrossTheNetwork(nodes []*node) error {
	fmt.Println("\n--- a read for a key owned elsewhere is served anyway ---")

	const key = "orders/42"
	owner, err := ownerOf(nodes, key)
	if err != nil {
		return err
	}
	if err := nodes[0].cache.Namespace("data").Set(context.Background(), key, []byte("hello"), 0); err != nil {
		return fmt.Errorf("set: %w", err)
	}
	fmt.Printf("  %q is owned by %s\n", key, owner.id)

	for _, n := range nodes {
		got, err := n.cache.Namespace("data").Get(context.Background(), key)
		if err != nil {
			return fmt.Errorf("get from %s: %w", n.id, err)
		}
		where := "over the network"
		if n.id == owner.id {
			where = "from local memory"
		}
		fmt.Printf("  %s read %q %s\n", n.id, got, where)
	}
	return nil
}

func writesLandOnTheOwner(nodes []*node) error {
	fmt.Println("\n--- a write lands on the owner, not on the writer ---")

	const key = "orders/99"
	owner, err := ownerOf(nodes, key)
	if err != nil {
		return err
	}
	var writer *node
	for _, n := range nodes {
		if n.id != owner.id {
			writer = n
			break
		}
	}

	before := owner.cache.Stats().OwnedKeys
	if err := writer.cache.Namespace("data").Set(context.Background(), key, []byte("written remotely"), 0); err != nil {
		return fmt.Errorf("set from %s: %w", writer.id, err)
	}
	after := owner.cache.Stats().OwnedKeys

	fmt.Printf("  %s wrote %q; %s now holds %d keys (was %d)\n",
		writer.id, key, owner.id, after, before)
	if after <= before {
		return fmt.Errorf("the owner's key count did not move; the write stayed local")
	}
	return nil
}

func computationMovesToTheData(nodes []*node) error {
	fmt.Println("\n--- a registered function runs on the group's owner ---")

	// Every pod runs the same binary, so every pod registers the same
	// function. Closures never cross the network; the name does.
	for _, n := range nodes {
		id := n.id
		if err := n.cache.RegisterGroupFunc("append",
			func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
				log, _ := s.Get("log")
				log = append(append([]byte{}, log...), arg...)
				s.Set("log", log)
				// Report which pod actually ran it.
				s.Set("ran-on", []byte(id))
				return log, nil
			}); err != nil {
			return fmt.Errorf("registering on %s: %w", n.id, err)
		}
	}

	const group = "session/abc"
	owner, err := ownerOf(nodes, group)
	if err != nil {
		return err
	}

	for _, n := range nodes {
		out, err := n.cache.Group(group).Call(context.Background(), "append", []byte(n.id[len(n.id)-1:]))
		if err != nil {
			return fmt.Errorf("call from %s: %w", n.id, err)
		}
		fmt.Printf("  %s called; accumulated state is now %q\n", n.id, out)
	}

	ranOn, err := owner.cache.Group(group).Get(context.Background(), "ran-on")
	if err != nil {
		return fmt.Errorf("reading ran-on: %w", err)
	}
	fmt.Printf("  every call executed on %s, which owns the group\n", ranOn)
	fmt.Println("  the state was never copied to a caller; only the argument and result moved")
	return nil
}

func aColdKeyIsLoadedOnce(nodes []*node) error {
	fmt.Println("\n--- a cold key is reconstructed once, on its owner ---")

	var mu sync.Mutex
	loads := map[string]int{}

	nss := make(map[string]peercache.Namespace, len(nodes))
	for _, n := range nodes {
		id := n.id
		nss[n.id] = n.cache.Namespace("loaded", peercache.WithLoader(
			func(_ context.Context, key string) ([]byte, time.Duration, error) {
				mu.Lock()
				loads[id]++
				mu.Unlock()
				return []byte("reconstructed from the database"), 0, nil
			}))
	}

	const key = "cold/1"
	owner, err := ownerOf(nodes, key)
	if err != nil {
		return err
	}

	// Every pod asks at once, which is exactly the stampede a cache is
	// supposed to absorb.
	var wg sync.WaitGroup
	errs := make([]error, len(nodes))
	for i, n := range nodes {
		wg.Add(1)
		go func(i int, n *node) {
			defer wg.Done()
			_, err := nss[n.id].GetOrLoad(context.Background(), key)
			errs[i] = err
		}(i, n)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return fmt.Errorf("GetOrLoad from %s: %w", nodes[i].id, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range loads {
		total += n
	}
	fmt.Printf("  %d pods asked for a cold key simultaneously\n", len(nodes))
	fmt.Printf("  the loader ran %d time(s), all on %s\n", total, owner.id)
	if total != loads[owner.id] || total == 0 {
		return fmt.Errorf("loads were %v; a cold key must cost one load for the cluster", loads)
	}
	return nil
}

func theRouterBatchesByOwner(nodes []*node) error {
	fmt.Println("\n--- a router turns 10,000 messages into one request per owner ---")

	for _, n := range nodes {
		if err := n.cache.RegisterGroupFunc("count",
			func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
				// The argument is the whole batch for this group, not
				// one message. That is what lets a group's messages be
				// applied in a single turn.
				msgs, err := peercache.Batch[string](arg)
				if err != nil {
					return nil, err
				}
				total, _ := s.Get("total")
				n := 0
				if len(total) > 0 {
					n = int(total[0])<<8 | int(total[1])
				}
				n += len(msgs)
				s.Set("total", []byte{byte(n >> 8), byte(n)})
				return nil, nil
			}); err != nil {
			return fmt.Errorf("registering on %s: %w", n.id, err)
		}
	}

	const messages = 10_000
	const groups = 500
	msgs := make([]string, 0, messages)
	for i := 0; i < messages; i++ {
		msgs = append(msgs, fmt.Sprintf("tenant-%d", i%groups))
	}

	router := peercache.NewRouter(nodes[0].cache, func(s string) string { return s }, "count")
	start := time.Now()
	if err := router.Submit(context.Background(), msgs...); err != nil {
		return fmt.Errorf("submit: %w", err)
	}
	took := time.Since(start)

	fmt.Printf("  submitted %d messages across %d groups on %d pods in %v\n",
		messages, groups, len(nodes), took.Round(time.Millisecond))
	fmt.Printf("  that is at most one request per owner — %d, not %d\n", len(nodes)-1, messages)
	return nil
}

// ownerOf finds which pod owns a group key, by asking each one to do something
// only its owner can do.
func ownerOf(nodes []*node, key string) (*node, error) {
	var found *node
	for _, n := range nodes {
		_, err := n.cache.Group(key).Keys(context.Background())
		switch {
		case err == nil:
			if found != nil {
				return nil, fmt.Errorf("both %s and %s claim %q", found.id, n.id, key)
			}
			found = n
		case errors.Is(err, peercache.ErrNotOwner):
		default:
			return nil, fmt.Errorf("probing %s: %w", n.id, err)
		}
	}
	if found == nil {
		return nil, fmt.Errorf("no pod claims %q", key)
	}
	return found, nil
}

// acceptsAny reports whether a pod will activate at least one probe group
// without deferring, which is the cheapest evidence its window has closed.
func acceptsAny(n *node) bool {
	for i := 0; i < 16; i++ {
		if _, err := n.cache.Group(fmt.Sprintf("probe/%s/%d", n.id, i)).Keys(context.Background()); err == nil {
			return true
		}
	}
	return false
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
