// Command crossversion is one node of a mixed-release cluster.
//
// scripts/crossversion.sh builds it twice — once against the previous
// release, once against this tree — and runs the two builds side by side over
// real gossip and real gRPC on localhost. Every node writes keys and group
// state, then reads every other node's, so each message a release sends is
// read by the other release. One node then drains and the rest read
// everything again, so the hand-off stream crosses releases too.
//
// It uses only API present in the previous release, which is why it is its
// own module: the same source must compile against both.
//
// Nodes coordinate through marker files in a shared directory. It exits 0
// when every check passed and 1 with the failures otherwise.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/membership"
	"github.com/arwenizEr/peercache/persist"
	"github.com/arwenizEr/peercache/transport"
)

type config struct {
	id      string
	label   string
	gossip  int
	data    int
	seed    string
	peers   []string
	drainer string
	dir     string
	keys    int
	timeout time.Duration
	// store is a directory every node persists the "durable" namespace to;
	// empty for none. writers, when set, makes this a restore node: it writes
	// nothing and checks that what those nodes persisted reads back.
	store   string
	writers []string
}

func main() {
	var cfg config
	var peers string
	flag.StringVar(&cfg.id, "id", "", "this node's id")
	flag.StringVar(&cfg.label, "label", "", "which release this build is, for the log")
	flag.IntVar(&cfg.gossip, "gossip", 0, "gossip port on 127.0.0.1")
	flag.IntVar(&cfg.data, "data", 0, "data port on 127.0.0.1")
	flag.StringVar(&cfg.seed, "seed", "", "gossip address to join, empty for the first node")
	flag.StringVar(&peers, "peers", "", "comma-separated ids of every node, this one included")
	flag.StringVar(&cfg.drainer, "drainer", "", "id of the node that drains after the first check")
	flag.StringVar(&cfg.dir, "dir", "", "shared directory for marker files")
	flag.IntVar(&cfg.keys, "keys", 200, "keys each node writes")
	flag.DurationVar(&cfg.timeout, "timeout", 90*time.Second, "how long any one phase may take")
	flag.StringVar(&cfg.store, "store", "", "directory the durable namespace persists to")
	var writers string
	flag.StringVar(&writers, "verify-writers", "", "comma-separated ids whose persisted keys to read back, writing nothing")
	flag.Parse()
	cfg.peers = strings.Split(peers, ",")
	if writers != "" {
		cfg.writers = strings.Split(writers, ",")
	}
	if err := run(cfg); err != nil {
		_ = mark(cfg, "failed")
		fmt.Fprintf(os.Stderr, "%s (%s): FAIL: %v\n", cfg.id, cfg.label, err)
		os.Exit(1)
	}
	fmt.Printf("%s (%s): ok\n", cfg.id, cfg.label)
}

func run(cfg config) error {
	ctx := context.Background()
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(cfg.data))
	if err != nil {
		return fmt.Errorf("listening: %w", err)
	}
	self := peercache.Node{ID: cfg.id, Addr: l.Addr().String()}
	tr, err := transport.New(transport.Options{Self: self})
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	mcfg := memberlist.DefaultLocalConfig()
	mcfg.Name = cfg.id
	mcfg.BindAddr = "127.0.0.1"
	mcfg.BindPort = cfg.gossip
	mcfg.AdvertiseAddr = "127.0.0.1"
	mcfg.AdvertisePort = cfg.gossip
	mcfg.LogOutput = discard{}
	gossip, err := membership.New(membership.Options{Self: self, Config: mcfg})
	if err != nil {
		return fmt.Errorf("gossip: %w", err)
	}
	cache, err := peercache.New(
		peercache.WithNodeID(cfg.id),
		peercache.WithMembership(gossip),
		peercache.WithTransport(tr),
		peercache.WithListener(l),
	)
	if err != nil {
		return fmt.Errorf("cache: %w", err)
	}
	defer func() { _ = cache.Close(context.Background()) }()

	// One function, registered by name on every node, so a Call made on one
	// release runs on an owner of the other.
	if err := cache.RegisterGroupFunc("append", func(_ context.Context, s peercache.GroupState, arg []byte) ([]byte, error) {
		old, _ := s.Get("log")
		v := append(append([]byte{}, old...), arg...)
		s.Set("log", v)
		return v, nil
	}); err != nil {
		return err
	}
	// Group.Get answers only on the owner; a read from anywhere is a call.
	if err := cache.RegisterGroupFunc("read", func(_ context.Context, s peercache.GroupState, _ []byte) ([]byte, error) {
		v, _ := s.Get("log")
		return v, nil
	}); err != nil {
		return err
	}
	kv := cache.Namespace("kv", peercache.WithReplicationFactor(2))
	// Persisted by every node of whichever release to one directory, then
	// read back by a cluster of the other: the storage format crosses
	// releases as the wire does.
	var durable peercache.Namespace
	if cfg.store != "" {
		bs, err := persist.Dir(cfg.store)
		if err != nil {
			return fmt.Errorf("persist.Dir: %w", err)
		}
		durable = cache.Namespace("durable", peercache.WithReplicationFactor(2), peercache.WithPersistence(bs))
	}

	if cfg.seed != "" {
		if err := poll(ctx, cfg.timeout, "joining "+cfg.seed, func() (bool, error) {
			return gossip.Join([]string{cfg.seed}) == nil, nil
		}); err != nil {
			return err
		}
	}
	if err := waitMembers(ctx, cfg, cache, len(cfg.peers)); err != nil {
		return err
	}
	if err := mark(cfg, "joined"); err != nil {
		return err
	}
	if err := waitAll(ctx, cfg, "joined", cfg.peers); err != nil {
		return err
	}
	if len(cfg.writers) > 0 {
		if durable == nil {
			return errors.New("-verify-writers needs -store")
		}
		if err := within(ctx, cfg.timeout, func(ctx context.Context) error {
			return checkKeys(ctx, cfg, durable, cfg.writers)
		}); err != nil {
			return fmt.Errorf("reading what %v persisted: %w", cfg.writers, err)
		}
		fmt.Printf("%s (%s): read every key %v persisted\n", cfg.id, cfg.label, cfg.writers)
		if err := mark(cfg, "checked"); err != nil {
			return err
		}
		return waitAll(ctx, cfg, "checked", cfg.peers)
	}

	// Writes: plain keys, a batch, and a group call per key group.
	if err := within(ctx, cfg.timeout, func(ctx context.Context) error {
		batch := map[string][]byte{}
		for i := 0; i < cfg.keys; i++ {
			k := key(cfg.id, i)
			if i%2 == 0 {
				if err := kv.Set(ctx, k, value(cfg.id, i), 0); err != nil {
					return fmt.Errorf("set %s: %w", k, err)
				}
			} else {
				batch[k] = value(cfg.id, i)
			}
		}
		if err := kv.BatchSet(ctx, batch, 0); err != nil {
			return fmt.Errorf("BatchSet: %w", err)
		}
		if durable != nil {
			for i := 0; i < cfg.keys; i++ {
				if err := durable.Set(ctx, key(cfg.id, i), value(cfg.id, i), 0); err != nil {
					return fmt.Errorf("durable set %s: %w", key(cfg.id, i), err)
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	// Once each, never retried: a retry after a call that ran but whose
	// answer was lost would append twice, and the check counts appends.
	for g := 0; g < groups; g++ {
		cctx, cancel := context.WithTimeout(ctx, cfg.timeout)
		_, err := cache.Group(groupID(g)).Call(cctx, "append", []byte(cfg.id+";"))
		cancel()
		if err != nil {
			return fmt.Errorf("call append on %s: %w", groupID(g), err)
		}
	}
	if err := mark(cfg, "wrote"); err != nil {
		return err
	}
	if err := waitAll(ctx, cfg, "wrote", cfg.peers); err != nil {
		return err
	}

	if err := within(ctx, cfg.timeout, func(ctx context.Context) error {
		if err := checkKeys(ctx, cfg, kv, cfg.peers); err != nil {
			return err
		}
		return checkGroups(ctx, cfg, cache)
	}); err != nil {
		return fmt.Errorf("first check: %w", err)
	}
	fmt.Printf("%s (%s): read every node's keys and groups\n", cfg.id, cfg.label)
	if err := mark(cfg, "checked"); err != nil {
		return err
	}
	if err := waitAll(ctx, cfg, "checked", cfg.peers); err != nil {
		return err
	}

	// Hand-off: the drainer pushes what it owns to owners on whichever
	// release, then waits for the rest to finish reading before closing.
	var rest []string
	for _, p := range cfg.peers {
		if p != cfg.drainer {
			rest = append(rest, p)
		}
	}
	if cfg.id == cfg.drainer {
		if err := within(ctx, cfg.timeout, cache.Drain); err != nil {
			return fmt.Errorf("drain: %w", err)
		}
		fmt.Printf("%s (%s): drained\n", cfg.id, cfg.label)
		if err := mark(cfg, "drained"); err != nil {
			return err
		}
		return waitAll(ctx, cfg, "final", rest)
	}
	if err := waitAll(ctx, cfg, "drained", []string{cfg.drainer}); err != nil {
		return err
	}
	if err := waitMembers(ctx, cfg, cache, len(rest)); err != nil {
		return err
	}
	if err := within(ctx, cfg.timeout, func(ctx context.Context) error {
		return checkKeys(ctx, cfg, kv, cfg.peers)
	}); err != nil {
		return fmt.Errorf("after %s drained: %w", cfg.drainer, err)
	}
	fmt.Printf("%s (%s): read every key after %s drained\n", cfg.id, cfg.label, cfg.drainer)
	if err := mark(cfg, "final"); err != nil {
		return err
	}
	return waitAll(ctx, cfg, "final", rest)
}

const groups = 8

// errWrong marks an answer that is wrong rather than missing. A missing key
// can be the cluster still settling and is asked again; a wrong value never
// is, because retrying until it looks right would hide the bug this exists
// to find.
var errWrong = errors.New("wrong answer")

func wrong(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errWrong, fmt.Sprintf(format, args...))
}

func key(id string, i int) string   { return id + "-" + strconv.Itoa(i) }
func value(id string, i int) []byte { return []byte(id + ":" + strconv.Itoa(i)) }
func groupID(g int) string          { return "g" + strconv.Itoa(g) }

// checkKeys reads every node's keys, one at a time and in one batch per node.
func checkKeys(ctx context.Context, cfg config, kv peercache.Namespace, ids []string) error {
	for _, p := range ids {
		keys := make([]string, cfg.keys)
		for i := range keys {
			k := key(p, i)
			keys[i] = k
			got, err := kv.Get(ctx, k)
			if err != nil {
				return fmt.Errorf("get %s: %w", k, err)
			}
			if !bytes.Equal(got, value(p, i)) {
				return wrong("Get %s = %q, want %q", k, got, value(p, i))
			}
		}
		got, err := kv.BatchGet(ctx, keys)
		if err != nil {
			return fmt.Errorf("BatchGet of %s's keys: %w", p, err)
		}
		for i, k := range keys {
			if !bytes.Equal(got[k], value(p, i)) {
				return wrong("BatchGet %s = %q, want %q", k, got[k], value(p, i))
			}
		}
	}
	return nil
}

// checkGroups reads each group's log, which every node appended to once.
func checkGroups(ctx context.Context, cfg config, cache peercache.Cache) error {
	for g := 0; g < groups; g++ {
		got, err := cache.Group(groupID(g)).Call(ctx, "read", nil)
		if err != nil {
			return fmt.Errorf("group %s read: %w", groupID(g), err)
		}
		for _, p := range cfg.peers {
			if strings.Count(string(got), p+";") != 1 {
				return wrong("group %s log %q: want %s exactly once", groupID(g), got, p)
			}
		}
	}
	return nil
}

func waitMembers(ctx context.Context, cfg config, cache peercache.Cache, n int) error {
	return poll(ctx, cfg.timeout, fmt.Sprintf("a view of %d members", n), func() (bool, error) {
		return cache.Stats().Members == n, nil
	})
}

func mark(cfg config, phase string) error {
	return os.WriteFile(filepath.Join(cfg.dir, phase+"-"+cfg.id), nil, 0o644)
}

func waitAll(ctx context.Context, cfg config, phase string, ids []string) error {
	return poll(ctx, cfg.timeout, "every node "+phase, func() (bool, error) {
		for _, p := range ids {
			if _, err := os.Stat(filepath.Join(cfg.dir, phase+"-"+p)); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return false, nil
				}
				return false, err
			}
			if _, err := os.Stat(filepath.Join(cfg.dir, "failed-"+p)); err == nil {
				return false, fmt.Errorf("%s failed", p)
			}
		}
		return true, nil
	})
}

// poll asks cond every 50ms until it holds, fails, or the timeout passes.
func poll(ctx context.Context, timeout time.Duration, what string, cond func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		ok, err := cond()
		if err != nil {
			return fmt.Errorf("waiting for %s: %w", what, err)
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for %s: %w", what, ctx.Err())
		case <-t.C:
		}
	}
}

// within retries f, each attempt bounded by what is left of the timeout, so a
// check that races the cluster settling gets another try rather than failing
// on the first stale view.
func within(ctx context.Context, timeout time.Duration, f func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		err := f(ctx)
		if err == nil || errors.Is(err, errWrong) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-t.C:
		}
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
