package tests

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/ownership"
	"github.com/arwenizEr/peercache/internal/testkit"
	"github.com/arwenizEr/peercache/persist"
)

// TestDurableSimulation is TestSimulation for the features kind keeps finding
// bugs in, and that the original simulation never combined: persistence, the
// memory budget, an object store that some nodes cannot reach, and counters,
// whose read-modify-write makes "absent" and "zero" the same answer.
//
// Open questions 57, 60 and 61 were all found on kind, each in a combination
// of two or more of those, and none was reachable from TestSimulation: one
// namespace, Set and Get, nothing persisted, no budget. Each schedule here
// draws from all of them at once.
//
// The invariant is the shop's ledger check. Every decrement is recorded as
// promised (acknowledged on a side of the cluster the design promises to keep),
// unpromised (acknowledged where the design does not: a minority side, a writer
// whose view was smaller than the replication factor, or since lost copies
// whose every holder could have died unflushed), or unknown (an error: it may
// or may not have been applied). A counter's final value must account for
// every promised decrement and for no more than all of them together:
//
//	promised <= -value <= promised + unpromised + unknown
//
// Losing promised decrements is the failure kind showed as a stock count above
// the order ledger; exceeding the upper bound would be a decrement applied
// twice.
//
// Two more workloads share the schedule. Counters in a namespace without
// persistence ("carts"), whose promise is replication alone: only a node killed
// without draining can take their copies, so only those kills excuse a loss —
// and the memory budget, whose shed of such a key is a documented loss. Which
// keys were shed is read from the nodes' key traces after every step, before a
// later kill can take a node's history with it; a shed counter is checked only
// for decrements applied twice.
// And order claims ("orders"), the shop's idempotency pattern: SetIfAbsent on
// a few keys from many nodes. A claim acknowledged where the design keeps it
// must read back with the value it claimed, and no key may be claimed twice
// with different values while the first claim stands. A drain, the graceful
// restart a rolling upgrade is made of, is among the faults; it excuses
// nothing.
func TestDurableSimulation(t *testing.T) {
	seeds := simSeeds(t)
	if raceEnabled && seeds > 1 {
		// Under the race detector a schedule costs about seventy seconds,
		// and its job there is to run the code paths for the detector, not
		// to search schedules: one does that, and the gate's tests package
		// ran 1,389 s of its 1,500 s timeout with two. The search runs
		// without it.
		seeds = 1
	}
	base := testkit.Seed(t)
	t.Logf("running %d durable schedules from base seed %d", seeds, base)
	for i := 0; i < seeds; i++ {
		seed := base + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runDurableSimulation(t, seed)
		})
	}
}

const (
	durSteps    = 120
	durCounters = 6
	// durBudget is small enough that a few fills push every node into
	// shedding owned data, persistent namespaces included.
	durBudget = 256 << 10
)

// durLedger is one counter's accounting.
type durLedger struct {
	// promised holds, per acknowledged decrement the design keeps, the
	// number of unflushed crashes there had been when it was made.
	promised   []int
	unpromised int
	unknown    int
	// forked records a write acknowledged on a minority side. Both sides
	// then wrote the key, and the later write by order wins the heal
	// whichever side made it: the AP window decision 5 documents, in which
	// the majority's writes may be lost too. Counted as a fork since open
	// question 66; lost all the same.
	forked bool
}

// durClaim is one acknowledged order claim and the destruction counts when it
// was made.
type durClaim struct {
	val       string
	crashesAt int
}

type durableSim struct {
	t      *testing.T
	f      *fabric
	rng    *rand.Rand
	stores map[string]*cutOffStore

	ledger map[string]*durLedger
	// carts is the non-persistent counters' ledger, judged against
	// hardKills rather than unflushedCrashes.
	carts map[string]*durLedger
	// claims holds, per order key, the claim acknowledged where the design
	// keeps it.
	claims map[string]durClaim
	// violations are failures found while the schedule ran: a key claimed
	// twice. Reported with the rest at the end.
	violations []string
	// hardKills counts every node killed without a drain: what can take a
	// non-persistent copy.
	hardKills int
	// cartsShed names the carts keys some node's budget shed.
	cartsShed map[string]bool
	// forkedClaims names the order keys also claimed on a minority side.
	forkedClaims map[string]bool

	partitioned bool
	minority    map[string]bool
	// outage is a store outage for every node at once.
	outage bool
	// unflushedCrashes counts nodes killed while their store was
	// unreachable: the only kills that can lose a persistent write, since a
	// kill closes the cache and Close flushes.
	unflushedCrashes int

	events []string
}

func (s *durableSim) logf(format string, args ...any) {
	s.events = append(s.events, fmt.Sprintf(format, args...))
}

func (s *durableSim) history() string {
	out := ""
	for i, e := range s.events {
		out += fmt.Sprintf("  %2d. %s\n", i+1, e)
	}
	return out
}

func (s *durableSim) openStock(id string) peercache.Namespace {
	return s.f.caches[id].Namespace("stock",
		peercache.WithReplicationFactor(simRF),
		peercache.WithSyncReplication(),
		peercache.WithPersistence(s.stores[id]))
}

func (s *durableSim) openCarts(id string) peercache.Namespace {
	return s.f.caches[id].Namespace("carts",
		peercache.WithReplicationFactor(simRF),
		peercache.WithSyncReplication())
}

func (s *durableSim) openOrders(id string) peercache.Namespace {
	return s.f.caches[id].Namespace("orders",
		peercache.WithReplicationFactor(simRF),
		peercache.WithSyncReplication(),
		peercache.WithPersistence(s.stores[id]))
}

func (s *durableSim) open(id string) {
	s.openStock(id)
	s.openCarts(id)
	s.openOrders(id)
	s.f.caches[id].Namespace("bulk")
}

func runDurableSimulation(t *testing.T, seed int64) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay,
		withCacheOptions(peercache.WithMemoryBudget(durBudget), peercache.WithKeyTrace("carts", ""), peercache.WithKeyTrace("stock", "")))

	backing := persist.Memory()
	s := &durableSim{
		t:            t,
		f:            f,
		rng:          rand.New(rand.NewSource(seed)),
		stores:       map[string]*cutOffStore{},
		ledger:       map[string]*durLedger{},
		carts:        map[string]*durLedger{},
		cartsShed:    map[string]bool{},
		forkedClaims: map[string]bool{},
		claims:       map[string]durClaim{},
		minority:     map[string]bool{},
	}
	for _, id := range f.ids {
		s.stores[id] = &cutOffStore{BlobStore: backing}
	}
	for _, id := range f.ids {
		s.open(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)
	// Every node catches up before it serves, as the shop does and as a
	// revived one does below. Until then a node gives no durable bound, and
	// its replicas keep its copies for the flush window alone (open question
	// 68): an owner cut off from the store past that window lost a promised
	// decrement to its replicas' budget, 2 of 200 schedules.
	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(context.Background()); err != nil {
			t.Fatalf("CatchUp(%s): %v", id, err)
		}
	}

	for step := 0; step < durSteps; step++ {
		s.step()
		s.noteCartsShed()
	}

	// Repair everything, then judge.
	s.healIfSplit()
	s.setOutage(false)
	for _, id := range f.ids {
		if f.dead[id] {
			f.revive(t, id)
			s.open(id)
			s.logf("revive %s (final)", id)
		}
	}
	f.clk.Advance(4 * testkit.DefaultJoinDelay)
	f.convergeTo(t, len(f.ids))
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)
	ctx := context.Background()
	for _, id := range f.ids {
		// The budget may stop a catch-up; that is reported, not fatal.
		if err := f.caches[id].CatchUp(ctx); err != nil {
			s.logf("final CatchUp(%s): %v", id, err)
		}
	}
	f.settle(t)
	// The repair sheds too: a revived node's catch-up can take it over its
	// budget, and a carts key it then sheds is lost by design, as during the
	// schedule. Unnoted, that loss was judged a disagreement: 8 of the 10
	// failures of a 200-schedule run on 2026-11-20 ended in a shed here.
	s.noteCartsShed()

	wrong := append([]string(nil), s.violations...)
	checked := 0
	wrong = append(wrong, s.judgeCounters("stock", s.ledger, s.unflushedCrashes, s.openStock, nil, &checked)...)
	wrong = append(wrong, s.judgeCounters("carts", s.carts, s.hardKills, s.openCarts, s.cartsShed, &checked)...)
	wrong = append(wrong, s.judgeClaims(&checked)...)
	var forks int64
	for _, id := range f.ids {
		forks += f.caches[id].Stats().SplitBrainDetections
	}
	if len(wrong) > 0 {
		t.Logf("split-brain detections counted by the nodes at the end: %d", forks)
		t.Fatalf("seed %d: %d disagreements with the ledgers.\n"+
			"Replay with PEERCACHE_SEED=%d.\n%v\nschedule:\n%s",
			seed, len(wrong), seed, wrong, s.history())
	}
	var shed int64
	for _, id := range f.ids {
		shed += f.caches[id].Stats().ShedOwned
	}
	t.Logf("seed %d: %d counters and claims agree with their ledgers; %d owned keys shed, "+
		"%d unflushed crashes, %d hard kills", seed, checked, shed, s.unflushedCrashes, s.hardKills)
}

// judgeCounters checks each counter of a ledger against its final value.
// destroyed is the count that excuses a promised decrement once it has grown
// by the replication factor since.
func (s *durableSim) judgeCounters(name string, ledger map[string]*durLedger, destroyed int,
	open func(string) peercache.Namespace, shed map[string]bool, checked *int) []string {
	keys := make([]string, 0, len(ledger))
	for k := range ledger {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var wrong []string
	for _, key := range keys {
		l := ledger[key]
		promised, excused := 0, 0
		for _, at := range l.promised {
			if destroyed-at >= simRF {
				excused++
			} else {
				promised++
			}
		}
		if promised+excused+l.unpromised == 0 {
			continue // never acknowledged: absent and present are both right
		}
		if shed[key] || l.forked {
			// Shed is lost and the count started again from zero; or both
			// sides of a partition wrote it and one side's writes lost
			// the heal. Either way only the upper bound still holds: no
			// decrement applied more often than it was made.
			excused += promised
			promised = 0
		}
		v, err := s.read(open, key)
		if errors.Is(err, peercache.ErrNotFound) && promised == 0 {
			continue // every acknowledgement excused or unpromised: absent is allowed
		}
		if err != nil {
			wrong = append(wrong, fmt.Sprintf("%s/%s: unreadable: %v\n%s", name, key, err, s.keyHistory(name, key)))
			continue
		}
		got, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			wrong = append(wrong, fmt.Sprintf("%s/%s holds %q", name, key, v))
			continue
		}
		*checked++
		lo, hi := int64(promised), int64(promised+excused+l.unpromised+l.unknown)
		if -got < lo || -got > hi {
			wrong = append(wrong, fmt.Sprintf("%s/%s = %d: %d decrements applied, ledger allows %d..%d "+
				"(promised %d, excused %d, unpromised %d, unknown %d)\n%s",
				name, key, got, -got, lo, hi, promised, excused, l.unpromised, l.unknown, s.keyHistory(name, key)))
		}
	}
	return wrong
}

// keyHistory merges key's history from every live node by time: a failure's
// sequence of events, as far as the nodes still alive remember it.
func (s *durableSim) keyHistory(namespace, key string) string {
	type ev struct {
		node string
		peercache.KeyEvent
	}
	var all []ev
	for _, id := range s.f.live() {
		for _, e := range s.f.caches[id].KeyHistory(namespace, key) {
			all = append(all, ev{id, e})
		}
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].At < all[j].At })
	out := ""
	for _, e := range all {
		out += fmt.Sprintf("      %d %s %-16s order=%d writer=%s v=%d hash=%s %s\n",
			e.At, e.node, e.Kind, e.Order, e.Writer, e.Version, e.Hash, e.Detail)
	}
	return out
}

// keepHistories writes into the schedule what victim remembers of each stock
// key, before killing it takes that memory: a failure's history is otherwise
// only what the survivors saw.
func (s *durableSim) keepHistories(victim string) {
	for key := range s.ledger {
		for _, e := range s.f.caches[victim].KeyHistory("stock", key) {
			s.logf("  [%s stock/%s] %d %s order=%d writer=%s v=%d hash=%s %s",
				victim, key, e.At, e.Kind, e.Order, e.Writer, e.Version, e.Hash, e.Detail)
		}
	}
}

// noteCartsShed records the carts keys any live node's budget has shed, from
// their key traces.
func (s *durableSim) noteCartsShed() {
	for key := range s.carts {
		if s.cartsShed[key] {
			continue
		}
		for _, id := range s.f.live() {
			h := s.f.caches[id].KeyHistory("carts", key)
			_, shed := lastOf(h, "shed")
			_, replicaShed := lastOf(h, "replica-shed")
			if shed || replicaShed {
				s.cartsShed[key] = true
				s.logf("carts/%s was shed on %s", key, id)
				break
			}
		}
	}
}

// judgeClaims checks that every standing claim reads back with its value.
func (s *durableSim) judgeClaims(checked *int) []string {
	keys := make([]string, 0, len(s.claims))
	for k := range s.claims {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var wrong []string
	for _, key := range keys {
		c := s.claims[key]
		if s.unflushedCrashes-c.crashesAt >= simRF || s.forkedClaims[key] {
			continue
		}
		v, err := s.read(s.openOrders, key)
		*checked++
		if err != nil || v != c.val {
			wrong = append(wrong, fmt.Sprintf("orders/%s = %q, %v; claimed as %q", key, v, err, c.val))
		}
	}
	return wrong
}

// read reads key from any live node, asking again while the answer is "ask
// again".
func (s *durableSim) read(open func(string) peercache.Namespace, key string) (string, error) {
	var last error
	for attempt := 0; attempt < 50; attempt++ {
		for _, id := range s.f.live() {
			v, err := open(id).Get(context.Background(), key)
			if err == nil {
				return string(v), nil
			}
			if errors.Is(err, peercache.ErrNotFound) {
				return "", err
			}
			last = err
		}
		s.f.clk.Advance(time.Second)
		s.f.settle(s.t)
	}
	return "", last
}

func (s *durableSim) step() {
	switch roll := s.rng.Intn(100); {
	case roll < 32:
		s.decrement()
	case roll < 42:
		s.decrementCart()
	case roll < 52:
		s.claim()
	case roll < 64:
		s.fill()
	case roll < 69:
		s.kill()
	case roll < 73:
		s.drain()
	case roll < 80:
		s.revive()
	case roll < 88:
		s.togglePartition()
	case roll < 94:
		s.setOutage(!s.outage)
	default:
		s.f.clk.Advance(flushInterval)
		s.f.settle(s.t)
		s.logf("advance %v", flushInterval)
	}
}

func (s *durableSim) decrement() {
	s.decrementIn("stock", s.ledger, s.unflushedCrashes, s.openStock)
}

func (s *durableSim) decrementCart() {
	s.decrementIn("carts", s.carts, s.hardKills, s.openCarts)
}

// promisedVia reports whether a write acknowledged through via is one the
// design promises to keep, and if not, why.
func (s *durableSim) promisedVia(via string) (bool, string) {
	switch {
	case s.partitioned && s.minority[via]:
		return false, "minority side"
	case s.f.caches[via].Stats().Members < simRF:
		return false, "view below RF"
	}
	return true, ""
}

func (s *durableSim) decrementIn(name string, ledger map[string]*durLedger, destroyed int, open func(string) peercache.Namespace) {
	live := s.f.live()
	if len(live) == 0 {
		return
	}
	via := live[s.rng.Intn(len(live))]
	key := fmt.Sprintf("item-%d", s.rng.Intn(durCounters))
	l := ledger[key]
	if l == nil {
		l = &durLedger{}
		ledger[key] = l
	}
	got, err := open(via).Increment(context.Background(), key, -1, 0)
	if err != nil {
		l.unknown++
		s.logf("decrement %s/%s via %s -> %v", name, key, via, err)
		return
	}
	if ok, why := s.promisedVia(via); !ok {
		l.unpromised++
		l.forked = l.forked || (s.partitioned && s.minority[via])
		s.logf("decrement %s/%s via %s -> %d (%s)", name, key, via, got, why)
		return
	}
	l.promised = append(l.promised, destroyed)
	s.logf("decrement %s/%s via %s -> %d", name, key, via, got)
}

// claim tries to claim one of a few order keys with a fresh value, as a
// checkout claims its order ID.
func (s *durableSim) claim() {
	live := s.f.live()
	if len(live) == 0 {
		return
	}
	via := live[s.rng.Intn(len(live))]
	key := fmt.Sprintf("order-%d", s.rng.Intn(durCounters))
	val := fmt.Sprintf("by-%s-%d", via, s.rng.Intn(1_000_000))
	stored, err := s.openOrders(via).SetIfAbsent(context.Background(), key, []byte(val), 0)
	switch {
	case err != nil:
		s.logf("claim %s via %s -> %v", key, via, err)
		return
	case !stored:
		s.logf("claim %s via %s -> already claimed", key, via)
		return
	}
	ok, why := s.promisedVia(via)
	if !ok {
		if s.partitioned && s.minority[via] {
			s.forkedClaims[key] = true
		}
		s.logf("claim %s via %s -> %s (%s)", key, via, val, why)
		return
	}
	if prev, had := s.claims[key]; had && s.unflushedCrashes-prev.crashesAt < simRF && !s.forkedClaims[key] {
		s.violations = append(s.violations, fmt.Sprintf("orders/%s claimed twice: %q, then %q via %s",
			key, prev.val, val, via))
	}
	s.claims[key] = durClaim{val: val, crashesAt: s.unflushedCrashes}
	s.logf("claim %s via %s -> %s", key, via, val)
}

// drain restarts a node gracefully: Drain hands its keys on, Close flushes.
// It loses nothing the design keeps, so it excuses nothing — unless the store
// is unreachable from the node, when the final flush fails as a crash's would.
func (s *durableSim) drain() {
	if len(s.f.live()) <= simRF {
		return
	}
	s.healIfSplit()
	live := s.f.live()
	victim := live[s.rng.Intn(len(live))]
	err := s.f.caches[victim].Drain(context.Background())
	cut := s.stores[victim].cut.Load()
	s.keepHistories(victim)
	s.f.dead[victim] = true
	_ = s.f.caches[victim].Close(context.Background())
	s.f.memberships[victim].Close()
	s.f.net.Unregister(victim)
	if cut {
		s.unflushedCrashes++
	}
	if err != nil {
		// A drain that could not hand everything on is a kill for what
		// it did not.
		s.hardKills++
	}
	s.f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	s.f.convergeTo(s.t, len(s.f.live()))
	s.f.settle(s.t)
	s.logf("drain %s (drain error: %v, store unreachable: %v, %d live)", victim, err, cut, len(s.f.live()))
}

// fill writes enough unrelated data through one node to push it over its
// budget, so that the next relief round sheds owned keys.
func (s *durableSim) fill() {
	live := s.f.live()
	if len(live) == 0 {
		return
	}
	via := live[s.rng.Intn(len(live))]
	bulk := s.f.caches[via].Namespace("bulk")
	base := s.rng.Int()
	ok := 0
	for i := 0; i < 64; i++ {
		if bulk.Set(context.Background(), fmt.Sprintf("b%d-%d", base, i), make([]byte, 4<<10), 0) == nil {
			ok++
		}
	}
	s.logf("fill via %s (%d/64 written, shed owned %d)", via, ok, s.f.caches[via].Stats().ShedOwned)
}

func (s *durableSim) kill() {
	if len(s.f.live()) <= simRF {
		return
	}
	s.healIfSplit()
	live := s.f.live()
	victim := live[s.rng.Intn(len(live))]
	cut := s.stores[victim].cut.Load()
	if !cut && s.rng.Intn(2) == 0 {
		// A crash while the store is unreachable: the final flush fails.
		s.stores[victim].cut.Store(true)
		cut = true
	}
	s.keepHistories(victim)
	s.f.dead[victim] = true
	_ = s.f.caches[victim].Close(context.Background())
	s.f.memberships[victim].Close()
	s.f.net.Unregister(victim)
	if cut {
		s.unflushedCrashes++
	}
	s.hardKills++
	s.stores[victim].cut.Store(s.outage)
	s.f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	s.f.convergeTo(s.t, len(s.f.live()))
	s.f.settle(s.t)
	s.logf("kill %s (store unreachable: %v, %d live)", victim, cut, len(s.f.live()))
}

func (s *durableSim) revive() {
	var dead []string
	for _, id := range s.f.ids {
		if s.f.dead[id] {
			dead = append(dead, id)
		}
	}
	if len(dead) == 0 {
		return
	}
	s.healIfSplit()
	id := dead[s.rng.Intn(len(dead))]
	s.f.revive(s.t, id)
	s.open(id)
	s.f.clk.Advance(2 * testkit.DefaultJoinDelay)
	s.f.convergeTo(s.t, len(s.f.live()))
	s.f.settle(s.t)
	err := s.f.caches[id].CatchUp(context.Background())
	s.logf("revive %s, catch up: %v", id, err)
}

// togglePartition splits the live nodes unevenly, cutting the smaller side off
// from the store as well, or heals the split.
func (s *durableSim) togglePartition() {
	if s.partitioned {
		s.healIfSplit()
		return
	}
	live := s.f.live()
	if len(live) < 4 {
		return
	}
	cut := 1 + s.rng.Intn((len(live)-1)/2)
	minority := append([]string(nil), live[:cut]...)
	majority := append([]string(nil), live[cut:]...)
	s.f.net.Partition(minority, majority)
	s.partitioned = true
	for _, id := range minority {
		s.minority[id] = true
		s.stores[id].cut.Store(true)
	}
	s.f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	s.f.clk.Advance(2 * ownership.DefaultActivationDelay)
	s.f.settle(s.t)
	s.logf("partition %v | %v (minority cut off from the store)", minority, majority)
}

func (s *durableSim) healIfSplit() {
	if !s.partitioned {
		return
	}
	s.f.net.Heal()
	s.partitioned = false
	for id := range s.minority {
		s.stores[id].cut.Store(s.outage)
	}
	clear(s.minority)
	s.f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	s.f.convergeTo(s.t, len(s.f.live()))
	s.f.clk.Advance(2 * ownership.DefaultActivationDelay)
	s.f.settle(s.t)
	s.logf("heal")
}

// setOutage makes the store unreachable from every node, or reachable again
// from every node not cut off by a partition.
func (s *durableSim) setOutage(on bool) {
	if s.outage == on {
		return
	}
	s.outage = on
	for id, st := range s.stores {
		st.cut.Store(on || s.minority[id])
	}
	s.logf("store outage: %v", on)
}
