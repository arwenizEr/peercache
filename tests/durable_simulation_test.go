package tests

import (
	"context"
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
func TestDurableSimulation(t *testing.T) {
	seeds := simSeeds(t)
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
}

type durableSim struct {
	t      *testing.T
	f      *fabric
	rng    *rand.Rand
	stores map[string]*cutOffStore

	ledger map[string]*durLedger

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

func (s *durableSim) open(id string) {
	s.openStock(id)
	s.f.caches[id].Namespace("bulk")
}

func runDurableSimulation(t *testing.T, seed int64) {
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay,
		withCacheOptions(peercache.WithMemoryBudget(durBudget)))

	backing := persist.Memory()
	s := &durableSim{
		t:        t,
		f:        f,
		rng:      rand.New(rand.NewSource(seed)),
		stores:   map[string]*cutOffStore{},
		ledger:   map[string]*durLedger{},
		minority: map[string]bool{},
	}
	for _, id := range f.ids {
		s.stores[id] = &cutOffStore{BlobStore: backing}
	}
	for _, id := range f.ids {
		s.open(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	for step := 0; step < durSteps; step++ {
		s.step()
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

	keys := make([]string, 0, len(s.ledger))
	for k := range s.ledger {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var wrong []string
	checked := 0
	for _, key := range keys {
		l := s.ledger[key]
		promised, excused := 0, 0
		for _, at := range l.promised {
			if s.unflushedCrashes-at >= simRF {
				excused++
			} else {
				promised++
			}
		}
		if promised+excused+l.unpromised == 0 {
			continue // never acknowledged: absent and present are both right
		}
		got, err := s.readCounter(key)
		if err != nil {
			wrong = append(wrong, fmt.Sprintf("%s: unreadable: %v", key, err))
			continue
		}
		checked++
		lo, hi := int64(promised), int64(promised+excused+l.unpromised+l.unknown)
		if -got < lo || -got > hi {
			wrong = append(wrong, fmt.Sprintf("%s = %d: %d decrements applied, ledger allows %d..%d "+
				"(promised %d, excused %d, unpromised %d, unknown %d)",
				key, got, -got, lo, hi, promised, excused, l.unpromised, l.unknown))
		}
	}
	if len(wrong) > 0 {
		t.Fatalf("seed %d: %d of %d counters disagree with their ledger.\n"+
			"Replay with PEERCACHE_SEED=%d.\n%v\nschedule:\n%s",
			seed, len(wrong), len(keys), seed, wrong, s.history())
	}
	var shed int64
	for _, id := range f.ids {
		shed += f.caches[id].Stats().ShedOwned
	}
	t.Logf("seed %d: %d counters agree with their ledger; %d owned keys shed, %d unflushed crashes",
		seed, checked, shed, s.unflushedCrashes)
}

// readCounter reads key from any live node, asking again while the answer is
// "ask again".
func (s *durableSim) readCounter(key string) (int64, error) {
	var last error
	for attempt := 0; attempt < 50; attempt++ {
		for _, id := range s.f.live() {
			v, err := s.openStock(id).Get(context.Background(), key)
			if err == nil {
				return strconv.ParseInt(string(v), 10, 64)
			}
			last = err
		}
		s.f.clk.Advance(time.Second)
		s.f.settle(s.t)
	}
	return 0, last
}

func (s *durableSim) step() {
	switch roll := s.rng.Intn(100); {
	case roll < 45:
		s.decrement()
	case roll < 62:
		s.fill()
	case roll < 69:
		s.kill()
	case roll < 76:
		s.revive()
	case roll < 85:
		s.togglePartition()
	case roll < 93:
		s.setOutage(!s.outage)
	default:
		s.f.clk.Advance(flushInterval)
		s.f.settle(s.t)
		s.logf("advance %v", flushInterval)
	}
}

func (s *durableSim) decrement() {
	live := s.f.live()
	if len(live) == 0 {
		return
	}
	via := live[s.rng.Intn(len(live))]
	key := fmt.Sprintf("item-%d", s.rng.Intn(durCounters))
	l := s.ledger[key]
	if l == nil {
		l = &durLedger{}
		s.ledger[key] = l
	}
	got, err := s.openStock(via).Increment(context.Background(), key, -1, 0)
	switch {
	case err != nil:
		l.unknown++
		s.logf("decrement %s via %s -> %v", key, via, err)
	case s.partitioned && s.minority[via]:
		l.unpromised++
		s.logf("decrement %s via %s -> %d (minority side)", key, via, got)
	case s.f.caches[via].Stats().Members < simRF:
		l.unpromised++
		s.logf("decrement %s via %s -> %d (view below RF)", key, via, got)
	default:
		l.promised = append(l.promised, s.unflushedCrashes)
		s.logf("decrement %s via %s -> %d", key, via, got)
	}
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
	s.f.dead[victim] = true
	_ = s.f.caches[victim].Close(context.Background())
	s.f.memberships[victim].Close()
	s.f.net.Unregister(victim)
	if cut {
		s.unflushedCrashes++
	}
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
