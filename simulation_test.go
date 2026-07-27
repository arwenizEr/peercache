package peercache_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/peercache/peercache"
	"github.com/peercache/peercache/internal/ownership"
	"github.com/peercache/peercache/internal/testkit"
)

// Deterministic simulation: one seed drives an entire fault schedule, and the
// same seed reproduces it exactly.
//
// # Why this is different from the chaos test
//
// TestChaosLosesNoSynchronouslyWrittenKey runs one hand-written schedule: kill a
// random node every two seconds for sixty. It is a good test and it has found
// real bugs, but it explores one shape of failure. What it cannot do is find the
// schedule nobody thought to write — the partition that heals while a drain is
// in flight, the node that returns during its own replacement's catch-up.
//
// This generates schedules instead. Every decision — which fault, which node,
// how long, which key — comes from one seeded generator, so a failure is a
// single integer that reproduces the entire run.
//
// # Why it is deterministic
//
// Operations are issued sequentially from the test goroutine and time is
// advanced explicitly. Nothing here races: the interleaving is the schedule, not
// the Go scheduler. That is the whole point — a simulation whose outcome depends
// on which goroutine woke first is not reproducible from a seed, and a seed you
// cannot replay is a bug report you cannot act on.
//
// Concurrency is covered elsewhere, under -race and by the linearizability
// checks. This covers *fault schedules*, which need reproducibility more than
// they need parallelism.
//
// # Running it
//
//	go test . -run TestSimulation                       # 8 seeds, the CI gate
//	PEERCACHE_SIM_SEEDS=2000 go test . -run TestSimulation -timeout 2h
//	PEERCACHE_SEED=12345 go test . -run TestSimulation   # replay one failure
//
// # Reading a skipped seed
//
// A SKIP means that schedule excused every key it acknowledged — enough nodes
// were destroyed after each write that losing it is correct behaviour — so it
// asserted nothing. That is a sampling outcome rather than a defect, and it is
// not failed. What is failed is a *sweep* where more than a quarter of the seeds
// land there: at that point either the fault rate has outrun what the
// replication factor survives, or the excusal rule has grown too generous, and a
// green sweep would mean nothing either way.

// simSeeds is how many schedules to run. Small by default so the suite stays
// usable; the nightly workflow raises it.
func simSeeds(t *testing.T) int {
	t.Helper()
	raw := os.Getenv("PEERCACHE_SIM_SEEDS")
	if raw == "" {
		return 8
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		t.Fatalf("PEERCACHE_SIM_SEEDS=%q: want a positive integer", raw)
	}
	return n
}

// simSteps is how many scheduled events each run performs.
const simSteps = 60

// simRF is the replication factor every simulated namespace uses.
const simRF = 3

// simStepClock is how far simulated time advances per step. Sixty steps at
// thirty seconds is half an hour of simulated time per seed; two thousand seeds
// is roughly forty simulated days.
const simStepClock = 30 * time.Second

// TestSimulation runs randomized fault schedules and asserts the one property
// that must survive all of them.
//
// The property: a key whose write was acknowledged under synchronous
// replication is still readable once the cluster is healed and settled. That is
// the strongest claim this system makes about durability, and CLAUDE.md is
// explicit there is no other — nothing is written to disk, so a key that no
// live node holds is gone.
func TestSimulation(t *testing.T) {
	seeds := simSeeds(t)
	base := testkit.Seed(t)
	t.Logf("running %d schedules from base seed %d", seeds, base)

	// A schedule can legitimately prove nothing: if enough nodes were destroyed
	// after a key was written, losing it is correct behaviour and the key is
	// excused. Such a seed is skipped rather than failed -- it is a sampling
	// outcome, not a defect -- but a *corpus* that mostly proves nothing is
	// worthless, so the proportion is gated here.
	//
	// The distinction matters because the excusal rule is deliberately
	// conservative: it counts every destruction in the run rather than the
	// destruction of a key's own holders, so a long schedule with many kills
	// excuses keys that were in fact safe. Making individual seeds fail for that
	// would train people to ignore the simulation; making the aggregate fail
	// keeps the guarantee that a green sweep means something.
	vacuous := 0
	for i := 0; i < seeds; i++ {
		seed := base + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			if !runSimulation(t, seed) {
				vacuous++
				t.Skip("this schedule excused every acknowledged key")
			}
		})
	}

	if vacuous == seeds {
		t.Fatalf("all %d schedules excused every key they acknowledged; the sweep "+
			"asserted nothing and passing means nothing", seeds)
	}
	if limit := seeds / 4; seeds >= 8 && vacuous > limit {
		t.Fatalf("%d of %d schedules (%d%%) excused every key they acknowledged, "+
			"above the %d permitted. Either the fault rate has outrun what "+
			"replication factor %d can survive, or the excusal rule has grown too "+
			"generous -- both make a green sweep meaningless",
			vacuous, seeds, 100*vacuous/seeds, limit, simRF)
	}
	t.Logf("%d of %d schedules asserted at least one key", seeds-vacuous, seeds)
}

// simulation holds one run's state.
type simulation struct {
	t   *testing.T
	f   *fabric
	rng *rand.Rand

	// durable records keys acknowledged under synchronous replication, with
	// the value the write reported success for.
	durable map[string]string

	// destroyedAt records how many nodes had been destroyed when each key was
	// written; destroyed counts them.
	//
	// This is what makes the assertion sound. Replication factor 3 promises a
	// key survives losing up to two of its copies and promises nothing once
	// all three are gone -- CLAUDE.md is explicit that a hard kill loses that
	// node's memory and a revived node comes back empty.
	//
	// The first version asserted survival unconditionally and found real
	// losses: keys written during a partition whose every holder was then
	// killed, one at a time, across the schedule. That is documented
	// behaviour, and a simulation that reports it as a bug is one nobody will
	// trust the next time it fires.
	//
	// Counting every destruction rather than only a key's own holders is
	// deliberately conservative. It can excuse a key that was actually safe;
	// it can never manufacture a failure.
	destroyedAt map[string]int
	destroyed   int

	// degraded counts writes acknowledged by a node whose view was too small
	// to satisfy the replication factor. See question 37.
	degraded int

	// duringSplit counts writes acknowledged on the *minority* side of a
	// split, which the design does not promise to keep.
	duringSplit int

	// majorityDurable records keys acknowledged on the majority side of a
	// split, with the value the write reported success for. They are asserted
	// separately from `durable` because they answer question 38 rather than
	// the general durability question -- see the assertion in runSimulation.
	majorityDurable map[string]string
	majorityAt      map[string]int

	// partitioned tracks whether the network is currently split, so the
	// schedule can heal what it broke. minority holds the smaller side while
	// it lasts, so a write can be attributed to a side rather than merely to
	// "during a partition".
	partitioned bool
	minority    map[string]bool

	events []string
}

func (s *simulation) logf(format string, args ...any) {
	s.events = append(s.events, fmt.Sprintf(format, args...))
}

// history renders the schedule for a failure message. A failing seed is only
// useful if the thing it reproduces can be read.
func (s *simulation) history() string {
	out := ""
	for i, e := range s.events {
		out += fmt.Sprintf("  %2d. %s\n", i+1, e)
	}
	return out
}

// runSimulation runs one schedule and reports whether it asserted anything.
func runSimulation(t *testing.T, seed int64) bool {
	// Detection faster than the fault cadence. M5 recorded why: a cluster
	// that loses a node faster than it notices never converges, and the run
	// then measures a permanently confused system rather than a recovering
	// one.
	f := newFabricFull(t, 5, false, testkit.MembershipConfig{
		FailureTimeout: 300 * time.Millisecond,
		JoinDelay:      200 * time.Millisecond,
	}, ownership.DefaultActivationDelay)

	s := &simulation{
		t:               t,
		f:               f,
		rng:             rand.New(rand.NewSource(seed)),
		durable:         make(map[string]string),
		destroyedAt:     make(map[string]int),
		majorityDurable: make(map[string]string),
		majorityAt:      make(map[string]int),
		minority:        make(map[string]bool),
	}

	openNS := func(id string) peercache.Namespace {
		return f.caches[id].Namespace("sim",
			peercache.WithReplicationFactor(simRF),
			peercache.WithSyncReplication())
	}
	for _, id := range f.ids {
		openNS(id)
	}
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	for step := 0; step < simSteps; step++ {
		s.step(step, openNS)
	}

	// Heal everything and let the cluster finish reacting. A property
	// asserted mid-fault would be asserting about a transient, which is a
	// real state but never the one being claimed.
	if s.partitioned {
		f.net.Heal()
		s.logf("heal (final)")
		s.partitioned = false
		clear(s.minority)
	}
	for _, id := range f.ids {
		if f.dead[id] {
			f.revive(t, id)
			openNS(id)
			s.logf("revive %s (final)", id)
		}
	}
	f.clk.Advance(4 * testkit.DefaultJoinDelay)
	f.convergeTo(t, len(f.ids))
	f.clk.Advance(2 * ownership.DefaultActivationDelay)
	f.settle(t)

	// Every revived node needs catch-up; M5 made that an explicit call
	// rather than an automatic one, and a simulation that skipped it would
	// be testing a cluster no operator would run.
	ctx := context.Background()
	for _, id := range f.ids {
		if err := f.caches[id].CatchUp(ctx); err != nil {
			t.Fatalf("seed %d: CatchUp(%s): %v\n%s", seed, id, err, s.history())
		}
	}
	f.settle(t)

	// The assertion.
	var lost []string
	asserted, excused := 0, 0
	for key, want := range s.durable {
		if s.destroyed-s.destroyedAt[key] >= simRF {
			// Every copy of this key could legitimately have been
			// destroyed since it was written. See destroyedAt.
			excused++
			continue
		}
		asserted++
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil || got != want {
			lost = append(lost, fmt.Sprintf("%s (want %q, got %q, err %v)", key, want, got, err))
			if len(lost) >= 5 {
				break
			}
		}
	}
	if len(lost) > 0 {
		t.Fatalf("seed %d: %d of %d synchronously acknowledged keys did not survive "+
			"fewer than %d node destructions.\n"+
			"Replay with PEERCACHE_SEED=%d.\nlost: %v\nschedule:\n%s",
			seed, len(lost), asserted, simRF, seed, lost, s.history())
	}
	// Question 38's assertion, stated separately because a failure here means
	// something different from a failure above.
	//
	// Question 38 records a seed that lost two synchronously acknowledged keys
	// after a single destruction at RF=3, which RF=3 is meant to survive. The
	// investigation ruled out replica eviction by reproducing it and failing
	// to, and settled on the documented AP window as the remaining
	// explanation -- by inference. The note says plainly what would close it:
	// assert that a write acknowledged on the *majority* side of a split
	// survives the heal.
	//
	// If this holds across seeds, the earlier losses were minority-side and
	// the system is behaving exactly as CLAUDE.md non-negotiable 5 describes.
	// If it fails, post-partition reconciliation is dropping writes the
	// majority accepted, and open question 37's option 3 -- refusing a
	// synchronous write that cannot place its copies -- becomes considerably
	// more attractive.
	var lostMajority []string
	majorityAsserted, majorityExcused := 0, 0
	for key, want := range s.majorityDurable {
		if s.destroyed-s.majorityAt[key] >= simRF {
			majorityExcused++
			continue
		}
		majorityAsserted++
		got, err := readAnywhere(t, f, openNS, key)
		if err != nil || got != want {
			lostMajority = append(lostMajority, fmt.Sprintf("%s (want %q, got %q, err %v)", key, want, got, err))
			if len(lostMajority) >= 5 {
				break
			}
		}
	}
	if len(lostMajority) > 0 {
		t.Fatalf("seed %d: %d of %d keys acknowledged on the MAJORITY side of a partition "+
			"did not survive the heal.\n"+
			"This is open question 38's assertion, and a failure means the losses it "+
			"records were not the documented AP window: the larger side of a split "+
			"accepted these writes and reconciliation discarded them anyway.\n"+
			"Replay with PEERCACHE_SEED=%d.\nlost: %v\nschedule:\n%s",
			seed, len(lostMajority), majorityAsserted, seed, lostMajority, s.history())
	}

	// A schedule that excused every key proved nothing, and a green run that
	// proved nothing is worse than a red one: it is a seed the corpus counts as
	// covered.
	//
	// Both pools count. Splitting majority-side writes out for question 38 left
	// this guard reading only the general pool, and it began failing on
	// schedules that had asserted a dozen keys -- correctly, by its own wording,
	// and uselessly. A vacuity check that fires on a run which did assert
	// something teaches people to ignore vacuity checks.
	if asserted+majorityAsserted == 0 {
		t.Logf("seed %d: all %d acknowledged keys (%d general, %d majority-side) were "+
			"excused because %d nodes were destroyed",
			seed, len(s.durable)+len(s.majorityDurable), len(s.durable),
			len(s.majorityDurable), s.destroyed)
		return false
	}

	t.Logf("seed %d: %d keys survived %d faults (%d excused: >=%d destructions; "+
		"%d acknowledged below RF, %d on a minority side); "+
		"%d majority-side writes across partitions survived the heal (%d excused)",
		seed, asserted, simSteps, excused, simRF, s.degraded, s.duringSplit,
		majorityAsserted, majorityExcused)
	return true
}

// step performs one scheduled event.
func (s *simulation) step(n int, openNS func(string) peercache.Namespace) {
	f := s.f
	live := f.live()

	// Weighted so writes dominate: a schedule that is mostly faults never
	// accumulates enough state for the faults to threaten anything.
	switch roll := s.rng.Intn(100); {
	case roll < 45:
		s.write(n, openNS)

	case roll < 65:
		s.read(n, openNS)

	case roll < 78:
		// Kill, but never below the replication factor: with RF=3, losing
		// a third node can legitimately lose data, and asserting no loss
		// would be asserting something the design does not claim.
		if len(live) <= 3 {
			s.write(n, openNS)
			return
		}
		// Membership cannot converge across a partition, so a kill while
		// split would wait for a state that cannot happen. Heal first and
		// record it: the schedule has to stay readable to be replayable.
		s.healIfSplit()
		live = s.f.live()
		victim := live[s.rng.Intn(len(live))]
		f.hardKill(s.t, victim)
		s.destroyed++
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		f.convergeTo(s.t, len(f.live()))
		f.settle(s.t)
		s.logf("kill %s (%d live)", victim, len(f.live()))

	case roll < 88:
		var dead []string
		for _, id := range f.ids {
			if f.dead[id] {
				dead = append(dead, id)
			}
		}
		if len(dead) == 0 {
			s.write(n, openNS)
			return
		}
		id := dead[s.rng.Intn(len(dead))]
		s.healIfSplit()
		f.revive(s.t, id)
		openNS(id)
		f.clk.Advance(2 * testkit.DefaultJoinDelay)
		f.convergeTo(s.t, len(f.live()))
		f.settle(s.t)
		if err := f.caches[id].CatchUp(context.Background()); err != nil {
			s.t.Fatalf("CatchUp(%s) after revive: %v", id, err)
		}
		s.logf("revive %s and catch up", id)

	case roll < 95:
		if s.partitioned {
			f.net.Heal()
			s.partitioned = false
			clear(s.minority)
			f.clk.Advance(2 * testkit.DefaultFailureTimeout)
			f.convergeTo(s.t, len(f.live()))
			f.settle(s.t)
			s.logf("heal")
			return
		}
		if len(live) < 4 {
			s.write(n, openNS)
			return
		}
		// A minority/majority split. The minority stops activating, which
		// is the M3 guard doing its job.
		//
		// `cut` is drawn so that the first side is always *strictly* smaller
		// than the second. That is what makes "minority" and "majority" mean
		// something here rather than being two labels for two halves, and
		// question 38's assertion depends on the distinction: a write is
		// promised to survive the heal only on the larger side.
		//
		// The bound is (len-1)/2 rather than len/2. With four live nodes the
		// latter admits cut=2, an even 2|2 split with no majority at all —
		// which is a legitimate partition to test and not one this assertion
		// can say anything about. Even splits are therefore not generated;
		// what they would exercise is already covered by the minority-side
		// path, which asserts nothing about survival either way.
		cut := 1 + s.rng.Intn((len(live)-1)/2)
		minority := append([]string(nil), live[:cut]...)
		majority := append([]string(nil), live[cut:]...)
		if len(minority) >= len(majority) {
			s.t.Fatalf("the schedule split %d live nodes into %d | %d; question 38's "+
				"assertion is about the larger side, so a split that is not strictly "+
				"uneven would assert the wrong thing",
				len(live), len(minority), len(majority))
		}
		f.net.Partition(minority, majority)
		s.partitioned = true
		clear(s.minority)
		for _, id := range minority {
			s.minority[id] = true
		}
		f.clk.Advance(2 * testkit.DefaultFailureTimeout)
		s.logf("partition %v | %v", minority, majority)

	default:
		f.clk.Advance(simStepClock)
		f.settle(s.t)
		s.logf("advance %v", simStepClock)
	}
}

// healIfSplit repairs a partition before a membership change.
//
// Convergence to full membership is impossible across a split, so a kill or
// revive issued while partitioned waits for a state that cannot occur. The
// first version of this simulation did exactly that and every seed failed with
// "membership did not converge" -- a harness bug that looked like a cluster bug,
// which is the failure mode a simulation must not have.
func (s *simulation) healIfSplit() {
	if !s.partitioned {
		return
	}
	s.f.net.Heal()
	s.partitioned = false
	clear(s.minority)
	s.f.clk.Advance(2 * testkit.DefaultFailureTimeout)
	// convergeTo the live count, not converge(): nodes killed earlier in the
	// schedule are still dead and never coming back on their own, so waiting
	// for the full roster waits forever. That was the second harness bug to
	// present itself as "membership did not converge".
	s.f.convergeTo(s.t, len(s.f.live()))
	s.f.settle(s.t)
	s.logf("heal (before a membership change)")
}

// write attempts a synchronous write and records it as durable only if the
// cluster said it succeeded.
//
// The distinction is the entire basis of the assertion. A write that returned an
// error was never acknowledged, so losing it is correct behaviour; recording it
// would make the simulation fail on a system that is working.
func (s *simulation) write(n int, openNS func(string) peercache.Namespace) {
	live := s.f.live()
	if len(live) == 0 {
		return
	}
	via := live[s.rng.Intn(len(live))]
	key := fmt.Sprintf("sim/%d", n)
	val := fmt.Sprintf("v%d-%d", n, s.rng.Intn(1000))

	err := openNS(via).Set(context.Background(), key, []byte(val), 0)
	switch {
	case err == nil:
		// A write acknowledged by a node whose own view is smaller than the
		// replication factor cannot have placed RF copies, whatever it
		// returned. That is question 37: an isolated node ranks itself the
		// owner of everything, replicates to the peers it can see -- itself
		// -- and reports success.
		//
		// Not asserted, because the behaviour is currently by design and a
		// simulation that fails on documented behaviour trains people to
		// ignore it. Counted and logged, so the schedule shows how often the
		// window was entered.
		if view := s.f.caches[via].Stats().Members; view < simRF {
			s.degraded++
			s.logf("write %s via %s -> ok BUT the writer saw only %d members "+
				"(question 37: acknowledged below RF)", key, via, view)
			return
		}
		// A write acknowledged on the *minority* side of a split is not
		// promised to survive the heal. CLAUDE.md non-negotiable 5 is
		// explicit that a partition can produce two activations of one group
		// and that conflicting writes are detected, not merged -- so the
		// losing side's acknowledged write is discarded by design.
		//
		// Excluding these was the third correction this invariant needed.
		// Filtering only on the writer's view size was not enough: a writer
		// on the smaller side of a split can still see enough members to
		// satisfy RF, place its copies within that side, and lose them
		// anyway when the sides reconcile.
		if s.partitioned && s.minority[via] {
			s.duringSplit++
			s.logf("write %s via %s -> ok BUT the writer was on the minority side "+
				"(AP window: not promised to survive the heal)", key, via)
			return
		}
		// The majority side is question 38's open assertion, and it is
		// tracked separately rather than folded into `durable`.
		//
		// Question 38 records a seed that lost two synchronously acknowledged
		// keys after a single destruction at RF=3, and closes by saying the
		// remaining explanation -- that those writes were on the losing side
		// of a split -- was reached by inference and not by demonstration.
		// What would settle it is precisely this: assert that a write taken
		// by the *larger* side survives the heal. If it holds, the earlier
		// losses were minority-side and the design is behaving as documented.
		// If it does not, post-partition reconciliation has a real defect.
		//
		// Separate from `durable` because a failure here means something
		// different from a failure there, and a single message covering both
		// would send the next reader to the wrong place.
		if s.partitioned {
			s.majorityDurable[key] = val
			s.majorityAt[key] = s.destroyed
			s.logf("write %s via %s -> ok on the majority side (question 38)", key, via)
			return
		}
		s.durable[key] = val
		s.destroyedAt[key] = s.destroyed
		s.logf("write %s via %s -> ok", key, via)
	case retryable(err):
		s.logf("write %s via %s -> retryable (%v)", key, via, err)
	default:
		s.t.Fatalf("write %s via %s: unexpected error %v", key, via, err)
	}
}

func (s *simulation) read(n int, openNS func(string) peercache.Namespace) {
	live := s.f.live()
	if len(live) == 0 || len(s.durable) == 0 {
		return
	}
	// Read something that was actually written.
	for key, want := range s.durable {
		via := live[s.rng.Intn(len(live))]
		got, err := openNS(via).Get(context.Background(), key)
		switch {
		case err == nil && string(got) == want:
			s.logf("read %s via %s -> ok", key, via)
		case err == nil:
			// A non-owned copy may be stale within its TTL; that is
			// documented and not a failure. The final assertion, made
			// after healing and settling, is the one that counts.
			s.logf("read %s via %s -> stale (%q, want %q)", key, via, got, want)
		case retryable(err) || errors.Is(err, peercache.ErrNotFound):
			s.logf("read %s via %s -> %v", key, via, err)
		default:
			s.t.Fatalf("read %s via %s: unexpected error %v", key, via, err)
		}
		return
	}
}

// readAnywhere tries every live node, because ownership may have moved and a
// single node's answer is not the cluster's.
//
// # Why it advances the clock
//
// A membership change starts the activation guard's delay on every group the
// change moved, and a read arriving inside that window is refused with
// [peercache.ErrActivationPending] — a retryable error meaning "ask again
// shortly", not an answer about whether the key exists. Retrying without letting
// time pass asks again at the same instant and gets the same refusal, so a
// helper that only loops reports a deferral as a loss.
//
// That is not hypothetical: it accounted for more than half of what the first
// version of the question 38 assertion called lost keys. The distinction matters
// exactly here, because the difference between "deferred" and "not found" is the
// difference between a working cluster and a defect.
//
// Advancing the fake clock is how every other wait in this suite is expressed;
// CLAUDE.md forbids sleeping and requires the fake clock, so this is the
// sanctioned form of "shortly".
func readAnywhere(t *testing.T, f *fabric, openNS func(string) peercache.Namespace, key string) (string, error) {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		for _, id := range f.live() {
			got, err := openNS(id).Get(context.Background(), key)
			if err == nil {
				return string(got), nil
			}
			lastErr = err
		}
		if !errors.Is(lastErr, peercache.ErrActivationPending) {
			// Nothing else here is fixed by waiting. A not-found stays
			// not-found, and looping on it only makes a failure slower
			// to report.
			return "", lastErr
		}
		f.clk.Advance(2 * ownership.DefaultActivationDelay)
		f.settle(t)
	}
	return "", lastErr
}
