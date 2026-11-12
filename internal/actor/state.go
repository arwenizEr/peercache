package actor

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
)

// State is the actor-turn view of a group's memory, satisfying
// [core.GroupState].
//
// Its methods take no context and return no error because they are plain map
// access on the group's single activation goroutine. There is nothing to fail
// and nothing to wait for; that is the payoff of moving computation to the data.
//
// # Validity
//
// A State is valid for exactly one turn. Every method compares the generation
// it was stamped with against the group's current generation, and does nothing
// if they differ.
//
// That check is not bookkeeping — it is what stops an escaped State from being
// a data race. A group function that stores its State in a goroutine and writes
// through it later would otherwise be writing to a map that a *different* turn
// is reading, with no lock between them, and the race detector would only find
// it if that interleaving happened to occur during a test.
//
// Violating this must take a panic-free error path.
// There is a subtlety: the goroutine holding an escaped State is not inside a
// Call, so there is nobody to return an error to. The methods therefore behave
// as no-ops and the violation is counted. The counter reaches Stats().StateEscapes
// and peercache_state_escapes_total. Silence would be wrong; a panic in someone
// else's goroutine would be worse.
type State struct {
	rec *record
	gen uint64

	// escapes points at the runtime's counter so that a violation is
	// observable without reaching back through the record.
	escapes *atomic.Int64
}

// valid reports whether this State still belongs to the running turn, counting
// the violation if it does not.
func (s *State) valid() bool {
	if s.rec.generation.Load() == s.gen {
		return true
	}
	s.escapes.Add(1)
	return false
}

// ID implements [core.GroupState].
func (s *State) ID() string { return s.rec.id }

// Get implements [core.GroupState]. The returned slice is the stored one and
// must not be modified, matching the aliasing contract used everywhere else.
func (s *State) Get(key string) ([]byte, bool) {
	if !s.valid() {
		return nil, false
	}
	val, ok := s.rec.state[key]
	return val, ok
}

// Set implements [core.GroupState]. val is copied, so the caller may reuse the
// buffer.
func (s *State) Set(key string, val []byte) {
	if !s.valid() {
		return
	}
	if s.rec.state == nil {
		// Allocated on first write. An addressable group that is never
		// written to should not carry a map header, because the whole
		// point of separating records from activations is that a million
		// idle groups stay cheap.
		s.rec.state = make(map[string][]byte, 1)
	}
	stored := make([]byte, len(val))
	copy(stored, val)

	_, existed := s.rec.state[key]
	s.rec.state[key] = stored
	if existed {
		s.rec.emit(core.Event{Kind: core.EventUpdate, GroupID: s.rec.id, Key: key, Value: stored})
	} else {
		s.rec.emit(core.Event{Kind: core.EventCreate, GroupID: s.rec.id, Key: key, Value: stored})
	}
}

// Delete implements [core.GroupState]. Deleting an absent key is not an error.
func (s *State) Delete(key string) {
	if !s.valid() {
		return
	}
	if _, existed := s.rec.state[key]; !existed {
		return
	}
	delete(s.rec.state, key)
	s.rec.emit(core.Event{Kind: core.EventDelete, GroupID: s.rec.id, Key: key})
}

// Keys implements [core.GroupState]. The result is sorted, so that a group
// function iterating its own keys behaves the same way on every pod — map order
// would make one node's output differ from another's for no visible reason.
func (s *State) Keys() []string {
	if !s.valid() {
		return nil
	}
	out := make([]string, 0, len(s.rec.state))
	for key := range s.rec.state {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// ScheduleTimer implements [core.GroupState].
//
// When the timer fires it runs the registered function fn against this group as
// another turn. Re-arming an existing name replaces it, so a timer can be
// pushed back without accumulating duplicates.
//
// The schedule outlives deactivation: it is stored on the record with an
// absolute deadline and re-armed on the next activation.
func (s *State) ScheduleTimer(name string, after time.Duration, fn string, arg []byte) {
	if !s.valid() {
		return
	}
	s.rec.scheduleTimer(name, after, fn, arg)
}

// CancelTimer implements [core.GroupState]. Cancelling an unknown name is a
// no-op.
func (s *State) CancelTimer(name string) {
	if !s.valid() {
		return
	}
	s.rec.cancelTimer(name)
}

// Emit implements [core.GroupState]. Delivery is best effort; nothing may
// depend on an event being observed.
func (s *State) Emit(event core.Event) {
	if !s.valid() {
		return
	}
	if event.GroupID == "" {
		event.GroupID = s.rec.id
	}
	s.rec.emit(event)
}

var _ core.GroupState = (*State)(nil)
