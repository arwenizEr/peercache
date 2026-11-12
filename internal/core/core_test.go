package core_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arwenizEr/peercache/internal/core"
)

// TestNodeCloneIsDeep is small and load-bearing. Nodes are handed out of
// membership views by value, and a shallow copy would let a caller mutate the
// Meta map inside somebody else's view.
func TestNodeCloneIsDeep(t *testing.T) {
	original := core.Node{ID: "n0", Addr: "n0:7946", Meta: map[string]string{"epoch": "1"}}
	cloned := original.Clone()
	cloned.Meta["epoch"] = "2"

	if original.Meta["epoch"] != "1" {
		t.Fatalf("Clone shared the Meta map: original now reads %q", original.Meta["epoch"])
	}
}

func TestNodeCloneHandlesNilMeta(t *testing.T) {
	cloned := core.Node{ID: "n0"}.Clone()
	if cloned.Meta != nil {
		t.Fatalf("Clone invented a Meta map: %v", cloned.Meta)
	}
}

func TestNodeIsZero(t *testing.T) {
	if !(core.Node{}).IsZero() {
		t.Fatal("the zero Node does not report IsZero")
	}
	if (core.Node{ID: "n0"}).IsZero() {
		t.Fatal("a named Node reports IsZero")
	}
}

func TestNodeString(t *testing.T) {
	if got := (core.Node{ID: "n0", Addr: "n0:1"}).String(); got != "n0(n0:1)" {
		t.Fatalf("String() = %q", got)
	}
	if got := (core.Node{ID: "n0"}).String(); got != "n0" {
		t.Fatalf("String() without an address = %q", got)
	}
}

// TestKindStringsAreStable guards log and metric labels. An unnamed kind
// rendering as a bare integer is the kind of thing that only shows up in a
// dashboard at 3am.
func TestKindStringsAreStable(t *testing.T) {
	changes := map[core.ChangeKind]string{
		core.ChangeJoin:   "join",
		core.ChangeLeave:  "leave",
		core.ChangeFail:   "fail",
		core.ChangeUpdate: "update",
	}
	for kind, want := range changes {
		if got := kind.String(); got != want {
			t.Errorf("ChangeKind(%d).String() = %q, want %q", int(kind), got, want)
		}
	}
	if got := core.ChangeKind(99).String(); got != "ChangeKind(99)" {
		t.Errorf("unknown ChangeKind rendered as %q", got)
	}

	events := map[core.EventKind]string{
		core.EventCreate:     "create",
		core.EventUpdate:     "update",
		core.EventDelete:     "delete",
		core.EventActivate:   "activate",
		core.EventDeactivate: "deactivate",
		core.EventCustom:     "custom",
	}
	for kind, want := range events {
		if got := kind.String(); got != want {
			t.Errorf("EventKind(%d).String() = %q, want %q", int(kind), got, want)
		}
	}
	if got := core.EventKind(99).String(); got != "EventKind(99)" {
		t.Errorf("unknown EventKind rendered as %q", got)
	}
}

func TestDefaultOptionsAreValid(t *testing.T) {
	opts := core.DefaultOptions()
	if err := opts.Validate(); !errors.Is(err, core.ErrInvalidOption) {
		t.Fatalf("defaults without a node ID validated as %v, want ErrInvalidOption", err)
	}
	opts.NodeID = "n0"
	if err := opts.Validate(); err != nil {
		t.Fatalf("defaults with a node ID = %v, want nil", err)
	}
	if opts.Clock == nil {
		t.Fatal("default options have no clock")
	}
}

func TestDefaultNamespaceOptionsAreValid(t *testing.T) {
	opts := core.DefaultNamespaceOptions()
	if err := opts.Validate(); err != nil {
		t.Fatalf("default namespace options = %v, want nil", err)
	}
	if opts.ReplicationFactor != core.DefaultReplicationFactor {
		t.Fatalf("default replication factor = %d", opts.ReplicationFactor)
	}
	if opts.L1TTL <= 0 {
		t.Fatalf("default L1 TTL = %v, want a positive bound", opts.L1TTL)
	}
}

// TestNegativeL1TTLDisablesTheTier pins the meaning of a negative bound: it
// disables the tier.
//
// Zero is still rejected: it is what an unset field looks like, and silently
// treating "I forgot" as "I want no caching" would be a trap. Negative is
// explicit — nobody types minus one by accident — and it means the namespace
// wants every non-owned read to be a round trip, with nothing ever served
// stale.
func TestNegativeL1TTLDisablesTheTier(t *testing.T) {
	opts := core.DefaultNamespaceOptions()
	opts.L1TTL = -time.Second
	if err := opts.Validate(); err != nil {
		t.Fatalf("Validate with a negative L1 TTL = %v; negative must be accepted as "+
			"'no non-owned tier'", err)
	}
}

func TestNamespaceOptionsValidation(t *testing.T) {
	tests := map[string]func(*core.NamespaceOptions){
		"replication factor 0": func(o *core.NamespaceOptions) { o.ReplicationFactor = 0 },
		"replication factor 4": func(o *core.NamespaceOptions) { o.ReplicationFactor = 4 },
		"zero L1 TTL":          func(o *core.NamespaceOptions) { o.L1TTL = 0 },
		"nil partitioner":      func(o *core.NamespaceOptions) { o.Partitioner = nil },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			opts := core.DefaultNamespaceOptions()
			mutate(&opts)
			if err := opts.Validate(); !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("Validate = %v, want ErrInvalidOption", err)
			}
		})
	}
}

func TestNopLoggerAcceptsOddArguments(t *testing.T) {
	// An odd number of key/value arguments is a caller bug, and a logger
	// that panicked on one would turn a typo in a debug line into an
	// outage.
	var l core.Logger = core.NopLogger{}
	l.Debug("msg", "key")
	l.Info("msg", "key")
	l.Warn("msg", "key")
	l.Error("msg", "key")
}

func TestHandlerFuncImplementsHandler(t *testing.T) {
	var h core.Handler = core.HandlerFunc(
		func(_ context.Context, from core.Node, method string, req []byte) ([]byte, error) {
			return append([]byte(from.ID+"|"+method+"|"), req...), nil
		})

	got, err := h.Handle(context.Background(), core.Node{ID: "n0"}, "ping", []byte("x"))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if want := "n0|ping|x"; string(got) != want {
		t.Fatalf("Handle = %q, want %q", got, want)
	}
}
