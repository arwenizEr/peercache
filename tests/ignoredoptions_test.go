package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	peercache "github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/clock"
)

const ignoredOptionsMsg = "namespace options ignored: the namespace is already open"

// TestOptionsGivenToAnOpenNamespaceAreWarnedAbout covers open question 26.
//
// Namespace returns the existing namespace and ignores its options, which is
// documented and deliberate — but silent, and it once cost an afternoon: a
// benchmark asked for a one-nanosecond L1 bound on a namespace already opened
// with the default five seconds, and measured local hits as remote reads. The
// first call still wins; a later one whose options would change something is
// now logged, naming the options, so the mistake is visible where it is made.
func TestOptionsGivenToAnOpenNamespaceAreWarnedAbout(t *testing.T) {
	loader := peercache.WithLoader(func(context.Context, string) ([]byte, time.Duration, error) {
		return []byte("v"), 0, nil
	})
	cases := []struct {
		name  string
		first []peercache.NamespaceOption
		again []peercache.NamespaceOption
		warn  string // the options named, or "" for no warning
	}{
		{"no options fetches the handle", []peercache.NamespaceOption{peercache.WithReplicationFactor(2)}, nil, ""},
		{"the same options again", []peercache.NamespaceOption{peercache.WithReplicationFactor(2), peercache.WithL1TTL(time.Second)},
			[]peercache.NamespaceOption{peercache.WithReplicationFactor(2), peercache.WithL1TTL(time.Second)}, ""},
		{"a subset of the same options", []peercache.NamespaceOption{peercache.WithReplicationFactor(2), peercache.WithL1TTL(time.Second)},
			[]peercache.NamespaceOption{peercache.WithL1TTL(time.Second)}, ""},
		{"a different L1 bound", nil, []peercache.NamespaceOption{peercache.WithL1TTL(time.Nanosecond)}, "L1TTL"},
		{"two different options", []peercache.NamespaceOption{peercache.WithReplicationFactor(2)},
			[]peercache.NamespaceOption{peercache.WithReplicationFactor(3), peercache.WithSyncReplication()}, "SyncReplication,ReplicationFactor"},
		{"a loader for a namespace without one", nil, []peercache.NamespaceOption{loader}, "Loader"},
		{"a loader for a namespace with one", []peercache.NamespaceOption{loader}, []peercache.NamespaceOption{loader}, ""},
		{"a nil option", nil, []peercache.NamespaceOption{nil}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &kvWarnings{}
			c, err := peercache.New(peercache.WithNodeID("n0"),
				peercache.WithClock(clock.NewFake(clock.Epoch)), peercache.WithLogger(log))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = c.Close(context.Background()) }()

			c.Namespace("ns", tc.first...)
			if got := log.options(); got != nil {
				t.Fatalf("opening the namespace warned: %v", got)
			}
			c.Namespace("ns", tc.again...)
			got := log.options()
			switch {
			case tc.warn == "" && got != nil:
				t.Fatalf("warned about %v, want no warning", got)
			case tc.warn != "" && (len(got) != 1 || got[0] != tc.warn):
				t.Fatalf("warned about %v, want exactly one warning naming %q", got, tc.warn)
			}
		})
	}
}

// TestIgnoredOptionsAreWarnedAboutOnce keeps the warning from flooding a log:
// Namespace is documented as cheap enough for a hot path, so a mistaken call
// may run millions of times.
func TestIgnoredOptionsAreWarnedAboutOnce(t *testing.T) {
	log := &kvWarnings{}
	c, err := peercache.New(peercache.WithNodeID("n0"),
		peercache.WithClock(clock.NewFake(clock.Epoch)), peercache.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	c.Namespace("ns")
	for i := 0; i < 100; i++ {
		c.Namespace("ns", peercache.WithL1TTL(time.Nanosecond))
	}
	if got := log.options(); len(got) != 1 {
		t.Fatalf("warned %d times, want once: %v", len(got), got)
	}
	// Another namespace is another mistake.
	c.Namespace("other")
	c.Namespace("other", peercache.WithL1TTL(time.Nanosecond))
	if got := log.options(); len(got) != 2 {
		t.Fatalf("a second namespace's ignored options were not warned about: %v", got)
	}
}

// kvWarnings records the options named by each ignored-options warning.
type kvWarnings struct {
	mu    sync.Mutex
	named []string
}

func (w *kvWarnings) Debug(string, ...any) {}
func (w *kvWarnings) Info(string, ...any)  {}
func (w *kvWarnings) Error(string, ...any) {}
func (w *kvWarnings) Warn(msg string, kv ...any) {
	if msg != ignoredOptionsMsg {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i] == "options" {
			w.named = append(w.named, fmt.Sprint(kv[i+1]))
		}
	}
}

func (w *kvWarnings) options() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.named...)
}

// TestATypedNamespaceOpenedAgainIsNotWarnedAbout: NamespaceOf is typically
// called wherever the typed handle is needed, with the same typed loader each
// time. Opening the namespace turns that loader into a byte-valued one, which
// must not then read as a difference.
func TestATypedNamespaceOpenedAgainIsNotWarnedAbout(t *testing.T) {
	log := &kvWarnings{}
	c, err := peercache.New(peercache.WithNodeID("n0"),
		peercache.WithClock(clock.NewFake(clock.Epoch)), peercache.WithLogger(log))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close(context.Background()) }()

	load := peercache.WithTypedLoader(func(context.Context, string) (int, time.Duration, error) { return 1, 0, nil })
	peercache.NamespaceOf[int](c, "typed", load)
	peercache.NamespaceOf[int](c, "typed", load)
	if got := log.options(); got != nil {
		t.Fatalf("reopening a typed namespace with its own loader warned: %v", got)
	}

	peercache.NamespaceOf[int](c, "plain")
	peercache.NamespaceOf[int](c, "plain", load)
	if got := log.options(); len(got) != 1 || got[0] != "TypedLoader" {
		t.Fatalf("a typed loader for a namespace without one: warned %v, want [TypedLoader]", got)
	}
}
