package peercache_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The tests in this file enforce the engineering rules from CLAUDE.md that no
// compiler can. They read the repository as text, which is crude, but a rule
// that is only written down is a rule that erodes.
//
// The forbidden identifiers are assembled at runtime rather than written as
// literals, so that this file does not trip its own checks.

// clockPackage is the primary directory allowed to call into the time package
// for the current instant or to block on it.
const clockPackage = "internal/clock"

// realTimePackages are the directories exempt from the injected-clock rule.
//
// Both exemptions are real, not conveniences. Each wraps a third-party library
// whose timers run on the wall clock and which no injected clock reaches, so
// the adapter has to speak the same units.
//
// internal/membership wraps memberlist, whose probes, timeouts and suspicion
// timers are its own — bounding a graceful Leave means handing memberlist a
// real duration.
//
// internal/transport wraps gRPC, whose keepalive pings, dial backoff and
// deadline propagation are likewise internal. A fake clock cannot make gRPC
// believe a connection has been idle.
//
// The exemption is narrow on purpose. Everything that *depends* on membership —
// ownership, the activation guard — takes an injected clock and is tested
// deterministically against internal/testkit instead.
var realTimePackages = []string{
	"internal/clock",
	"internal/membership",
	"internal/transport",
}

// realTimeFiles are individual files exempt from the same rule, for the same
// reason: each one drives a real gossip cluster, and gossip converges on
// memberlist's own timers where no injected clock reaches.
//
// They are listed by name rather than by directory so that the exemption cannot
// spread. acceptance_m4_test.go measures latency over real gRPC, which no
// injected clock can time. cluster_test.go is the only test in the repository
// that runs two real caches against real gossip — it exists because every other M3 test verifies a
// seam in isolation and none of them proves the seams are connected. examples/m3
// demonstrates the same thing to a reader.
//
// Both bound every wait and poll rather than sleeping, so the rule they are
// exempt from is the injected-clock rule only. The no-time.Sleep rule still
// applies to them and is enforced for the whole repository, with no exemptions
// at all.
var realTimeFiles = []string{
	"cluster_test.go",
	"acceptance_m4_test.go",
	"lossfabric_test.go",
	"examples/m3/main.go",
	"examples/m4/main.go",
	"examples/m5/main.go",
	"examples/m6/main.go",
}

// exemptFromClockRule reports whether path may use the time package directly.
func exemptFromClockRule(path string) bool {
	for _, dir := range realTimePackages {
		if strings.HasPrefix(path, dir+"/") {
			return true
		}
	}
	for _, file := range realTimeFiles {
		if path == file {
			return true
		}
	}
	return false
}

// walkGoFiles calls fn for every .go file in the repository, with a
// slash-separated path relative to the root.
func walkGoFiles(t *testing.T, fn func(path string, content string)) {
	t.Helper()
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fn(filepath.ToSlash(path), string(body))
		return nil
	})
	if err != nil {
		t.Fatalf("walking the repository: %v", err)
	}
}

// TestNoSleepAnywhere enforces the hardest of CLAUDE.md's rules. A sleeping
// test is a broken test: it is slow when it passes and flaky when it does not,
// and it hides the very interleavings it pretends to exercise.
func TestNoSleepAnywhere(t *testing.T) {
	needle := "time." + "Sleep("
	walkGoFiles(t, func(path, content string) {
		if path == "discipline_test.go" {
			return
		}
		if strings.Contains(content, needle) {
			t.Errorf("%s calls %s; use the fake clock", path, needle)
		}
	})
}

// TestNoDirectTimeAccessOutsideClockPackage enforces the injected clock. A
// single stray time.Now is enough to make a timeout untestable without waiting
// for it.
func TestNoDirectTimeAccessOutsideClockPackage(t *testing.T) {
	forbidden := []string{
		"time." + "Now(",
		"time." + "Since(",
		"time." + "After(",
		"time." + "Tick(",
		"time." + "NewTimer(",
		"time." + "NewTicker(",
		"time." + "AfterFunc(",
	}
	walkGoFiles(t, func(path, content string) {
		if path == "discipline_test.go" || exemptFromClockRule(path) {
			return
		}
		for _, needle := range forbidden {
			if strings.Contains(content, needle) {
				t.Errorf("%s calls %s outside %s; take a clock.Clock instead", path, needle, clockPackage)
			}
		}
	})
}

// TestNoContextTimeoutOutsideClockPackage catches the subtler version of the
// same mistake. context.WithTimeout reads the wall clock, so a component that
// uses it cannot have its deadline behaviour tested without real waiting;
// clock.WithTimeout exists for exactly this.
func TestNoContextTimeoutOutsideClockPackage(t *testing.T) {
	forbidden := []string{
		"context." + "WithTimeout(",
		"context." + "WithDeadline(",
	}
	walkGoFiles(t, func(path, content string) {
		if path == "discipline_test.go" || exemptFromClockRule(path) {
			return
		}
		for _, needle := range forbidden {
			if strings.Contains(content, needle) {
				t.Errorf("%s calls %s outside %s; use clock.WithTimeout", path, needle, clockPackage)
			}
		}
	})
}

// TestNoLogPackage enforces the rule that a library does not choose an
// application's logging.
func TestNoLogPackage(t *testing.T) {
	walkGoFiles(t, func(path, content string) {
		if path == "discipline_test.go" {
			return
		}
		for _, needle := range []string{"\n\t\"log\"\n", "\n\t\"log/slog\"\n"} {
			if strings.Contains(content, needle) {
				t.Errorf("%s imports the log package; use the Logger interface", path)
			}
		}
	})
}

// TestInternalPackagesDoNotImportRoot enforces the layering that keeps
// internal/core worth having. The day this fails is the day the import cycle
// comes back.
func TestInternalPackagesDoNotImportRoot(t *testing.T) {
	const rootImport = `"github.com/peercache/peercache"`
	walkGoFiles(t, func(path, content string) {
		if !strings.HasPrefix(path, "internal/") || strings.HasSuffix(path, "_test.go") {
			return
		}
		if strings.Contains(content, rootImport) {
			t.Errorf("%s imports the root package; internal packages must not", path)
		}
	})
}

// TestEveryPackageHasDocGo enforces CLAUDE.md's documentation rule at the
// package level. A package without a doc.go is a package whose purpose lives
// only in whoever wrote it.
func TestEveryPackageHasDocGo(t *testing.T) {
	packages := make(map[string]bool)
	walkGoFiles(t, func(path, _ string) {
		if strings.HasSuffix(path, "_test.go") {
			return
		}
		// Commands document themselves above `package main`, which is the
		// convention go doc expects; a separate doc.go there would be
		// worse, not better.
		if strings.HasPrefix(path, "examples/") {
			return
		}
		dir := filepath.ToSlash(filepath.Dir(path))
		packages[dir] = packages[dir] || filepath.Base(path) == "doc.go"
	})
	for dir, hasDoc := range packages {
		if !hasDoc {
			t.Errorf("package %s has no doc.go", dir)
		}
	}
}

// TestNoTODOMarkers keeps the "finish something" rule honest. Scaffolding is
// declared by returning ErrNotImplemented, which is typed and testable, rather
// than by a comment nobody greps for.
func TestNoTODOMarkers(t *testing.T) {
	needle := "TO" + "DO"
	walkGoFiles(t, func(path, content string) {
		if path == "discipline_test.go" {
			return
		}
		if strings.Contains(content, needle) {
			t.Errorf("%s contains a %s marker; return ErrNotImplemented instead", path, needle)
		}
	})
}
