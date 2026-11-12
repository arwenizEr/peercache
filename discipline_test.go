package peercache_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/metrics"
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
	"soak_test.go",
	"examples/m3/main.go",
	"examples/m4/main.go",
	"examples/m5/main.go",
	"examples/m6/main.go",
	"examples/m7/main.go",
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
	walkGoFiles(t, func(path, content string) {
		if strings.HasSuffix(path, "_test.go") {
			return
		}
		// Commands document themselves above `package main`, which is the
		// convention go doc expects; a separate doc.go there would be
		// worse, not better.
		//
		// This tested `examples/` as a path prefix until M8, which is the
		// directory every command happened to live in rather than the
		// property being reasoned about. The first command written outside
		// it would have been asked for a doc.go the convention says not to
		// write. Matching on the declaration matches the reason.
		if isCommand(content) {
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

// isCommand reports whether a Go file declares `package main`.
//
// Matched on a line of its own so that the words in a comment or a string do not
// count, which is the same reason the checks above assemble their needles at
// runtime.
func isCommand(content string) bool {
	for _, line := range strings.Split(content, "\n") {
		if strings.TrimSpace(line) == "package main" {
			return true
		}
	}
	return false
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

// TestEveryExportedSymbolIsDocumented enforces the other half of CLAUDE.md's
// documentation rule.
//
// `TestEveryPackageHasDocGo` covers "every package has a doc.go". This covers
// "every exported symbol has a doc comment", which had no check at all — and was
// the one of the two that was being violated. `WithTransport`, a public
// constructor option sitting between two documented neighbours, had no comment
// and therefore no entry in `go doc`. Every other rule in this file was already
// enforced by this file and every other rule held.
//
// # Why it parses rather than greps
//
// The rest of this file reads Go as text, which is crude and sufficient for
// "does this identifier appear". It is not sufficient here. A regex over
// declaration lines reports 45 violations, 44 of which are methods on
// *unexported* types — `(*cache).Stats`, `(*group).Get` — whose documentation
// belongs on the interfaces they implement, which is where Go's own convention
// puts it and where this project already puts it.
//
// A check that is wrong 44 times out of 45 gets switched off, so this one uses
// the parser and looks only at genuine top-level declarations.
func TestEveryExportedSymbolIsDocumented(t *testing.T) {
	fset := token.NewFileSet()

	walkGoFiles(t, func(path, content string) {
		if strings.HasSuffix(path, "_test.go") {
			return
		}
		// Commands are documented above `package main`; their internals are
		// not an API anyone imports.
		if strings.HasPrefix(path, "examples/") {
			return
		}

		file, err := parser.ParseFile(fset, path, content, parser.ParseComments)
		if err != nil {
			t.Errorf("parsing %s: %v", path, err)
			return
		}

		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				// Methods are documented on the interface they satisfy.
				if d.Recv != nil || !d.Name.IsExported() {
					continue
				}
				if d.Doc == nil {
					t.Errorf("%s: func %s has no doc comment; it is reachable as "+
						"peercache.%s and therefore appears in go doc without one",
						path, d.Name.Name, d.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range d.Specs {
					name, ok := exportedSpecName(spec)
					if !ok {
						continue
					}
					// A grouped declaration may carry one comment for
					// the whole block, which is the idiomatic way to
					// document a set of sentinels or option constants.
					if d.Doc == nil && specDoc(spec) == nil {
						t.Errorf("%s: %s %s has no doc comment",
							path, d.Tok, name)
					}
				}
			}
		}
	})
}

// exportedSpecName returns the declared name if the spec declares exactly one
// exported identifier.
//
// Grouped `var a, b = ...` is deliberately not handled specially: the first
// exported name is enough to report the line, and this project does not use
// that form for exported declarations.
func exportedSpecName(spec ast.Spec) (string, bool) {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		if s.Name.IsExported() {
			return s.Name.Name, true
		}
	case *ast.ValueSpec:
		for _, n := range s.Names {
			if n.IsExported() {
				return n.Name, true
			}
		}
	}
	return "", false
}

// specDoc returns the comment attached to an individual spec inside a grouped
// declaration, which is where a `const (` block documents its members.
func specDoc(spec ast.Spec) *ast.CommentGroup {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		return s.Doc
	case *ast.ValueSpec:
		return s.Doc
	}
	return nil
}

// evalModule is the L1 cache-library comparison, which lives in its own module.
//
// It is a benchmark harness for three candidates that were rejected, kept out of
// the shipping dependency graph on purpose, and `go list ./...` does not report
// it. Panicking on a setup error there is the right thing for a throwaway
// program and says nothing about library discipline.
const evalModule = "internal/l1/eval"

// TestNoPanicInLibraryPaths enforces the CLAUDE.md rule that no library path
// panics.
//
// It is the last of that file's mechanical rules to get a check, and it was
// added for the reason the exported-symbol rule demonstrates: every rule in this
// file held, the two without a check were the two worth worrying about, and one
// of them had already been broken.
//
// # What a panic here would cost
//
// The library recovers panics raised by *application* callbacks — a loader or a
// group function — and reports them as [ErrLoaderPanic] and
// [ErrGroupFuncPanic], so an application bug stays the application's. A panic
// raised by the library itself has no such boundary: it unwinds whichever
// caller's goroutine happened to be running, which in a cache embedded in every
// pod means taking down the application over a bug in its cache.
//
// Question 34 is what that looks like in practice. Eighteen exported methods
// dereferenced a nil context, and it survived seven milestones because nobody
// passes nil on purpose — it was found by a throwaway program, not by a test.
func TestNoPanicInLibraryPaths(t *testing.T) {
	needle := "pan" + "ic("
	walkGoFiles(t, func(path, content string) {
		if strings.HasSuffix(path, "_test.go") {
			return
		}
		// Commands may panic: a failed example should stop loudly, and
		// nothing imports them.
		if strings.HasPrefix(path, "examples/") || strings.HasPrefix(path, evalModule) {
			return
		}
		if strings.Contains(content, needle) {
			t.Errorf("%s calls %s; library paths return errors, and a panic here "+
				"unwinds the caller's goroutine rather than the library's",
				path, needle)
		}
	})
}

// budgetedDependencies is CLAUDE.md's dependency budget, written out.
//
// The rule names categories — "memberlist, grpc, a local cache library, a codec,
// and test deps" — and each slot below records which module fills it and the
// decision that put it there. A category with no named module is a rule that
// argues with itself the first time somebody swaps one library for another.
var budgetedDependencies = map[string]string{
	"github.com/hashicorp/memberlist":   "gossip membership, named in the budget",
	"google.golang.org/grpc":            "peer transport, named in the budget",
	"github.com/maypok86/otter":         "the local cache library slot; decision 0005",
	"github.com/vmihailenco/msgpack/v5": "the codec slot; decision 0014",
	"github.com/cespare/xxhash/v2":      "rendezvous hashing; decision 0009",
	"github.com/pierrec/lz4/v4":         "opt-in compression; decision 0008",
	"go.uber.org/goleak":                "test dep",
	"github.com/anishathalye/porcupine": "test dep; decision 0018",
}

// TestDependencyBudgetIsUnchanged makes adding a dependency a deliberate act.
//
// CLAUDE.md: "Adding anything else requires a note in `docs/decisions/` first."
// Nothing enforced that, so the requirement rested on remembering it at exactly
// the moment `go get` makes it easiest not to.
//
// This does not and cannot check that the decision note exists. What it does is
// make the diff say so: adding a module fails this test until someone adds it to
// the map above with the slot it fills, which is the same sentence the note has
// to make anyway.
func TestDependencyBudgetIsUnchanged(t *testing.T) {
	body, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}

	direct := make(map[string]bool)
	inBlock := false
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		}
		if !inBlock || line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		// Indirect requirements are the graph, not the budget: they arrive
		// because a budgeted module asked for them.
		if strings.Contains(line, "// indirect") {
			continue
		}
		if module, _, ok := strings.Cut(line, " "); ok {
			direct[module] = true
		}
	}

	if len(direct) == 0 {
		t.Fatal("parsed no direct requirements from go.mod; this check is not looking " +
			"at what it thinks it is")
	}

	for module := range direct {
		if _, ok := budgetedDependencies[module]; !ok {
			t.Errorf("%s is a direct dependency and is not in the budget. CLAUDE.md "+
				"requires a note in docs/decisions/ before adding one; record the slot "+
				"it fills in budgetedDependencies once that note exists", module)
		}
	}
	for module := range budgetedDependencies {
		if !direct[module] {
			t.Errorf("%s is in the budget and is no longer a direct dependency; drop it "+
				"from budgetedDependencies so the list stays a description rather than "+
				"a wish", module)
		}
	}
}

// TestApiDocExampleTableMatchesReality keeps a documentation table honest against
// the code it describes.
//
// `docs/api.md` opens with a table of every runnable example and what it covers,
// and that table is load-bearing: it is how a reader decides whether the public
// API has been demonstrated or merely described. It was accurate when checked,
// which is exactly when to nail it down — the failure mode is not writing it
// wrong, it is adding a thirteenth example and forgetting the row.
//
// This is the same move as `TestProtoDeclaresEveryMessage`, applied to prose
// rather than to a schema. Both directions matter: a row with no example is a
// promise the package does not keep, and an example with no row is a
// demonstration nobody will find.
func TestApiDocExampleTableMatchesReality(t *testing.T) {
	doc, err := os.ReadFile(filepath.Join("docs", "api.md"))
	if err != nil {
		t.Fatalf("reading docs/api.md: %v", err)
	}
	src, err := os.ReadFile("example_test.go")
	if err != nil {
		t.Fatalf("reading example_test.go: %v", err)
	}

	// Rows look like: | `ExampleFoo` | what it covers |
	documented := make(map[string]bool)
	rowName := regexp.MustCompile("^\\|\\s*`(Example[A-Za-z0-9_]*)`\\s*\\|")
	for _, line := range strings.Split(string(doc), "\n") {
		if m := rowName.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			documented[m[1]] = true
		}
	}

	defined := make(map[string]bool)
	funcName := regexp.MustCompile(`^func (Example[A-Za-z0-9_]*)\(`)
	for _, line := range strings.Split(string(src), "\n") {
		if m := funcName.FindStringSubmatch(line); m != nil {
			defined[m[1]] = true
		}
	}

	if len(documented) == 0 || len(defined) == 0 {
		t.Fatalf("found %d documented and %d defined examples; this check is not "+
			"parsing what it thinks it is", len(documented), len(defined))
	}

	for name := range documented {
		if !defined[name] {
			t.Errorf("docs/api.md lists %s, which example_test.go does not define", name)
		}
	}
	for name := range defined {
		if !documented[name] {
			t.Errorf("example_test.go defines %s, which docs/api.md's table does not "+
				"list; an example nobody can find demonstrates nothing", name)
		}
	}
}

// citedButNotDefined are the names in the documentation that deliberately do not
// resolve to a test, each with the reason it does not.
//
// An allowlist rather than a looser pattern, because "this name is prose" is a
// judgement that should be recorded once and then enforced, not re-made by a
// regex every time somebody writes a sentence.
var citedButNotDefined = map[string]string{
	"TestMain":      "the Go concept, discussed in open question 11's goleak note",
	"TestSomething": "a placeholder in the mutation audit's own description",
	"TestReport": "a -run prefix pattern matching TestReportHitRatio and two " +
		"others in the internal/l1/eval module; named in prose by " +
		"docs/verification.md when explaining that this check skips fenced " +
		"commands, which is how it stopped being a false positive",
	"TestGetOrLoadRechecksAfterWinningTheFlight": "deleted on purpose; " +
		"docs/verification.md records it as the test that proved nothing",
}

// TestDocsCiteOnlyRealTests stops the documentation from naming tests that do not
// exist.
//
// The records in `docs/` lean hard on test names — a claim is credible here
// because it says which test would fail if it stopped being true. That makes a
// stale name worse than no name: it reads as evidence and resolves to nothing,
// and the reader who checks is the one who finds out.
//
// The risk is ordinary. `TestGetOrLoadRechecksAfterWinningTheFlight` was renamed
// once already, and this milestone found a metrics claim citing a test that did
// not cover what the sentence said. Renaming a test is a refactor nobody thinks
// of as a documentation change.
func TestDocsCiteOnlyRealTests(t *testing.T) {
	defined := make(map[string]bool)
	walkGoFiles(t, func(path, content string) {
		if !strings.HasSuffix(path, "_test.go") {
			return
		}
		for _, m := range regexp.MustCompile(`func (Test[A-Za-z0-9_]+)\(`).FindAllStringSubmatch(content, -1) {
			defined[m[1]] = true
		}
	})
	if len(defined) == 0 {
		t.Fatal("found no test functions; this check is not reading what it thinks it is")
	}

	docs, err := filepath.Glob(filepath.Join("docs", "*.md"))
	if err != nil {
		t.Fatalf("globbing docs: %v", err)
	}
	more, err := filepath.Glob(filepath.Join("docs", "decisions", "*.md"))
	if err != nil {
		t.Fatalf("globbing decisions: %v", err)
	}
	docs = append(docs, more...)
	docs = append(docs, "README.md")

	// Long enough to be a real name rather than a word that happens to start
	// with Test, and anchored on the capital that follows.
	cite := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]{5,}`)
	for _, path := range docs {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for _, name := range cite.FindAllString(proseOnly(string(body)), -1) {
			if defined[name] {
				continue
			}
			if _, allowed := citedButNotDefined[name]; allowed {
				continue
			}
			t.Errorf("%s cites %s, which no test defines. If it was renamed, update "+
				"the prose; if it is not a test name, add it to citedButNotDefined "+
				"with the reason", filepath.ToSlash(path), name)
		}
	}

	for name, why := range citedButNotDefined {
		if defined[name] {
			t.Errorf("%s is in citedButNotDefined as %q, but a test by that name now "+
				"exists; drop the exception", name, why)
		}
	}
}

// proseOnly strips fenced code blocks from a Markdown document.
//
// Inside a fence a name like TestReport is a `-run` *pattern* — a prefix
// matching TestReportHitRatio and two others — not a claim that a test by that
// name exists. Treating the two alike costs a false positive for every command
// the documentation shows, and a checker that is wrong about commands is one
// people stop reading.
//
// In prose the same string is a citation, which is the thing worth enforcing.
func proseOnly(doc string) string {
	var out strings.Builder
	fenced := false
	for _, line := range strings.Split(doc, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "```") {
			fenced = !fenced
			continue
		}
		if !fenced {
			out.WriteString(line)
			out.WriteString("\n")
		}
	}
	return out.String()
}

// TestReadmeListsEveryInternalPackage keeps the repository map complete.
//
// README's layout block is the first thing a reader uses to decide where
// something lives. A package missing from it is a package they will not look in,
// which is worse than a map with no detail at all — the omission reads as
// "there is nothing there".
//
// `internal/wire` was missing from it. That is the protocol codec, the only code
// in the repository that interprets bytes it did not write, and the package
// where two of this milestone's four library defects lived. It had been absent
// since the package was created and nothing noticed, because nothing was
// looking.
//
// The check is one-directional on purpose: every real package must appear, but a
// line in the block that no longer matches a directory is caught by the same
// diff that removed the directory, and demanding exact equality would fight the
// block's sub-entries and prose.
func TestReadmeListsEveryInternalPackage(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}

	entries, err := os.ReadDir("internal")
	if err != nil {
		t.Fatalf("reading internal/: %v", err)
	}

	body := string(readme)
	found := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		found++
		// The block writes each package with a trailing slash, which keeps
		// the match from succeeding on a passing mention in prose.
		if !strings.Contains(body, e.Name()+"/") {
			t.Errorf("README.md's repository layout does not list internal/%s; a "+
				"package missing from the map is one nobody will look in", e.Name())
		}
	}
	if found == 0 {
		t.Fatal("found no packages under internal/; this check is not reading what " +
			"it thinks it is")
	}
}

// numberWords maps the counts the documentation states in prose.
//
// The map spells its numbers out because the surrounding text does, and a table
// whose size is written as a word is exactly as capable of being wrong as one
// written as a digit — twice in one paragraph, as it turned out.
//
// One map for every prose count in the repository, not one per check. Two of
// these had started to exist, which is the same duplication the checks
// themselves exist to catch.
var numberWords = map[int]string{
	2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven",
	8: "eight", 9: "nine", 10: "ten", 11: "eleven", 12: "twelve",
	13: "thirteen", 14: "fourteen", 15: "fifteen", 16: "sixteen",
	17: "seventeen", 18: "eighteen", 19: "nineteen", 20: "twenty",
	21: "twenty-one", 22: "twenty-two", 23: "twenty-three",
	24: "twenty-four", 25: "twenty-five",
}

// numberFromWord is the reverse lookup, for prose that states a count as a word
// where the check has to compare it against something counted.
func numberFromWord(word string) (int, bool) {
	for n, w := range numberWords {
		if w == word {
			return n, true
		}
	}
	return 0, false
}

// TestAcceptanceMapCountsAreCorrect checks the two numbers the acceptance map
// states about itself.
//
// It exists because both were wrong, in the same paragraph, written by someone
// who had spent the preceding hour fixing five other wrong derived figures and
// was at that moment writing the argument against them. The first said
// "eighteen distinct criteria" above twenty-one rows. The second said
// "twenty-two names" where the table cites twenty-three, because the loop that
// verified them was written from memory of which tests to check rather than from
// the table.
//
// That is the whole case for this test. The recompute habit is not acquired by
// being persuaded of it; the count has to be run, and a count that depends on
// remembering to run it will eventually not be.
func TestAcceptanceMapCountsAreCorrect(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "verification.md"))
	if err != nil {
		t.Fatalf("reading docs/verification.md: %v", err)
	}

	// The section runs from its heading to the next top-level heading.
	start := strings.Index(string(body), "## The acceptance map")
	if start < 0 {
		t.Fatal("docs/verification.md has no acceptance map section")
	}
	section := string(body)[start:]
	if end := strings.Index(section[1:], "\n## "); end >= 0 {
		section = section[:end+1]
	}

	rowPattern := regexp.MustCompile(`(?m)^\| M[0-9] \|`)
	rows := len(rowPattern.FindAllString(section, -1))

	// Names are counted from the table rows only, not the whole section. The
	// claim is "all N names in the table were verified", and the prose around
	// it legitimately mentions tests — including this one, which is how the
	// first version of this check failed the moment it was documented.
	names := make(map[string]bool)
	for _, row := range rowPattern.Split(section, -1)[1:] {
		if i := strings.Index(row, "\n"); i >= 0 {
			row = row[:i]
		}
		for _, m := range regexp.MustCompile("`(Test[A-Za-z0-9_]+)`").FindAllStringSubmatch(row, -1) {
			names[m[1]] = true
		}
	}

	if rows == 0 || len(names) == 0 {
		t.Fatalf("parsed %d rows and %d names from the acceptance map; this check is "+
			"not reading what it thinks it is", rows, len(names))
	}

	if word, ok := numberWords[rows]; !ok {
		t.Errorf("the acceptance map has %d rows, which numberWords does not spell; "+
			"extend it", rows)
	} else if !strings.Contains(section, "come to "+word) {
		t.Errorf("the acceptance map has %d rows but does not say %q; the count in the "+
			"prose has drifted from the table under it", rows, word)
	}

	if word, ok := numberWords[len(names)]; !ok {
		t.Errorf("the acceptance map cites %d test names, which numberWords does not "+
			"spell; extend it", len(names))
	} else if !strings.Contains(section, "All "+word+" names") {
		t.Errorf("the acceptance map cites %d distinct test names but does not say "+
			"%q; the claim about what was verified has drifted from what is listed",
			len(names), word)
	}

	// Every acceptance block in SPEC.md must have at least one row. A milestone
	// silently dropping out of the map is the failure this table exists to make
	// impossible.
	spec, err := os.ReadFile("SPEC.md")
	if err != nil {
		t.Fatalf("reading SPEC.md: %v", err)
	}
	blocks := strings.Count(string(spec), "**Acceptance:**")
	for i := 0; i < blocks; i++ {
		milestone := "| M" + string(rune('0'+i)) + " |"
		if !strings.Contains(section, milestone) {
			t.Errorf("SPEC.md states an acceptance block for M%d and the map has no row "+
				"for it", i)
		}
	}
}

// TestLossCountingTestsUseARetryAwareRead stops the sixth instance of a bug this
// milestone found five times.
//
// A distributed cache returns a large class of errors meaning "not yet" —
// `ErrActivationPending` while a moved group serves out its activation delay,
// `ErrNotOwner` while a view settles — and a small class meaning "never". A test
// that counts any error as a lost key does not fail less often than one that
// gets this right; it fails at a rate set by machine load, and every failure is a
// specific, credible, false report of data loss. That is the most expensive kind
// of flake, because the message is convincing.
//
// It happened five times: `readAnywhere` itself, `TestKeysAdoptedIsCounted`,
// `TestDrainSavesDataWithoutReplication`, and both chaos tests — one of them the
// M5 acceptance criterion. The first three were fixed as they appeared; only
// after the third did anybody ask where else, and the answer was the acceptance
// test.
//
// # Why the check is coarse
//
// It asserts a file-level property — a test file that counts losses must use one
// of the retry-aware helpers — rather than trying to recognise the dangerous
// shape in situ. Three text-based checks written this milestone were wrong on
// their first version by being too literal, and a checker that over-fires is one
// that gets switched off. This one cannot say *where* in a file the bare read is;
// it says the file has no way to be doing the right thing, which is enough to
// start the conversation and impossible to argue with.
func TestLossCountingTestsUseARetryAwareRead(t *testing.T) {
	countsLoss := []string{"lost++", "lost = append", "missing++", "lost, kept"}
	retryAware := []string{"readAnywhere", "readWithRetryFrom"}

	checked := 0
	walkGoFiles(t, func(path, content string) {
		if !strings.HasSuffix(path, "_test.go") || path == "discipline_test.go" {
			return
		}
		counts := false
		for _, needle := range countsLoss {
			if strings.Contains(content, needle) {
				counts = true
				break
			}
		}
		if !counts {
			return
		}
		checked++
		for _, helper := range retryAware {
			if strings.Contains(content, helper) {
				return
			}
		}
		t.Errorf("%s counts keys as lost but never calls a retry-aware read. A read "+
			"that returns ErrActivationPending or ErrNotOwner means \"ask again\", "+
			"not \"gone\"; counting it as loss reports data loss that did not happen",
			path)
	})

	if checked == 0 {
		t.Fatal("no loss-counting test files found; this check is not reading what it " +
			"thinks it is")
	}
}

// fuzzSpec is one place a build file names a fuzz target: which package it runs
// in, the -fuzz pattern, and where it was written, so a failure can say which
// line to fix.
type fuzzSpec struct {
	pkgDir  string // slash-separated, relative to the repository root
	pattern string
	source  string
}

// TestFuzzTargetsNamedInCIExist checks that every fuzz target the Makefile and
// the workflows name is one that exists, and that each pattern selects exactly
// one target.
//
// It exists because both halves failed at once and neither was visible.
//
// `go test -fuzz` takes a regexp, and when that regexp matches no target it
// does not fail — it runs the ordinary tests selected by `-run`, which is
// `'^$'`, which is none, and exits 0 in under two seconds. nightly.yml was
// written with seven fuzz jobs, three of which named targets nobody had written:
// FuzzCodecRoundTrip, FuzzMethodName and FuzzPartitionByPrefix. All three would
// have reported success every night, having fuzzed nothing, and the failure
// leaves no trace in the log beyond a job that finished suspiciously fast.
//
// The opposite failure happened in the same milestone. Patterns were written
// unanchored, so `-fuzz FuzzDecode` meant "any target whose name contains
// FuzzDecode". Adding FuzzDecodeEveryMessage to the same package made that two,
// and Go refuses: "will not fuzz, -fuzz matches more than one fuzz test". A
// green CI step was broken by a test being added in a file it does not mention.
// The Makefile was fixed by hand; ci.yml was not, and nothing noticed.
//
// One is a gate that cannot fail and the other is a gate that cannot pass. Both
// come from a build file naming a test by a string nothing resolves, which is
// the same defect this file's other checks exist for, one level out from the
// source tree.
func TestFuzzTargetsNamedInCIExist(t *testing.T) {
	specs := fuzzSpecsFromBuildFiles(t)
	// Ten is below the count at the time of writing and above zero by enough
	// that a parser silently matching nothing is a failure rather than a
	// pass. The number is a floor, not the count: adding a target should not
	// have to edit this.
	if len(specs) < 10 {
		t.Fatalf("found only %d fuzz invocations across the build files; this check "+
			"is not reading what it thinks it is", len(specs))
	}

	for _, spec := range specs {
		re, err := regexp.Compile(spec.pattern)
		if err != nil {
			t.Errorf("%s: -fuzz %q is not a valid regexp: %v", spec.source, spec.pattern, err)
			continue
		}
		defined := fuzzTargetsIn(t, spec.pkgDir)
		var matched []string
		for _, name := range defined {
			if re.MatchString(name) {
				matched = append(matched, name)
			}
		}
		switch len(matched) {
		case 1:
			// The only good case.
		case 0:
			t.Errorf("%s runs `-fuzz %s` in %s, where no fuzz target matches. "+
				"go test exits 0 here, so this gate passes without fuzzing "+
				"anything. Targets in that package: %v",
				spec.source, spec.pattern, spec.pkgDir, defined)
		default:
			t.Errorf("%s runs `-fuzz %s` in %s, which matches %v. go test refuses "+
				"to fuzz an ambiguous pattern, so this gate cannot pass. Anchor "+
				"it as '^Name$'", spec.source, spec.pattern, spec.pkgDir, matched)
		}
	}
}

// fuzzSpecsFromBuildFiles collects every fuzz invocation in the Makefile and the
// workflow files.
//
// Two forms are recognised. A literal command line — `go test ./internal/wire
// -run '^$' -fuzz '^FuzzDecode$'` — and nightly.yml's matrix entries, which name
// the package and the target as data and are expanded into an anchored pattern
// by the step's own template. A command whose package or pattern is a template
// expansion is skipped, because its values come from the matrix that was already
// read.
func fuzzSpecsFromBuildFiles(t *testing.T) []fuzzSpec {
	t.Helper()

	files := []string{"Makefile"}
	workflows, err := filepath.Glob(filepath.Join(".github", "workflows", "*.yml"))
	if err != nil {
		t.Fatalf("globbing workflows: %v", err)
	}
	files = append(files, workflows...)

	// `go test <pkg> ... -fuzz <pattern>`, where pkg is the first argument
	// and the pattern may be quoted.
	command := regexp.MustCompile(`test\s+(\S+)[^\n]*?-fuzz\s+('[^']*'|"[^"]*"|\S+)`)
	// nightly.yml's matrix rows.
	matrix := regexp.MustCompile(`\{\s*pkg:\s*([^\s,]+)\s*,\s*fn:\s*([^\s}]+)\s*\}`)

	var specs []fuzzSpec
	for _, path := range files {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		text := string(body)
		slash := filepath.ToSlash(path)

		for _, m := range command.FindAllStringSubmatch(text, -1) {
			pkg, pattern := m[1], unquoteBuildToken(m[2])
			if isTemplated(pkg) || isTemplated(pattern) {
				continue
			}
			specs = append(specs, fuzzSpec{
				pkgDir:  packageDir(pkg),
				pattern: pattern,
				source:  slash,
			})
		}
		for _, m := range matrix.FindAllStringSubmatch(text, -1) {
			specs = append(specs, fuzzSpec{
				pkgDir: packageDir(m[1]),
				// The step anchors the name; this must match what it
				// builds, or the check tests a pattern nothing runs.
				pattern: "^" + m[2] + "$",
				source:  slash + " (matrix)",
			})
		}
	}
	return specs
}

// isTemplated reports whether a build-file token is filled in by GitHub Actions
// or by make rather than being a literal.
func isTemplated(token string) bool {
	return strings.Contains(token, "${{") || strings.Contains(token, "$(")
}

// unquoteBuildToken strips the shell or make quoting around a -fuzz pattern and
// undoes Make's doubled dollar sign, which is how '^FuzzDecode$' has to be
// written in a recipe.
func unquoteBuildToken(token string) string {
	token = strings.Trim(token, `'"`)
	return strings.ReplaceAll(token, "$$", "$")
}

// packageDir turns a go test package argument into a directory relative to the
// repository root. Only the single-package forms are used here; a pattern like
// ./... names no one package and would have to be expanded, which no caller
// needs.
func packageDir(pkg string) string {
	pkg = strings.TrimPrefix(pkg, "./")
	if pkg == "" || pkg == "." {
		return "."
	}
	return pkg
}

// fuzzTargetsIn returns the names of the fuzz targets defined in one directory.
// Fuzz targets are per-package, so this deliberately does not recurse.
func fuzzTargetsIn(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s, named by a build file: %v", dir, err)
	}
	declared := regexp.MustCompile(`func (Fuzz[A-Za-z0-9_]*)\(`)
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", filepath.Join(dir, entry.Name()), err)
		}
		for _, m := range declared.FindAllStringSubmatch(string(body), -1) {
			names = append(names, m[1])
		}
	}
	return names
}

// TestReadmeListsEveryRootFile extends the repository map to the directory the
// map is least able to describe from the outside.
//
// The root is the public API — Go does not let a package span directories, so
// every file a caller's import reaches is here, nineteen of them. README's
// layout block listed seven, and had listed seven since M4 while twelve more
// were added underneath it. `handoff.go`, written this milestone, would have
// been the thirteenth.
//
// That is the same defect `TestReadmeListsEveryInternalPackage` was written for,
// found one directory over, and it is worse here: a reader who wants to know
// where the remote path lives is looking at the one directory the map claims to
// describe completely.
//
// One-directional, for the same reason as its sibling: every real file must
// appear, but a stale line naming a deleted file is caught by the diff that
// deleted it.
func TestReadmeListsEveryRootFile(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("reading README.md: %v", err)
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the repository root: %v", err)
	}
	var listed int
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if strings.HasSuffix(name, "_test.go") {
			// Test files are not API and are named for what they
			// verify, which the records in docs/ cover.
			continue
		}
		if !strings.Contains(string(readme), name) {
			t.Errorf("README.md does not mention %s. The root directory is the "+
				"public API, and the layout block is where a reader looks for "+
				"which file holds what", name)
			continue
		}
		listed++
	}
	if listed < 15 {
		t.Fatalf("only %d root files were found and matched; this check is not "+
			"reading what it thinks it is", listed)
	}
}

// TestApiDocMetricCountsAreCorrect checks the two derived figures in
// docs/api.md's metrics section against the code they describe.
//
// `docs/api.md` tells an operator that "38 series come out of it" and that "most
// of the 38 are ordinary levels and rates. These ten are not". Both numbers are
// recomputed by hand whenever a counter is added. The first was 35 until this
// milestone added three, and it was corrected by someone who happened to
// remember — which is the mechanism that produced five wrong derived figures out
// of six checked earlier in M8, and two more in the paragraph that reported
// them.
//
// The series count is taken from [metrics.Names], the same list the handler
// writes, so this cannot drift from the output the way a hand-maintained list
// would. The alerting count is taken from the table's own rows, so the sentence
// introducing a table has to agree with the table underneath it.
//
// It reads every occurrence rather than the first. The figure appears twice, in
// two sections written months apart, and a check that fixed one and left the
// other would be the failure it is meant to prevent.
func TestApiDocMetricCountsAreCorrect(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("docs", "api.md"))
	if err != nil {
		t.Fatalf("reading docs/api.md: %v", err)
	}
	doc := string(body)
	want := len(metrics.Names())
	if want == 0 {
		t.Fatal("metrics.Names() is empty; this check is not reading what it thinks it is")
	}

	claims := 0
	for _, pattern := range []string{`(\d+) series`, `Most of the (\d+)`} {
		for _, m := range regexp.MustCompile(pattern).FindAllStringSubmatch(proseOnly(doc), -1) {
			claims++
			got, err := strconv.Atoi(m[1])
			if err != nil {
				t.Errorf("docs/api.md: %q: %v", m[0], err)
				continue
			}
			if got != want {
				t.Errorf("docs/api.md says %q, but the handler exports %d series. "+
					"Recount rather than adjust: metrics.Names() is the list the "+
					"output is written from", m[0], want)
			}
		}
	}
	if claims < 2 {
		t.Errorf("found %d series-count claims in docs/api.md, expected at least 2; "+
			"either the prose was reworded or this check no longer matches it", claims)
	}

	// The alerting table: "These ten are not" over a table of ten rows.
	intro := regexp.MustCompile(`These ([a-z]+) are not`).FindStringSubmatch(doc)
	if intro == nil {
		t.Fatal("docs/api.md no longer introduces the alerting table with " +
			"\"These N are not\"; this check needs updating with the prose")
	}
	claimed, ok := numberFromWord(intro[1])
	if !ok {
		t.Fatalf("docs/api.md says %q, which is not a number word this check knows; "+
			"add it to numberWords", intro[0])
	}
	rows := 0
	for _, line := range strings.Split(doc[strings.Index(doc, intro[0]):], "\n") {
		if strings.HasPrefix(line, "| `peercache_") {
			rows++
			continue
		}
		// The table ends at the next heading. Anything after it is a
		// different table and not what the sentence counts.
		if strings.HasPrefix(line, "#") && rows > 0 {
			break
		}
	}
	if rows != claimed {
		t.Errorf("docs/api.md says %q above a table of %d rows. The sentence and "+
			"the table have to agree, and the table is the one that is right",
			intro[0], rows)
	}
}

// TestMutationAuditCitesOnlyRealTests applies TestDocsCiteOnlyRealTests to the
// one file that names more tests than any document does.
//
// `scripts/mutation-audit.sh` cites over a hundred test names, one per entry, as
// the `-run` argument it checks each mutation against. A renamed test does not
// go unnoticed there — `go test -run` matching nothing passes, so the entry
// reports VACUOUS and the run exits non-zero — but it reports the wrong thing.
// VACUOUS means "this test did not notice the behaviour breaking", which sends
// whoever reads it to the test. The actual fault is a stale string in a shell
// script, and the two look identical in the output.
//
// This names the real cause before the audit has to guess at it, and it runs in
// two seconds against an audit that takes an hour.
//
// Comment text is skipped. The guard at the top of that file documents itself
// with 'TestSomething' and 'TestSomethingSpecific$', which are illustrations
// rather than citations — the same distinction between prose and a pattern that
// proseOnly draws for Markdown, and the same one the audit's own orphan check
// makes when it skips comments.
func TestMutationAuditCitesOnlyRealTests(t *testing.T) {
	defined := make(map[string]bool)
	walkGoFiles(t, func(path, content string) {
		if !strings.HasSuffix(path, "_test.go") {
			return
		}
		for _, m := range regexp.MustCompile(`func ((?:Test|Fuzz)[A-Za-z0-9_]+)\(`).FindAllStringSubmatch(content, -1) {
			defined[m[1]] = true
		}
	})
	if len(defined) == 0 {
		t.Fatal("found no test functions; this check is not reading what it thinks it is")
	}

	body, err := os.ReadFile(filepath.Join("scripts", "mutation-audit.sh"))
	if err != nil {
		t.Fatalf("reading the mutation audit: %v", err)
	}

	// A whole quoted token that is nothing but a test-name pattern. The
	// `from` and `to` arguments of an entry are quoted Go source, which
	// contains spaces and braces and so cannot match this.
	cite := regexp.MustCompile(`'\^?((?:Test|Fuzz)[A-Za-z0-9_|]+)\$?'`)

	cited := 0
	for i, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for _, m := range cite.FindAllStringSubmatch(line, -1) {
			// An entry may select several tests with an alternation.
			for _, name := range strings.Split(m[1], "|") {
				if name == "" {
					continue
				}
				cited++
				if !defined[name] {
					t.Errorf("scripts/mutation-audit.sh:%d cites %s, which no test "+
						"defines. Left alone this reports as VACUOUS, which blames "+
						"the test for a stale name in the script", i+1, name)
				}
			}
		}
	}
	if cited < 50 {
		t.Fatalf("found only %d cited test names in the audit; this check is not "+
			"reading what it thinks it is", cited)
	}
}

// TestNightlySoakFitsItsTimeouts checks the arithmetic of the nightly soak job.
//
// PEERCACHE_SOAK is a per-test duration, and the step's -run pattern selects
// every soak test, which run one after another. The step's -timeout therefore
// has to cover the duration times the number of tests it selects, and the job's
// timeout-minutes has to cover the step. When this was written the step ran two
// tests of 60m each under -timeout 85m: it could never have passed, and nobody
// knew, because the repository has no remote and the workflow had never run.
//
// It is the same shape as the race gate's 300s bound in M8 — a number that was
// right for the suite it was written against — so it is pinned the same way.
func TestNightlySoakFitsItsTimeouts(t *testing.T) {
	body, err := os.ReadFile(filepath.Join(".github", "workflows", "nightly.yml"))
	if err != nil {
		t.Fatalf("reading nightly.yml: %v", err)
	}
	text := string(body)

	start := strings.Index(text, "\n  soak:\n")
	if start < 0 {
		t.Fatal("nightly.yml has no soak job; this check is not reading what it thinks it is")
	}
	job := text[start+1:]
	if next := regexp.MustCompile(`\n  [a-z][a-z-]*:\n`).FindStringIndex(job); next != nil {
		job = job[:next[0]]
	}

	find := func(what, pattern string) string {
		t.Helper()
		m := regexp.MustCompile(pattern).FindStringSubmatch(job)
		if m == nil {
			t.Fatalf("nightly.yml soak job: no %s matching %s", what, pattern)
		}
		return unquoteBuildToken(m[1])
	}
	minutes, err := strconv.Atoi(find("timeout-minutes", `timeout-minutes:\s*(\d+)`))
	if err != nil {
		t.Fatalf("timeout-minutes: %v", err)
	}
	perTest, err := time.ParseDuration(find("PEERCACHE_SOAK", `PEERCACHE_SOAK:\s*(\S+)`))
	if err != nil {
		t.Fatalf("PEERCACHE_SOAK: %v", err)
	}
	stepTimeout, err := time.ParseDuration(find("-timeout", `go test [^\n]*-timeout\s+(\S+)`))
	if err != nil {
		t.Fatalf("-timeout: %v", err)
	}
	selector, err := regexp.Compile(find("-run pattern", `go test [^\n]*-run\s+('[^']*'|"[^"]*"|\S+)`))
	if err != nil {
		t.Fatalf("-run pattern: %v", err)
	}

	// go test matches -run against top-level test names, unanchored.
	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("globbing tests: %v", err)
	}
	decl := regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\)`)
	var selected []string
	for _, path := range files {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		for _, m := range decl.FindAllStringSubmatch(string(src), -1) {
			if selector.MatchString(m[1]) {
				selected = append(selected, m[1])
			}
		}
	}
	if len(selected) == 0 {
		t.Fatalf("the soak step's -run %q selects no test in the root package", selector)
	}

	// Setup, settling and the final collection are seconds; ten minutes is
	// generous and keeps the check about the multiplication, not the margin.
	const slack = 10 * time.Minute
	need := time.Duration(len(selected))*perTest + slack
	if stepTimeout < need {
		t.Errorf("the soak step runs %d tests (%s) of PEERCACHE_SOAK=%v each, one after "+
			"another, under -timeout %v. That needs at least %v; the step would be "+
			"killed part-way through and report a timeout, not a result",
			len(selected), strings.Join(selected, ", "), perTest, stepTimeout, need)
	}
	if job := time.Duration(minutes) * time.Minute; job < stepTimeout+5*time.Minute {
		t.Errorf("the soak job's timeout-minutes: %d is not above the step's -timeout %v; "+
			"GitHub would cancel the job before go test could report which test hung",
			minutes, stepTimeout)
	}
}
