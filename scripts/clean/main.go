// Command clean removes the build and test artefacts .gitignore names, using
// the Go toolchain rather than a POSIX shell.
//
// # Why it reads .gitignore
//
// This is the reason that matters. The list of artefacts existed twice — once in
// .gitignore, once in the Makefile recipe — and the two had already drifted: the
// recipe removed two of the seven patterns .gitignore names. Two 17 MB example
// binaries sat in the repository root for a day as a result, matched by
// .gitignore's `*.exe` and by nothing in the target whose job was removing them.
//
// Two lists that must agree with nothing checking them drift apart. Reading
// .gitignore leaves one list.
//
// Only the first section is read. The second names editor and OS directories —
// .idea/, .vscode/ — which are the developer's, not this project's, and deleting
// them is well outside what a clean target may do.
//
// # Why it is a Go program and not `rm -f`
//
// The smaller reason, and worth stating precisely because the first version of
// this comment overstated it. `rm -f` works: on CI, and on Windows from a shell
// whose PATH carries Git's POSIX utilities. It fails when make is invoked from
// one that does not, which on this machine means PowerShell:
//
//	PS> mingw32-make clean
//	process_begin: CreateProcess(NULL, rm -f coverage.out cov bench.txt ...) failed.
//	make (e=2): The system cannot find the file specified.
//
// GNU make runs a recipe through SHELL when the command line contains something
// it recognises as needing one and through CreateProcess when it does not; the
// glob in that recipe did not trigger it, so `rm` had to be a real executable on
// PATH and was not. Pipes and `&&` do trigger it, which is why the three other
// recipes here that use POSIX utilities are unaffected — that was measured, not
// assumed, after the first draft of this comment claimed all of them were broken.
//
// So this is portability rather than repair. `go` is the one tool this project
// can assume, because nothing else here builds without it.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// artefactSection is the .gitignore heading whose entries this command removes.
// Matched exactly: a renamed heading should stop the removal rather than silently
// widen it to whatever section follows.
const artefactSection = "# Build and test artefacts"

func main() {
	root, err := repoRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "clean:", err)
		os.Exit(1)
	}
	patterns, err := artefactPatterns(filepath.Join(root, ".gitignore"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "clean:", err)
		os.Exit(1)
	}
	if len(patterns) == 0 {
		fmt.Fprintf(os.Stderr, "clean: no patterns under %q in .gitignore; "+
			"refusing to run rather than silently removing nothing\n", artefactSection)
		os.Exit(1)
	}

	removed, err := removeMatching(root, patterns)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clean:", err)
		os.Exit(1)
	}
	for _, name := range removed {
		fmt.Println("removed", name)
	}
}

// repoRoot walks up from the working directory to the directory holding go.mod.
//
// `go run ./scripts/clean` runs with the working directory the user invoked it
// from, which is the repository root when make runs it and need not be otherwise.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod in any parent of the working directory")
		}
		dir = parent
	}
}

// artefactPatterns returns the entries in .gitignore's artefact section, with the
// leading slash that anchors a pattern to the repository root stripped.
func artefactPatterns(path string) ([]string, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var patterns []string
	inSection := false
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "#") {
			inSection = line == artefactSection
			continue
		}
		if line == "" || !inSection {
			continue
		}
		patterns = append(patterns, strings.TrimPrefix(line, "/"))
	}
	return patterns, nil
}

// removeMatching deletes the entries in root matching any pattern, and returns
// what it removed.
//
// Three deliberate restrictions, because this deletes files. It reads one
// directory and does not recurse, so a pattern cannot reach a testdata corpus or
// a nested module. It skips directories, so a pattern that happens to match one
// removes nothing. And it matches names only, never paths, so nothing outside
// the repository root is reachable however a pattern is written.
func removeMatching(root string, patterns []string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	var removed []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		for _, pattern := range patterns {
			ok, err := filepath.Match(pattern, name)
			if err != nil {
				return nil, fmt.Errorf("pattern %q from .gitignore: %w", pattern, err)
			}
			if !ok {
				continue
			}
			if err := os.Remove(filepath.Join(root, name)); err != nil {
				return nil, err
			}
			removed = append(removed, name)
			break
		}
	}
	return removed, nil
}
