package repo

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain runs every test here from the repository root.
//
// These tests read the repository as text — source files, docs, build files —
// and were written with paths relative to the root. Changing directory once is
// simpler and less fragile than threading a root through every path.
func TestMain(m *testing.M) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err == nil {
		_, err = os.Stat(filepath.Join(root, "go.mod"))
	}
	if err == nil {
		err = os.Chdir(root)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "tests/repo: cannot find the repository root: %v\n", err)
		os.Exit(2)
	}
	os.Exit(m.Run())
}
