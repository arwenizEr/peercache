package persist

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// recordingFS is the real file system with every call logged and any one of
// them made to fail, which is how a test stands in for a crash at that point.
type recordingFS struct {
	osFS
	mu   sync.Mutex
	ops  []string
	fail string // the op that fails, or ""
}

var errInjected = errors.New("injected failure")

func (f *recordingFS) note(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ops = append(f.ops, op)
	if op == f.fail {
		return errInjected
	}
	return nil
}

func (f *recordingFS) WriteTemp(dir, prefix string, data []byte) (string, error) {
	if err := f.note("write"); err != nil {
		return "", err
	}
	return f.osFS.WriteTemp(dir, prefix, data)
}

func (f *recordingFS) Rename(from, to string) error {
	if err := f.note("rename"); err != nil {
		return err
	}
	return f.osFS.Rename(from, to)
}

func (f *recordingFS) SyncDir(path string) error {
	if err := f.note("syncdir"); err != nil {
		return err
	}
	return f.osFS.SyncDir(path)
}

func (f *recordingFS) reset(fail string) {
	f.mu.Lock()
	f.ops, f.fail = nil, fail
	f.mu.Unlock()
}

func newRecordingDir(t *testing.T) (*dir, *recordingFS) {
	t.Helper()
	fsys := &recordingFS{}
	d, err := openDir(t.TempDir(), fsys)
	if err != nil {
		t.Fatalf("openDir: %v", err)
	}
	return d, fsys
}

// TestPutIsWriteSyncRenameSyncDir: the order is the durability argument. The
// data is fsynced before it is renamed into place, so the name never points at
// unsynced bytes; the directory is fsynced after, so the rename itself
// survives a power loss.
func TestPutIsWriteSyncRenameSyncDir(t *testing.T) {
	d, fsys := newRecordingDir(t)
	if err := d.Put(context.Background(), "ns/p001/seg-1.bin", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(fsys.ops, ","); got != "write,rename,syncdir" {
		t.Fatalf("Put made %s; want write (which fsyncs), then rename, then syncdir", got)
	}
}

// TestAnInterruptedPutLeavesTheOldObjectOrTheNew: a failure at any step of a
// Put — standing in for a crash there — leaves a whole object.
func TestAnInterruptedPutLeavesTheOldObjectOrTheNew(t *testing.T) {
	ctx := context.Background()
	for _, step := range []string{"write", "rename", "syncdir"} {
		t.Run(step, func(t *testing.T) {
			d, fsys := newRecordingDir(t)
			if err := d.Put(ctx, "k", []byte("old")); err != nil {
				t.Fatal(err)
			}
			fsys.reset(step)
			if err := d.Put(ctx, "k", []byte("new, and longer")); !errors.Is(err, errInjected) {
				t.Fatalf("Put failing at %s returned %v", step, err)
			}
			got, err := d.Get(ctx, "k")
			if err != nil {
				t.Fatalf("Get after a Put failed at %s: %v", step, err)
			}
			if string(got) != "old" && string(got) != "new, and longer" {
				t.Fatalf("after a Put failed at %s the object is %q: neither the old nor the new", step, got)
			}
			names, _ := d.List(ctx, "")
			if len(names) != 1 {
				t.Fatalf("after a Put failed at %s List = %v; a temporary file is visible", step, names)
			}
		})
	}
}

// TestACrashsLeftoversAreInvisibleAndCleanedUp: a temporary file a crash left
// behind is never listed and is removed when the directory is next opened.
func TestACrashsLeftoversAreInvisibleAndCleanedUp(t *testing.T) {
	root := t.TempDir()
	d, err := openDir(root, osFS{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := d.Put(ctx, "ns/k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	leftover := filepath.Join(root, "ns", "k"+tempMarker+"12345")
	if err := os.WriteFile(leftover, []byte("half a segm"), 0o644); err != nil {
		t.Fatal(err)
	}
	if names, _ := d.List(ctx, ""); len(names) != 1 || names[0] != "ns/k" {
		t.Fatalf("List = %v; a crash's temporary file is visible", names)
	}
	if _, err := openDir(root, osFS{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(leftover); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reopening left the temporary file: %v", err)
	}
}

func TestNamesThatAreNotFileNamesRoundTrip(t *testing.T) {
	d, _ := newRecordingDir(t)
	ctx := context.Background()
	names := []string{"a:b/p001/seg-1.bin", "orders%2F*?/x", "ü/日本", "with space/and~tilde"}
	for _, n := range names {
		if err := d.Put(ctx, n, []byte(n)); err != nil {
			t.Fatalf("Put(%q): %v", n, err)
		}
		if got, err := d.Get(ctx, n); err != nil || string(got) != n {
			t.Fatalf("Get(%q) = %q, %v", n, got, err)
		}
	}
	listed, _ := d.List(ctx, "")
	for _, n := range names {
		found := false
		for _, l := range listed {
			found = found || l == n
		}
		if !found {
			t.Fatalf("List = %v, missing %q", listed, n)
		}
	}
}

func TestNamesThatEscapeAreRefused(t *testing.T) {
	d, _ := newRecordingDir(t)
	for _, n := range []string{"", "../outside", "a/../../b", "a//b", "./a", "a/."} {
		if err := d.Put(context.Background(), n, []byte("x")); err == nil {
			t.Errorf("Put(%q) succeeded; a name with an empty, . or .. component must be refused", n)
		}
	}
}

func TestFilesNotWrittenByTheStoreAreIgnored(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "bad%Zescape"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	d, err := openDir(root, osFS{})
	if err != nil {
		t.Fatal(err)
	}
	if names, _ := d.List(context.Background(), ""); len(names) != 0 {
		t.Fatalf("List = %v; a file with an invalid escape is not an object", names)
	}
}
