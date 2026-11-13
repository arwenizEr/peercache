package persist

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// tempMarker is in the name of every file Dir writes before renaming it into
// place. Encoded object names can never contain it: '~' is escaped.
const tempMarker = "~tmp-"

// Dir returns a BlobStore that keeps each object as one file under root,
// creating root if it does not exist.
//
// # Durability
//
// Put writes the object to a temporary file in the target's directory, fsyncs
// it, renames it over the target, and fsyncs the directory, so that once Put
// returns the object survives a crash or power loss of the machine, and a Put
// interrupted at any point leaves the previous object or the new one, never a
// prefix. Temporary files a crash leaves behind are invisible to List and are
// removed when the next Dir opens root.
//
// On Windows a directory cannot be fsynced. The rename itself is atomic there,
// but whether it survives a power loss immediately depends on the file system's
// own journal; that is the platform's guarantee, not this package's.
//
// # Names
//
// Each slash-separated component of a name is stored with every byte outside
// [A-Za-z0-9._-] written as %XX, so a namespace holding ':' or '*' is a legal
// file name on every platform, and no name can reach outside root. An empty
// component, "." or ".." is refused.
//
// A Dir is for one process at a time, and every node of a cluster must reach
// the same files — a shared volume — for restore after a node's loss to work.
// On a volume only one node can see, it survives that node's restart and
// nothing more.
func Dir(root string) (BlobStore, error) {
	return openDir(root, osFS{})
}

func openDir(root string, fsys fileSystem) (*dir, error) {
	if root == "" {
		return nil, errors.New("persist: Dir needs a directory")
	}
	if err := fsys.MkdirAll(root); err != nil {
		return nil, fmt.Errorf("persist: creating %s: %w", root, err)
	}
	d := &dir{root: root, fs: fsys}
	if err := d.removeTemps(); err != nil {
		return nil, err
	}
	return d, nil
}

type dir struct {
	root string
	fs   fileSystem
	// locks serialise operations on one name. On POSIX a rename may replace
	// a file another reader has open; on Windows it fails with "Access is
	// denied" while one does, and two renames of one name race the same
	// way. A Dir is one process's, so in-process locks make that conflict
	// impossible rather than something to retry.
	locks [64]sync.RWMutex
}

func (d *dir) lockFor(target string) *sync.RWMutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(target))
	return &d.locks[h.Sum32()%uint32(len(d.locks))]
}

// path maps an object name to its file, refusing names that could escape.
func (d *dir) path(name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("persist: empty object name")
	}
	parts := strings.Split(name, "/")
	for i, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", fmt.Errorf("persist: object name %q has an empty, . or .. component", name)
		}
		parts[i] = encodeComponent(part)
	}
	return filepath.Join(append([]string{d.root}, parts...)...), nil
}

func encodeComponent(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			b.WriteByte(c)
			continue
		}
		fmt.Fprintf(&b, "%%%02X", c)
	}
	return b.String()
}

func decodeComponent(s string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			b.WriteByte(s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", false
		}
		var c byte
		if _, err := fmt.Sscanf(s[i+1:i+3], "%02X", &c); err != nil {
			return "", false
		}
		b.WriteByte(c)
		i += 2
	}
	return b.String(), true
}

func (d *dir) Put(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target, err := d.path(name)
	if err != nil {
		return err
	}
	parent := filepath.Dir(target)
	if err := d.fs.MkdirAll(parent); err != nil {
		return fmt.Errorf("persist: %s: %w", name, err)
	}
	tmp, err := d.fs.WriteTemp(parent, filepath.Base(target)+tempMarker, data)
	if err != nil {
		return fmt.Errorf("persist: writing %s: %w", name, err)
	}
	lock := d.lockFor(target)
	lock.Lock()
	err = d.fs.Rename(tmp, target)
	lock.Unlock()
	if err != nil {
		_ = d.fs.Remove(tmp)
		return fmt.Errorf("persist: placing %s: %w", name, err)
	}
	if err := d.fs.SyncDir(parent); err != nil {
		return fmt.Errorf("persist: syncing the directory of %s: %w", name, err)
	}
	return nil
}

func (d *dir) Get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	target, err := d.path(name)
	if err != nil {
		return nil, err
	}
	lock := d.lockFor(target)
	lock.RLock()
	data, err := d.fs.ReadFile(target)
	lock.RUnlock()
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("persist: %q: %w", name, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("persist: reading %s: %w", name, err)
	}
	return data, nil
}

func (d *dir) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var names []string
	err := d.fs.Walk(d.root, func(rel string) {
		if strings.Contains(rel, tempMarker) {
			return
		}
		parts := strings.Split(rel, "/")
		for i, part := range parts {
			decoded, ok := decodeComponent(part)
			if !ok {
				return // not a file this store wrote
			}
			parts[i] = decoded
		}
		if name := strings.Join(parts, "/"); strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("persist: listing %s: %w", d.root, err)
	}
	sort.Strings(names)
	return names, nil
}

func (d *dir) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	target, err := d.path(name)
	if err != nil {
		return err
	}
	lock := d.lockFor(target)
	lock.Lock()
	err = d.fs.Remove(target)
	lock.Unlock()
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("persist: deleting %s: %w", name, err)
	}
	return nil
}

// removeTemps deletes what interrupted Puts left behind.
func (d *dir) removeTemps() error {
	var temps []string
	if err := d.fs.Walk(d.root, func(rel string) {
		if strings.Contains(rel, tempMarker) {
			temps = append(temps, filepath.Join(d.root, filepath.FromSlash(rel)))
		}
	}); err != nil {
		return fmt.Errorf("persist: scanning %s: %w", d.root, err)
	}
	for _, t := range temps {
		if err := d.fs.Remove(t); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("persist: removing %s: %w", t, err)
		}
	}
	return nil
}

// fileSystem is every file operation Dir makes, so that a test can fail any
// one of them and check what a crash at that point leaves behind.
type fileSystem interface {
	MkdirAll(path string) error
	// WriteTemp creates a new file in dir whose name starts with prefix,
	// writes data, fsyncs and closes it, and returns its path.
	WriteTemp(dir, prefix string, data []byte) (string, error)
	Rename(from, to string) error
	SyncDir(path string) error
	ReadFile(path string) ([]byte, error)
	Remove(path string) error
	// Walk calls fn with the slash-separated path, relative to root, of
	// every regular file under root.
	Walk(root string, fn func(rel string)) error
}

type osFS struct{}

func (osFS) MkdirAll(path string) error { return os.MkdirAll(path, 0o755) }

func (osFS) WriteTemp(dir, prefix string, data []byte) (string, error) {
	f, err := os.CreateTemp(dir, prefix)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(name)
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

func (osFS) Rename(from, to string) error { return os.Rename(from, to) }

func (osFS) SyncDir(path string) error {
	if runtime.GOOS == "windows" {
		// Directories cannot be opened for fsync on Windows; see Dir.
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (osFS) ReadFile(path string) ([]byte, error) { return os.ReadFile(path) }

func (osFS) Remove(path string) error { return os.Remove(path) }

func (osFS) Walk(root string, fn func(rel string)) error {
	return filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // removed while walking
			}
			return err
		}
		if !e.Type().IsRegular() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fn(filepath.ToSlash(rel))
		return nil
	})
}
