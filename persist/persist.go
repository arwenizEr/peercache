package persist

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/arwenizEr/peercache/internal/core"
)

// BlobStore holds a persistent namespace's segments and snapshots.
//
// Every method must be safe for concurrent use and must honour ctx. Names are
// slash-separated paths of printable ASCII, at most 1024 bytes; a backend may
// map them onto its own layout but must return them unchanged from List.
//
//   - Put stores data under name, replacing any object already there. It must
//     be atomic: a concurrent or later Get sees the whole of data or the
//     previous object, never a prefix or a mixture. Put returns only once the
//     object is as durable as the backend can make it.
//   - Get returns the object stored under name. An absent object is an error
//     wrapping ErrNotFound.
//   - List returns the names that start with prefix, sorted. It may miss
//     objects whose Put has not returned, and may still report objects whose
//     Delete has not; it never reports an object that was never Put.
//   - Delete removes name. Deleting an absent object is not an error.
//
// package persisttest checks all of it.
type BlobStore = core.BlobStore

// ErrNotFound is returned, wrapped, by Get for an absent object. It is the same
// error as peercache.ErrNotFound.
var ErrNotFound = core.ErrNotFound

// Memory returns a BlobStore held in this process's memory.
//
// It survives nothing — not a restart, not the loss of the node it runs on —
// and exists for tests, and for trying persistence out before choosing a real
// backend. Objects are copied on the way in and out, so a caller can never
// change what is stored by changing its own slice.
func Memory() BlobStore {
	return &memory{objects: map[string][]byte{}}
}

type memory struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func (m *memory) Put(ctx context.Context, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	m.mu.Lock()
	m.objects[name] = stored
	m.mu.Unlock()
	return nil
}

func (m *memory) Get(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	data, ok := m.objects[name]
	m.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("persist: %q: %w", name, ErrNotFound)
	}
	out := make([]byte, len(data))
	copy(out, data)
	return out, nil
}

func (m *memory) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.RLock()
	var names []string
	for name := range m.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	m.mu.RUnlock()
	sort.Strings(names)
	return names, nil
}

func (m *memory) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.objects, name)
	m.mu.Unlock()
	return nil
}
