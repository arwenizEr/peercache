package peercache

import "sync"

// writerDurability is what each writer has said, per namespace, about its
// records being in storage: the highest wire.ReplicateRequest.Durable this node
// has received from it. The memory budget reads it to choose which replica
// copies of persistent writes it may shed (pressure.keepFrom).
//
// A writer's bound only rises. A writer that restarts under the same name sends
// none until it has caught up, so what its previous run said stands, and
// covers nothing its previous run left unflushed. It is never forgotten either:
// a name is a handful of bytes per namespace, and forgetting it would put that
// writer's copies back under the time window alone, which is the rule that lost
// the copies of a dead owner's unflushed writes (docs/decisions/open.md
// question 68).
type writerDurability struct {
	mu sync.RWMutex
	by map[string]map[string]uint64 // namespace -> writer -> bound
}

// note records that writer's records in namespace are in storage up to bound.
func (w *writerDurability) note(namespace, writer string, bound uint64) {
	if bound == 0 || writer == "" {
		return
	}
	w.mu.RLock()
	cur, ok := w.by[namespace][writer]
	w.mu.RUnlock()
	if ok && cur >= bound {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.by == nil {
		w.by = map[string]map[string]uint64{}
	}
	m := w.by[namespace]
	if m == nil {
		m = map[string]uint64{}
		w.by[namespace] = m
	}
	if bound > m[writer] {
		m[writer] = bound
	}
}

// bound returns the bound writer has given for namespace, zero if none.
func (w *writerDurability) bound(namespace, writer string) uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.by[namespace][writer]
}

// snapshot returns a copy of namespace's bounds, nil if no writer has sent one.
func (w *writerDurability) snapshot(namespace string) map[string]uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	m := w.by[namespace]
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]uint64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
