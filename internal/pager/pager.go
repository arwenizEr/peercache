package pager

import (
	"sort"
	"sync"
	"time"

	"github.com/arwenizEr/peercache/internal/clock"
)

// Defaults for [New].
const (
	// DefaultIdle is how long a pass may go without asking for a page
	// before its key list is dropped.
	DefaultIdle = time.Minute
	// DefaultMaxPasses bounds how many passes keep a key list at once.
	DefaultMaxPasses = 16
)

// Pager holds the sorted key lists of passes in progress. It is safe for
// concurrent use.
type Pager struct {
	clk  clock.Clock
	idle time.Duration
	max  int

	mu     sync.Mutex
	passes map[string]*pass
}

type pass struct {
	keys []string
	used time.Time
}

// New returns a Pager keeping at most max passes, each dropped after idle
// without a page. Non-positive values take the defaults.
func New(clk clock.Clock, idle time.Duration, max int) *Pager {
	if idle <= 0 {
		idle = DefaultIdle
	}
	if max <= 0 {
		max = DefaultMaxPasses
	}
	return &Pager{clk: clk, idle: idle, max: max, passes: map[string]*pass{}}
}

// Page returns up to limit keys after after, in order, and whether more
// remain, for the pass named id.
//
// A pass begins when after is empty: build is called for the keys, which are
// sorted once and kept. Later pages of the pass binary-search the kept list.
// A pass that has no list — dropped as idle or to make room, or begun before a
// restart — gets a new one from build, searched from after, which is correct
// and only as slow as one page used to be. The list is a point-in-time sample:
// keys added since are not in it, which callers rely on live replication for,
// and keys removed since are skipped by the caller when it reads them. The
// last page of a pass drops its list.
func (p *Pager) Page(id, after string, limit int, build func() []string) ([]string, bool) {
	if limit <= 0 {
		return nil, false
	}
	now := p.clk.Now()
	p.mu.Lock()
	ps := p.passes[id]
	if after == "" || ps == nil {
		p.mu.Unlock()
		keys := build()
		sort.Strings(keys)
		ps = &pass{keys: keys}
		p.mu.Lock()
		p.passes[id] = ps
		p.evictLocked(now, id)
	}
	ps.used = now
	keys := ps.keys
	p.mu.Unlock()

	i := sort.SearchStrings(keys, after)
	for i < len(keys) && keys[i] <= after {
		i++
	}
	end := i + limit
	if end >= len(keys) {
		p.mu.Lock()
		if p.passes[id] == ps {
			delete(p.passes, id)
		}
		p.mu.Unlock()
		return keys[i:], false
	}
	return keys[i:end], true
}

// Passes returns how many passes hold a key list.
func (p *Pager) Passes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.passes)
}

// evictLocked drops idle passes, then the least recently used beyond the
// bound, never keep.
func (p *Pager) evictLocked(now time.Time, keep string) {
	for id, ps := range p.passes {
		if id != keep && now.Sub(ps.used) >= p.idle {
			delete(p.passes, id)
		}
	}
	for len(p.passes) > p.max {
		var oldest string
		var at time.Time
		for id, ps := range p.passes {
			if id == keep {
				continue
			}
			if oldest == "" || ps.used.Before(at) {
				oldest, at = id, ps.used
			}
		}
		if oldest == "" {
			return
		}
		delete(p.passes, oldest)
	}
}
