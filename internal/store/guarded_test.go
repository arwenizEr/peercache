package store_test

import "testing"

// held reports the keys in set, and counts how often it was asked.
type held struct {
	set   map[string]bool
	asked int
}

func (h *held) fn(key string) bool {
	h.asked++
	return h.set[key]
}

func TestLookupGuardedAsksOnlyOnAMiss(t *testing.T) {
	s, _ := newStore(t)
	s.Set("present", []byte("v"), 0)
	h := &held{set: map[string]bool{"shed": true}}

	if val, _, _, ok, isHeld := s.LookupGuarded("present", h.fn); !ok || isHeld || string(val) != "v" {
		t.Fatalf("present key: %q ok=%v held=%v", val, ok, isHeld)
	}
	if h.asked != 0 {
		t.Fatalf("held was asked %d times about a key memory holds; a hit must not pay for it", h.asked)
	}
	if _, _, _, ok, isHeld := s.LookupGuarded("shed", h.fn); ok || !isHeld {
		t.Fatalf("shed key: ok=%v held=%v, want a miss reported as held", ok, isHeld)
	}
	if _, _, _, ok, isHeld := s.LookupGuarded("absent", h.fn); ok || isHeld {
		t.Fatalf("absent key: ok=%v held=%v, want a plain miss", ok, isHeld)
	}
}

func TestSetIfAbsentGuardedStoresNothingOverAHeldKey(t *testing.T) {
	s, _ := newStore(t)
	h := &held{set: map[string]bool{"shed": true}}

	if _, stored, isHeld := s.SetIfAbsentGuarded("shed", []byte("loaded"), 0, h.fn); stored || !isHeld {
		t.Fatalf("stored=%v held=%v over a held key; storing would outrank what storage holds", stored, isHeld)
	}
	if _, _, ok := s.Get("shed"); ok {
		t.Fatal("a value was stored over a held key")
	}
	if _, stored, isHeld := s.SetIfAbsentGuarded("absent", []byte("loaded"), 0, h.fn); !stored || isHeld {
		t.Fatalf("absent key: stored=%v held=%v, want stored", stored, isHeld)
	}
	s.Set("present", []byte("v"), 0)
	asked := h.asked
	if resident, stored, _ := s.SetIfAbsentGuarded("present", []byte("loaded"), 0, h.fn); stored || string(resident) != "v" {
		t.Fatalf("present key: stored=%v resident=%q, want the resident value", stored, resident)
	}
	if h.asked != asked {
		t.Fatal("held was asked about a key memory holds")
	}
}
