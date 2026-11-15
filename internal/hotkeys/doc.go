// Package hotkeys estimates which keys a node serves most, in bounded memory,
// for WithHotKeyTracking. See docs/decisions/0040-hot-keys.md.
//
// It is Space-Saving (Metwally, Agrawal and El Abbadi, 2005) over a sample of
// accesses: a fixed set of counters, the smallest replaced when an untracked
// key arrives, so a key whose true share exceeds 1/capacity is always present
// and its count overestimated by at most the replaced minimum. Counts are
// halved every window, so the list describes what is hot now, not what was hot
// once.
package hotkeys
