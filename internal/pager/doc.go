// Package pager serves a sorted key list one page at a time, sorting it once per
// pass instead of once per page.
//
// Catch-up and hand-off page through a store 512 keys at a time, a cursor being
// the last key sent. Finding the next page by scanning and sorting every key
// above the cursor made one page cost O(n log n) and a full pass over n keys
// O(n² log n / 512): a 400,000-key pass took 57 seconds and 13.7 GB. A Pager
// keeps, per pass, the sorted key list built when the pass began, and answers
// each page with a binary search and a slice.
//
// The cost is memory: a kept list is one string header per key, 16 bytes,
// sharing the keys' bytes with the store — 16 MB for a million-key pass — and
// at most [DefaultMaxPasses] are kept, each dropped at its last page or after
// [DefaultIdle] without one.
package pager
