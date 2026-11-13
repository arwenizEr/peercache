// Package persist defines where a persistent namespace keeps its data, and
// provides the backends that need nothing beyond the standard library.
//
// A [BlobStore] stores whole, immutable objects by name. That is all a backend
// does: it is never asked to order writes, compare versions or apply anything
// conditionally. The cache writes segments and snapshots through it and
// resolves every question of which write wins itself, by merging on read. A
// backend is therefore a few dozen lines, and the part that has to be right
// under failure is written and tested once.
//
// Backends that need a vendor SDK — S3, GCS, Azure Blob — live in their own
// modules, with their own go.mod, so that the core module's dependencies do not
// grow with every cloud. They implement [BlobStore] and run the contract tests
// in package persisttest.
//
// See docs/decisions/0029-persistence-as-a-segment-log.md.
package persist
