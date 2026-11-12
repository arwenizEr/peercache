// Package eval compares candidate local cache libraries for the L1 tier.
//
// docs/design.md section 4 (M1) requires evaluating at least two candidates under this
// workload and recording the comparison, rather than assuming the first one is
// best. This package is that evaluation, kept in the tree so the numbers can be
// re-derived rather than merely believed.
//
// # Why this is a separate module
//
// Evaluating three libraries means depending on three libraries. Two of them
// lose, and a losing candidate has no business in the shipping dependency
// graph — docs/design.md's dependency budget exists precisely to stop that kind of
// accumulation. This directory carries its own go.mod, so `go build ./...` and
// `go test ./...` in the parent module never see any of them.
//
// Run it explicitly:
//
//	cd internal/l1/eval && go test -bench . -benchmem
//	cd internal/l1/eval && go test -run TestReport -v
//
// The results are recorded in docs/decisions/0005-l1-cache-library.md.
//
// # What is being measured
//
// The L1 tier caches copies of keys this node does not own. Its workload is:
// read-heavy, small values, a keyspace much larger than the capacity, and a
// skewed access distribution — a few hot keys and a long tail. So the
// measurements are throughput under parallel reads, throughput under a mixed
// read/write load, and hit ratio under a Zipf distribution at a capacity well
// below the keyspace.
//
// Hit ratio is the one that matters most. At these sizes every candidate is
// fast enough that the difference between them is smaller than the cost of a
// cache miss, and a miss in L1 is a network round trip to the owner.
package eval
