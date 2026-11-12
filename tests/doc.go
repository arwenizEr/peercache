// Package tests holds PeerCache's behavioural, cluster and acceptance tests.
//
// They use only the public API of the root package, plus the in-process
// harness in internal/testkit, and live here rather than beside the code so the
// root directory holds the library and nothing else. Benchmarks, fuzz targets
// and godoc examples stay in the root: benchmarks next to the code they
// measure, and examples where godoc shows them.
//
// Run them with:
//
//	go test ./tests/...
package tests
