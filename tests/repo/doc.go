// Package repo holds tests that read the repository as text.
//
// They enforce the engineering rules no compiler can — no sleeps, no wall
// clock outside internal/clock, no panics on library paths, a documented
// public API, a dependency budget — and check that docs, CI and the build files
// agree with the code. One of them builds a cluster from a separate module, the
// way an application would.
package repo
