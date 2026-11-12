// Package metrics emits Prometheus and OpenTelemetry instrumentation.
//
// Beyond the usual throughput and latency series, this package carries the
// correctness counters, and those are the ones that matter: split-brain
// detections, stale-epoch activation rejections, version conflicts, mailbox
// rejections, and transfer failures.
//
// The reasoning is that PeerCache's guarantees are honest about having windows
// in them. A window that is not counted is indistinguishable from a window that
// does not exist, and an operator deserves to be able to tell the difference.
package metrics
