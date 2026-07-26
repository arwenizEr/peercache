package testkit

import (
	"strings"

	"github.com/hashicorp/memberlist"
)

// LocalGossipConfig returns a memberlist configuration for a loopback node on
// an OS-chosen port.
func LocalGossipConfig(name string) *memberlist.Config {
	cfg := memberlist.DefaultLocalConfig()
	cfg.Name = name
	cfg.BindAddr = "127.0.0.1"
	cfg.BindPort = 0 // ephemeral
	cfg.AdvertisePort = 0
	return cfg
}

// GossipBindAttempts is how many times to retry a bind the OS refuses.
const GossipBindAttempts = 12

// IsPortRefused reports whether an error is the OS declining a port it just
// handed out, rather than a real bind failure.
//
// Windows reserves large contiguous ephemeral ranges for Hyper-V and friends —
// 50510-51109 on the machine this was written on — and memberlist's port-0
// selection lands in them often enough to fail a run. The bind fails with
// "access permissions", which reads like a defect and is not one: the port was
// allocated and then refused.
//
// Retrying is what a real deployment does too, so this is not test-only
// sleight of hand. Callers bound the retry and report the last error, so a
// genuine bind problem still fails loudly rather than looping.
//
// See docs/decisions/open.md question 27.
//
// # Why this lives in testkit rather than beside its first caller
//
// It was written in M6 as an unexported helper in internal/membership's tests,
// which fixed the failures in that package and left the identical ones in the
// root package's cluster_test.go untouched. Those failed under `-count=2`
// during M7's gate, having been latent since M6 — two tests binding real
// sockets through a second code path nobody remembered was there.
//
// One exported copy means the next caller inherits the fix instead of
// rediscovering the bug.
func IsPortRefused(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "access permissions") ||
		strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "Only one usage of each socket address")
}

// RetryBind calls start until it succeeds, the error is not a refused port, or
// the attempt budget runs out. The last error is returned.
func RetryBind[T any](start func() (T, error)) (T, error) {
	var zero T
	var lastErr error
	for i := 0; i < GossipBindAttempts; i++ {
		v, err := start()
		if err == nil {
			return v, nil
		}
		lastErr = err
		if !IsPortRefused(err) {
			return zero, err
		}
	}
	return zero, lastErr
}
