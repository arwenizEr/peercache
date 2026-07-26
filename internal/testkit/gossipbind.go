package testkit

import (
	"fmt"
	"net"
	"strings"

	"github.com/hashicorp/memberlist"
)

// LocalGossipConfig returns a memberlist configuration for a loopback node on a
// port this process has just proved it can bind.
//
// It does not use port 0. See [FreeLoopbackPort] for why that matters.
func LocalGossipConfig(name string) *memberlist.Config {
	cfg := memberlist.DefaultLocalConfig()
	cfg.Name = name
	cfg.BindAddr = "127.0.0.1"
	if port, err := FreeLoopbackPort(); err == nil {
		cfg.BindPort = port
		cfg.AdvertisePort = port
	} else {
		// Fall back to letting the OS choose. The caller's RetryBind still
		// applies, and a failure here is reported by memberlist rather
		// than swallowed silently.
		cfg.BindPort = 0
		cfg.AdvertisePort = 0
	}
	return cfg
}

// PortProbeAttempts bounds the search in [FreeLoopbackPort].
//
// Sized against the failure it exists for: Windows reserves contiguous blocks
// of the ephemeral range, 600 ports wide on the machine this was written on,
// and allocation is sequential. A budget smaller than the block cannot cross
// it. This is generous enough to cross several.
const PortProbeAttempts = 2000

// GossipBindAttempts is how many times [RetryBind] retries a refused bind.
//
// Small on purpose. With [FreeLoopbackPort] doing the searching, this only has
// to cover the race between probing a port and binding it, which is one or two
// attempts in practice.
const GossipBindAttempts = 8

// FreeLoopbackPort returns a loopback port that this process has just bound and
// released on both UDP and TCP.
//
// # Why probing beats retrying
//
// The previous approach asked memberlist for port 0 and retried the whole
// construction when the OS refused the port it had just handed out. That was
// measured and does not work, for a reason worth writing down:
//
// Windows allocates ephemeral ports strictly sequentially. Forty consecutive
// binds on this machine returned 51111 through 51150 with no gaps. Windows also
// reserves contiguous blocks of that range for Hyper-V and friends — 50510 to
// 51109 here, 600 ports wide — and a bind inside a reserved block fails with
// "access permissions" rather than being skipped.
//
// So when the allocator's cursor is inside a reserved block, every subsequent
// request is refused until the cursor walks out the far side. Retrying an
// expensive operation twelve times advances the cursor by a few dozen ports and
// then gives up, still inside a 600-port block. That is exactly what happened:
// three membership tests failed at ports 50788, 50908 and 51028 — marching
// upward, each after exhausting its retries.
//
// Probing fixes it by making each attempt cheap. Opening and closing a socket
// advances the cursor just as a full memberlist construction does, but costs
// nothing, so the budget can be large enough to cross the block.
//
// # Why both UDP and TCP
//
// memberlist listens on both at the same port number. A port free on one and
// taken on the other would pass a UDP-only probe and then fail the bind, which
// is the same bug with extra steps.
//
// # The residual race
//
// Between releasing the probe and memberlist binding, another process can take
// the port. That window is real and small, and [RetryBind] covers it — with the
// search already done, a retry now lands on a fresh port rather than re-entering
// the reserved block.
func FreeLoopbackPort() (int, error) {
	var lastErr error
	for i := 0; i < PortProbeAttempts; i++ {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			lastErr = err
			if IsPortRefused(err) {
				// The cursor advanced; try the next one.
				continue
			}
			return 0, err
		}
		port := pc.LocalAddr().(*net.UDPAddr).Port

		l, lerr := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		_ = pc.Close()
		if lerr != nil {
			lastErr = lerr
			continue
		}
		_ = l.Close()
		return port, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("testkit: no bindable loopback port in %d attempts", PortProbeAttempts)
	}
	return 0, fmt.Errorf("testkit: no bindable loopback port in %d attempts: %w",
		PortProbeAttempts, lastErr)
}

// IsPortRefused reports whether an error is the OS declining a port rather than
// a real bind failure.
//
// See docs/decisions/open.md question 27.
func IsPortRefused(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "access permissions") ||
		strings.Contains(msg, "address already in use") ||
		strings.Contains(msg, "Only one usage of each socket address") ||
		strings.Contains(msg, "cannot assign requested address")
}

// RetryBind calls start until it succeeds, the error is not a refused port, or
// the attempt budget runs out. The last error is returned.
//
// It covers the gap between [FreeLoopbackPort] proving a port bindable and the
// caller actually binding it. It is not the primary defence; the probe is.
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
