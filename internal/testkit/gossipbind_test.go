package testkit_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/arwenizEr/peercache/internal/testkit"
)

// TestFreeLoopbackPortReturnsABindablePort is the regression test for the bug
// that made three membership tests fail intermittently through M6 and M7.
//
// The contract is narrow and worth stating exactly: the returned port must be
// bindable on both UDP and TCP, because memberlist listens on both at the same
// number. A port that satisfies only one of those passes a naive probe and then
// fails the real bind, which is the original bug with extra steps.
//
// The loop count matters. Windows allocates ephemeral ports sequentially and
// reserves contiguous blocks of them, so a single call proves almost nothing —
// it proves the cursor was not in a reserved block at that instant, which was
// true of every green run this bug ever produced. Two hundred calls walk the
// cursor two hundred ports forward and will cross a reserved boundary on any
// machine that has one.
func TestFreeLoopbackPortReturnsABindablePort(t *testing.T) {
	const probes = 200

	seen := make(map[int]int, probes)
	for i := 0; i < probes; i++ {
		port, err := testkit.FreeLoopbackPort()
		if err != nil {
			t.Fatalf("probe %d: %v", i, err)
		}
		if port <= 0 || port > 65535 {
			t.Fatalf("probe %d returned port %d", i, port)
		}
		seen[port]++

		// The actual contract: both protocols, same number.
		pc, err := net.ListenPacket("udp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			t.Fatalf("probe %d returned port %d, which does not bind on UDP: %v\n"+
				"FreeLoopbackPort must not return a port it has not proved bindable",
				i, port, err)
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			_ = pc.Close()
			t.Fatalf("probe %d returned port %d, which binds on UDP but not TCP: %v\n"+
				"memberlist needs both at the same number", i, port, err)
		}
		_ = l.Close()
		_ = pc.Close()
	}
}

// TestIsPortRefusedRecognisesTheOSRefusal pins the string matching, which is
// the weakest part of this and the part most likely to rot.
//
// Matching on message text is unpleasant. The alternative — comparing
// syscall.Errno values — is worse here: the Windows error is WSAEACCES delivered
// through a wrapped *net.OpError whose unwrapping differs by Go version, and the
// three messages below are stable across every version this project has built
// on. A false negative means a retry that should have happened does not, and the
// test that would catch it is the flaky one this whole mechanism exists to fix.
func TestIsPortRefusedRecognisesTheOSRefusal(t *testing.T) {
	refused := []string{
		// Windows, a port inside a reserved block.
		`listen udp 127.0.0.1:50788: bind: An attempt was made to access a socket in a way forbidden by its access permissions.`,
		// Unix, port already taken.
		`listen udp 127.0.0.1:9999: bind: address already in use`,
		// Windows, port already taken.
		`listen tcp 127.0.0.1:9999: bind: Only one usage of each socket address is normally permitted`,
		// The address itself is unusable, which a different port fixes.
		`listen udp 127.0.0.1:0: bind: cannot assign requested address`,
	}
	for _, msg := range refused {
		if !testkit.IsPortRefused(fmt.Errorf("%s", msg)) {
			t.Errorf("not recognised as a refused port, so no retry would happen:\n  %s", msg)
		}
	}

	notRefused := []string{
		"connection refused",
		"no such host",
		"context deadline exceeded",
		"memberlist: failed to start",
	}
	for _, msg := range notRefused {
		if testkit.IsPortRefused(fmt.Errorf("%s", msg)) {
			t.Errorf("treated as a refused port, so a real failure would be retried "+
				"and then reported as a bind problem:\n  %s", msg)
		}
	}

	if testkit.IsPortRefused(nil) {
		t.Error("a nil error is not a refused port")
	}
}

// TestRetryBindStopsOnARealError checks that the retry does not mask failures
// it cannot fix. Looping eight times over a genuine misconfiguration turns one
// clear error into the same error eight times slower.
func TestRetryBindStopsOnARealError(t *testing.T) {
	calls := 0
	_, err := testkit.RetryBind(func() (int, error) {
		calls++
		return 0, fmt.Errorf("memberlist: config is invalid")
	})
	if err == nil {
		t.Fatal("RetryBind returned no error for a permanently failing start")
	}
	if calls != 1 {
		t.Errorf("start was called %d times for a non-port error; want 1", calls)
	}
}

// TestRetryBindRetriesARefusedPort is the other half: a refused port must be
// retried, or the probe's residual race becomes a flake.
func TestRetryBindRetriesARefusedPort(t *testing.T) {
	calls := 0
	got, err := testkit.RetryBind(func() (int, error) {
		calls++
		if calls < 3 {
			return 0, fmt.Errorf("listen udp 127.0.0.1:50788: bind: " +
				"An attempt was made to access a socket in a way forbidden by its access permissions.")
		}
		return 42, nil
	})
	if err != nil {
		t.Fatalf("RetryBind gave up on a retryable error: %v", err)
	}
	if got != 42 {
		t.Errorf("RetryBind returned %d, want 42", got)
	}
	if calls != 3 {
		t.Errorf("start was called %d times, want 3", calls)
	}
}
