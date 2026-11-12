package membership

import (
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
)

// TestNodeMetaRoundTripsTheStartTime pins the gossip encoding join probation
// reads a node's age from.
func TestNodeMetaRoundTripsTheStartTime(t *testing.T) {
	in := nodeMeta{Epoch: 7, RPCAddr: "10.0.0.1:9000", Started: 1_700_000_000_123_456_789}
	out := decodeNodeMeta(in.encode())
	if out != in {
		t.Fatalf("round trip: %+v -> %q -> %+v", in, in.encode(), out)
	}
}

// TestNodeMetaFromAnOlderBuildIsAccepted keeps a rolling upgrade working. A
// peer on a build from before M9 advertises "epoch|rpcaddr" and one from before
// M4 just "epoch"; both must decode, with no start time, which probation reads
// as an established node.
func TestNodeMetaFromAnOlderBuildIsAccepted(t *testing.T) {
	for raw, want := range map[string]nodeMeta{
		"7|10.0.0.1:9000": {Epoch: 7, RPCAddr: "10.0.0.1:9000"},
		"7":               {Epoch: 7},
	} {
		if got := decodeNodeMeta(raw); got != want {
			t.Fatalf("decodeNodeMeta(%q) = %+v, want %+v", raw, got, want)
		}
	}
	if _, ok := (core.Node{}).StartedAt(); ok {
		t.Fatal("a node with no metadata reports a start time")
	}
}
