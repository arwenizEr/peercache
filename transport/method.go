package transport

// Method names, matching the rpc entries in proto/peercache.proto.
//
// They are full gRPC method paths because that is what gRPC puts on the wire
// and what a proxy, a trace or a tcpdump will show. Writing them out here means
// the string in the service descriptor and the string a caller passes to
// [core.PeerClient.Invoke] are the same constant rather than two spellings that
// agree today.
//
// TestProtoMatchesMethodConstants asserts that this list and the .proto file
// name the same set of methods, so that adding an rpc to one without the other
// fails a test rather than producing a peer that answers "unknown method" at
// three in the morning.
const (
	// ServiceName is the gRPC service these methods belong to.
	ServiceName = "peercache.v1.Peer"

	MethodGet        = "/peercache.v1.Peer/Get"
	MethodReplicaGet = "/peercache.v1.Peer/ReplicaGet"
	MethodLoad       = "/peercache.v1.Peer/Load"
	MethodSet        = "/peercache.v1.Peer/Set"
	MethodDelete     = "/peercache.v1.Peer/Delete"
	MethodCondSet    = "/peercache.v1.Peer/CondSet"
	MethodIncrement  = "/peercache.v1.Peer/Increment"
	MethodTouch      = "/peercache.v1.Peer/Touch"
	MethodBatchGet   = "/peercache.v1.Peer/BatchGet"
	MethodCall       = "/peercache.v1.Peer/Call"
	MethodSubmit     = "/peercache.v1.Peer/Submit"
	MethodReplicate  = "/peercache.v1.Peer/Replicate"
	MethodCatchUp    = "/peercache.v1.Peer/CatchUp"
	MethodInvalidate = "/peercache.v1.Peer/Invalidate"
	MethodDescribe   = "/peercache.v1.Peer/Describe"
)

// Methods returns every method this transport serves, in the order they appear
// in proto/peercache.proto. Each call returns a fresh slice: this was once an
// exported variable, and a caller appending to it before New changed what the
// server registered.
func Methods() []string { return append([]string(nil), methods...) }

var methods = []string{
	MethodGet,
	MethodReplicaGet,
	MethodLoad,
	MethodSet,
	MethodDelete,
	MethodCondSet,
	MethodIncrement,
	MethodTouch,
	MethodBatchGet,
	MethodCall,
	MethodSubmit,
	MethodReplicate,
	MethodCatchUp,
	MethodInvalidate,
	MethodDescribe,
}
