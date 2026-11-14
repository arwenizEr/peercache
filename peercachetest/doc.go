// Package peercachetest runs a PeerCache cluster inside one test, so that an
// application can test its own code — group functions, loaders, the way it
// handles ErrNotOwner or a lost key — under failover, without sockets, real
// gossip or sleeping.
//
//	func TestCheckoutSurvivesTheOwnerCrashing(t *testing.T) {
//	    c := peercachetest.New(t, 3, peercachetest.WithCacheOptions(myOptions...))
//	    ns := c.Cache(0).Namespace("carts", peercache.WithReplicationFactor(2), peercache.WithSyncReplication())
//	    ... // open it on every node, write through c.Cache(i)
//	    c.Kill(c.Owner("carts", "cart:7"))
//	    ... // assert what the application sees now
//	}
//
// Every node runs the real library. Only three things are simulated: the
// network between them, membership (a deterministic stand-in for gossip), and
// the clock. Nothing moves until the test moves it: [Cluster.Advance] drives
// TTLs and timeouts, and [Cluster.Kill], [Cluster.Restart],
// [Cluster.Partition] and [Cluster.Heal] advance it past failure detection
// themselves and wait until the cluster has reacted, so the next line of the
// test sees the cluster as an application would after the event.
//
// Nodes are numbered 0 to n-1. Each is a peercache.Cache built with the
// options the test gives; the harness supplies the node ID, clock, membership
// and transport. It is the harness PeerCache's own failover tests are built on.
package peercachetest
