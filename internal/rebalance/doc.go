// Package rebalance moves ownership when membership changes.
//
// Planning computes only the affected partitions. A full reshuffle on every
// membership change would make a rolling upgrade of a nine-node cluster move
// the entire keyspace nine times, which is the failure mode consistent hashing
// exists to avoid and which rendezvous hashing also avoids.
//
// Transfer is streamed, with backpressure and resumability, so that a move
// interrupted by a second membership change resumes instead of restarting.
package rebalance
