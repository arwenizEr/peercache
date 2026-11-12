// Package replication maintains copies of owned data on other nodes.
//
// Replication factor is 1 to 3 per namespace. Fan-out is asynchronous by
// default and synchronous on request. When a member
// fails, a replica is promoted on the epoch bump that follows; a node returning
// from the dead catches up rather than serving what it remembers.
//
// The async data-loss window is a measured, documented number, not an unknown.
// The measurement is in docs/benchmarks.md and in the README's guarantees
// section.
package replication
