#!/usr/bin/env bash
#
# Mutation audit: break each core invariant on purpose and check that the test
# claiming to guard it actually fails.
#
# CLAUDE.md requires every acceptance criterion to be "covered by a named test
# that would fail if the behavior regressed". A test that exists is not evidence
# of that — it can pass because the behaviour is right, or because it never
# really looked. This tells the two apart.
#
# It is not run by CI. It rewrites source files, and a mutation left behind by a
# crashed run would be far worse than the coverage it buys. Run it by hand when
# adding a test that guards a specific invariant:
#
#     bash scripts/mutation-audit.sh
#
# Results are recorded in docs/verification.md.

set -u
cd "$(dirname "$0")/.." || exit 1

# Refuse to run on a dirty tree. Reverting is done with `git checkout --`, which
# would silently discard uncommitted work in the files being mutated.
if [ -n "$(git status --porcelain)" ]; then
  echo "refusing to run: the working tree has uncommitted changes." >&2
  echo "this script reverts mutations with 'git checkout --', which would discard them." >&2
  git status --short >&2
  exit 1
fi

caught=0
vacuous=0
skipped=0

# mutate NAME FILE FROM TO TEST PACKAGE
#
# Replaces the first occurrence of FROM with TO in FILE, runs TEST in PACKAGE,
# and reports whether the test noticed. The file is always restored.
mutate() {
  local name="$1" file="$2" from="$3" to="$4" test="$5" pkg="$6"

  if ! python - "$file" "$from" "$to" <<'PY'
import sys
path, frm, to = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(path, encoding="utf-8").read()
if frm not in s:
    sys.exit(1)
open(path, "w", encoding="utf-8", newline="").write(s.replace(frm, to, 1))
PY
  then
    echo "SKIP     $name (the code has moved; update this script)"
    skipped=$((skipped + 1))
    git checkout -- "$file"
    return
  fi

  if go test "$pkg" -run "$test" -count=1 -timeout 120s >/dev/null 2>&1; then
    echo "VACUOUS  $name -- $test PASSED with the behaviour broken"
    vacuous=$((vacuous + 1))
  else
    echo "caught   $name -- by $test"
    caught=$((caught + 1))
  fi
  git checkout -- "$file"
}

echo "=== invariant mutation audit ==="

mutate "Set no longer copies the caller's buffer" \
  internal/store/store.go \
  'stored := make([]byte, len(val))
	copy(stored, val)' \
  'stored := val' \
  'TestSetCopiesTheCallersBuffer' ./internal/store/

mutate "Get copies instead of aliasing" \
  internal/store/store.go \
  'sh.hits.Add(1)
	return e.val, e.version, true' \
  'sh.hits.Add(1)
	cp := make([]byte, len(e.val)); copy(cp, e.val); return cp, e.version, true' \
  'TestGetReturnsTheStoredSlice' ./internal/store/

mutate "read-path expiry disabled" \
  internal/store/store.go \
  'if e.expiresAt != 0 && s.clk.Now().UnixNano() >= e.expiresAt {' \
  'if false && e.expiresAt != 0 && s.clk.Now().UnixNano() >= e.expiresAt {' \
  'TestExpiryIsObservedOnRead' ./internal/store/

mutate "shard selection collapses to one shard" \
  internal/store/store.go \
  'return s.shards[xxhash.Sum64String(key)&s.mask]' \
  'return s.shards[0]' \
  'TestKeysSpreadAcrossShards' ./internal/store/

mutate "TTL deadline overflow reintroduced" \
  internal/store/store.go \
  'expiresAt = deadlineNanos(s.clk.Now(), ttl)' \
  'expiresAt = s.clk.Now().Add(ttl).UnixNano()' \
  'TestUnrepresentableTTLDoesNotExpireImmediately' ./internal/store/

mutate "L1 accepts an older version" \
  internal/l1/l1.go \
  'if live && version <= existing.version {' \
  'if false && live && version <= existing.version {' \
  'TestOlderVersionIsRejected' ./internal/l1/

mutate "L1 stops clamping TTL to MaxTTL" \
  internal/l1/l1.go \
  'if ttl > c.maxTTL {
		ttl = c.maxTTL
	}' \
  '' \
  'TestEveryEntryIsTTLBounded' ./internal/l1/

mutate "L1 write stripe removed" \
  internal/l1/l1.go \
  'stripe := c.stripeFor(key)
	stripe.Lock()
	defer stripe.Unlock()

	now := c.clk.Now().UnixNano()' \
  'now := c.clk.Now().UnixNano()' \
  'TestConcurrentPutsNeverLeaveAnOlderVersion' ./internal/l1/

mutate "frame parser accepts an unknown code" \
  internal/codec/frame.go \
  'if !known {
			return nil, fmt.Errorf("codec: unknown frame code %d: %w", code, errCorruptFrame)
		}' \
  'if !known {
			return src[1:], nil
		}' \
  'TestUnframeRejectsCorruptInput' ./internal/codec/

mutate "namespaces share one store" \
  peercache.go \
  'ns = c.newNamespaceLocked(name, resolved)' \
  'if len(c.order) > 0 { ns = &namespace{cache: c, name: name, opts: resolved, store: c.namespaces[c.order[0]].store} } else { ns = c.newNamespaceLocked(name, resolved) }' \
  'TestNamespacesAreIsolated' .

mutate "singleflight bypassed" \
  namespace.go \
  'result := n.flight.Do(ctx, key, func(loadCtx context.Context) ([]byte, time.Duration, error) {' \
  'if true {
		v, ttl, err := n.opts.Loader(ctx, key)
		if err != nil {
			return nil, err
		}
		n.store.Set(key, n.encode(v), ttl)
		return v, nil
	}
	result := n.flight.Do(ctx, key, func(loadCtx context.Context) ([]byte, time.Duration, error) {' \
  'TestGetOrLoadCollapsesConcurrentMisses' .

mutate "store version stops incrementing" \
  internal/store/store.go \
  'version := sh.entries[key].version + 1' \
  'version := uint64(1)' \
  'TestVersionsIncrementPerKey' ./internal/store/

mutate "L1 invalidation ignores the resident version" \
  internal/l1/l1.go \
  'if !ok || e.version >= version {
		return false
	}' \
  'if !ok {
		return false
	}' \
  'TestInvalidateOlderThanKeepsNewerCopies' ./internal/l1/

mutate "loader panic is no longer contained" \
  internal/singleflight/singleflight.go \
  'if r := recover(); r != nil {
				recovered = true
				c.err = fmt.Errorf("%w: %v", core.ErrLoaderPanic, r)
			}' \
  '_ = recovered' \
  'TestPanickingLoaderBecomesAnError' ./internal/singleflight/

mutate "completed flights are never released" \
  internal/singleflight/singleflight.go \
  'g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()' \
  '' \
  'TestSequentialCallsDoNotCollapse' ./internal/singleflight/

mutate "fake clock drops the creation-order tie-break" \
  internal/clock/fake.go \
  'return h[i].seq < h[j].seq' \
  'return false' \
  'TestFakeEqualDeadlinesFireInCreationOrder' ./internal/clock/

mutate "namespace skips the context check" \
  namespace.go \
  'func (n *namespace) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}' \
  'func (n *namespace) Get(ctx context.Context, key string) ([]byte, error) {' \
  'TestCancelledContextIsReportedFirst' .

mutate "loader runs without the namespace deadline" \
  namespace.go \
  'loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()' \
  '' \
  'TestLoaderTimeoutIsTheNamespacesNotTheCallers' .

mutate "escaped group state is honoured instead of ignored" \
  internal/actor/state.go \
  'if s.rec.generation.Load() == s.gen {
		return true
	}
	s.escapes.Add(1)
	return false' \
  'return true' \
  'TestEscapedStateIsInertAndCounted' ./internal/actor/

mutate "a full mailbox blocks instead of rejecting" \
  internal/actor/record.go \
  'select {
	case r.act.mailbox <- req:
		r.mu.Unlock()
	default:
		r.mu.Unlock()
		r.rt.rejections.Add(1)
		return nil, fmt.Errorf("actor: group %q: %w", r.id, core.ErrGroupOverloaded)
	}' \
  'mb := r.act.mailbox
	r.mu.Unlock()
	mb <- req' \
  'TestFullMailboxRejectsRatherThanBlocking' ./internal/actor/

mutate "a group function panic is no longer contained" \
  internal/actor/record.go \
  'if p := recover(); p != nil {
			res = result{err: fmt.Errorf("%w: group %q: %v", core.ErrGroupFuncPanic, r.id, p)}
			r.rt.opts.Logger.Error("group function panicked", "group", r.id, "panic", p)
		}' \
  '_ = r' \
  'TestPanickingGroupFuncDoesNotWedgeTheGroup' ./internal/actor/

mutate "deactivation discards group state" \
  internal/actor/record.go \
  'r.stopTimers()
	r.act = nil' \
  'r.stopTimers()
	r.state = nil
	r.act = nil' \
  'TestDeactivationPreservesState' ./internal/actor/

mutate "deactivation discards the timer schedule" \
  internal/actor/record.go \
  'func (r *record) stopTimers() {
	for _, gt := range r.timers {
		if gt.timer != nil {
			gt.timer.Stop()
			gt.timer = nil
		}
	}
}' \
  'func (r *record) stopTimers() {
	for _, gt := range r.timers {
		if gt.timer != nil {
			gt.timer.Stop()
		}
	}
	r.timers = nil
}' \
  'TestDeactivationPreservesPendingTimers' ./internal/actor/

mutate "a group with pending timers is idle-deactivated" \
  internal/actor/record.go \
  'if !forced && len(r.timers) > 0 {' \
  'if false && !forced && len(r.timers) > 0 {' \
  'TestDeactivationPreservesPendingTimers' ./internal/actor/

mutate "the active-group cap is ignored" \
  internal/actor/runtime.go \
  'for rt.active-len(victims) > rt.opts.MaxActive {' \
  'for false && rt.active-len(victims) > rt.opts.MaxActive {' \
  'TestActiveCapEvictsLeastRecentlyActivated' ./internal/actor/

mutate "rescheduling a timer name accumulates duplicates" \
  internal/actor/record.go \
  'if existing, ok := r.timers[name]; ok && existing.timer != nil {
		existing.timer.Stop()
	}' \
  '' \
  'TestReschedulingATimerReplacesIt' ./internal/actor/

mutate "the Items snapshot aliases group state" \
  group.go \
  'dup := make([]byte, len(val))
				copy(dup, val)
				items[key] = dup' \
  'items[key] = val' \
  'TestItemsSnapshotDoesNotAliasGroupState' .

mutate "ownership ignores the node identity" \
  internal/ownership/table.go \
  'x := keyHash ^ (nodeSeed * 0x9E3779B97F4A7C15)' \
  'x := keyHash' \
  'TestOwnershipIsUniform' ./internal/ownership/

mutate "ownership hashes with a bare XOR" \
  internal/ownership/table.go \
  'x := keyHash ^ (nodeSeed * 0x9E3779B97F4A7C15)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x' \
  'return keyHash ^ nodeSeed' \
  'TestOwnershipIsUniform' ./internal/ownership/

# Note: removing the sort in Update is deliberately NOT audited. Owner takes the
# maximum score, which is order-independent, so the sort changes nothing an
# ownership test can observe. Auditing it produced a vacuous result and led to
# the over-claiming comment on Update being corrected instead.
mutate "the replica ranking is unsorted" \
  internal/ownership/table.go \
  'sort.Slice(ranked, func(i, j int) bool {
		si, sj := mix(keyHash, ranked[i].seed), mix(keyHash, ranked[j].seed)
		if si != sj {
			return si > sj
		}
		// Deterministic tie-break, so two nodes never disagree because
		// their sorts were unstable in different ways.
		return ranked[i].node.ID < ranked[j].node.ID
	})' \
  '' \
  'TestReplicasAreARankedPrefix' ./internal/ownership/

mutate "an unknown membership claims everything" \
  internal/ownership/guard.go \
  'if len(snap.nodes) == 0 {
		// A node that has not learned the membership yet owns nothing.
		// Answering "yes" here would let a starting pod activate the
		// entire keyspace for as long as it took to hear from anyone.
		g.refusals.Add(1)
		return fmt.Errorf("ownership: membership is unknown: %w", core.ErrNotOwner)
	}' \
  'if len(snap.nodes) == 0 {
		return nil
	}' \
  'TestGuardWithNoMembershipOwnsNothing' ./internal/ownership/

mutate "the guard activates groups it does not own" \
  internal/ownership/guard.go \
  'if !ok || current.ID != g.self.ID {' \
  'if false {' \
  'TestGuardRefusesGroupsItDoesNotOwn' ./internal/ownership/

mutate "newly acquired groups activate immediately" \
  internal/ownership/guard.go \
  'if elapsed >= int64(g.activationDelay) {
		return nil
	}' \
  'if true {
		return nil
	}' \
  'TestGuardDefersOnlyNewlyAcquiredGroups' ./internal/ownership/

mutate "the guard compares against the previous view, not the settled one" \
  internal/ownership/guard.go \
  'if now-g.changedAt.Load() >= int64(g.activationDelay) {
		g.settled.Store(g.table.current.Load())
	}' \
  'g.settled.Store(g.table.current.Load())' \
  'TestPartitionedMinorityStopsActivating' ./internal/ownership/

mutate "a stale epoch is accepted"   internal/ownership/guard.go   'if epoch < snap.epoch {'   'if false {'   'TestStaleEpochIsRefused' ./internal/ownership/

mutate "a caller ahead of this node is not deferred"   internal/ownership/guard.go   'if epoch > snap.epoch {'   'if false {'   'TestFutureEpochDefersRatherThanRefusing' ./internal/ownership/

mutate "the epoch is checked before ownership"   internal/ownership/guard.go   '	current, ok := owner(snap, groupKey)'   '	if epoch < snap.epoch {
		return fmt.Errorf("mutant: %w", core.ErrStaleEpoch)
	}
	current, ok := owner(snap, groupKey)'   'TestNotOwnerBeatsStaleEpoch' ./internal/ownership/

mutate "local activation does not use the current epoch"   internal/ownership/guard.go   'return g.CanActivateAt(groupKey, g.table.Epoch())'   'return g.CanActivateAt(groupKey, 0)'   'TestCanActivateUsesTheCurrentEpoch' ./internal/ownership/

mutate "gossip metadata updates advance the epoch" \
  internal/membership/gossip.go \
  'kind, transition = core.ChangeUpdate, false' \
  'kind, transition = core.ChangeUpdate, true' \
  'TestEpochDoesNotRunAway' ./internal/membership/

mutate "the gossip event goroutine is never released" \
  internal/membership/gossip.go \
  'close(g.stopConsume)
	<-g.consumeDone' \
  '_ = g.consumeDone' \
  'TestCloseLeavesNoGoroutines' ./internal/membership/

# The two below are wiring mutations rather than logic mutations. Every M3
# invariant above holds inside internal/ownership whether or not the library
# ever calls it; these check that peercache.New connects the guard to the
# membership authority and to activation. A seam nobody tests is a seam that
# breaks silently.
mutate "the guard is built but never consulted" \
  peercache.go \
  'canActivate = guard.CanActivate' \
  'canActivate = nil' \
  'TestTwoNodesAgreeOnOwnership' .

mutate "the guard never learns the membership" \
  peercache.go \
  'guard.Watch(resolved.Membership)' \
  '_ = resolved.Membership' \
  'TestJoiningNodeDefersNewlyAcquiredGroups' .

# ---------------------------------------------------------------- M4

mutate "a length prefix buys an allocation" \
  internal/wire/bounded.go \
  'var out []BatchEntry
	for i := 0; i < n; i++ {' \
  'out := make([]BatchEntry, 0, n)
	for i := 0; i < n; i++ {' \
  'TestALengthPrefixCannotBuyAnAllocation' ./internal/wire/

mutate "the L1 tier compares versions, ignoring the epoch" \
  internal/l1/l1.go \
  'if live && !stamp.NewerThan(existing.stamp) {' \
  'if live && stamp.Version <= existing.stamp.Version {' \
  'TestANewOwnerSupersedesAHigherVersion' ./internal/l1/

mutate "invalidation compares versions, ignoring the epoch" \
  internal/l1/l1.go \
  'if !ok || !stamp.NewerThan(e.stamp) {' \
  'if !ok || stamp.Version <= e.stamp.Version {' \
  'TestInvalidationRespectsTheEpoch' ./internal/l1/

mutate "the epoch does not dominate the version" \
  internal/core/node.go \
  'if s.Epoch != other.Epoch {
		return s.Epoch > other.Epoch
	}
	return s.Version > other.Version' \
  'return s.Version > other.Version' \
  'TestStampOrdering' ./internal/wire/

mutate "the transport codec drops the body" \
  internal/transport/codec.go \
  'b.payload = data.Materialize()' \
  'b.payload = nil' \
  'TestRoundTripOverLoopback' ./internal/transport/

mutate "a caller does not identify itself" \
  internal/transport/client.go \
  'ctx = metadata.AppendToOutgoingContext(ctx, metadataFromNode, c.transport.self.ID)' \
  '_ = metadataFromNode' \
  'TestRoundTripOverLoopback' ./internal/transport/

mutate "sentinel errors do not survive the hop" \
  internal/transport/client.go \
  'return fmt.Errorf("transport: %s: %s: %w", node.ID, st.Message(), sentinel)' \
  'return errors.New(st.Message())' \
  'TestSentinelErrorsSurviveTheHop' ./internal/transport/

mutate "Unavailable is read as a deferred activation" \
  internal/transport/client.go \
  'sentinel = core.ErrNodeUnreachable
		}' \
  'sentinel = core.ErrActivationPending
		}' \
  'TestUnavailableDefaultsToUnreachable' ./internal/transport/

mutate "connections are not pooled" \
  internal/transport/client.go \
  'if c, ok := t.clients[node.ID]; ok && c.addr == node.Addr {' \
  'if c, ok := t.clients[node.ID]; ok && false {' \
  'TestDialPoolsConnections' ./internal/transport/

mutate "the server does not verify client certificates" \
  internal/transport/tls.go \
  'ClientAuth:   tls.RequireAndVerifyClientCert,' \
  'ClientAuth:   tls.RequireAnyClientCert,' \
  'TestMutualTLSRejectsAnImpostorCA' ./internal/transport/

mutate "the client does not verify the server" \
  internal/transport/tls.go \
  'RootCAs:            pool,' \
  'InsecureSkipVerify: true, RootCAs: pool,' \
  'TestMutualTLSRejectsAnUntrustedServer' ./internal/transport/

mutate "the TLS floor drops to 1.0" \
  internal/transport/tls.go \
  'MinVersion:   tls.VersionTLS13,' \
  'MinVersion:   tls.VersionTLS10,' \
  'TestTLSFloorIsEnforced' ./internal/transport/

mutate "the router batches per group rather than per owner" \
  router.go \
  '		work, seen := byOwner[id]' \
  '		id = id + g
		work, seen := byOwner[id]' \
  'TestRouterIssuesOneRequestPerOwner' .

mutate "a group batch is applied one message at a time" \
  router.go \
  'work.batches = append(work.batches, wire.GroupBatch{Group: g, Messages: byGroup[g]})' \
  'for _, m := range byGroup[g] {
			work.batches = append(work.batches, wire.GroupBatch{Group: g, Messages: [][]byte{m}})
		}' \
  'TestRouterSubmit' .

mutate "every node runs its own loader" \
  namespace.go \
  '	if p.local {
		return n.localGetOrLoad(ctx, key)
	}
	return n.remoteGetOrLoad(ctx, p, key, true)' \
  '	_ = p
	return n.localGetOrLoad(ctx, key)' \
  'TestTheLoaderRunsOnlyOnTheOwner' .

mutate "gossip advertises the SWIM port as the RPC address" \
  internal/membership/gossip.go \
  'if meta.RPCAddr != "" {
				node.Addr = meta.RPCAddr' \
  'if false {
				node.Addr = meta.RPCAddr' \
  'TestGossipAdvertisesTheRPCAddress' ./internal/membership/
mutate "enqueueing an invalidation blocks when the queue is full" \
  invalidate.go \
  '	select {
	case iv.queue <- invalidation{namespace: namespace, keys: owned, stamp: stamp}:
	default:
		iv.dropped.Add(1)
	}' \
  '	iv.queue <- invalidation{namespace: namespace, keys: owned, stamp: stamp}' \
  'TestInvalidationBacklogIsDroppedNotQueued' .
echo
echo "caught: $caught   vacuous: $vacuous   skipped: $skipped"

if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: mutations were left behind:" >&2
  git status --short >&2
  exit 1
fi
echo "tree clean"

if [ "$vacuous" -ne 0 ] || [ "$skipped" -ne 0 ]; then
  exit 1
fi
