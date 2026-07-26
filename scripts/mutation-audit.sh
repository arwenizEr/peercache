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

# Refuse to run if an entry has lost its `mutate` line.
#
# This file is a long list of `mutate` calls with backslash-continued argument
# lists. A bad edit that drops the `mutate` leaves the arguments behind as a
# command of their own: bash runs it, fails with "not found" on a stderr nobody
# reads, and carries on. The entry then sits in this file looking audited while
# never running. That happened to M6's acceptance criterion and was not noticed
# until M7.
#
# Neither `set -e` nor a command_not_found_handle catches it. The failure is one
# of a list of top-level commands whose exit statuses this script deliberately
# ignores, and bash runs the not-found handler in a context where `exit` does
# not stop the script — both were tried.
#
# So the check is static, structural, and runs before anything is mutated:
# backslash continuations are joined into logical lines, and every logical line
# carrying a 'TestSomething' argument must begin with `mutate`. Checking the
# shape of the last line instead is not enough — deleting only the `mutate` line
# leaves a continuation chain starting at the filename, which bash runs just as
# silently and which a last-line check does not see.
if ! python - "$0" <<'PY'
import re, sys

BACKSLASH = chr(92)


def logical_lines(text):
    """Split shell source into logical lines, respecting quotes and continuations.

    Splitting on backslash-terminated lines alone is not enough: most arguments
    here are multi-line Go snippets inside single quotes, and a newline in one
    of those does not end the command.
    """
    out, buf, start, line = [], [], 1, 1
    in_s = in_d = False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "\n":
            line += 1
            if in_s or in_d:            # newline inside a quoted argument
                buf.append(" ")
                i += 1
                continue
            if buf and buf[-1] == BACKSLASH:   # line continuation
                buf.pop()
                buf.append(" ")
                i += 1
                continue
            out.append((start, "".join(buf)))
            buf, start = [], line
            i += 1
            continue
        if ch == "#" and not in_s and not in_d and (not buf or buf[-1].isspace()):
            while i < n and text[i] != "\n":
                i += 1
            continue
        if ch == "'" and not in_d:
            in_s = not in_s
        elif ch == '"' and not in_s:
            in_d = not in_d
        elif ch == BACKSLASH and in_d and i + 1 < n:
            buf.append(ch)
            buf.append(text[i + 1])
            i += 2
            continue
        buf.append(ch)
        i += 1
    if buf:
        out.append((start, "".join(buf)))
    return out


testarg = re.compile(r"'Test[A-Za-z0-9_|]*'")
bad = [(n, t.strip()) for n, t in logical_lines(open(sys.argv[1], encoding="utf-8").read())
       if testarg.search(t) and not t.strip().startswith("mutate ")]
if bad:
    for n, text in bad:
        print("  line %d: %s" % (n, text[:100]), file=sys.stderr)
    sys.exit(1)
PY
then
  echo "refusing to run: an entry above is not attached to a 'mutate' command," >&2
  echo "so it would be silently skipped. Restore its 'mutate' line." >&2
  exit 1
fi

# Refuse to run on a dirty tree. Reverting is done with `git checkout --`, which
# would silently discard uncommitted work in the files being mutated.
if [ -n "$(git status --porcelain)" ]; then
  echo "refusing to run: the working tree has uncommitted changes." >&2
  echo "this script reverts mutations with 'git checkout --', which would discard them." >&2
  git status --short >&2
  exit 1
fi

# Restore every mutated file on *any* exit, including Ctrl-C and a crash.
#
# This is not belt-and-braces. A run that died between applying a mutation and
# reverting it left the mutant in the tree, a later `git add -A` swept it into a
# commit, and the backpressure guarantee — a full mailbox rejects rather than
# blocks — was silently deleted from the shipping code for three commits. It was
# caught by `go test -race` hanging for twenty minutes, not by review.
#
# The trap makes the revert unconditional. `git checkout --` on a clean tree is
# a no-op, so running it on every exit costs nothing and closes the window.
mutated=""
cleanup() {
  if [ -n "$mutated" ]; then
    echo "restoring mutated files: $mutated" >&2
    # shellcheck disable=SC2086
    git checkout -- $mutated 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

caught=0
vacuous=0
skipped=0

# mutate NAME FILE FROM TO TEST PACKAGE
#
# Replaces the first occurrence of FROM with TO in FILE, runs TEST in PACKAGE,
# and reports whether the test noticed. The file is always restored.
mutate() {
  local name="$1" file="$2" from="$3" to="$4" test="$5" pkg="$6"

  mutated="$mutated $file"
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

# These two moved in M4, when the L1 tier began comparing (epoch, version)
# stamps rather than bare versions. The audit reported them as SKIP rather
# than passing them silently, which is the whole reason SKIP is a failure: a
# mutation whose pattern no longer matches verifies nothing, and a refactor
# would otherwise retire coverage while the total kept looking healthy.
mutate "L1 accepts an older version" \
  internal/l1/l1.go \
  'if live && !stamp.NewerThan(existing.stamp) {' \
  'if false {' \
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
  'if !ok || !stamp.NewerThan(e.stamp) {' \
  'if !ok {' \
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
# ---------------------------------------------------------------- M5

mutate "an older replica copy overwrites a newer one" \
  internal/replication/replica.go \
  'if !e.Stamp.NewerThan(existing.Stamp) {' \
  'if false {' \
  'TestAnOlderCopyIsRejected' ./internal/replication/

mutate "the replica store compares versions, ignoring the epoch" \
  internal/replication/replica.go \
  'if !e.Stamp.NewerThan(existing.Stamp) {' \
  'if e.Stamp.Version <= existing.Stamp.Version {' \
  'TestALaterEpochWinsWhateverTheVersion' ./internal/replication/

mutate "split brain is not detected" \
  internal/replication/replica.go \
  'existing.From != e.From {
			conflict = true' \
  'false {
			conflict = true' \
  'TestTwoOwnersInOneEpochIsAConflict' ./internal/replication/

mutate "any writer change counts as a split brain" \
  internal/replication/replica.go \
  'existing.Stamp.Epoch == e.Stamp.Epoch && existing.From != "" && e.From != "" &&
			existing.From != e.From' \
  'existing.From != e.From' \
  'TestDifferentEpochsAreNotAConflict' ./internal/replication/

mutate "Drain leaves a stranded copy behind" \
  internal/replication/replica.go \
  '		taken[key] = e
		delete(nr.entries, key)' \
  '		taken[key] = e' \
  'TestDrainTakesOnlyWhatItClaims' ./internal/replication/

mutate "promotion restarts the version sequence" \
  internal/store/store.go \
  '	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version}
	sh.sets.Add(1)
	return true' \
  '	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: 1}
	sh.sets.Add(1)
	return true' \
  'TestRestoreContinuesTheVersionSequence' ./internal/store/

mutate "Restore overwrites a newer value" \
  internal/store/store.go \
  'if existing, ok := sh.entries[key]; ok && existing.version >= version {
		return false
	}' \
  '' \
  'TestRestoreRefusesAnOlderVersion' ./internal/store/

mutate "snapshot ignores its page limit" \
  internal/store/store.go \
  'if len(candidates) > limit {
		return candidates[:limit], true
	}' \
  '' \
  'TestSnapshotPagesInKeyOrder' ./internal/store/

mutate "snapshot ignores the paging cursor" \
  internal/store/store.go \
  'if key <= after {
				continue
			}' \
  '' \
  'TestSnapshotPagesInKeyOrder' ./internal/store/

mutate "snapshot hands over expired keys" \
  internal/store/store.go \
  'if e.expiresAt != 0 && now >= e.expiresAt {
				continue
			}' \
  '' \
  'TestSnapshotSkipsExpiredEntries' ./internal/store/

mutate "sync replication does not wait" \
  replicate.go \
  'if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)' \
  'r.enqueue(w)
	return nil' \
  'TestSyncReplicationPlacesCopiesBeforeReturning' .

mutate "async replication waits for its replicas" \
  replicate.go \
  'if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)' \
  'return r.fanOut(ctx, w)' \
  'TestAsyncReplicationDoesNotBlockTheWrite' .

mutate "replication does nothing at all" \
  replicate.go \
  'if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)' \
  'return nil' \
  'TestChaosLosesNoSynchronouslyWrittenKey' .

mutate "deletions are not replicated" \
  namespace.go \
  'if err := n.fanOut(ctx, key, nil, 0, tombstone, true); err != nil {
		return err
	}' \
  '' \
  'TestADeletionReplicates' .

mutate "a remote write skips replication" \
  handler.go \
  'if err := ns.localSet(ctx, in.Key, in.Value, time.Duration(in.TTLNanos)); err != nil {
		return nil, err
	}' \
  'ns.store.Set(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos))' \
  'TestKillingAnOwnerPromotesAReplica' .

mutate "promotion drains nothing" \
  promote.go \
  'taken := c.replicas.Drain(name, func(key string) bool {' \
  'taken := c.replicas.Drain(name, func(key string) bool { return false
			_ = func() bool {' \
  'TestKillingAnOwnerPromotesAReplica' .

mutate "catch-up sends whatever is asked for" \
  handler.go \
  'for _, n := range ranked {
			if n.ID == in.Requester {
				return true
			}
		}
		return false' \
  'return true' \
  'TestCatchUpOnlySendsKeysTheAskerRanksFor' .

mutate "catch-up does not reconcile what it pulled" \
  catchup.go \
  'defer c.promoter.reconcile()' \
  '' \
  'TestARestartedNodeRecoversItsColdReplicas' .
# ---------------------------------------------------------------- M6

mutate "readiness ignores catch-up" \
  ready.go \
  'if !st.CaughtUp {
		st.Reason = "replicas not yet pulled; call CatchUp"
		return st
	}' \
  '' \
  'TestANodeIsNotReadyBeforeCatchUp' .

mutate "readiness ignores whether the view has settled" \
  ready.go \
  'st.Settled = c.guard.Settled()
	if !st.Settled {
		st.Reason = "membership has not settled"
		return st
	}' \
  'st.Settled = true' \
  'TestANodeIsNotReadyWhileMembershipMoves' .

mutate "a draining node still reports ready" \
  ready.go \
  'case st.Draining:
		st.Reason = "draining"
		return st' \
  'case false:
		return st' \
  'TestDrainingIsPermanentlyNotReady' .

mutate "Settled always says yes" \
  internal/ownership/guard.go \
  'elapsed := g.clk.Now().UnixNano() - g.changedAt.Load()
	return elapsed >= int64(g.activationDelay)' \
  'return true' \
  'TestANodeIsNotReadyWhileMembershipMoves' .

mutate "drain does not hand off its keys" \
  drain.go \
  'if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}' \
  '' \
  'TestDrainSavesDataWithoutReplication' .

mutate "drain does not announce its departure" \
  drain.go \
  'if err := c.opts.Membership.Leave(ctx); err != nil {' \
  'if err := error(nil); err != nil {' \
  'TestDrainStopsPeersRoutingImmediately' .

mutate "handed-off keys are not adopted by their new owner" \
  handler.go \
  'if ns.store.Restore(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos), in.Stamp.Version) {
			h.cache.keysAdopted.Add(1)
		}
		return wire.Encode(&wire.ReplicateResponse{})' \
  'return wire.Encode(&wire.ReplicateResponse{})' \
  'TestDrainSavesDataWithoutReplication' .

mutate "an implicitly created namespace is not counted" \
  handler.go \
  'if !existed {
		h.cache.implicitNamespaces.Add(1)' \
  'if false {
		h.cache.implicitNamespaces.Add(1)' \
  'TestAnImplicitNamespaceIsCountedAndReplicatesNothing' .

mutate "the plan counts promotions as moves" \
  internal/rebalance/plan.go \
  'if _, held := was[n.ID]; held {' \
  'if false {' \
  'TestRemovingANodeMovesOnlyItsShare' ./internal/rebalance/

mutate "an empty view plans an evacuation" \
  internal/rebalance/plan.go \
  'if newErr != nil {
			continue
		}' \
  '' \
  'TestAnEmptyNewViewPlansNothing' ./internal/rebalance/

mutate "unchanged keys are never counted" \
  internal/rebalance/plan.go \
  'if !moved {
			plan.Unchanged++
		}' \
  '' \
  'TestNoChangeMeansNoMoves' ./internal/rebalance/

# --- M6 lifecycle: the entry that was truncated -------------------------------
#
# What stood here was an orphaned argument list with no `mutate` in front of it,
# left by a bad edit. Under `set -u` without `set -e` that line is a command
# named TestRollingUpgradeLosesNoRequests, which fails with "not found" on
# stderr the run discards, and the script carries on. So M6's acceptance
# criterion sat in this file looking audited and was not.
#
# Reconstructing it found that the criterion could not have caught either
# mutation anyway: it runs at replication factor 2 with synchronous replication,
# so the replicas hold every key and deleting the handoff costs it nothing. The
# two tests below were written for M7 to close that, at replication factor 1
# where the handoff is the only copy.
mutate "drain hands off nothing" \
  drain.go \
  '	// Step 3: push what this node owns to whoever owns it now.
	if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}' \
  '' \
  'TestDrainHandsOffKeysNothingElseWouldSave' .

mutate "drain transfers before announcing departure" \
  drain.go \
  '	var problems error
	if err := c.opts.Membership.Leave(ctx); err != nil {
		problems = errors.Join(problems, fmt.Errorf("peercache: announcing departure: %w", err))
		c.opts.Logger.Warn("could not announce departure; peers will detect the failure instead",
			"error", err)
	}

	// Step 3: push what this node owns to whoever owns it now.
	if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}' \
  '	var problems error
	if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}

	if err := c.opts.Membership.Leave(ctx); err != nil {
		problems = errors.Join(problems, fmt.Errorf("peercache: announcing departure: %w", err))
		c.opts.Logger.Warn("could not announce departure; peers will detect the failure instead",
			"error", err)
	}' \
  'TestDrainAnnouncesDepartureBeforeTransferring' .

# --- M7 observability ---------------------------------------------------------
#
# A metric that reports the wrong number is worse than no metric: it is an
# alerting rule that does not fire and a dashboard that says the system is fine.

mutate "quantile rounds down instead of up" \
  internal/metrics/histogram.go \
  'want := uint64(math.Ceil(q * float64(total)))' \
  'want := uint64(math.Floor(q * float64(total)))' \
  'TestQuantileRoundsUp' ./internal/metrics/

mutate "snapshot buckets are exclusive, not cumulative" \
  internal/metrics/histogram.go \
  '		cumulative += h.buckets[i].Load()
		buckets = append(buckets, Bucket{UpperBound: bound, Count: cumulative})' \
  '		buckets = append(buckets, Bucket{UpperBound: bound, Count: h.buckets[i].Load()})' \
  'TestBucketsAreCumulative' ./internal/metrics/

mutate "the overflow bucket is dropped" \
  internal/metrics/histogram.go \
  '	h.buckets[len(bucketBounds)].Add(1)' \
  '' \
  'TestTheInfBucketMatchesTheCount' ./internal/metrics/

mutate "histogram bounds emitted in nanoseconds, not seconds" \
  internal/metrics/histogram.go \
  "sb.WriteString(strconv.FormatFloat(float64(b.UpperBound)/1e9, 'g', -1, 64))" \
  'sb.WriteString(strconv.FormatUint(b.UpperBound, 10))' \
  'TestHistogramTextFormat' ./internal/metrics/

mutate "a correctness counter is exported as a gauge" \
  internal/metrics/metrics.go \
  '{"peercache_split_brain_detections_total", Counter,' \
  '{"peercache_split_brain_detections_total", Gauge,' \
  'TestCorrectnessMetricsAreCounters' ./internal/metrics/

mutate "a stats field is exported under the wrong name" \
  internal/metrics/metrics.go \
  '{"peercache_split_brain_detections_total", Counter,' \
  '{"peercache_split_brains_total", Counter,' \
  'TestEveryStatsFieldIsExported' ./internal/metrics/

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
