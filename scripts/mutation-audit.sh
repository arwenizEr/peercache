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


# The character class carries the regex metacharacters a -run argument may
# legitimately contain, not just the ones a bare test name does, and the prefix
# alternation carries both kinds of target.
#
# It has now been widened twice, and the second time is the instructive one.
#
# It matched only names beginning with Test until the first Fuzz entry was
# written, at which point an orphaned fuzz entry would have been exactly as
# invisible as the orphaned M6 entry this guard was built for. Adding the
# alternation looked like the fix and was not: a -run argument is a regexp, the
# new entries are written anchored as '^FuzzCodecRoundTrip$', and a string
# starting with a caret begins with neither Test nor Fuzz. The widened guard was
# tested by orphaning one of those entries and it did not fire.
#
# So the leading anchor is now optional too. The pattern this guard has to
# survive is that every widening is written against the entries that exist,
# while the thing it protects against is an entry nobody has written yet -- which
# is why it is verified by orphaning a real entry each time it changes rather
# than by reading it.
#
# It was [A-Za-z0-9_|] until M8, which is every character a name or an
# alternation needs and none of the anchors. An entry passing
# 'TestSomethingSpecific$' -- an anchor, to select one test where another name
# extends it -- therefore did not match, so this check could not see the entry
# at all, and dropping its `mutate` line was silently undetected. Confirmed by
# removing that line and watching the run proceed.
#
# That is precisely the failure this check exists to prevent, reintroduced
# through the argument it inspects rather than the line it inspects. Anchors,
# character classes and escapes are all ordinary in a -run pattern.
testarg = re.compile(r"'\^?(?:Test|Fuzz)[A-Za-z0-9_|$^.*+?()\[\]{}\-]*'")
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

# ONLY set but empty is almost always a filter that a caller computed and got
# nothing from, and treating it as "no filter" runs every entry — an hour on the
# development machine, started by accident. That happened twice in one session.
# Unset means a full run; set and empty means a mistake.
if [ "${ONLY+set}" = set ] && [ -z "$ONLY" ]; then
  echo "refusing to run: ONLY is set but empty. Unset it for a full run." >&2
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
filtered=0

# mutate NAME FILE FROM TO TEST PACKAGE
#
# Replaces the first occurrence of FROM with TO in FILE, runs TEST in PACKAGE,
# and reports whether the test noticed. The file is always restored.
mutate() {
  local name="$1" file="$2" from="$3" to="$4" test="$5" pkg="$6"

  # ONLY runs a subset, matched against the entry name as a POSIX ERE.
  #
  # A full run is a hundred-odd entries, each compiling and running a test, and
  # it refuses to start on a dirty tree -- so verifying one newly written entry
  # meant committing first and then paying for every other entry to learn
  # whether the new one works. The result was predictable: entries were added
  # and reasoned about rather than run, which is the exact failure this script
  # exists to catch, committed by the script itself.
  #
  #     ONLY='degraded|hand-off' bash scripts/mutation-audit.sh
  #
  # Skipped entries are not counted as caught, vacuous or skipped: a filtered
  # run makes no claim about what it did not execute, and the summary says how
  # many it passed over so that a filtered run cannot be mistaken for a full one.
  if [ -n "${ONLY:-}" ] && ! printf '%s' "$name" | grep -Eq "$ONLY"; then
    filtered=$((filtered + 1))
    return
  fi

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
  '	version := old.version + 1' \
  '	version := uint64(1)' \
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
	ctx = ctxOrBackground(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}' \
  'func (n *namespace) Get(ctx context.Context, key string) ([]byte, error) {
	ctx = ctxOrBackground(ctx)' \
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
  membership/gossip.go \
  'kind, transition = core.ChangeUpdate, false' \
  'kind, transition = core.ChangeUpdate, true' \
  'TestEpochDoesNotRunAway' ./membership/

mutate "the gossip event goroutine is never released" \
  membership/gossip.go \
  'close(g.stopConsume)
	<-g.consumeDone' \
  '_ = g.consumeDone' \
  'TestCloseLeavesNoGoroutines' ./membership/

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

# Retargeted in M8. This guarded bounded.go through
# TestALengthPrefixCannotBuyAnAllocation until the frame validator landed in
# front of the decoders, at which point that test's inputs were refused before
# any decoder saw them and the entry went VACUOUS -- the audit said so on its
# first full run after the change.
#
# The bounded decoders still have a job the validator cannot do. A declared count
# no larger than the bytes behind it is structurally honest, so validation passes,
# and the elements can still fail to decode: 100,000 fixints are not BatchEntry
# values. Appending as they arrive costs nothing when the first one fails;
# preallocating from the count costs megabytes to learn the same thing.
mutate "a length prefix buys an allocation" \
  internal/wire/bounded.go \
  'var out []BatchEntry
	for i := 0; i < n; i++ {' \
  'out := make([]BatchEntry, 0, n)
	for i := 0; i < n; i++ {' \
  'TestABoundedDecoderStillAllocatesOnlyForWhatArrives' ./internal/wire/

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
  transport/codec.go \
  'b.payload = data.Materialize()' \
  'b.payload = nil' \
  'TestRoundTripOverLoopback' ./transport/

mutate "a caller does not identify itself" \
  transport/client.go \
  'ctx = metadata.AppendToOutgoingContext(ctx, metadataFromNode, c.transport.self.ID)' \
  '_ = metadataFromNode' \
  'TestRoundTripOverLoopback' ./transport/

mutate "sentinel errors do not survive the hop" \
  transport/client.go \
  'return fmt.Errorf("transport: %s: %s: %w", node.ID, st.Message(), sentinel)' \
  'return errors.New(st.Message())' \
  'TestSentinelErrorsSurviveTheHop' ./transport/

mutate "Unavailable is read as a deferred activation" \
  transport/client.go \
  'sentinel = core.ErrNodeUnreachable
		}' \
  'sentinel = core.ErrActivationPending
		}' \
  'TestUnavailableDefaultsToUnreachable' ./transport/

mutate "connections are not pooled" \
  transport/client.go \
  'if c, ok := t.clients[node.ID]; ok && c.addr == node.Addr {' \
  'if c, ok := t.clients[node.ID]; ok && false {' \
  'TestDialPoolsConnections' ./transport/

mutate "the server does not verify client certificates" \
  transport/tls.go \
  'ClientAuth:   tls.RequireAndVerifyClientCert,' \
  'ClientAuth:   tls.RequireAnyClientCert,' \
  'TestMutualTLSRejectsAnImpostorCA' ./transport/

mutate "the client does not verify the server" \
  transport/tls.go \
  'RootCAs:            pool,' \
  'InsecureSkipVerify: true, RootCAs: pool,' \
  'TestMutualTLSRejectsAnUntrustedServer' ./transport/

mutate "the TLS floor drops to 1.0" \
  transport/tls.go \
  'MinVersion:   tls.VersionTLS13,' \
  'MinVersion:   tls.VersionTLS10,' \
  'TestTLSFloorIsEnforced' ./transport/

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
  membership/gossip.go \
  'if meta.RPCAddr != "" {
				node.Addr = meta.RPCAddr' \
  'if false {
				node.Addr = meta.RPCAddr' \
  'TestGossipAdvertisesTheRPCAddress' ./membership/
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
	delta := size(key, stored)
	if ok {' \
  '	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: 1}
	delta := size(key, stored)
	if ok {' \
  'TestRestoreContinuesTheVersionSequence' ./internal/store/

mutate "Restore overwrites a newer value" \
  internal/store/store.go \
  '	if ok && existing.version >= version {
		return false
	}
	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version}' \
  '	sh.entries[key] = entry{val: stored, expiresAt: expiresAt, version: version}' \
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

# NOT DETERMINISTIC. Four full audit runs disagreed about this entry: caught,
# VACUOUS, caught, caught. Run on its own it is caught 5 times out of 5, so the
# escape needs the load of a full audit -- 8 of 9 observed runs caught it.
#
# The likely mechanism, stated as likely because it has not been demonstrated
# directly: a drain announces its departure and then transfers, with nothing
# ordering the two. When the receiver has already processed the departure it owns
# the key and takes the adoption branch this mutation deletes, so the key is
# dropped and the test fails. When it has not, the transfer is filed as a replica
# instead and the next reconciliation promotes it — which the hard kill below
# guarantees will happen — so the data survives and the test passes.
#
# What that means for the entry: the *guarantee* (a graceful drain at RF=1 loses
# nothing) holds on both paths, so this mutation targets which store the key
# lands in rather than whether it is kept. Left in place because it is caught
# in 8 of 9 observed runs and a mostly-reliable detector is still a detector;
# recorded here so the next disagreement is recognised rather than investigated
# from scratch. See
# docs/verification.md, and TestKeysAdoptedIsCounted for the same race found and
# fixed on the counter side.
mutate "handed-off keys are not adopted by their new owner" \
  handler.go \
  'if ns.store.Restore(in.Key, in.Value, time.Duration(in.TTLNanos), in.Stamp.Version) {
			h.cache.keysAdopted.Add(1)
		}
		h.cache.pressure.noteWrite(len(in.Value))' \
  'h.cache.pressure.noteWrite(len(in.Value))' \
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

mutate "an out-of-order copy is not counted as a version conflict" \
  internal/replication/replica.go \
  '			if existing.Stamp.NewerThan(e.Stamp) {
				s.versionConflicts.Add(1)
			}' \
  '' \
  'TestVersionConflictsAreCountedButRetransmissionsAreNot' ./internal/replication/

mutate "a retransmission is counted as a version conflict" \
  internal/replication/replica.go \
  '			if existing.Stamp.NewerThan(e.Stamp) {
				s.versionConflicts.Add(1)
			}' \
  '			s.versionConflicts.Add(1)' \
  'TestVersionConflictsAreCountedButRetransmissionsAreNot' ./internal/replication/

mutate "mailbox rejections never reach Stats" \
  peercache.go \
  'MailboxRejections: groups.Rejections,' \
  'MailboxRejections: 0,' \
  'TestMailboxRejectionsAreCounted' .

mutate "a stats field is exported under the wrong name" \
  internal/metrics/metrics.go \
  '{"peercache_split_brain_detections_total", Counter,' \
  '{"peercache_split_brains_total", Counter,' \
  'TestEveryStatsFieldIsExported' ./internal/metrics/


# --- M8 -------------------------------------------------------------------

# The frame validator is the whole defence against a declared length buying an
# allocation. Removing the call restores the defect decision 0019 describes: a
# five-byte bin32 header becomes a 4 GiB make. The test asserts the allocation
# rather than the error, because a decoder that rejects the message *after*
# allocating is still a remote out-of-memory.
mutate "a frame is decoded without validating its declared lengths" \
  internal/wire/wire.go \
  '	if err := validate(data); err != nil {' \
  '	if err := error(nil); err != nil {' \
  'TestNoMessageAllocatesFromADeclaredLength' ./internal/wire/

# The depth bound is what keeps the recursive walk off the stack. Raising it far
# enough that a frame can nest as deep as its byte count allows is the shape of
# the hazard, and the test names the limit rather than merely observing an error.
mutate "the validator's nesting bound is effectively unbounded" \
  internal/wire/validate.go \
  'const MaxNestingDepth = 16' \
  'const MaxNestingDepth = 1 << 30' \
  'TestValidateRejectsUnboundedNesting' ./internal/wire/

# Hand-off before eviction is decision 0020. Dropping the orphans instead of
# queueing them is precisely the pre-M8 behaviour, and it loses keys whenever a
# partition heals -- which is what the test constructs.
mutate "a moved replica is dropped instead of handed off" \
  promote.go \
  '		for key, e := range orphans {
			p.evicted.Add(1)
			c.handoff.enqueue(handoffWork{namespace: name, key: key, entry: e})
		}' \
  '		for range orphans {
			p.evicted.Add(1)
		}' \
  'TestMajoritySideWritesSurviveAHealAndTwoDestructions' .

# Every hand-off failure path must keep the copy. Dropping on failure is the
# same data loss by a slower route: a peer that is briefly unreachable during a
# membership change is the ordinary case, not the exceptional one.
mutate "a hand-off that cannot be placed drops the copy" \
  handoff.go \
  '	if !placed {
		h.restore(w)
		return
	}' \
  '	if !placed {
		return
	}' \
  'TestAHandOffThatCannotBePlacedKeepsTheCopy' .

# A hand-off is a relocation, not a write. Stamping it with the forwarding node's
# ID makes the receiver see the original owner's replication and the forward as
# two writers under one epoch, and manufacture split-brain detections -- in the
# one counter the README says to investigate whenever it is non-zero.
mutate "a hand-off claims to be the writer of what it forwards" \
  handoff.go \
  'from := w.entry.From
	if from == "" {
		from = c.self().ID
	}' \
  'from := c.self().ID' \
  'TestMajoritySideWritesSurviveAHeal$' .

# The degraded-write counter is the only signal separating a synchronous write
# that placed RF copies from one that placed one. Question 37 is about it being
# invisible; a counter that never increments restores exactly that.
mutate "a synchronous write below the replication factor is not counted" \
  namespace.go \
  '	if n.cache.degradedWrites.Add(1) == 1 {' \
  '	if false {' \
  'TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy' .

# --- Question 37, options 2 and 3: WithRequireReplicationFactor ---
#
# Decision 0021. Each entry was applied and reverted by hand before being
# written here.

# The opt-in refusal itself. Without it the strict namespace acknowledges the
# isolated write exactly as the default one does, which is the behaviour the
# option exists to change.
mutate "a namespace requiring its factor still acknowledges below it"   namespace.go   'return n.opts.RequireReplicationFactor && n.opts.SyncReplication &&'   'return false && n.opts.RequireReplicationFactor && n.opts.SyncReplication &&'   'TestRequireReplicationFactorRefusesAnIsolatedWrite$' .

# The refusal after a partial placement. An isolated node has no targets and
# takes the other branch, so only a cluster with some replicas but not enough
# reaches this one.
mutate "a write placed on some replicas but not enough is acknowledged"   namespace.go   '	// and takes precedence: it is the more specific diagnosis.
	if n.underFactor(len(targets)) {'   '	// and takes precedence: it is the more specific diagnosis.
	if false {'   'TestRequireReplicationFactorRefusesThroughAForwardedWrite$' .

# The sentinel has to survive gRPC, or a write through any non-owner sees a
# generic unreachable error and the distinction exists only for local writes.
mutate "ErrUnderReplicated stops surviving the gRPC hop"   transport/client.go   '	case codes.Internal:
		// gRPC raises Internal itself'   '	case codes.DataLoss:
		// gRPC raises Internal itself'   'TestSentinelErrorsSurviveTheHop$' ./transport/

# Requiring the factor under asynchronous replication cannot be honoured. Letting
# it through would be an option that silently does nothing.
mutate "requiring the factor without sync replication is accepted"   internal/core/options.go   'if o.RequireReplicationFactor && !o.SyncReplication {'   'if false {'   'TestNamespaceOptionValidationSurfaces$' .

# The nightly soak's step timeout, put back to the figure that fitted one test
# when the pattern selects two.
mutate "the nightly soak timeout fits one soak test, not two" \
  .github/workflows/nightly.yml \
  'run: go test . -run TestSoak -count=1 -timeout 135m' \
  'run: go test . -run TestSoak -count=1 -timeout 85m' \
  'TestNightlySoakFitsItsTimeouts$' .

# --- Question 40: ReplicasCurrent, and the CatchUp error it exposed ---

# The bug from M5: the per-peer error is computed and discarded, so CatchUp
# returns nil after reaching nobody.
mutate "CatchUp drops the errors of the peers it could not reach" \
  catchup.go \
  'failures = errors.Join(failures, fmt.Errorf(' \
  '_ = errors.Join(failures, fmt.Errorf(' \
  'TestCatchUpReportsAPeerItCouldNotReach$' .

# Without the epoch comparison, one catch-up makes a node current forever, which
# is the one-shot CaughtUp flag question 40 was opened about.
mutate "ReplicasCurrent ignores membership changes" \
  ready.go \
  'return at != 0 && at-1 == uint64(c.guard.Stats().Epoch)' \
  'return at != 0' \
  'TestReplicasCurrentFollowsCatchUpAndMembership$' .

# A catch-up that failed against some peers must not count as current.
mutate "a failed catch-up still marks replicas current" \
  catchup.go \
  '	if err == nil {
		c.readiness.markCurrent(epoch)' \
  '	if true {
		c.readiness.markCurrent(epoch)' \
  'TestCatchUpReportsAPeerItCouldNotReach$' .

# --- M9, part one: the memory budget (decision 0022) ---

# The version floor is what keeps a shed-then-rewritten key above a replica's
# copy without depending on the removal message arriving.
mutate "shedding no longer raises the version floor" \
  internal/store/store.go \
  '			if e.version > sh.floor {
				sh.floor = e.version
			}' \
  '' \
  'TestARewriteAfterShedIsVersionedAboveWhatWasShed$' ./internal/store/

# The removal sent to a shed key's replicas, which frees their memory.
mutate "a shed owned key sends its replicas no removal" \
  pressure.go \
  '	if n.opts.ReplicationFactor < 2 {
		return
	}
	targets' \
  '	if true {
		return
	}
	targets' \
  'TestAShedKeyIsNeverServedAtAnOlderValue$' .

# Replicas before owned data.
mutate "owned data is shed while replicas could have been" \
  pressure.go \
  '	// 2. Replicas.
	if excess > 0 {' \
  '	// 2. Replicas.
	if false {' \
  'TestSheddingTakesL1AndReplicasBeforeOwnedData$' .

# L1 before everything.
mutate "the L1 tier is never shed" \
  pressure.go \
  '		entries := ns.l1.Len()
		excess -= ns.l1.Shed()' \
  '		entries := 0
		_ = ns.l1.Len()' \
  'TestSheddingTakesL1AndReplicasBeforeOwnedData$' .

mutate "catch-up ignores the memory budget" \
  catchup.go \
  '		if c.pressure.over() {' \
  '		if false {' \
  'TestCatchUpStopsAtTheMemoryBudget$' .

mutate "local writes never check the budget" \
  pressure.go \
  '	p.check()
}

// check sheds' \
  '}

// check sheds' \
  'TestASingleNodeStaysWithinItsMemoryBudget$' .

# The inflow a failover produces is copies from peers, not local writes.
mutate "replica arrivals never check the budget" \
  handler.go \
  '	h.cache.pressure.noteWrite(len(in.Value))
	if conflict {' \
  '	if conflict {' \
  'TestAReplicaOnlyNodeStaysWithinItsBudget$' .

# --- M9, part two: join probation (decision 0023) ---

mutate "probation hides nobody" \
  probation.go \
  '		if !ok || now-started >= int64(period) {' \
  '		if true {' \
  'TestACrashloopingNodeIsNeverRanked$' .

mutate "a probation ending never reaches the ranking" \
  probation.go \
  '	before := p.lastIDs
	p.refreshLocked()' \
  '	return
	before := p.lastIDs
	p.refreshLocked()' \
  'TestAProbationedNodeIsRankedOnceItHasLasted$' .

mutate "a cold start ranks nobody" \
  probation.go \
  '	if len(established) == 0 {' \
  '	if false {' \
  'TestColdStartRanksEveryNode$' .

# --- M9, part three: a slow node ---

# Asynchronous writes that waited on their replicas would make every write
# whose replica is a throttled node as slow as that node.
mutate "asynchronous writes wait for their replicas"   namespace.go   '	if err := n.cache.replicator.replicate(ctx, n.opts.SyncReplication, replicaWork{'   '	if err := n.cache.replicator.replicate(ctx, true, replicaWork{'   'TestAStarvedNodeSlowsOnlyItsOwnKeys$' .

# --- TTL across failover: replicas hold the owner's deadline ---
#
# Found by writing tests for code coverage reported as never run. The
# promotion-time and hand-off-time expiry checks are not entered here: the
# replica sweep always removes an expired copy first in this harness, so
# removing either check changes nothing a test can see. See
# docs/verification.md.

mutate "promotion restarts a key's lifetime"   promote.go   'if ns.store.Restore(key, e.Value, time.Duration(left), e.Stamp.Version) {'   'if ns.store.Restore(key, e.Value, time.Duration(e.ExpiresAt-now+int64(10*time.Second)), e.Stamp.Version) {'   'TestAPromotedKeyKeepsItsRemainingTTL$' .

mutate "a caught-up replica's deadline is counted late"   catchup.go   'ExpiresAt: replication.DeadlineFrom(c.opts.Clock.Now().UnixNano(), e.TTLNanos),'   'ExpiresAt: replication.DeadlineFrom(c.opts.Clock.Now().UnixNano()+int64(6*time.Second), e.TTLNanos),'   'TestACaughtUpKeyKeepsItsRemainingTTL$' .

mutate "expired replicas are never swept"   peercache.go   '	c.replicas.SweepExpired(c.opts.Clock.Now().UnixNano())'   ''   'TestExpiredReplicasAreReclaimed$' .

mutate "a replica at its deadline still counts as live"   internal/replication/replica.go   '	return left, left > 0'   '	return left, left >= 0'   'TestRemainingAndDeadline$' ./internal/replication/

# --- The coverage sweep: paths that had never run ---

mutate "a Set gives up when ownership moved under it" \
  namespace.go \
  '			return n.cache.route.remoteSet(ctx, next, n.name, key, val, ttl)
		}' \
  '			return err
		}' \
  'TestASetFollowsOwnershipThatMovedUnderIt$' .

mutate "a Call gives up when ownership moved under it" \
  group.go \
  '	return g.cache.route.remoteCall(ctx, next, g.id, fn, arg)
}' \
  '	return nil, err
}' \
  'TestAGroupCallFollowsOwnershipThatMovedUnderIt$' .

mutate "GetOrLoad without a loader asks the owner anyway" \
  namespace.go \
  '	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	p, err := n.cache.route.locate' \
  '	p, err := n.cache.route.locate' \
  'TestRemoteGetOrLoadWithoutALoaderOnTheOwner$' .

mutate "a peer's own failure is reported as unreachable" \
  transport/client.go \
  '	code := codes.Unknown' \
  '	code := codes.Internal' \
  'TestAnApplicationErrorIsNotReportedAsUnreachable$' ./transport/

mutate "probation swallows the departure announcement" \
  probation.go \
  'func (p *probationAuthority) Leave(ctx context.Context) error { return p.inner.Leave(ctx) }' \
  'func (p *probationAuthority) Leave(ctx context.Context) error { return nil }' \
  'TestDrainUnderJoinProbationAnnouncesTheDeparture$' .

mutate "a second Drain hands everything off again" \
  drain.go \
  '	if !c.readiness.draining.CompareAndSwap(false, true) {' \
  '	if c.readiness.draining.Swap(true) && false {' \
  'TestASecondDrainDoesNothing$' .

mutate "Drain hides a failed departure announcement" \
  drain.go \
  '		problems = errors.Join(problems, fmt.Errorf("peercache: announcing departure: %w", err))' \
  '		_ = fmt.Errorf("peercache: announcing departure: %w", err)' \
  'TestDrainReportsAFailedDepartureAndStillHandsOff$' .

mutate "a remote BatchGet returns nothing" \
  handler.go \
  '		out.Entries = append(out.Entries, wire.BatchEntry{' \
  '		_ = append(out.Entries, wire.BatchEntry{' \
  'TestBatchGetAcrossNodes$' .

mutate "a remote Exists is always false" \
  namespace.go \
  '	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):' \
  '	case err == nil:
		return false, nil
	case errors.Is(err, ErrNotFound):' \
  'TestExistsAcrossNodes$' .

mutate "remote latency is never observed" \
  remote.go \
  '			r.cache.remoteLatency.Observe(uint64(elapsed))' \
  '			_ = elapsed' \
  'TestLatencyHistogramsRecordRemoteOperations$' .

mutate "lifecycle events are never delivered" \
  internal/actor/record.go \
  '		r.rt.opts.OnEvent(e)' \
  '		_ = e' \
  'TestEventHandlerSeesEveryLifecycleEvent$' .

# --- Decision 0024: WithSeeds joins ---

mutate "seeds are never joined"   peercache.go   '	if len(resolved.Seeds) > 0 {
		if err := resolved.Membership.Join(resolved.Seeds); err != nil {'   '	if false {
		if err := resolved.Membership.Join(resolved.Seeds); err != nil {'   'TestWithSeedsJoinsThroughTheMembership$' .

mutate "seeds without a membership are accepted"   internal/core/options.go   '	if len(o.Seeds) > 0 && o.Membership == nil {'   '	if false {'   'TestSeedsWithoutMembershipAreRejected$' .

# --- M8, part two: the fuzz targets and the checks on the build files ---
#
# Every entry below was applied and reverted by hand before being written here,
# and the result recorded in docs/verification.md. They are entered so that the
# hand-check becomes a standing one; the pairs are not new claims.
#
# The three that mutate a .md or a .yml are the first entries here that mutate
# something other than Go source. That is the point of them: a check that reads
# the documentation or a workflow is exactly as capable of being vacuous as one
# that reads code, and rather more likely to be, because nothing compiles it.

# The two codecs are configured to key on the json struct tag so that neither is
# positional. Making the encoder key on something else leaves the decoder
# looking for fields the encoder never wrote.
mutate "the codecs stop agreeing on field names" \
  internal/codec/msgpack.go \
  'enc.SetCustomStructTag("json")' \
  'enc.SetCustomStructTag("msgpack")' \
  '^FuzzCodecRoundTrip$' ./internal/codec/

# The group key decides ownership. Splitting on the last separator rather than
# the first still returns a prefix of the key, so only the idempotence property
# notices -- which is the property standing between this and a key with two
# owners.
mutate "grouping splits on the last separator, not the first" \
  internal/core/group.go \
  'if i := strings.Index(key, sep); i >= 0 {' \
  'if i := strings.LastIndex(key, sep); i >= 0 {' \
  '^FuzzPartitionByPrefix$' .

# An unanchored -fuzz pattern is a prefix waiting for a sibling. This is the
# exact line FuzzDecodeEveryMessage broke when it was added.
mutate "a fuzz pattern in CI goes back to being ambiguous" \
  .github/workflows/ci.yml \
  "-fuzz '^FuzzDecode\$' -fuzztime 60s" \
  "-fuzz FuzzDecode -fuzztime 60s" \
  'TestFuzzTargetsNamedInCIExist' .

# A derived figure in the operator-facing documentation, moved by one.
mutate "the documented metric count drifts from the handler" \
  docs/api.md \
  '46 series come out of it' \
  '45 series come out of it' \
  'TestApiDocMetricCountsAreCorrect' .

# A public-API file dropping out of the repository map, which is how twelve of
# the nineteen came to be missing in the first place.
mutate "a root file disappears from the repository map" \
  README.md \
  'handoff.go        Moves replicas on a membership change rather' \
  'handoff_go        Moves replicas on a membership change rather' \
  'TestReadmeListsEveryRootFile' .

# --- M10: typed namespaces (decision 0025) ---

mutate "one name may hold two types" \
  namespace.go \
  '	if n.valueType != t {' \
  '	if false {' \
  'TestOneNameTwoTypesIsInvalid$' .

mutate "a typed loader of the wrong type is accepted" \
  typed.go \
  'if spec := ns.opts.TypedLoader; spec != nil && spec.ValueType() != want {' \
  'if spec := ns.opts.TypedLoader; false && spec != nil {' \
  'TestATypedLoaderOfTheWrongTypeIsInvalid$' .

mutate "WithLoader and WithTypedLoader together are accepted" \
  internal/core/options.go \
  '	if o.TypedLoader != nil && o.Loader != nil {' \
  '	if false {' \
  'TestTypedLoaderAndLoaderConflict$' .

mutate "the default typed codec is JSON" \
  typed.go \
  'const DefaultTypedCodec = codec.NameMsgPack' \
  'const DefaultTypedCodec = codec.NameJSON' \
  'TestInvalidUTF8SurvivesTheDefaultCodec$' .

mutate "a decode failure is not ErrCodec" \
  typed.go \
  'return zero, fmt.Errorf("peercache: decoding %q with %s: %v: %w", key, t.codec.Name(), err, ErrCodec)' \
  'return zero, fmt.Errorf("peercache: decoding %q with %s: %w", key, t.codec.Name(), err)' \
  'TestAnUndecodableValueIsErrCodec$' .

mutate "the typed loader is never installed" \
  peercache.go \
  '		ns.opts.Loader = opts.TypedLoader.Loader(valueCodec.Marshal)' \
  '		_ = opts.TypedLoader' \
  'TestTypedGetOrLoadAcrossNodes$' .

mutate "a typed BatchGet drops what it decoded" \
  typed.go \
  '		out[key] = v' \
  '		_ = v' \
  'TestTypedValuesRoundTripAcrossNodes$' .

# A two-digit milestone's row. The map's row pattern was M[0-9], so M10 rows
# were not counted at all until it became M[0-9]+; renaming one must be noticed.
mutate "a two-digit milestone row stops being counted" \
  docs/verification.md \
  '| M10 | a typed value set on one node reads back equal on every node through Get and BatchGet |' \
  '| X10 | a typed value set on one node reads back equal on every node through Get and BatchGet |' \
  'TestAcceptanceMapCountsAreCorrect$' .

# --- Question 42: a lost tombstone ---

mutate "a deletion no longer raises the version floor" \
  internal/store/store.go \
  '		// must start above it, as after a shed. Open question 42.
		if e.version > sh.floor {
			sh.floor = e.version
		}' \
  '		// must start above it, as after a shed. Open question 42.' \
  'TestALostDeleteDoesNotResurrectAnOlderValue$' .

# --- Typed group functions ---

mutate "CallTyped does not check the registered types" \
  typed_group.go \
  '		if sig.arg != reflect.TypeFor[A]() || sig.result != reflect.TypeFor[R]() {' \
  '		if false && sig.arg != nil {' \
  'TestCallTypedWithTheWrongTypesIsInvalid$' .

mutate "an undecodable typed argument is not ErrCodec" \
  typed_group.go \
  '			return nil, fmt.Errorf("peercache: group function %q: decoding its argument as %v: %v: %w",
				name, reflect.TypeFor[A](), err, ErrCodec)' \
  '			return nil, fmt.Errorf("peercache: group function %q: decoding its argument as %v: %w",
				name, reflect.TypeFor[A](), err)' \
  'TestATypedGroupFuncRejectsAnUndecodableArgument$' .

mutate "ErrCodec stops surviving the gRPC hop" \
  transport/client.go \
  '		} else if containsSentinel(st.Message(), core.ErrCodec.Error()) {' \
  '		} else if false {' \
  'TestSentinelErrorsSurviveTheHop$' ./transport/

mutate "typed signatures are never recorded" \
  typed_group.go \
  '		cc.typedFuncs.Store(name, typedSignature{arg: reflect.TypeFor[A](), result: reflect.TypeFor[R]()})' \
  '		_ = cc' \
  'TestATypedGroupFuncRunsOnTheOwnerAcrossNodes$' .

# --- Code review findings ---

# The handler checked ownership by the raw key; callers route by group key.
mutate "the peer handler checks ownership by the raw key" \
  handler.go \
  '		groupKey = ns.groupKeyFor(key)' \
  '		_ = ns' \
  'TestAPartitionedNamespaceServesRemoteCallers$' .

# BatchSet wrote into the calling node's own store, bypassing routing.
mutate "BatchSet writes locally instead of routing"   namespace.go   '		if err := n.Set(ctx, key, val, ttl); err != nil {'   '		if err := (error)(nil); n.store.Set(key, n.encode(val), ttl) == 0 && err != nil {'   'TestBatchSetRoutesEachKeyToItsOwner$' .

# A peer write to a namespace misconfigured on its owner reached a nil store
# and panicked on the handler goroutine.
mutate "a misconfigured namespace's nil store is reachable from a peer"   handler.go   '	ns := h.cache.Namespace(in.Namespace).(*namespace)
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}
	if !existed {'   '	ns := h.cache.Namespace(in.Namespace).(*namespace)
	if !existed {'   'TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash$' .

# Compression across nodes: the wire and L1 carry plain values, the stores
# framed ones. Each side once got the other's form.
mutate "a remote Get unframes a value the owner already unframed"   namespace.go   '		n.l1.Put(key, value, stamp, n.opts.L1TTL)
	}
	return value, nil
}

'   '		n.l1.Put(key, value, stamp, n.opts.L1TTL)
	}
	return n.decode(value)
}

'   'TestCompressedValuesSurviveEveryRemotePath$' .

mutate "an adopted hand-off is framed a second time"   handler.go   '		if ns.store.Restore(in.Key, in.Value, time.Duration(in.TTLNanos), in.Stamp.Version) {'   '		if ns.store.Restore(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos), in.Stamp.Version) {'   'TestACompressedHandOffIsNotFramedTwice$' .

# A load that raced a Set stored its older value over the newer one.
mutate "a load overwrites a write that raced it"   internal/store/store.go   '	if had && (old.expiresAt == 0 || now.UnixNano() < old.expiresAt) {
		return old.val, false
	}'   ''   'TestALoadDoesNotOverwriteAWriteThatRacedIt$' .

# A replicated tombstone removed the replica's copy whatever its stamp.
mutate "a stale tombstone deletes a newer replica"   internal/replication/replica.go   '	if !held || e.Stamp.NewerThan(stamp) {'   '	if !held {'   'TestAStaleTombstoneDoesNotDeleteANewerReplica$' .

# A timer whose turn could not be queued was dropped, its entry left behind.
mutate "a timer due on a full mailbox is dropped"   internal/actor/record.go   '	if r.timers[gt.name] == gt && r.act != nil {
		gt.timer = r.rt.opts.Clock.AfterFunc(timerRetryDelay, func() { r.fireTimer(gt) })
	}'   ''   'TestATimerDueWhileTheMailboxIsFullStillFires$' ./internal/actor/

mutate "a timer naming no function keeps its entry"   internal/actor/record.go   '		if current {
			delete(r.timers, gt.name)
		}'   '		if current && ok {
			delete(r.timers, gt.name)
		}'   'TestATimerNamingAnUnregisteredFunctionDoesNotPinTheGroup$' ./internal/actor/

# Leaks found by the code review.
mutate "Close leaves each L1 tier's goroutine running"   peercache.go   '			if ns.l1 != nil {
				if err := ns.l1.Close(); err != nil {'   '			if false {
				if err := ns.l1.Close(); err != nil {'   'TestCloseReleasesTheL1Tier$' .

mutate "a failed New leaves the sweep ticker armed"   peercache.go   '				c.ticker.Stop()
				_ = groups.Close(context.Background())'   '				_ = groups.Close(context.Background())'   'TestAFailedNewLeavesNothingRunning$' .

echo
echo "caught: $caught   vacuous: $vacuous   skipped: $skipped"
if [ "$filtered" -ne 0 ]; then
  echo "FILTERED RUN: $filtered entries were not executed (ONLY=$ONLY)."
  echo "this run makes no claim about them."
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: mutations were left behind:" >&2
  git status --short >&2
  exit 1
fi
echo "tree clean"

if [ "$vacuous" -ne 0 ] || [ "$skipped" -ne 0 ]; then
  exit 1
fi
