#!/usr/bin/env bash
#
# Mutation audit: break each core invariant on purpose and check that the test
# claiming to guard it actually fails.
#
# docs/design.md requires every acceptance criterion to be "covered by a named test
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
# never running. That happened to the drain acceptance criterion and was not
# noticed for weeks.
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
# invisible as the orphaned drain entry this guard was built for. Adding the
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
# It was once [A-Za-z0-9_|], which is every character a name or an
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
# would silently discard uncommitted work in the files being mutated. Untracked
# files are not at risk -- checkout never touches them -- so they do not count.
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
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
broken=0
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

  # A package written dir::pkg is in a nested module: go test -C dir pkg runs
  # it from inside that module, so its own go.mod applies. The mutated file
  # is still named from the repository root, like every other entry's.
  local run=(go test "$pkg")
  if [[ "$pkg" == *::* ]]; then
    run=(go test -C "${pkg%%::*}" "${pkg#*::}")
  fi
  # A mutation that does not compile fails its test for a reason that has
  # nothing to do with the test, and used to be counted as caught. Build the
  # package's tests first, running none, so that such an entry is reported
  # for what it is: an entry that checks nothing.
  local built
  if ! built=$("${run[@]}" -run '^$' -count=1 2>&1); then
    echo "BROKEN   $name (the mutation does not compile; fix this entry)"
    printf '%s\n' "$built" | grep -E '\.go:[0-9]+' | head -3 | sed 's/^/           /'
    broken=$((broken + 1))
    git checkout -- "$file"
    return
  fi
  if "${run[@]}" -run "$test" -count=1 -timeout 120s >/dev/null 2>&1; then
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
  '	return deadlineNanos(now, ttl)' \
  '	return now.Add(ttl).UnixNano()' \
  'TestUnrepresentableTTLDoesNotExpireImmediately' ./internal/store/
mutate "a restored key's deadline overflows" \
  internal/store/store.go \
  'expiresAt = deadlineNanos(s.clk.Now(), ttl)' \
  'expiresAt = s.clk.Now().Add(ttl).UnixNano()' \
  'TestARestoreWithAnUnrepresentableTTLDoesNotExpireImmediately' ./internal/store/

# These two moved when the L1 tier began comparing (epoch, version)
# stamps rather than bare versions. The audit reported them as SKIP rather
# than passing them silently, which is the whole reason SKIP is a failure: a
# mutation whose pattern no longer matches verifies nothing, and a refactor
# would otherwise retire coverage while the total kept looking healthy.
mutate "L1 accepts an older version" \
  internal/l1/l1.go \
  '		stale := !stamp.NewerThan(existing.stamp)' \
  '		stale := false' \
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
  'TestNamespacesAreIsolated' ./tests

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
  'TestGetOrLoadCollapsesConcurrentMisses' ./tests

mutate "store version stops incrementing" \
  internal/store/store.go \
  '	version := old.version + 1' \
  '	version := uint64(1)' \
  'TestVersionsIncrementPerKey' ./internal/store/

mutate "L1 invalidation ignores the resident version" \
  internal/l1/l1.go \
  'if !ok || !stamp.NewerThan(e.stamp) {' \
  'if !ok || (false && !stamp.NewerThan(e.stamp)) {' \
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
  'TestCancelledContextIsReportedFirst' ./tests

mutate "loader runs without the namespace deadline" \
  namespace.go \
  'loadCtx, cancel := clock.WithTimeout(loadCtx, n.cache.opts.Clock, n.opts.LoaderTimeout)
		defer cancel()' \
  '' \
  'TestLoaderTimeoutIsTheNamespacesNotTheCallers' ./tests

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
  'TestItemsSnapshotDoesNotAliasGroupState' ./tests

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
  '_ = keyHash' \
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
  '	if !ok || current.ID != g.self.ID {
		g.refusals.Add(1)' \
  '	if _, _ = ok, current; false {
		g.refusals.Add(1)' \
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

mutate "the epoch is checked before ownership"   internal/ownership/guard.go   '	current, ok := owner(snap, groupKey)
	if !ok || current.ID != g.self.ID {
		g.refusals.Add(1)'   '	if epoch < snap.epoch {
		return fmt.Errorf("mutant: %w", core.ErrStaleEpoch)
	}
	current, ok := owner(snap, groupKey)
	if !ok || current.ID != g.self.ID {
		g.refusals.Add(1)'   'TestNotOwnerBeatsStaleEpoch' ./internal/ownership/

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

# The two below are wiring mutations rather than logic mutations. Every ownership
# invariant above holds inside internal/ownership whether or not the library
# ever calls it; these check that peercache.New connects the guard to the
# membership authority and to activation. A seam nobody tests is a seam that
# breaks silently.
mutate "the guard is built but never consulted" \
  peercache.go \
  'canActivate = guard.CanActivate' \
  'canActivate = nil' \
  'TestTwoNodesAgreeOnOwnership' ./tests

mutate "the guard never learns the membership" \
  peercache.go \
  'guard.Watch(resolved.Membership)' \
  '_ = resolved.Membership' \
  'TestJoiningNodeDefersNewlyAcquiredGroups' ./tests

# ---------------------------------------------------------------- remote

# Retargeted once already. This guarded bounded.go through
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
  '		stale := !stamp.NewerThan(existing.stamp)' \
  '		stale := stamp.Version <= existing.stamp.Version' \
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
  '_ = sentinel; return errors.New(st.Message())' \
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
  'InsecureSkipVerify: c.InsecureSkipVerify,' \
  'InsecureSkipVerify: true,' \
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
  'TestRouterIssuesOneRequestPerOwner' ./tests

mutate "a group batch is applied one message at a time" \
  router.go \
  'work.batches = append(work.batches, wire.GroupBatch{Group: g, Messages: byGroup[g]})' \
  'for _, m := range byGroup[g] {
			work.batches = append(work.batches, wire.GroupBatch{Group: g, Messages: [][]byte{m}})
		}' \
  'TestRouterSubmit' ./tests

mutate "every node runs its own loader" \
  namespace.go \
  '	if p.local {
		return n.localGetOrLoad(ctx, key)
	}
	return n.remoteGetOrLoad(ctx, p, key, true)' \
  '	_ = p
	return n.localGetOrLoad(ctx, key)' \
  'TestTheLoaderRunsOnlyOnTheOwner' ./tests

mutate "gossip advertises the SWIM port as the RPC address" \
  membership/gossip.go \
  'if meta.RPCAddr != "" {
				node.Addr = meta.RPCAddr' \
  'if false {
				node.Addr = meta.RPCAddr' \
  'TestGossipAdvertisesTheRPCAddress' ./membership/
# "enqueueing an invalidation blocks when the queue is full" was here until
# hints were batched per peer (#90). Since then the goroutine draining the
# queue rarely waits: it hands rounds to per-peer buffers without blocking, so
# making the node's send blocking changes nothing a test can see. A backlog
# is now bounded twice — the node's queue drops, and each peer's buffer drops
# past hintPeerMax hints — and from outside the two drops are one counter, so
# disabling either alone is not observable. Disabling both makes
# TestInvalidationBacklogIsDroppedNotQueued fail (checked by hand when this
# entry was retired: "none dropped: the backlog is held without bound").
# ---------------------------------------------------------------- replication

mutate "an older replica copy overwrites a newer one" \
  internal/replication/replica.go \
  'if !Newer(e, existing) {' \
  'if false {' \
  'TestAnOlderCopyIsRejected' ./internal/replication/

mutate "the replica store compares versions, ignoring the epoch" \
  internal/replication/replica.go \
  '	return a.Stamp.NewerThan(b.Stamp)' \
  '	return a.Stamp.Version > b.Stamp.Version' \
  'TestALaterEpochWinsWhateverTheVersion' ./internal/replication/

mutate "Drain leaves a stranded copy behind" \
  internal/replication/replica.go \
  '		taken[key] = e
		nr.del(key)' \
  '		taken[key] = e' \
  'TestDrainTakesOnlyWhatItClaims' ./internal/replication/

mutate "snapshot ignores its page limit" \
  internal/store/store.go \
  '	more := len(candidates) > limit' \
  '	more := false' \
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
  '_ = e' \
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
  'TestSyncReplicationPlacesCopiesBeforeReturning' ./tests

mutate "async replication waits for its replicas" \
  replicate.go \
  'if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)' \
  'return r.fanOut(ctx, w)' \
  'TestAsyncReplicationDoesNotBlockTheWrite' ./tests

mutate "replication does nothing at all" \
  replicate.go \
  'if !sync {
		r.enqueue(w)
		return nil
	}
	return r.fanOut(ctx, w)' \
  'return nil' \
  'TestChaosLosesNoSynchronouslyWrittenKey' ./tests

mutate "a remote write skips replication" \
  handler.go \
  'if err := ns.localSet(ctx, in.Key, in.Value, time.Duration(in.TTLNanos)); err != nil {
		return nil, err
	}' \
  'ns.store.Set(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos))' \
  'TestKillingAnOwnerPromotesAReplica' ./tests

mutate "promotion drains nothing" \
  promote.go \
  'taken := c.replicas.Drain(name, func(key string) bool {' \
  'taken := c.replicas.Drain(name, func(key string) bool {
			if true {
				return false
			}' \
  'TestKillingAnOwnerPromotesAReplica' ./tests

mutate "catch-up sends whatever is asked for" \
  handler.go \
  'for _, node := range ranked[:n] {
			if node.ID == in.Requester {
				return true
			}
		}
		return false' \
  '_ = ranked[:n]
		return true' \
  'TestCatchUpOnlySendsKeysTheAskerRanksFor' ./tests

mutate "catch-up does not reconcile what it pulled" \
  catchup.go \
  'defer c.promoter.reconcile()' \
  '' \
  'TestARestartedNodeRecoversItsColdReplicas' ./tests
# ---------------------------------------------------------------- lifecycle

mutate "readiness ignores catch-up" \
  ready.go \
  'if !st.CaughtUp {
		st.Reason = "replicas not yet pulled; call CatchUp"
		return st
	}' \
  '' \
  'TestANodeIsNotReadyBeforeCatchUp' ./tests

mutate "readiness ignores whether the view has settled" \
  ready.go \
  'st.Settled = c.guard.Settled()
	if !st.Settled {
		st.Reason = "membership has not settled"
		return st
	}' \
  'st.Settled = true' \
  'TestANodeIsNotReadyWhileMembershipMoves' ./tests

mutate "a draining node still reports ready" \
  ready.go \
  'case st.Draining:
		st.Reason = "draining"
		return st' \
  'case false:
		return st' \
  'TestDrainingIsPermanentlyNotReady' ./tests

mutate "Settled always says yes" \
  internal/ownership/guard.go \
  'elapsed := g.clk.Now().UnixNano() - g.changedAt.Load()
	return elapsed >= int64(g.activationDelay)' \
  'return true' \
  'TestANodeIsNotReadyWhileMembershipMoves' ./tests

mutate "drain does not hand off its keys" \
  drain.go \
  'if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}' \
  '' \
  'TestDrainSavesDataWithoutReplication' ./tests

mutate "drain does not announce its departure" \
  drain.go \
  'if err := c.opts.Membership.Leave(ctx); err != nil {' \
  'if err := error(nil); err != nil {' \
  'TestDrainStopsPeersRoutingImmediately' ./tests

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
# Since question 32 was reversed, a peer request for an unopened namespace is
# refused before anything could create it, so these replace the entry for the
# implicit-namespace counter.
mutate "a peer request for an unopened namespace is served" \
  handler.go \
  '	if !ok {
		h.cache.unopenedRefusals.Add(1)' \
  '	if false {
		h.cache.unopenedRefusals.Add(1)' \
  'TestAPeerRequestForAnUnopenedNamespaceIsRefused$' ./tests
mutate "a refusal for an unopened namespace is not counted" \
  handler.go \
  '		h.cache.unopenedRefusals.Add(1)' \
  '' \
  'TestAPeerRequestForAnUnopenedNamespaceIsRefused$' ./tests

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
  '_ = newErr' \
  'TestAnEmptyNewViewPlansNothing' ./internal/rebalance/

mutate "unchanged keys are never counted" \
  internal/rebalance/plan.go \
  'if !moved {
			plan.Unchanged++
		}' \
  '_ = moved' \
  'TestNoChangeMeansNoMoves' ./internal/rebalance/

# --- lifecycle: the entry that was truncated ----------------------------------
#
# What stood here was an orphaned argument list with no `mutate` in front of it,
# left by a bad edit. Under `set -u` without `set -e` that line is a command
# named TestRollingUpgradeLosesNoRequests, which fails with "not found" on
# stderr the run discards, and the script carries on. So the drain acceptance
# criterion sat in this file looking audited and was not.
#
# Reconstructing it found that the criterion could not have caught either
# mutation anyway: it runs at replication factor 2 with synchronous replication,
# so the replicas hold every key and deleting the handoff costs it nothing. The
# two tests below were written to close that, at replication factor 1
# where the handoff is the only copy.
mutate "drain hands off nothing" \
  drain.go \
  '	// Step 3: push what this node owns to whoever owns it now.
	if err := c.handOff(ctx); err != nil {
		problems = errors.Join(problems, err)
	}' \
  '' \
  'TestDrainHandsOffKeysNothingElseWouldSave' ./tests

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
  'TestDrainAnnouncesDepartureBeforeTransferring' ./tests

# --- observability ------------------------------------------------------------
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
  '			if Newer(existing, e) {
				s.versionConflicts.Add(1)
			}' \
  '' \
  'TestVersionConflictsAreCountedButRetransmissionsAreNot' ./internal/replication/

mutate "a retransmission is counted as a version conflict" \
  internal/replication/replica.go \
  '			if Newer(existing, e) {
				s.versionConflicts.Add(1)
			}' \
  '			s.versionConflicts.Add(1)' \
  'TestVersionConflictsAreCountedButRetransmissionsAreNot' ./internal/replication/

mutate "mailbox rejections never reach Stats" \
  peercache.go \
  'MailboxRejections: groups.Rejections,' \
  'MailboxRejections: 0,' \
  'TestMailboxRejectionsAreCounted' ./tests

mutate "a stats field is exported under the wrong name" \
  internal/metrics/metrics.go \
  '{"peercache_split_brain_detections_total", Counter,' \
  '{"peercache_split_brains_total", Counter,' \
  'TestEveryStatsFieldIsExported' ./internal/metrics/


# --- failure guarantees ----------------------------------------------------

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
# queueing them is precisely the old eviction behaviour, and it loses keys whenever a
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
  'TestMajoritySideWritesSurviveAHealAndTwoDestructions' ./tests

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
  'TestAHandOffThatCannotBePlacedKeepsTheCopy' ./tests

# A hand-off is a relocation, not a write. Stamping it with the forwarding node's
# ID makes the receiver see the original owner's replication and the forward as
# two writers under one epoch, and manufacture split-brain detections -- in the
# one counter the README says to investigate whenever it is non-zero.
mutate "a hand-off claims to be the writer of what it forwards" \
  handoff.go \
  'from := w.entry.From' \
  'from := c.selfID()' \
  'TestMajoritySideWritesSurviveAHeal$' ./tests

# No entry for a hand-off filling in its own ID for a copy with no writer.
# Reaching it takes a node that adopts a key, loses it, and then ranks for it
# no longer — two view changes in a fixed order, which rendezvous ranking does
# not produce from one. TestMajoritySideWritesSurviveAHeal reaches it in some
# runs and not others, and an entry that passes when the run misses it is the
# vacuous kind this script refuses.

# Demotion names no writer: the owned store does not know one, and the
# demoting node may have adopted the key rather than written it.
mutate "a demoted copy names the demoting node as its writer" \
  promote.go \
  '			// carries no Prev either, so it was never judged one before.
		})' \
  '			// carries no Prev either, so it was never judged one before.
			From: self,
		})' \
  'TestADemotedCopyNamesNoWriterItDidNotWrite$' ./tests

# The degraded-write counter is the only signal separating a synchronous write
# that placed RF copies from one that placed one. Question 37 is about it being
# invisible; a counter that never increments restores exactly that.
mutate "a synchronous write below the replication factor is not counted" \
  namespace.go \
  '	if n.cache.degradedWrites.Add(1) == 1 {' \
  '	if false {' \
  'TestAnIsolatedNodeAcknowledgesSyncWritesWithOneCopy' ./tests

# --- Question 37, options 2 and 3: WithRequireReplicationFactor ---
#
# Decision 0021. Each entry was applied and reverted by hand before being
# written here.

# The opt-in refusal itself. Without it the strict namespace acknowledges the
# isolated write exactly as the default one does, which is the behaviour the
# option exists to change.
mutate "a namespace requiring its factor still acknowledges below it"   namespace.go   'return n.opts.RequireReplicationFactor && n.opts.SyncReplication &&'   'return false && n.opts.RequireReplicationFactor && n.opts.SyncReplication &&'   'TestRequireReplicationFactorRefusesAnIsolatedWrite$' ./tests

# The refusal after a partial placement. An isolated node has no targets and
# takes the other branch, so only a cluster with some replicas but not enough
# reaches this one.
mutate "a write placed on some replicas but not enough is acknowledged" \
  namespace.go \
  '	// than only by the owner.
	if n.underFactor(len(targets)) {' \
  '	// than only by the owner.
	if false {' \
  'TestRequireReplicationFactorRefusesThroughAForwardedWrite$' ./tests

# The sentinel has to survive gRPC, or a write through any non-owner sees a
# generic unreachable error and the distinction exists only for local writes.
mutate "ErrUnderReplicated stops surviving the gRPC hop"   transport/client.go   '	case codes.Internal:
		// gRPC raises Internal itself'   '	case codes.DataLoss:
		// gRPC raises Internal itself'   'TestSentinelErrorsSurviveTheHop$' ./transport/

# Requiring the factor under asynchronous replication cannot be honoured. Letting
# it through would be an option that silently does nothing.
mutate "requiring the factor without sync replication is accepted"   internal/core/options.go   'if o.RequireReplicationFactor && !o.SyncReplication {'   'if false {'   'TestNamespaceOptionValidationSurfaces$' ./tests

# The nightly soak's step timeout, put back to the figure that fitted one test
# when the pattern selects two.
mutate "the nightly soak timeout fits one soak test, not two" \
  .github/workflows/nightly.yml \
  'run: go test ./tests -run TestSoak -count=1 -timeout 135m' \
  'run: go test ./tests -run TestSoak -count=1 -timeout 85m' \
  'TestNightlySoakFitsItsTimeouts$' ./tests/repo

# --- Question 40: ReplicasCurrent, and the CatchUp error it exposed ---

# The original bug: the per-peer error is computed and discarded, so CatchUp
# returns nil after reaching nobody.
mutate "CatchUp drops the errors of the peers it could not reach" \
  catchup.go \
  'failures = errors.Join(failures, fmt.Errorf(' \
  '_ = errors.Join(failures, fmt.Errorf(' \
  'TestCatchUpReportsAPeerItCouldNotReach$' ./tests

# Without the epoch comparison, one catch-up makes a node current forever, which
# is the one-shot CaughtUp flag question 40 was opened about.
mutate "ReplicasCurrent ignores membership changes" \
  ready.go \
  'return at != 0 && at-1 == uint64(c.guard.Stats().Epoch)' \
  'return at != 0' \
  'TestReplicasCurrentFollowsCatchUpAndMembership$' ./tests

# A catch-up that failed against some peers must not count as current.
mutate "a failed catch-up still marks replicas current" \
  catchup.go \
  '	if err == nil {
		c.readiness.markCurrent(epoch)' \
  '	if true {
		c.readiness.markCurrent(epoch)' \
  'TestCatchUpReportsAPeerItCouldNotReach$' ./tests

# --- resource pressure, part one: the memory budget (decision 0022) ---

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
  'TestAShedKeyIsNeverServedAtAnOlderValue$' ./tests

# Replicas before owned data.
mutate "owned data is shed while replicas could have been" \
  pressure.go \
  '	// 2. Replicas, but not the young copies of persistent writes.
	if excess > 0 {' \
  '	// 2. Replicas, but not the young copies of persistent writes.
	if false {' \
  'TestSheddingTakesL1AndReplicasBeforeOwnedData$' ./tests

# A persistent write's replica is its only other holder until the owner
# flushes it; the budget must not shed it then (open question 60).
mutate "the budget sheds replicas of unflushed persistent writes" \
  pressure.go \
  '			if floor, ok := keep[name]; ok {' \
  '			if floor, ok := keep[name]; ok && false {' \
  'TestAnUnflushedWriteSurvivesItsReplicaNodeShedding$' ./tests

mutate "the replica store ignores the floor it is given" \
  internal/replication/replica.go \
  '			if e.Order >= floor {
				continue
			}' \
  '			if e.Order >= floor && false {
				continue
			}' \
  'TestShedKeepsCopiesFromTheFloor$' ./internal/replication/

mutate "the budget keeps every replica of a persistent namespace" \
  pressure.go \
  '		keep[ns.name] = uint64(hlc.FromMillis(now.Add(-ns.persister.unflushedFor()).UnixMilli()))' \
  '		keep[ns.name] = uint64(hlc.FromMillis(now.Add(-ns.persister.unflushedFor()).UnixMilli())) * 0' \
  'TestAFlushedWritesReplicaIsShedAsBefore$' ./tests

mutate "the window is the flush interval alone, not its timeout too" \
  persistence.go \
  '	return p.flushInterval() + persistFlushTimeout' \
  '	return p.flushInterval()' \
  'TestAnUnflushedWriteSurvivesItsReplicaNodeShedding$' ./tests

# L1 before everything.
mutate "the L1 tier is never shed" \
  pressure.go \
  '		entries := ns.l1.Len()
		excess -= ns.l1.Shed()' \
  '		entries := 0
		_ = ns.l1.Len()' \
  'TestSheddingTakesL1AndReplicasBeforeOwnedData$' ./tests

mutate "catch-up ignores the memory budget" \
  catchup.go \
  '		if c.pressure.over() {' \
  '		if false {' \
  'TestCatchUpStopsAtTheMemoryBudget$' ./tests

mutate "local writes never check the budget" \
  pressure.go \
  '	p.check()
}

// check sheds' \
  '}

// check sheds' \
  'TestASingleNodeStaysWithinItsMemoryBudget$' ./tests

# The inflow a failover produces is copies from peers, not local writes.
mutate "replica arrivals never check the budget" \
  handler.go \
  '	h.cache.pressure.noteWrite(len(in.Value))
	if conflict {' \
  '	if conflict {' \
  'TestAReplicaOnlyNodeStaysWithinItsBudget$' ./tests

# --- resource pressure, part two: join probation (decision 0023) ---

mutate "probation hides nobody" \
  probation.go \
  '		if !ok || now-started >= int64(period) {' \
  '		if _ = ok; true {' \
  'TestACrashloopingNodeIsNeverRanked$' ./tests

mutate "a probation ending never reaches the ranking" \
  probation.go \
  '	before := p.lastIDs
	p.refreshLocked()' \
  '	return
	before := p.lastIDs
	p.refreshLocked()' \
  'TestAProbationedNodeIsRankedOnceItHasLasted$' ./tests

mutate "a cold start ranks nobody" \
  probation.go \
  '	if len(established) == 0 {' \
  '	if false {' \
  'TestColdStartRanksEveryNode$' ./tests

# --- resource pressure, part three: a slow node ---

# Asynchronous writes that waited on their replicas would make every write
# whose replica is a throttled node as slow as that node.
mutate "asynchronous writes wait for their replicas"   namespace.go   '	if err := n.cache.replicator.replicate(ctx, n.opts.SyncReplication, replicaWork{'   '	if err := n.cache.replicator.replicate(ctx, true, replicaWork{'   'TestAStarvedNodeSlowsOnlyItsOwnKeys$' ./tests

# --- TTL across failover: replicas hold the owner's deadline ---
#
# Found by writing tests for code coverage reported as never run. The
# promotion-time and hand-off-time expiry checks are not entered here: the
# replica sweep always removes an expired copy first in this harness, so
# removing either check changes nothing a test can see. See
# docs/verification.md.

mutate "a caught-up replica's deadline is counted late"   catchup.go   'ExpiresAt:  replication.DeadlineFrom(now, e.TTLNanos),'   'ExpiresAt:  replication.DeadlineFrom(now+int64(6*time.Second), e.TTLNanos),'   'TestACaughtUpKeyKeepsItsRemainingTTL$' ./tests

mutate "expired replicas are never swept"   peercache.go   '	_, _ = c.replicas.SweepExpired(c.opts.Clock.Now().UnixNano())'   ''   'TestExpiredReplicasAreReclaimed$' ./tests

mutate "a replica at its deadline still counts as live"   internal/replication/replica.go   '	return left, left > 0'   '	return left, left >= 0'   'TestRemainingAndDeadline$' ./internal/replication/

# --- The coverage sweep: paths that had never run ---

mutate "a Set gives up when ownership moved under it" \
  namespace.go \
  '			return n.cache.route.remoteSet(ctx, next, n.name, key, val, ttl)
		}' \
  '			return err
		}' \
  'TestASetFollowsOwnershipThatMovedUnderIt$' ./tests

mutate "a Call gives up when ownership moved under it" \
  group.go \
  '	return g.cache.route.remoteCall(ctx, next, g.id, fn, arg)
}' \
  '	return nil, err
}' \
  'TestAGroupCallFollowsOwnershipThatMovedUnderIt$' ./tests

mutate "GetOrLoad without a loader asks the owner anyway" \
  namespace.go \
  '	if n.opts.Loader == nil {
		return nil, fmt.Errorf("namespace %q: %w", n.name, ErrNoLoader)
	}

	p, err := n.cache.route.locate' \
  '	p, err := n.cache.route.locate' \
  'TestRemoteGetOrLoadWithoutALoaderOnTheOwner$' ./tests

mutate "a peer's own failure is reported as unreachable" \
  transport/client.go \
  '	code := codes.Unknown' \
  '	code := codes.Internal' \
  'TestAnApplicationErrorIsNotReportedAsUnreachable$' ./transport/

mutate "probation swallows the departure announcement" \
  probation.go \
  'func (p *probationAuthority) Leave(ctx context.Context) error { return p.inner.Leave(ctx) }' \
  'func (p *probationAuthority) Leave(ctx context.Context) error { return nil }' \
  'TestDrainUnderJoinProbationAnnouncesTheDeparture$' ./tests

mutate "a second Drain hands everything off again" \
  drain.go \
  '	if !c.readiness.draining.CompareAndSwap(false, true) {' \
  '	if c.readiness.draining.Swap(true) && false {' \
  'TestASecondDrainDoesNothing$' ./tests

mutate "Drain hides a failed departure announcement" \
  drain.go \
  '		problems = errors.Join(problems, fmt.Errorf("peercache: announcing departure: %w", err))' \
  '		_ = fmt.Errorf("peercache: announcing departure: %w", err)' \
  'TestDrainReportsAFailedDepartureAndStillHandsOff$' ./tests

mutate "a remote BatchGet returns nothing" \
  handler.go \
  '		out.Entries = append(out.Entries, wire.BatchEntry{' \
  '		_ = append(out.Entries, wire.BatchEntry{' \
  'TestBatchGetAcrossNodes$' ./tests

mutate "a remote Exists is always false" \
  namespace.go \
  '	case err == nil:
		return true, nil
	case errors.Is(err, ErrNotFound):' \
  '	case err == nil:
		return false, nil
	case errors.Is(err, ErrNotFound):' \
  'TestExistsAcrossNodes$' ./tests

mutate "remote latency is never observed" \
  remote.go \
  '			r.cache.remoteLatency.Observe(uint64(elapsed))' \
  '			_ = elapsed' \
  'TestLatencyHistogramsRecordRemoteOperations$' ./tests

mutate "lifecycle events are never delivered" \
  internal/actor/record.go \
  '		r.rt.opts.OnEvent(e)' \
  '		_ = e' \
  'TestEventHandlerSeesEveryLifecycleEvent$' ./tests

# --- Decision 0024: WithSeeds joins ---

mutate "seeds are never joined"   peercache.go   '	if len(resolved.Seeds) > 0 {
		if err := resolved.Membership.Join(resolved.Seeds); err != nil {'   '	if false {
		if err := resolved.Membership.Join(resolved.Seeds); err != nil {'   'TestWithSeedsJoinsThroughTheMembership$' ./tests

mutate "seeds without a membership are accepted"   internal/core/options.go   '	if len(o.Seeds) > 0 && o.Membership == nil {'   '	if false {'   'TestSeedsWithoutMembershipAreRejected$' ./tests

# --- failure guarantees, part two: the fuzz targets and the checks on the build files ---
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
  'TestFuzzTargetsNamedInCIExist' ./tests/repo

# A derived figure in the operator-facing documentation, moved by one.
mutate "the documented metric count drifts from the handler" \
  docs/api.md \
  '66 series come out of it' \
  '65 series come out of it' \
  'TestApiDocMetricCountsAreCorrect' ./tests/repo

# A public-API file dropping out of the repository map, which is how twelve of
# the nineteen came to be missing in the first place.
mutate "a root file disappears from the repository map" \
  README.md \
  'handoff.go        Moves replicas on a membership change rather' \
  'handoff_go        Moves replicas on a membership change rather' \
  'TestReadmeListsEveryRootFile' ./tests/repo

# --- typed namespaces (decision 0025) ---

mutate "one name may hold two types" \
  namespace.go \
  '	if n.valueType != t {' \
  '	if false {' \
  'TestOneNameTwoTypesIsInvalid$' ./tests

mutate "a typed loader of the wrong type is accepted" \
  typed.go \
  'if spec := ns.opts.TypedLoader; spec != nil && spec.ValueType() != want {' \
  'if spec := ns.opts.TypedLoader; false && spec != nil {' \
  'TestATypedLoaderOfTheWrongTypeIsInvalid$' ./tests

mutate "WithLoader and WithTypedLoader together are accepted" \
  internal/core/options.go \
  '	if o.TypedLoader != nil && o.Loader != nil {' \
  '	if false {' \
  'TestTypedLoaderAndLoaderConflict$' ./tests

mutate "the default typed codec is JSON" \
  typed.go \
  'const DefaultTypedCodec = codec.NameMsgPack' \
  'const DefaultTypedCodec = codec.NameJSON' \
  'TestInvalidUTF8SurvivesTheDefaultCodec$' ./tests

mutate "a decode failure is not ErrCodec" \
  typed.go \
  'return zero, fmt.Errorf("peercache: decoding %q with %s: %v: %w", key, t.codec.Name(), err, ErrCodec)' \
  'return zero, fmt.Errorf("peercache: decoding %q with %s: %w", key, t.codec.Name(), err)' \
  'TestAnUndecodableValueIsErrCodec$' ./tests

mutate "the typed loader is never installed" \
  peercache.go \
  '		ns.opts.Loader = opts.TypedLoader.Loader(valueCodec.Marshal)' \
  '		_ = opts.TypedLoader' \
  'TestTypedGetOrLoadAcrossNodes$' ./tests

mutate "a typed BatchGet drops what it decoded" \
  typed.go \
  '		out[key] = v' \
  '		_ = v' \
  'TestTypedValuesRoundTripAcrossNodes$' ./tests

# A two-digit milestone's row. The map's row pattern was M[0-9], so M10 rows
# were not counted at all until it became M[0-9]+; renaming one must be noticed.
mutate "a two-digit milestone row stops being counted" \
  docs/verification.md \
  '| M10 | a typed value set on one node reads back equal on every node through Get and BatchGet |' \
  '| X10 | a typed value set on one node reads back equal on every node through Get and BatchGet |' \
  'TestAcceptanceMapCountsAreCorrect$' ./tests/repo

# --- Question 42: a lost tombstone ---

# Checked in the store: since replicas order copies by HLC (question 50), the
# cluster test's replica takes the rewrite whatever its version, so it no
# longer depends on the floor. The floor still keeps a key's version from
# going backwards across a Delete, which L1 stamps compare.
mutate "a deletion no longer raises the version floor" \
  internal/store/store.go \
  '		// docs/decisions/open.md question 42.
		if e.version > sh.floor {
			sh.floor = e.version
		}' \
  '		// docs/decisions/open.md question 42.' \
  'TestARewriteAfterShedIsVersionedAboveWhatWasShed$' ./internal/store/

# --- Typed group functions ---

mutate "CallTyped does not check the registered types" \
  typed_group.go \
  '		if sig.arg != reflect.TypeFor[A]() || sig.result != reflect.TypeFor[R]() {' \
  '		if false && sig.arg != nil {' \
  'TestCallTypedWithTheWrongTypesIsInvalid$' ./tests

mutate "an undecodable typed argument is not ErrCodec" \
  typed_group.go \
  '			return nil, fmt.Errorf("peercache: group function %q: decoding its argument as %v: %v: %w",
				name, reflect.TypeFor[A](), err, ErrCodec)' \
  '			return nil, fmt.Errorf("peercache: group function %q: decoding its argument as %v: %w",
				name, reflect.TypeFor[A](), err)' \
  'TestATypedGroupFuncRejectsAnUndecodableArgument$' ./tests

mutate "ErrCodec stops surviving the gRPC hop" \
  transport/client.go \
  '		} else if containsSentinel(st.Message(), core.ErrCodec.Error()) {' \
  '		} else if false {' \
  'TestSentinelErrorsSurviveTheHop$' ./transport/

mutate "typed signatures are never recorded" \
  typed_group.go \
  '		cc.typedFuncs.Store(name, typedSignature{arg: reflect.TypeFor[A](), result: reflect.TypeFor[R]()})' \
  '		_ = cc' \
  'TestATypedGroupFuncRunsOnTheOwnerAcrossNodes$' ./tests

# --- Code review findings ---

# The handler checked ownership by the raw key; callers route by group key.
mutate "the peer handler checks ownership by the raw key" \
  handler.go \
  '		groupKey = ns.groupKeyFor(key)' \
  '		_ = ns' \
  'TestAPartitionedNamespaceServesRemoteCallers$' ./tests

# BatchSet wrote into the calling node's own store, bypassing routing.
mutate "BatchSet writes locally instead of routing"   namespace.go   '		if err := n.Set(ctx, key, val, ttl); err != nil {'   '		if err := (error)(nil); n.store.Set(key, n.encode(val), ttl) == 0 && err != nil {'   'TestBatchSetRoutesEachKeyToItsOwner$' ./tests

# A peer write to a namespace misconfigured on its owner reached a nil store
# and panicked on the handler goroutine.
mutate "a misconfigured namespace's nil store is reachable from a peer"   handler.go   '	ns := h.cache.Namespace(name).(*namespace)
	if ns.err != nil {
		return nil, h.misconfigured(ns)
	}
	if !existed {'   '	ns := h.cache.Namespace(name).(*namespace)
	if !existed {'   'TestAPeerRequestToAMisconfiguredNamespaceIsAnErrorNotACrash$' ./tests

# Compression across nodes: the wire and L1 carry plain values, the stores
# framed ones. Each side once got the other's form.
mutate "a remote Get unframes a value the owner already unframed" \
  namespace.go \
  '		n.l1.Put(key, value, stamp, l1Lifetime(n.opts.L1TTL, left))
	}
	return value, nil
}
' \
  '		n.l1.Put(key, value, stamp, l1Lifetime(n.opts.L1TTL, left))
	}
	return n.decode(value)
}
' \
  'TestCompressedValuesSurviveEveryRemotePath$' ./tests

# A load that raced a Set stored its older value over the newer one.
mutate "a load overwrites a write that raced it"   internal/store/store.go   '	if had && (old.expiresAt == 0 || now.UnixNano() < old.expiresAt) {
		return old.val, false
	}'   ''   'TestALoadDoesNotOverwriteAWriteThatRacedIt$' ./tests

# A replicated tombstone removed the replica's copy whatever its stamp.
mutate "a stale tombstone deletes a newer replica" \
  internal/replication/replica.go \
  '	if !held || Newer(e, Entry{Stamp: stamp, Order: order}) {' \
  '	if !held {' \
  'TestAStaleTombstoneDoesNotDeleteANewerReplica$' ./tests

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
				if err := ns.l1.Close(); err != nil {'   'TestCloseReleasesTheL1Tier$' ./tests

mutate "a failed New leaves the sweep ticker armed"   peercache.go   '				c.ticker.Stop()
				_ = groups.Close(context.Background())'   '				_ = groups.Close(context.Background())'   'TestAFailedNewLeavesNothingRunning$' ./tests

# A snapshot page that copies every candidate instead of its page.
mutate "a snapshot page copies the whole store"   internal/store/store.go   '	if more {
		candidates = candidates[:limit]
	}'   ''   'TestASnapshotPageAllocatesOnlyItsPage$' ./internal/store/

# A refused activation left an empty record behind for good.
mutate "a refused activation keeps its empty record"   internal/actor/record.go   '			if len(r.state) == 0 && len(r.timers) == 0 {
				r.removed = true'   '			if false {
				r.removed = true'   'TestARefusedActivationLeavesNoRecord$' ./internal/actor/

# The republisher replaced a synchronous UpdateNode in the event goroutine,
# which could deadlock memberlist. That deadlock cannot be staged without over
# sixty real nodes; what is held here is that republishing still happens.
mutate "this node's metadata is never republished"   membership/gossip.go   '			case g.republish <- struct{}{}:'   '			case <-make(chan struct{}):'   'TestAPeersAdvertisedEpochFollowsItsView$' ./membership/

# --- atomic key operations (decision 0027) ---

mutate "an update reads and writes under separate locks" \
  internal/store/update.go \
  '	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.forget(key)

	old, had := sh.entries[key]' \
  '	sh.mu.RLock()
	old, had := sh.entries[key]
	sh.mu.RUnlock()
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.forget(key)' \
  'TestUpdateIsAtomicUnderContention$' ./internal/store

mutate "compare-and-swap compares framed bytes" \
  atomic.go \
  '			cur = plain' \
  '			cur, _ = stored, plain' \
  'TestCompareAndSwapComparesWhatGetReturns$' ./tests

mutate "an increment extends its counters deadline" \
  atomic.go \
  '		if ok {
			m.KeepDeadline = true
		} else {
			m.TTL = ttl
		}' \
  '		m.TTL = ttl' \
  'TestIncrementKeepsTheCountersDeadline$' ./tests

mutate "a kept deadline reaches replicas as none" \
  atomic.go \
  '		ttl = time.Duration(res.ExpiresAt - n.cache.opts.Clock.Now().UnixNano())' \
  '		ttl = 0 * time.Duration(res.ExpiresAt-n.cache.opts.Clock.Now().UnixNano())' \
  'TestTouchOutlivesTheOriginalDeadlineAcrossFailover$|TestIncrementKeepsTheCountersDeadline$' ./tests

mutate "an atomic write retries after any error" \
  atomic.go \
  '	if err != nil && retryable(err) {' \
  '	if err != nil {' \
  'TestAnIncrementWhoseOwnerFailedAfterApplyingIsNotRetried$' ./tests

mutate "set-if-absent overwrites a live value" \
  atomic.go \
  '		} else if ok {
			return store.Mutation{}, false, nil
		}' \
  '		} else if false {
			return store.Mutation{}, false, nil
		}' \
  'TestExactlyOneConcurrentSetIfAbsentWins$' ./tests

mutate "an overflowing increment wraps" \
  atomic.go \
  '		if (delta > 0 && sum < current) || (delta < 0 && sum > current) {' \
  '		if false {' \
  'TestIncrementRefusesWhatIsNotAnInteger$' ./tests

# --- hot-key reads (decision 0028) ---

mutate "remote misses are not coalesced" \
  namespace.go \
  '	value, err := n.sharedFetch(ctx, p, key)' \
  '	value, err := n.fetchOwner(ctx, p, key)' \
  'TestConcurrentRemoteMissesShareOneFetch$' ./tests

mutate "the shared fetch runs on its starter's context" \
  namespace.go \
  '			val, err := n.fetchOwner(fetchCtx, p, key)' \
  '			val, err := n.fetchOwner(ctx, p, key)' \
  'TestACancelledWaiterLeavesTheSharedFetch$' ./tests

mutate "a starter's deadline fails every waiter" \
  namespace.go \
  '		if !errors.Is(res.Err, context.DeadlineExceeded) && !errors.Is(res.Err, context.Canceled) {' \
  '		if true {' \
  'TestALeadersDeadlineIsNotEveryones$' ./tests

mutate "the shared fetch has no deadline" \
  namespace.go \
  '			if deadline, ok := ctx.Deadline(); ok {' \
  '			if deadline, ok := ctx.Deadline(); false && ok {' \
  'TestALeadersDeadlineIsNotEveryones$' ./tests

mutate "an L1 floor reads as a value" \
  internal/l1/l1.go \
  '	if e.floor {
		c.misses.Add(1)' \
  '	if false {
		c.misses.Add(1)' \
  'TestAFloorReplacesAnOlderCopyAndReadsAsAMiss$' ./internal/l1

mutate "an L1 floor refuses its own stamp" \
  internal/l1/l1.go \
  '			stale = existing.stamp.NewerThan(stamp)' \
  '			stale = !stamp.NewerThan(existing.stamp)' \
  'TestAFloorRefusesOlderPutsAndAdmitsItsOwnStamp$' ./internal/l1

mutate "an L1 floor is set for a key with no copy" \
  internal/l1/l1.go \
  '	if !ok || now >= existing.expiresAt || !stamp.NewerThan(existing.stamp) {' \
  '	if ok && (now >= existing.expiresAt || !stamp.NewerThan(existing.stamp)) {' \
  'TestAFloorIsNotSetWithoutACopy$' ./internal/l1

mutate "a replica serves a copy older than the TTL" \
  namespace.go \
  '	if age < 0 || age >= n.opts.L1TTL {' \
  '	if age < 0 {' \
  'TestAReplicaCopyOlderThanTheTTLGoesToTheOwner$' ./tests

mutate "a replica serves a key it no longer ranks for" \
  namespace.go \
  '	if !ranked {
		return nil, core.Stamp{}, 0, 0, false
	}' \
  '	if _ = ranked; false {
		return nil, core.Stamp{}, 0, 0, false
	}' \
  'TestANodeThatNoLongerRanksRefersTheRead$' ./tests

mutate "a replica decline is reported as a miss" \
  namespace.go \
  '	if !served {
		return n.fetchOwner(ctx, p, key)
	}' \
  '	if !served {
		return nil, ErrNotFound
	}' \
  'TestAReplicaWithoutTheKeyIsNotAMiss$' ./tests

mutate "an invalidation deletes instead of leaving a floor" \
  handler.go \
  '	if ns.opts.ReplicaReads {
		ns.l1.Floor(' \
  '	if false {
		ns.l1.Floor(' \
  'TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded$' ./tests

mutate "a replica answer below a floor is used" \
  namespace.go \
  '		if n.l1.Below(key, stamp) {' \
  '		if false {' \
  'TestAReplicaAnswerBelowAnInvalidationFloorIsDiscarded$' ./tests

mutate "a self-replica reads over the network" \
  namespace.go \
  '	if holder.ID == n.cache.selfID() {' \
  '	if false {' \
  'TestASelfReplicaServesWithoutAnRPC$' ./tests

mutate "a replica copy is cached for a full TTL from the read" \
  namespace.go \
  '			n.l1.Put(key, val, stamp, l1Lifetime(remaining, left))' \
  '			n.l1.Put(key, val, stamp, l1Lifetime(n.opts.L1TTL, left))' \
  'TestAReplicaCopyIsCachedForWhatIsLeftOfItsTTL$' ./tests

mutate "replica reads always go to the owner" \
  namespace.go \
  '			if n.opts.ReplicaReads {
				val, err := n.fetchHolder(fetchCtx, p, key)' \
  '			if false {
				val, err := n.fetchHolder(fetchCtx, p, key)' \
  'TestReplicaReadsSpreadAcrossHolders$' ./tests

mutate "a failed replica fails the read" \
  namespace.go \
  '		if err != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}' \
  '		if err != nil {
			return nil, err
		}' \
  'TestAPeerWithoutReplicaGetFallsBackToTheOwner$' ./tests

mutate "a replica copy carries no receipt time" \
  handler.go \
  '		ReceivedAt: now,' \
  '		ReceivedAt: 0,' \
  'TestReplicaReadsSpreadAcrossHolders$' ./tests

mutate "replica reads are accepted at replication factor 1" \
  internal/core/options.go \
  '	if o.ReplicaReads && o.ReplicationFactor < 2 {' \
  '	if false {' \
  'TestReplicaReadsNeedReplicasAndAnL1$' ./tests

mutate "replica reads are accepted with the L1 tier disabled" \
  internal/core/options.go \
  '	if o.ReplicaReads && o.L1TTL < 0 {' \
  '	if false {' \
  'TestReplicaReadsNeedReplicasAndAnL1$' ./tests

# --- open question 45 ---

# --- open question 46 ---

mutate "the write path ranks one replica too few" \
  replicate.go \
  '	ranked, count, epoch := c.guard.Table().Holders(groupKey, rf)' \
  '	ranked, count, epoch := c.guard.Table().Holders(groupKey, rf-1)' \
  'TestReplicasAreTheRunnersUpInTheRanking$' ./tests

# --- persistence: hybrid logical clock (decision 0029) ---

mutate "an HLC order repeats on a stopped clock" \
  internal/hlc/hlc.go \
  '		if next <= last {
			next = last + 1
		}' \
  '		if false {
			next = last + 1
		}' \
  'TestNextIsStrictlyIncreasingOnAStoppedClock$' ./internal/hlc/

mutate "an observed HLC order is ignored" \
  internal/hlc/hlc.go \
  '		if uint64(o) <= last || c.last.CompareAndSwap(last, uint64(o)) {' \
  '		if _ = last; true {' \
  'TestAnObservedOrderIsOutranked$' ./internal/hlc/

mutate "a drifting peer drags the HLC ahead" \
  internal/hlc/hlc.go \
  '	if uint64(o) > limit {' \
  '	if _ = limit; false {' \
  'TestADriftingPeerCannotDragTheClockAhead$' ./internal/hlc/

# --- persistence: segment format and blob stores (decision 0029) ---

mutate "a corrupted segment is decoded" \
  internal/segment/segment.go \
  '	if crc32.Checksum(body, castagnoli) != sum {' \
  '	if false && crc32.Checksum(body, castagnoli) != sum {' \
  'TestEveryBitFlipIsCorrupt$' ./internal/segment/

mutate "a segment's declared count is believed" \
  internal/segment/segment.go \
  '	if !ok || v > uint64(len(d.b)/minEach) {' \
  '	if !ok {' \
  'TestADeclaredCountIsNotBelieved$' ./internal/segment/

mutate "a segment tombstone may carry a value" \
  internal/segment/segment.go \
  '		if r.Tombstone && len(r.Value) != 0 {' \
  '		if false {' \
  'TestStructuralDefectsAreCorrupt$' ./internal/segment/

mutate "the memory blob store keeps the caller's slice" \
  persist/persist.go \
  '	stored := make([]byte, len(data))
	copy(stored, data)' \
  '	stored := data' \
  'TestMemoryMeetsTheContract$' ./persist/

# --- persistence: orders in the store (decision 0029) ---

mutate "a store write reports an order it did not store" \
  internal/store/store.go \
  '	return Write{Version: version, Order: order, ExpiresAt: expiresAt, Prev: old.order}' \
  '	return Write{Version: version, Order: s.nextOrder(), ExpiresAt: expiresAt, Prev: old.order}' \
  'TestAKeysOrdersRiseWithItsVersions$' ./internal/store/

mutate "a promoted value is given a new order" \
  internal/store/store.go \
  '	} else if ok && existing.version >= version {
		return false
	}
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: version, order: order})' \
  '	} else if ok && existing.version >= version {
		return false
	}
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: version, order: s.nextOrder()})' \
  'TestRestoreInstallsTheGivenOrder$' ./internal/store/

mutate "an atomic update stores no order" \
  internal/store/update.go \
  '	order := s.nextOrder()' \
  '	order := uint64(0)' \
  'TestEveryWritePathTakesAnOrder$' ./internal/store/

# --- persistence: write-behind flush (decision 0029) ---

mutate "a pending record shares the caller's buffer" \
  persistence.go \
  '		r.Value = append([]byte(nil), value...)' \
  '		r.Value = value' \
  'TestThePendingRecordIsACopy$' ./tests

mutate "a persistent delete writes no tombstone" \
  namespace.go \
  '		n.persister.record(key, nil, 0, order, true)' \
  '		_ = order' \
  'TestADeleteIsATombstone$' ./tests

mutate "a persistent atomic write is not recorded" \
  atomic.go \
  '		n.persister.record(key, encoded, res.ExpiresAt, res.Order, false)' \
  '		_ = encoded' \
  'TestAnAtomicWriteIsPersisted$' ./tests

mutate "Close does not flush persistent namespaces" \
  peercache.go \
  '				if err := ns.persister.Close(ctx); err != nil {' \
  '				if err := error(nil); err != nil {' \
  'TestCloseFlushesWhatIsPending$' ./tests

# --- persistence: restore (decision 0029) ---

mutate "a restore brings back expired keys" \
  persistence.go \
  '		if !r.Tombstone && r.ExpiresAt != 0 && now >= r.ExpiresAt {' \
  '		if false && now >= r.ExpiresAt {' \
  'TestAColdStartRestoresWhatWasFlushed$' ./tests

mutate "a restore does not advance the clock" \
  persistence.go \
  '	c.observeOrder(maxOrder)' \
  '	_ = c' \
  'TestARestoredKeysNextWriteOutranksItsHistory$' ./tests

mutate "a delete runs ahead of the restore" \
  namespace.go \
  'func (n *namespace) localDelete(ctx context.Context, key string) error {
	if err := n.awaitRestore(ctx, key); err != nil {
		return err
	}' \
  'func (n *namespace) localDelete(ctx context.Context, key string) error {' \
  'TestAWriteBeforeTheFirstReadStillRestoresFirst$' ./tests

# Flushes write logs (decision 0030), and the test's seed is never split, so
# the object it corrupts is a log. The segment loop's identical check had been
# the target, which no test reached: the entry passed with it broken.
mutate "a restore skips a corrupt log" \
  persistence.go \
  '			if err != nil {
				return nil, err
			}
			loaded[name] = true
			merge(&segment.File{Records: records})' \
  '			if err != nil {
				continue
			}
			loaded[name] = true
			merge(&segment.File{Records: records})' \
  'TestACorruptSegmentFailsTheRestore$' ./tests

mutate "a failed restore is never retried" \
  persistence.go \
  '			if st.failedAt != 0 && p.clk.Now().UnixNano()-st.failedAt < int64(restoreRetry) {' \
  '			if st.failedAt != 0 {' \
  'TestAnUnavailableStoreDegradesToMemory$' ./tests

mutate "a restore replaces newer memory" \
  internal/store/store.go \
  '	if live && old.order >= order {
		return 0, false
	}
	if tombstone {' \
  '	if false {
		return 0, false
	}
	if tombstone {' \
  'TestMergeIfNewerKeepsWhateverIsNewer$' ./internal/store/

# --- persistence: cluster acceptance (decision 0029) ---

mutate "a restore installs keys this node does not own" \
  persistence.go \
  '		if !p.owns(key) {
			continue
		}' \
  '		if false {
			continue
		}' \
  'TestKillingEveryHolderRestoresFromStorage$' ./tests

mutate "a membership change does not trigger a restore" \
  promote.go \
  '		p.cache.restoreGen.Add(1)
' \
  '' \
  'TestKillingEveryHolderRestoresFromStorage$' ./tests

# --- persistence: shedding (decision 0029) ---

mutate "a shed persistent key is not marked for restore" \
  pressure.go \
  '				if ns.persister != nil {
					ns.persister.noteShed(key)
				}' \
  '				_ = key' \
  'TestAShedKeyIsRestoredNotLost$' ./tests

# --- persistence: compaction (decision 0029) ---

mutate "a restore reads only one snapshot" \
  persistence.go \
  '			if loaded[name] || !strings.HasPrefix(name[len(prefix):], "snap-") {' \
  '			if loaded[name] || !strings.HasPrefix(name[len(prefix):], "snap-") || len(sc.merged) > 0 {' \
  'TestARestoreMergesEverySnapshot$' ./tests

mutate "compaction deletes segments its snapshot failed to cover" \
  persistence.go \
  '	if err := p.store.Put(ctx, name, snap); err != nil {' \
  '	if err := p.store.Put(ctx, name, snap); false && err != nil {' \
  'TestAFailedSnapshotDeletesNothing$' ./tests

mutate "compaction drops tombstones inside their grace" \
  persistence.go \
  '		if r.Order < cutoff && (r.Tombstone || (r.ExpiresAt != 0 && now >= r.ExpiresAt)) {' \
  '		if _ = cutoff; r.Tombstone || (r.ExpiresAt != 0 && now >= r.ExpiresAt) {' \
  'TestATombstoneIsKeptForItsGrace$' ./tests

mutate "compaction keeps tombstones forever" \
  persistence.go \
  '		if r.Order < cutoff && (r.Tombstone || (r.ExpiresAt != 0 && now >= r.ExpiresAt)) {' \
  '		if _, _ = cutoff, now; false {' \
  'TestATombstoneIsKeptForItsGrace$' ./tests

# --- repointed after M14b moved their code ---

mutate "promotion restarts the version sequence" \
  internal/store/store.go \
  '	} else if ok && existing.version >= version {
		return false
	}
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: version, order: order})' \
  '	} else if ok && existing.version >= version {
		return false
	}
	s.putEntry(sh, key, entry{val: stored, expiresAt: expiresAt, version: 1, order: order})' \
  'TestRestoreContinuesTheVersionSequence' ./internal/store/

mutate "Restore overwrites a newer value" \
  internal/store/store.go \
  '	if ok && order != 0 && existing.order != 0 {
		if existing.order >= order {
			return false
		}
	} else if ok && existing.version >= version {
		return false
	}
' \
  '	_ = existing
' \
  'TestRestoreRefusesAnOlderVersion' ./internal/store/

mutate "deletions are not replicated" \
  namespace.go \
  '	err := n.fanOut(ctx, key, nil, 0, tombstone, order, 0, true)' \
  '	_ = order; err := error(nil)' \
  'TestADeletionReplicates' ./tests

mutate "handed-off keys are not adopted by their new owner" \
  handler.go \
  '		if ns.store.Restore(in.Key, in.Value, time.Duration(in.TTLNanos), in.Stamp.Version, in.Order) {' \
  '		if false {' \
  'TestDrainSavesDataWithoutReplication' ./tests

mutate "promotion restarts a key's lifetime" \
  promote.go \
  '		if ns.store.RestoreAt(key, e.Value, e.ExpiresAt, e.Stamp.Version, e.Order) {' \
  '		if ns.store.RestoreAt(key, e.Value, e.ExpiresAt+10_000_000_000, e.Stamp.Version, e.Order) {' \
  'TestAPromotedKeyKeepsItsRemainingTTL$' ./tests

mutate "an adopted hand-off is framed a second time" \
  handler.go \
  '		if ns.store.Restore(in.Key, in.Value, time.Duration(in.TTLNanos), in.Stamp.Version, in.Order) {' \
  '		if ns.store.Restore(in.Key, ns.encode(in.Value), time.Duration(in.TTLNanos), in.Stamp.Version, in.Order) {' \
  'TestACompressedHandOffIsNotFramedTwice$' ./tests

mutate "atomic writes are not replicated" \
  atomic.go \
  '	err = n.fanOut(ctx, key, encoded, ttl, stamp, res.Order, res.Prev, false)' \
  '	err = error(nil)' \
  'TestTouchOutlivesTheOriginalDeadlineAcrossFailover$' ./tests

mutate "a failed synchronous Set skips its invalidation" \
  namespace.go \
  '	err := n.fanOut(ctx, key, encoded, ttl, stamp, written.Order, written.Prev, false)
	n.hint(key, stamp)' \
  '	err := n.fanOut(ctx, key, encoded, ttl, stamp, written.Order, written.Prev, false)
	if err == nil {
		n.hint(key, stamp)
	}' \
  'TestAFailedSyncWriteStillInvalidates/Set$' ./tests

mutate "a failed synchronous Delete skips its invalidation" \
  namespace.go \
  '	err := n.fanOut(ctx, key, nil, 0, tombstone, order, 0, true)
	n.hint(key, tombstone)' \
  '	err := n.fanOut(ctx, key, nil, 0, tombstone, order, 0, true)
	if err == nil {
		n.hint(key, tombstone)
	}' \
  'TestAFailedSyncWriteStillInvalidates/Delete$' ./tests

mutate "a failed synchronous atomic write skips its invalidation" \
  atomic.go \
  '	err = n.fanOut(ctx, key, encoded, ttl, stamp, res.Order, res.Prev, false)
	n.hint(key, stamp)' \
  '	err = n.fanOut(ctx, key, encoded, ttl, stamp, res.Order, res.Prev, false)
	if err == nil {
		n.hint(key, stamp)
	}' \
  'TestAFailedSyncWriteStillInvalidates/Increment$' ./tests

mutate "a restore keeps the first record it reads" \
  persistence.go \
  '			if cur, ok := sc.newest[r.Key]; !ok || r.Newer(cur) {' \
  '			if _, ok := sc.newest[r.Key]; !ok {' \
  'TestAColdStartRestoresWhatWasFlushed$' ./tests

mutate "a promoted replica's order is not carried" \
  promote.go \
  '		if ns.store.RestoreAt(key, e.Value, e.ExpiresAt, e.Stamp.Version, e.Order) {' \
  '		if ns.store.RestoreAt(key, e.Value, e.ExpiresAt, e.Stamp.Version, 0) {' \
  'TestARestoreDoesNotUndoAPromotedWrite$' ./tests

# --- persist.Dir (decision 0029, M14c) ---

mutate "Dir renames before the data is written" \
  persist/dir.go \
  '	lock := d.lockFor(target)
	lock.Lock()
	err = d.fs.Rename(tmp, target)
	lock.Unlock()
	if err != nil {
		_ = d.fs.Remove(tmp)
		return fmt.Errorf("persist: placing %s: %w", name, err)
	}
	if err := d.fs.SyncDir(parent); err != nil {' \
  '	if err := d.fs.SyncDir(parent); err != nil {
		return err
	}
	lock := d.lockFor(target)
	lock.Lock()
	err = d.fs.Rename(tmp, target)
	lock.Unlock()
	if err != nil {
		_ = d.fs.Remove(tmp)
		return fmt.Errorf("persist: placing %s: %w", name, err)
	}
	if err := error(nil); err != nil {' \
  'TestPutIsWriteSyncRenameSyncDir$' ./persist/

mutate "Dir lists a crash's temporary files" \
  persist/dir.go \
  '		if strings.Contains(rel, tempMarker) {
			return
		}' \
  '		if false {
			return
		}' \
  'TestACrashsLeftoversAreInvisibleAndCleanedUp$' ./persist/

mutate "Dir never removes a crash's temporary files" \
  persist/dir.go \
  '	if err := d.removeTemps(); err != nil {
		return nil, err
	}' \
  '' \
  'TestACrashsLeftoversAreInvisibleAndCleanedUp$' ./persist/

mutate "Dir accepts names that escape its root" \
  persist/dir.go \
  '		if part == "" || part == "." || part == ".." {' \
  '		if part == "" {' \
  'TestNamesThatEscapeAreRefused$' ./persist/

mutate "Dir lists files it did not write" \
  persist/dir.go \
  '			if !ok {
				return // not a file this store wrote
			}' \
  '			_ = ok' \
  'TestFilesNotWrittenByTheStoreAreIgnored$' ./persist/

# --- persistence: one object per flush (decision 0030) ---

mutate "a split deletes logs its segments failed to cover" \
  persistence.go \
  '	wg.Wait()
	if failed != nil {
		return false, failed
	}
	for _, name := range logs {' \
  '	wg.Wait()
	for _, name := range logs {' \
  'TestAFailedSplitLosesNothing$' ./tests

mutate "a restore ignores logs not yet split" \
  persistence.go \
  '		logs, err := p.store.List(ctx, logPrefix(p.ns.name))
		if err != nil {
			return nil, fmt.Errorf("peercache: listing the logs of %q: %w", p.ns.name, err)
		}' \
  '		var logs []string' \
  'TestAColdStartRestoresWhatWasFlushed$' ./tests

mutate "a flush writes one object per partition" \
  persistence.go \
  '	name := logName(p.ns.name, maxOrder, p.ns.cache.selfID())
	data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})
	// Counted' \
  '	name := logName(p.ns.name, maxOrder, p.ns.cache.selfID())
	for i := range records {
		_ = p.store.Put(ctx, fmt.Sprintf("%s.%d", name, i), nil)
	}
	data := segment.Encode(&segment.File{Kind: segment.KindSegment, Records: records})
	// Counted' \
  'TestAFlushWritesOneObject$' ./tests

mutate "the persistence flush interval option is ignored" \
  persistence.go \
  '	interval := ns.opts.PersistFlushInterval' \
  '	interval := time.Duration(0)' \
  'TestWithFlushIntervalSetsTheCadence$' ./tests

mutate "a non-positive flush interval is accepted" \
  internal/core/options.go \
  '	if o.PersistFlushInterval < 0 {' \
  '	if false {' \
  'TestANonPositiveFlushIntervalIsRefused$' ./tests

# --- persistence: re-replicating restored keys ---

mutate "a restored key is not replicated again" \
  persistence.go \
  '		p.replicateRestored(key, r)' \
  '' \
  'TestARestoredKeyIsReplicatedAgain$' ./tests

# --- persistence: Close and restores in flight ---

mutate "Close does not cancel restores in flight" \
  persistence.go \
  '	p.rcancel()
	p.rwg.Wait()' \
  '	p.rwg.Wait()' \
  'TestCloseEndsARestoreInFlight$' ./tests

# --- repointed after the dirty map was recycled ---

mutate "a failed flush drops its records" \
  persistence.go \
  '		p.restorePending(records)
' \
  '' \
  'TestAnUnavailableStoreLosesNothing$' ./tests

mutate "a failed flush's records replace newer writes" \
  persistence.go \
  '		if cur, ok := p.pending[r.Key]; ok && !r.Newer(&cur) {
			continue
		} else if ok {' \
  '		if cur, ok := p.pending[r.Key]; false && !r.Newer(&cur) {
			continue
		} else if ok {' \
  'TestAFailedFlushDoesNotUndoAWriteMadeDuringIt$' ./tests

mutate "a pending record is never replaced by a newer write" \
  persistence.go \
  '		if !r.Newer(&cur) {
			// An older write recorded late' \
  '		if true {
			// An older write recorded late' \
  'TestAHotKeyCoalescesToOneRecord$' ./tests

# --- OpenTelemetry bridge, a nested module (decision 0032, M15b) ---

mutate "otel registers counters as gauges" \
  otel/otel.go \
  '		case peercache.CounterMetric:
			c, err := meter.Float64ObservableCounter(' \
  '		case -1:
			c, err := meter.Float64ObservableCounter(' \
  'TestEverySeriesIsBridged$' otel::./

mutate "otel keeps a counter's _total" \
  otel/otel.go \
  '		return strings.TrimSuffix(m.Name, "_total")' \
  '		_ = strings.TrimSuffix; return m.Name' \
  'TestCounterNamesDropTheirTotal$' otel::./

mutate "otel observes without the node attribute" \
  otel/otel.go \
  '				o.ObserveFloat64(c, m.Value, node)
			} else if g, ok := gauges[m.Name]; ok {
				o.ObserveFloat64(g, m.Value, node)' \
  '				_ = node; o.ObserveFloat64(c, m.Value)
			} else if g, ok := gauges[m.Name]; ok {
				o.ObserveFloat64(g, m.Value)' \
  'TestEverySeriesIsBridged$' otel::./

# --- persist/s3, a nested module (decision 0029, M14d) ---

mutate "persist/s3 reads every API error as not found" \
  persist/s3/s3.go \
  '		switch api.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return true
		}' \
  '		return true' \
  'TestOtherErrorsAreNotNotFound$' persist/s3::./

mutate "persist/s3 lists only the first page" \
  persist/s3/s3.go \
  '	for p.HasMorePages() {' \
  '	for first := true; first && p.HasMorePages(); first = false {' \
  'TestTheStoreMeetsTheContract$' persist/s3::./

# --- introspection (decision 0031, M15a) ---

mutate "DescribeKey never names this node the owner" \
  describe.go \
  '				if i == 0 {
					p.Role = "owner"
				}' \
  '' \
  'TestEveryNodeDescribesAKeyTheSameWay$' ./tests

mutate "the debug handler answers any method" \
  describe.go \
  '
	if r.Method != http.MethodGet && r.Method != http.MethodHead {' \
  '
	if false {' \
  'TestTheDebugHandlerServesJSONAndRefusesTheRest$' ./tests
mutate "the dashboard answers any method" \
  describe.go \
  '		if r.Method != http.MethodGet && r.Method != http.MethodHead {' \
  '		if false {' \
  'TestTheDebugHandlerServesJSONAndRefusesTheRest$' ./tests

mutate "the debug handler accepts a key lookup without a key" \
  describe.go \
  '			if namespace == "" || !q.Has("key") {' \
  '			if namespace == "" {' \
  'TestTheDebugHandlerServesJSONAndRefusesTheRest$' ./tests

mutate "an unknown namespace is not a 404" \
  describe.go \
  '			case errors.Is(err, ErrNotFound):
				return nil, http.StatusNotFound, err' \
  '			case false:
				return nil, http.StatusNotFound, err' \
  'TestTheDebugHandlerServesJSONAndRefusesTheRest$' ./tests

mutate "a persistent namespace is described without its persistence" \
  describe.go \
  '	if ns.persister != nil {
		pc := ns.persister.counters()' \
  '	if false {
		pc := ns.persister.counters()' \
  'TestDescribeReportsTheNodeAndItsNamespaces$' ./tests

# --- open question 37, option 2 (found by M16) ---

mutate "a failed sync replication is an opaque error" \
  namespace.go \
  '		n.countDegraded(0, degradedByFailure)
		return fmt.Errorf("%w: %w", ErrUnderReplicated, err)' \
  '		return err' \
  'TestAFailedSyncReplicationReportsTheWriteAsHeld$' ./tests
mutate "a held write forwards no result" \
  atomic.go \
  'return err.Error(), nil' \
  'return "", err' \
  'TestAForwardedHeldWriteKeepsItsResult$' ./tests
mutate "a forwarding node drops the owner report" \
  atomic.go \
  'if err != nil || held == "" {' \
  'if true {' \
  'TestAForwardedHeldWriteKeepsItsResult$' ./tests

# --- open question 49 (found by M16) ---

mutate "catch-up skips the replica pass" \
  catchup.go \
  'return cu.pages(ctx, ns, peer, true)' \
  'return nil' \
  'TestANodeRestartedFasterThanDetectionRecoversWhatItOwns$' ./tests
mutate "the replica pass ignores the request" \
  handler.go \
  '	if in.Replicas {
		return h.catchUpReplicas(ctx, ns, in, limit)
	}
' \
  '' \
  'TestANodeRestartedFasterThanDetectionRecoversWhatItOwns$' ./tests
mutate "the replica pass sends what the asker does not own" \
  handler.go \
  'return err == nil && owner.ID == in.Requester' \
  'return err == nil && owner.ID != ""' \
  'TestTheReplicaPassSendsOnlyWhatTheAskerOwns$' ./tests

mutate "a relayed catch-up copy is filed under its relay" \
  handler.go \
  '			From:     e.From,
' \
  '' \
  'TestARelayedCatchUpCopyNamesItsWriter$' ./tests

# --- open question 50 (found by M16) ---

mutate "replica copies order by the epoch stamp" \
  internal/replication/replica.go \
  '	if a.Order != 0 && b.Order != 0 {
		return a.Order > b.Order
	}
' \
  '' \
  'TestAWriteByAnOwnerWithALowerEpochSurvivesFailover$' ./tests
mutate "non-persistent namespaces issue no order" \
  peercache.go \
  '	order := func() uint64 { return uint64(c.orders.Next()) }' \
  '	var order func() uint64
	if opts.Persistence != nil {
		order = func() uint64 { return uint64(c.orders.Next()) }
	}' \
  'TestAWriteByAnOwnerWithALowerEpochSurvivesFailover$' ./tests

# --- open question 49, part two ---

mutate "the catch-up gate lets a forwarded write through" \
  handler.go \
  '!h.cache.readiness.caughtUp.Load() {' \
  'false {' \
  'TestTheCatchUpGateHoldsForwardedWritesUntilCaughtUp$' ./tests

# --- M16: hand-off backlog ---

mutate "reconciliation drains more orphans than the queue holds" \
  promote.go \
  'c.replicas.DrainUpTo(name, room, func' \
  'c.replicas.DrainUpTo(name, -1, func' \
  'TestAHandOffBacklogLargerThanItsQueueIsNotChurned$' ./tests

# --- open question 51 (found by M16) ---

mutate "an active group is not rechecked for ownership" \
  internal/actor/record.go \
  '} else if r.rt.opts.StillOwns != nil {' \
  '} else if false {' \
  'TestAnActiveGroupStopsServingWhenOwnershipMoves$' ./tests

# --- open question 50: fork detection ---

mutate "a handover is counted as a fork" \
  internal/replication/replica.go \
  'return in.Prev < held.Order && held.Prev < in.Order' \
  'return held.Prev < in.Order' \
  'TestAHandoverIsNotAFork$' ./internal/replication
mutate "a fork is not counted" \
  internal/replication/replica.go \
  'return in.Prev < held.Order && held.Prev < in.Order' \
  'return false' \
  'TestTwoWritersFromOneBaseIsAFork$' ./internal/replication
mutate "a write does not carry what it replaced" \
  replicate.go \
  '		Prev:      w.prev,
' \
  '' \
  'TestAWriteOverAValueItNeverReceivedIsAFork$' ./tests
mutate "a rolling upgrade's handovers are counted as forks" \
  internal/replication/replica.go \
  'return in.Prev < held.Order && held.Prev < in.Order' \
  'return held.Prev < in.Order' \
  'TestAGracefulRollingUpgradeCountsNoFork$' ./tests

# --- the L1 tier bounded in bytes ---

mutate "the L1 bound counts entries, not bytes" \
  internal/l1/l1.go \
  '		Cost(cost).
' \
  '' \
  'TestMaxBytesIsBounded$' ./internal/l1
mutate "a namespace's L1 bound never reaches the tier" \
  peercache.go \
  '			MaxBytes: opts.L1MaxBytes,
' \
  '' \
  'TestTheL1TierIsBoundedInBytes$' ./tests
mutate "an L1 bound of zero is accepted" \
  internal/core/options.go \
  '	if o.L1MaxBytes <= 0 {' \
  '	if o.L1MaxBytes < 0 {' \
  'TestAnL1BoundMustBePositive$' ./tests
# --- negative caching (decision 0034) ---

mutate "a remembered absence is never consulted" \
  namespace.go \
  'if n.opts.NegativeTTL <= 0 || !n.store.KnownAbsent(key) {' \
  'if true {' \
  'TestARememberedAbsenceSavesTheLoad$' ./tests
mutate "every loader error is remembered as an absence" \
  namespace.go \
  'if n.opts.NegativeTTL > 0 && errors.Is(err, ErrNotFound) {' \
  'if n.opts.NegativeTTL > 0 {' \
  'TestOnlyAnAbsenceIsRemembered$' ./tests
mutate "a Delete does not forget an absence" \
  internal/store/store.go \
  '	sh.forget(key)
	order := s.nextOrder()' \
  '	order := s.nextOrder()' \
  'TestEveryWriteForgetsAnAbsence$' ./tests
mutate "a Set does not forget an absence" \
  internal/store/store.go \
  '	sh.forget(key)
	old, had := sh.entries[key]
	version := old.version + 1' \
  '	old, had := sh.entries[key]
	version := old.version + 1' \
  'TestEveryWriteForgetsAnAbsence$' ./tests
mutate "an atomic update does not forget an absence" \
  internal/store/update.go \
  '	sh.forget(key)
' \
  '' \
  'TestEveryWriteForgetsAnAbsence$' ./tests
mutate "an absence is recorded over a write that landed during the load" \
  internal/store/absent.go \
  'if e, ok := sh.entries[key]; ok && (e.expiresAt == 0 || now.UnixNano() < e.expiresAt) {' \
  'if e, ok := sh.entries[key]; false && ok && (e.expiresAt == 0 || now.UnixNano() < e.expiresAt) {' \
  'TestAWriteDuringTheLoadIsNotHiddenByItsAbsence$' ./tests
mutate "absences grow without bound" \
  internal/store/absent.go \
  '				delete(sh.absent, k)
				break' \
  '				_ = k
				break' \
  'TestAbsencesAreBoundedPerShard$' ./tests
# --- batch loads (decision 0035) ---

mutate "an owner does not load the keys a batch is missing" \
  handler.go \
  '			if in.Load {
				missing = append(missing, key)
			}' \
  '' \
  'TestABatchLoadCostsOneLoaderCallPerOwner$' ./tests
mutate "the batch loader is never called" \
  batchload.go \
  '	if n.opts.BatchLoader == nil {' \
  '	if true {' \
  'TestABatchLoadCostsOneLoaderCallPerOwner$' ./tests
mutate "a load request goes out without load" \
  internal/wire/bounded.go \
  '	return enc.EncodeBool(r.Load)' \
  '	return enc.EncodeBool(false)' \
  'TestABatchLoadCostsOneLoaderCallPerOwner$' ./tests
mutate "a batch load stores over a write that landed during it" \
  batchload.go \
  '		resident, stored := n.store.SetIfAbsent(key, n.encode(val), ttl)' \
  '		n.store.Set(key, n.encode(val), ttl)
		resident, stored := []byte(nil), true' \
  'TestAWriteDuringTheBatchLoadWins$' ./tests
mutate "a key the batch loader omitted is stored empty" \
  batchload.go \
  '		val, ok := vals[key]
		if !ok {' \
  '		val, ok := vals[key]
		if ok = true; !ok {' \
  'TestKeysTheBatchLoaderOmitsAreAbsent$' ./tests
mutate "a failed batch load is reported as success" \
  batchload.go \
  '		if r.err != nil {' \
  '		if false && r.err != nil {' \
  'TestAFailingOwnersShareIsMissing$' ./tests
mutate "a misrouted key in a load looks like a missing one" \
  handler.go \
  '			if in.Load {
				out.Misrouted = append(out.Misrouted, key)' \
  '			if false {
				out.Misrouted = append(out.Misrouted, key)' \
  'TestAMisroutedLoadNamesTheKeys$' ./tests
mutate "a batch load asks for keys remembered as absent" \
  batchload.go \
  '			if !n.knownAbsent(key) {' \
  '			if true {' \
  'TestABatchLoadRemembersAndSkipsAbsences$' ./tests
mutate "a batch load does not remember the keys it omitted" \
  batchload.go \
  '				n.store.MarkAbsent(key, n.opts.NegativeTTL)' \
  '				_ = key' \
  'TestABatchLoadRemembersAndSkipsAbsences$' ./tests
mutate "a batch get from the earlier release cannot be read" \
  internal/wire/bounded.go \
  '	if n >= 4 {
		if r.Load, err = dec.DecodeBool(); err != nil {' \
  '	{
		if r.Load, err = dec.DecodeBool(); err != nil {' \
  'TestABatchLoadRequestCrossesARollingUpgrade$' ./internal/wire

# --- refresh-ahead (decision 0036) ---

mutate "a hit inside the window does not refresh" \
  namespace.go \
  '			n.maybeRefresh(key, version, left)
' \
  '			_, _ = version, left
' \
  'TestARefreshReloadsAKeyNearItsExpiry$' ./tests
mutate "every hit refreshes, whatever its remaining lifetime" \
  refresh.go \
  '	if left == 0 || left >= n.opts.RefreshAhead {' \
  '	if left == 0 {' \
  'TestARefreshReloadsAKeyNearItsExpiry$' ./tests
mutate "each hit in the window starts its own refresh" \
  refresh.go \
  '	if _, busy := n.refreshing.LoadOrStore(key, struct{}{}); busy {' \
  '	if _, busy := n.refreshing.LoadOrStore(key, struct{}{}); false && busy {' \
  'TestConcurrentHitsStartOneRefresh$' ./tests
mutate "a refresh stores over a write that landed during it" \
  internal/store/refresh.go \
  '	if !had || old.version != version || (old.expiresAt != 0 && now.UnixNano() >= old.expiresAt) {' \
  '	if !had {' \
  'TestAWriteDuringARefreshWins$' ./tests
mutate "Close does not wait for a refresh in flight" \
  refresh.go \
  '	n.refreshWG.Wait()
' \
  '' \
  'TestCloseWaitsForARefresh$' ./tests
mutate "refresh-ahead is accepted without a loader" \
  internal/core/options.go \
  '	if o.RefreshAhead > 0 && o.Loader == nil && o.TypedLoader == nil {' \
  '	if false {' \
  'TestRefreshAheadNeedsALoader$' ./tests
# --- TTL jitter (decision 0037) ---

mutate "a namespace's jitter never reaches its store" \
  peercache.go \
  '		TTLJitter:     opts.TTLJitter,
' \
  '' \
  'TestTTLJitterSpreadsAWarmUp$' ./tests
mutate "the store ignores its jitter" \
  internal/store/store.go \
  '	if s.jitter > 0 {' \
  '	if false {' \
  'TestJitterShortensByAnAmountFixedPerKey$' ./internal/store
mutate "jitter lengthens a TTL" \
  internal/store/store.go \
  '		ttl -= time.Duration(float64(ttl) * s.jitter * jitterFraction(key))' \
  '		ttl += time.Duration(float64(ttl) * s.jitter * jitterFraction(key))' \
  'TestJitterShortensByAnAmountFixedPerKey$' ./internal/store
mutate "jitter is drawn afresh on every write" \
  internal/store/store.go \
  '	h := xxhash.Sum64String(key) * 0x9E3779B97F4A7C15' \
  '	h := uint64(time.Now().UnixNano()) * 0x9E3779B97F4A7C15' \
  'TestJitterShortensByAnAmountFixedPerKey$' ./internal/store
mutate "an atomic update's TTL is not jittered" \
  internal/store/update.go \
  '		expiresAt = s.deadline(key, now, m.TTL)' \
  '		expiresAt = deadlineNanos(now, m.TTL)' \
  'TestTTLJitterSpreadsAWarmUp$' ./tests
mutate "a restored deadline is jittered again" \
  internal/store/store.go \
  '		expiresAt = deadlineNanos(s.clk.Now(), ttl)' \
  '		expiresAt = s.deadline(key, s.clk.Now(), ttl)' \
  'TestRestoreKeepsTheDeadlineItWasGiven$' ./internal/store
mutate "a jitter of one is accepted" \
  internal/core/options.go \
  '	if o.TTLJitter < 0 || o.TTLJitter >= 1 || o.TTLJitter != o.TTLJitter {' \
  '	if o.TTLJitter < 0 || o.TTLJitter != o.TTLJitter {' \
  'TestTTLJitterMustBeAFraction$' ./tests
# --- catch-up rate ---

mutate "catch-up ignores its rate" \
  catchup.go \
  '		if rate := c.opts.CatchUpRate; rate > 0 && len(resp.Entries) > 0 {' \
  '		if rate := c.opts.CatchUpRate; false && rate > 0 {' \
  'TestCatchUpPullsAtTheConfiguredRate$' ./tests
mutate "a negative catch-up rate is accepted" \
  internal/core/options.go \
  '	if o.CatchUpRate < 0 {' \
  '	if false {' \
  'TestCatchUpRateMustNotBeNegative$' ./tests
# --- peercachetest ---

mutate "Kill does not wait out failure detection" \
  peercachetest/cluster.go \
  '	c.net.Unregister(c.infos[i].ID)
	c.clk.Advance(2 * c.mcfg.FailureTimeout)' \
  '	c.net.Unregister(c.infos[i].ID)' \
  'TestAKilledOwnersKeyIsServedByItsReplica$' ./peercachetest
mutate "Restart does not rejoin the membership" \
  peercachetest/cluster.go \
  '	if err := c.members[i].Join(nil); err != nil {' \
  '	if err := error(nil); err != nil {' \
  'TestADrainedNodeRestartsIntoTheCluster$' ./peercachetest
mutate "Heal leaves the partition in place" \
  peercachetest/cluster.go \
  '	c.net.Heal()
' \
  '' \
  'TestAPartitionSplitsTheMembershipAndHealsBack$' ./peercachetest
mutate "a partition is not applied to the network" \
  peercachetest/cluster.go \
  '	c.net.Partition(ids...)
' \
  '' \
  'TestAPartitionSplitsTheMembershipAndHealsBack$' ./peercachetest
mutate "a restarted node loses its own options" \
  peercachetest/cluster.go \
  '	opts := append(append(base, c.cfg.all...), c.cfg.perNode[i]...)' \
  '	opts := append(base, c.cfg.all...)' \
  'TestNodeOptionsReachOneNode$' ./peercachetest

# --- a non-owned copy does not outlive its key (F10) ---

mutate "an L1 copy ignores its key's remaining lifetime" \
  namespace.go \
  '	if left > 0 && left < bound {' \
  '	if false {' \
  'TestANonOwnedCopyDoesNotOutliveItsKey$' ./tests
mutate "the owner's Get grants no remaining lifetime" \
  handler.go \
  '		TTLRemainingNanos: int64(left),
	})' \
  '		TTLRemainingNanos: int64(left) * 0,
	})' \
  'TestANonOwnedCopyDoesNotOutliveItsKey$' ./tests
mutate "the owner's load grants no remaining lifetime" \
  handler.go \
  '	left, _ := ns.store.Remaining(in.Key)' \
  '	var left time.Duration' \
  'TestALoadedCopyDoesNotOutliveItsKey$' ./tests
mutate "a batch entry grants no remaining lifetime" \
  handler.go \
  '			TTLRemainingNanos: int64(left),
		})' \
  '			TTLRemainingNanos: int64(left) * 0,
		})' \
  'TestABatchReadCopyDoesNotOutliveItsKey$' ./tests
# --- tracing (decision 0038) ---

mutate "the serving node does not continue the caller's trace" \
  trace.go \
  '		ctx = c.opts.Tracer.Extract(ctx, carrier)' \
  '		_ = carrier' \
  'TestARemoteReadIsOneTraceAcrossTwoNodes$' ./tests
mutate "the caller does not send its trace context" \
  trace.go \
  '	c.opts.Tracer.Inject(ctx, carrier)
' \
  '' \
  'TestARemoteReadIsOneTraceAcrossTwoNodes$' ./tests
mutate "trace headers are not sent over gRPC" \
  transport/client.go \
  '		ctx = metadata.AppendToOutgoingContext(ctx, metadataTracePrefix+k, v)' \
  '		_, _ = k, v' \
  'TestTraceHeadersCrossTheHop$' ./transport
mutate "trace headers are not read back on the server" \
  transport/server.go \
  '		ctx = core.WithCarrier(ctx, carrier)' \
  '		_ = carrier' \
  'TestTraceHeadersCrossTheHop$' ./transport
mutate "a load runs outside a span" \
  namespace.go \
  '		loadCtx, span := n.cache.traceLoad(loadCtx, n.name, 1)
		val, ttl, err := n.opts.Loader(loadCtx, key)
		span.End(err)' \
  '		val, ttl, err := n.opts.Loader(loadCtx, key)' \
  'TestAForwardedLoadIsTracedOnTheOwner$' ./tests
mutate "the OpenTelemetry adapter propagates nothing" \
  otel/trace.go \
  '	t.p.Inject(ctx, propagation.MapCarrier(carrier))' \
  '	_ = carrier' \
  'TestARemoteGetIsOneOpenTelemetryTrace$' otel::./

# --- cluster view and running digests (decision 0039) ---

mutate "a store delete leaves its digest behind" \
  internal/store/digest.go \
  '		s.digest.Remove(digest.KeyOf(key), old.version)' \
  '		_ = old' \
  'TestTheRunningDigestFollowsEveryWritePath$' ./internal/store
mutate "a store overwrite is counted as a new key" \
  internal/store/digest.go \
  '		s.digest.Replace(k, old.version, e.version)' \
  '		_ = old; s.digest.Add(k, e.version)' \
  'TestTheRunningDigestFollowsEveryWritePath$' ./internal/store
mutate "a replica delete leaves its digest behind" \
  internal/replication/replica.go \
  '		nr.digest.Remove(digest.KeyOf(key), old.Stamp.Version)' \
  '		_ = old' \
  'TestTheReplicaDigestFollowsEveryWritePath$' ./internal/replication
mutate "a replica's missing copies are not reported" \
  cluster.go \
  '		if t.held.Count == t.owned.Count*m && t.held.Sum == t.owned.Sum*m {' \
  '		if true {' \
  'TestAReplicaThatMissedWritesIsReported$' ./tests
mutate "an in-step partition is reported as out of step" \
  cluster.go \
  '		m := uint64(copies - 1)' \
  '		m := uint64(copies)' \
  'TestInStepReplicasMatchTheirOwners$' ./tests
mutate "a cluster with a member unreachable is reported as agreeing" \
  cluster.go \
  '		if n.Description == nil {
			agree = false' \
  '		if n.Description == nil {
			_ = agree' \
  'TestAnUnreachableMemberIsReported$' ./tests
mutate "a silent member holds DescribeCluster forever" \
  cluster.go \
  '			mctx, cancel := clock.WithTimeout(ctx, c.opts.Clock, DescribeTimeout)' \
  '			mctx, cancel := context.WithCancel(ctx); _ = clock.Real' \
  'TestAMemberThatNeverAnswersCostsOnlyTheTimeout$' ./tests
# --- catch-up and hand-off paging, sorted once per pass ---

mutate "every page rebuilds and re-sorts the key list" \
  internal/pager/pager.go \
  '	if after == "" || ps == nil {' \
  '	if true {' \
  'TestAPassSortsOnceAndCoversEveryKey$' ./internal/pager
mutate "a finished pass keeps its key list" \
  internal/pager/pager.go \
  '		if p.passes[id] == ps {
			delete(p.passes, id)
		}' \
  '		_ = ps' \
  'TestAPassSortsOnceAndCoversEveryKey$' ./internal/pager
mutate "passes are not bounded" \
  internal/pager/pager.go \
  '	for len(p.passes) > p.max {' \
  '	for false {' \
  'TestPassesAreBounded$' ./internal/pager
mutate "a page emptied by removals ends the pass" \
  internal/store/pages.go \
  '		if len(page) > 0 || !more {' \
  '		if true {' \
  'TestAPassSkipsKeysRemovedMidPassWithoutStopping$' ./internal/store
mutate "a pass sends keys the asker stopped being entitled to" \
  internal/store/pages.go \
  '			if ok && (e.expiresAt == 0 || now < e.expiresAt) && (want == nil || want(key)) {' \
  '			if ok && (e.expiresAt == 0 || now < e.expiresAt) {' \
  'TestAPassReappliesItsFilter$' ./internal/store

# --- demotion of keys a node no longer owns ---

mutate "a demoted owner keeps its keys in the owned store" \
  promote.go \
  '		p.demote(ns, self)
' \
  '' \
  'TestADemotedOwnerDoesNotResurrectAnOverwrittenValue$' ./tests

mutate "a namespace with a loader is not marked" \
  cluster.go \
  '			loaded[name] = ns.opts.Loader != nil || ns.opts.BatchLoader != nil || ns.opts.TypedLoader != nil' \
  '			loaded[name] = false' \
  'TestLoadedKeysAreMarkedNotMissing$' ./tests
# --- hot keys (decision 0040) ---

mutate "an owner's operations are not counted" \
  namespace.go \
  '		n.hot.Seen(key)' \
  '		_ = key' \
  'TestTheHottestKeyIsReportedFirst$' ./tests
mutate "hot-key counts never decay" \
  internal/hotkeys/hotkeys.go \
  '	for now.Sub(t.decayed) >= Window {' \
  '	for false {' \
  'TestHotKeysDecay$' ./tests
mutate "a new key does not inherit the replaced count" \
  internal/hotkeys/hotkeys.go \
  '	t.counts[key] = minCount + 1' \
  '	t.counts[key] = 1' \
  'TestAKeyThatTurnsHotLateIsFound$' ./internal/hotkeys
mutate "estimates are not scaled by the sample rate" \
  internal/hotkeys/hotkeys.go \
  '		out = append(out, Key{Key: k, Estimate: c * SampleEvery})' \
  '		out = append(out, Key{Key: k, Estimate: c})' \
  'TestTheHottestKeyIsReportedFirst$' ./tests
mutate "hot-key tracking accepts any bound" \
  internal/core/options.go \
  '	if o.HotKeys < 0 || o.HotKeys > MaxHotKeys {' \
  '	if o.HotKeys < 0 {' \
  'TestHotKeysAreOffByDefaultAndBounded$' ./tests

# --- a promoted key keeps its deadline (F12) ---

mutate "a restore past its deadline installs the key"   internal/store/store.go   '	if expiresAt != 0 && s.clk.Now().UnixNano() >= expiresAt {
		return false
	}
	return s.restore(key, val, expiresAt, version, order)'   '	return s.restore(key, val, expiresAt, version, order)'   'TestRestoreAtInstallsTheDeadlineAsGiven$' ./internal/store

# --- a restore does not hold up flushes (F13, open question 55) ---

mutate "a restore holds the flush lock while it reads" \
  persistence.go \
  '	defer p.releaseRestoreSlot()
	// Registered before the read' \
  '	defer p.releaseRestoreSlot()
	p.flushMu.Lock()
	defer p.flushMu.Unlock()
	// Registered before the read' \
  'TestAFlushIsNotHeldUpByARestore$' ./tests
mutate "a restore ignores what was flushed while it read" \
  persistence.go \
  '	for _, b := range p.flushedDuring[mark:] {
		add(b)
	}' \
  '	_ = mark' \
  'TestADeleteFlushedDuringARestoreIsNotUndone$' ./tests
mutate "a flush during a restore keeps nothing for it" \
  persistence.go \
  '		p.flushedDuring = append(p.flushedDuring, batch)
		kept = true' \
  '		_ = batch' \
  'TestADeleteFlushedDuringARestoreIsNotUndone$' ./tests
mutate "a restore ignores the batch being flushed" \
  persistence.go \
  '	add(p.inflight)
' \
  '' \
  'TestADeleteBeingFlushedDuringARestoreIsNotUndone$' ./tests
mutate "a compaction holds up the flusher" \
  persistence.go \
  '			p.compactOwned(ctx)
			cancel()
		case <-split.C():' \
  '			p.flushMu.Lock()
			p.compactOwned(ctx)
			p.flushMu.Unlock()
			cancel()
		case <-split.C():' \
  'TestAFlushIsNotHeldUpByACompaction$' ./tests

# --- local callers are held to what peers are (open question 56) ---

mutate "a drained node applies its own callers' writes" \
  remote.go \
  '	if c.readiness.draining.Load() {
		return fmt.Errorf("peercache: this node is draining and %q has passed to its successor: %w",
			groupKey, ErrNotOwner)
	}
	return c.guard.CanActivateAt(groupKey, epoch)' \
  '	return c.guard.CanActivateAt(groupKey, epoch)' \
  'TestADrainedNodeAppliesNoWrite$' ./tests
mutate "a local caller skips the activation delay" \
  remote.go \
  '	return c.guard.CanActivateAt(groupKey, epoch)
}' \
  '	return nil
}' \
  'TestANewOwnerCountsFromItsPromotedCopy$' ./tests
mutate "a drained node applies a peer's write" \
  handler.go \
  '	if h.cache.readiness.draining.Load() {
		return fmt.Errorf("peercache: this node is draining and %q has passed to its successor: %w",
			groupKey, core.ErrNotOwner)
	}' \
  '' \
  'TestADrainedNodeRefusesAPeerThatMissedTheDeparture$' ./tests

# --- a restore outlasts the splitter, and a failed one does not guess (open question 57) ---

mutate "a restore gives up on a log that moved" \
  persistence.go \
  '			records, err := p.logRecords(ctx, name, part)
			if gone(err) {
				vanished = true
				continue
			}' \
  '			records, err := p.logRecords(ctx, name, part)' \
  'TestARestoreOutlastsASplitterDeletingItsLogs$' ./tests
mutate "an unrestored partition answers absent keys from memory" \
  namespace.go \
  '		if n.store.Exists(key) {
			return nil // memory holds it: degraded, not wrong
		}' \
  '		return nil' \
  'TestAnUnavailableStoreDegradesToMemory$' ./tests
mutate "an unrestored partition takes blind writes" \
  namespace.go \
  '	if err := n.awaitRestore(ctx, key); err != nil {
		return err
	}
	// Again after the wait' \
  '	if err := n.awaitRestore(ctx, key); err != nil && !errors.Is(err, ErrActivationPending) {
		return err
	}
	// Again after the wait' \
  'TestAnUnavailableStoreDegradesToMemory$' ./tests

# --- an owner checks again before it applies (open question 58) ---

mutate "a read-modify-write is applied after its key moved" \
  atomic.go \
  '		if err := n.cache.stillOwner(groupKey); err != nil {
			return store.Mutation{}, false, err
		}' \
  '		_ = groupKey' \
  'TestAWriteWaitingOnARestoreIsRefusedIfOwnershipMoves$' ./tests
mutate "a Set is applied after its key moved" \
  namespace.go \
  '	if err := n.cache.stillOwner(n.groupKeyFor(key)); err != nil {
		return err
	}
	// Framed once.' \
  '	// Framed once.' \
  'TestAWriteWaitingOnARestoreIsRefusedIfOwnershipMoves$' ./tests

# --- two read paths that had no test of their own ---

mutate "a replica-read copy outlives its key" \
  namespace.go \
  '			n.l1.Put(key, val, stamp, l1Lifetime(remaining, left))' \
  '			n.l1.Put(key, val, stamp, l1Lifetime(remaining, 0*left))' \
  'TestAReplicaReadCopyDoesNotOutliveItsKey$' ./tests
mutate "a batch load drops keys that moved" \
  batchload.go \
  '	return n.batchGetOrLoadInto(ctx, misrouted, out, false)' \
  '	return nil' \
  'TestACallerAsksAgainForMisroutedKeys$' ./tests

# --- a fresh node's first alive message carries its RPC address ---

mutate "a fresh node first advertises no RPC address" \
  membership/gossip.go \
  '	g.meta.Store(&nodeMeta{RPCAddr: g.self.Addr, Started: g.started})

	cfg := opts.Config' \
  '	g.meta.Store(&nodeMeta{Started: g.started})

	cfg := opts.Config' \
  'TestTheFirstAliveMessageAdvertisesTheRPCAddress$' ./membership/

# --- partitions restore in parallel ---

mutate "partitions restore one at a time" \
  persistence.go \
  'const restoreParallelism = 8' \
  'const restoreParallelism = 1' \
  'TestPartitionsRestoreInParallel$' ./tests

# --- a restore reads each log once; compaction counts before it reads ---

mutate "every partition reads every log for itself" \
  persistence.go \
  'records, err := p.logRecords(ctx, name, part)' \
  'records, err := p.loadLogPart(ctx, name, part)' \
  'TestARestoreReadsEachLogOnce$' ./tests

mutate "compaction reads a partition before counting its segments" \
  persistence.go \
  '	if listed < threshold {' \
  '	if listed < 0 {' \
  'TestACompactionUnderItsThresholdReadsNothing$' ./tests

# --- asynchronous fan-out runs several keys at once, one key in order ---

mutate "async fan-out sends one copy at a time"   replicate.go   'const replicaWorkers = 8'   'const replicaWorkers = 1'   'TestAsyncFanOutRunsInParallel$' ./tests

mutate "one key's async copies travel on several senders"   replicate.go   'case r.queues[workerFor(w.namespace, w.key, len(r.queues))] <- w:'   'case r.queues[len(w.value)%len(r.queues)] <- w:'   'TestAsyncFanOutKeepsOneKeysWritesInOrder$' ./tests

# --- an idle tick reconciles nothing; an out-of-place copy marks the next one ---

mutate "an idle tick reconciles in full" \
  promote.go \
  'if p.dirty.Swap(false) {' \
  'if true {' \
  'TestAnIdleTickDoesNotReconcile$' ./tests

mutate "a stray replica is left for a membership change that never comes" \
  handler.go \
  '		// next tick must.
		h.cache.promoter.markDirty()' \
  '		// next tick must.' \
  'TestAStrayReplicaIsHandedOnOnAQuietCluster$' ./tests

mutate "a hand-off backlog waits for a membership change"   promote.go   '			// tick drains them as it frees up.
			p.markDirty()'   '			// tick drains them as it frees up.'   'TestAStrayBacklogLargerThanTheHandOffQueueIsHandedOn$' ./tests

mutate "a copy a hand-off puts back waits for a membership change"   handoff.go   '	h.cache.promoter.markDirty()'   ''   'TestACopyAHandOffPutsBackIsTriedAgainOnAQuietCluster$' ./tests

# --- options given to an open namespace are warned about (open question 26) ---

mutate "ignored namespace options are not warned about" \
  ignoredopts.go \
  '	if len(differ) == 0 || !ns.warnedOptions.CompareAndSwap(false, true) {' \
  '	if true || !ns.warnedOptions.CompareAndSwap(false, true) {' \
  'TestOptionsGivenToAnOpenNamespaceAreWarnedAbout$' ./tests
mutate "ignored namespace options are warned about on every call" \
  ignoredopts.go \
  '	if len(differ) == 0 || !ns.warnedOptions.CompareAndSwap(false, true) {' \
  '	if len(differ) == 0 {' \
  'TestIgnoredOptionsAreWarnedAboutOnce$' ./tests
mutate "options compared against defaults, not the open namespace" \
  ignoredopts.go \
  '	asked := ns.opts' \
  '	asked := ns.opts; asked.ReplicationFactor = 1' \
  'TestOptionsGivenToAnOpenNamespaceAreWarnedAbout$' ./tests
mutate "an option field is left out of the comparison" \
  internal/core/optdiff.go \
  '	add(o.L1TTL != asked.L1TTL, "L1TTL")' \
  '' \
  'TestDifferencesCoversEveryField$' ./internal/core/

# --- the memory budget says what it does not bound (open question 55) ---

mutate "a budget without a memory limit is not warned about" \
  peercache.go \
  '	if resolved.MemoryBudget > 0 && debug.SetMemoryLimit(-1) == math.MaxInt64 {' \
  '	if _, _ = debug.SetMemoryLimit, math.MaxInt; false {' \
  'TestABudgetWithoutAMemoryLimitWarns$' ./tests

# --- a write frames its value once ---

mutate "the persister frames a written value again" \
  namespace.go \
  'n.persister.record(key, encoded, written.ExpiresAt, written.Order, false)' \
  'n.persister.record(key, n.encode(val), written.ExpiresAt, written.Order, false)' \
  'TestAWriteIsCompressedOnce$' ./tests

mutate "the fan-out frames a written value again" \
  namespace.go \
  'err := n.fanOut(ctx, key, encoded, ttl, stamp, written.Order, written.Prev, false)' \
  'err := n.fanOut(ctx, key, n.encode(val), ttl, stamp, written.Order, written.Prev, false)' \
  'TestAWriteIsCompressedOnce$' ./tests

# --- invalidation hints: batched, per peer, none without L1 ---
# Hints batch twice — run takes what has queued, a peer's sender what has
# queued for it — so breaking either alone still batches, and no single-line
# mutation of one is caught. TestHintsQueuedTogetherShareMessages fails with
# both gone (one message per hint, as before); the entries cover the rest.

mutate "a namespace without L1 sends hints" \
  invalidate.go \
  '	if n.opts.L1TTL < 0 {' \
  '	if n.opts.L1TTL < -1<<62 {' \
  'TestANamespaceWithoutL1SendsNoHints$' ./tests

mutate "a batched invalidation drops its entries" \
  handler.go \
  '		for _, e := range in.Entries {' \
  '		for _, e := range in.Entries[:0] {' \
  'TestHintsQueuedTogetherShareMessages$' ./tests

# --- a write's copies leave at once ---

mutate "a write sends its copies one after another" \
  replicate.go \
  '		go func(i int, node core.Node) {' \
  '		func(i int, node core.Node) {' \
  'TestASyncWriteSendsItsCopiesAtOnce$' ./tests

# --- a node's ID is read once ---

mutate "a node asks membership for its ID on every use" \
  remote.go \
  'func (c *cache) selfID() string { return c.id }' \
  'func (c *cache) selfID() string { return c.self().ID }' \
  'TestAWriteDoesNotAskMembershipWhoItIs$' ./tests

# --- a dead node may rejoin under its name from a new address ---

mutate "a dead node's name is never reclaimed" \
  membership/gossip.go \
  '	if cfg.DeadNodeReclaimTime == 0 {' \
  '	if false {' \
  'TestADeadNodeRejoinsUnderItsNameFromANewAddress$' ./membership

# --- a backlog of logs is split a batch at a time ---

mutate "a split reads every log at once" \
  persistence.go \
  '	if len(logs) > persistSplitBatch {' \
  '	if false {' \
  'TestABacklogOfLogsIsSplitABatchAtATime$' ./tests

mutate "a split round stops after one batch" \
  persistence.go \
  'logs, more = logs[:persistSplitBatch], true' \
  'logs, more = logs[:persistSplitBatch], false' \
  'TestABacklogOfLogsIsSplitABatchAtATime$' ./tests

# --- the memory budget counts what entries take ---

mutate "an owned entry is charged the old estimate" \
  internal/store/store.go \
  'const EntryOverhead int64 = 160' \
  'const EntryOverhead int64 = 96' \
  'TestAccountedBytesCoverTheLiveHeap$' ./internal/store

mutate "a replica is charged the old estimate" \
  internal/replication/replica.go \
  'const EntryOverhead int64 = 240' \
  'const EntryOverhead int64 = 128' \
  'TestAccountedBytesCoverTheLiveHeap$' ./internal/replication

# --- restored keys fan out from a backlog of their own, dropping none ---

mutate "restored keys share the live replication queue" \
  replicate.go \
  '	if !r.restored.push(k, restoreBacklogMax) {
		r.restoreDropped.Add(1)
	}' \
  '	if w, ok := r.restoredWork(k); ok {
		r.enqueue(w)
	}' \
  'TestARestoreDoesNotCrowdOutLiveReplication$' ./tests

mutate "restored keys are dropped once a queue's worth is waiting" \
  replicate.go \
  'r.restored.push(k, restoreBacklogMax)' \
  'r.restored.push(k, 1024)' \
  'TestEveryRestoredKeyReachesItsReplicasAfterAColdStart$' ./tests

mutate "a restored key is never sent" \
  replicate.go \
  '		if !live || order != k.order {' \
  '		if !live || order == k.order {' \
  'TestEveryRestoredKeyReachesItsReplicasAfterAColdStart$' ./tests

# --- replica expiry samples a large namespace ---

mutate "a large namespace's replicas are swept whole every tick" \
  internal/replication/replica.go \
  '	if held < SweepWholeBelow {' \
  '	if true {' \
  'TestASweepOfALargeNamespaceVisitsAShareOfIt$' ./internal/replication

mutate "a sweep stops at its share however much it finds expired" \
  internal/replication/replica.go \
  '		if visited >= share && found*sweepMoreAbove <= SweepChunk {' \
  '		if visited >= share {' \
  'TestASweepGoesOnWhileItFindsExpiredReplicas$' ./internal/replication

mutate "a sweep that finds expired replicas goes on without bound" \
  internal/replication/replica.go \
  '	share, most := held/SweepShare, held/sweepMostShare' \
  '	share, most := held/SweepShare, held' \
  'TestASweepGoesOnWhileItFindsExpiredReplicas$' ./internal/replication

# --- hand-offs run in parallel ---

mutate "one hand-off at a time" \
  handoff.go \
  'const handoffWorkers = 8' \
  'const handoffWorkers = 1' \
  'TestHandOffsRunInParallel$' ./tests

# --- a dirty tick under a settled view does not scan for promotions ---

mutate "every dirty tick scans every replica for promotion" \
  promote.go \
  '		if full || ns.promotedEpoch.Load() != epoch {' \
  '		if true {' \
  'TestAStrayTickDoesNotScanForPromotions$' ./tests

mutate "a namespace opened late is never scanned on a quiet cluster" \
  promote.go \
  '		if full || ns.promotedEpoch.Load() != epoch {' \
  '		if full {' \
  'TestANamespaceOpenedLateOnAQuietClusterPromotesItsReplicas$' ./tests

echo
echo "caught: $caught   vacuous: $vacuous   skipped: $skipped   broken: $broken"
if [ "$filtered" -ne 0 ]; then
  echo "FILTERED RUN: $filtered entries were not executed (ONLY=$ONLY)."
  echo "this run makes no claim about them."
fi

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "ERROR: mutations were left behind:" >&2
  git status --short >&2
  exit 1
fi
echo "tree clean"

if [ "$vacuous" -ne 0 ] || [ "$skipped" -ne 0 ] || [ "$broken" -ne 0 ]; then
  exit 1
fi
