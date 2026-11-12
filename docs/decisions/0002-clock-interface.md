# 0002 — The Clock interface is wider than docs/design.md section 3.6

**Status:** accepted, M0
**Affects:** `internal/clock`, every package that waits

## What docs/design.md specifies

```go
type Clock interface {
    Now() time.Time
    After(d time.Duration) <-chan time.Time
    NewTimer(d time.Duration) Timer
}
```

## What is not enough about it

- **No `AfterFunc`.** M2 gives every activated group its own timers, cancelled
  on deactivation and re-registered on activation. With only channel timers,
  each armed timer needs a goroutine parked on a receive. For 20,000 active
  groups with a few timers each, that is a goroutine per timer for no reason.
- **No `Ticker`.** M3's failure detection, M2's idle sweep and M7's metrics
  export are all periodic.
- **No context-aware wait.** Every wait in this codebase has to be
  interruptible, because CLAUDE.md requires deadlines to propagate into group
  execution. A bare sleep is how a shutdown path outlives its deadline.
- **No `Since`.** Without it, callers reach for `time.Since`, which defeats the
  entire point.

Growing the interface in M2 would churn both implementations and every call
site. Growing it now costs nothing, because there are no call sites yet.

## Decision

```go
type Clock interface {
    Now() time.Time
    Since(t time.Time) time.Duration
    After(d time.Duration) <-chan time.Time
    NewTimer(d time.Duration) Timer
    AfterFunc(d time.Duration, f func()) Timer
    NewTicker(d time.Duration) Ticker
    Sleep(ctx context.Context, d time.Duration) error
}
```

Plus `clock.WithTimeout` and `clock.WithDeadline`, which are contexts whose
deadline is measured on an injected clock. `context.WithTimeout` reads the wall
clock, so a component using it cannot have its deadline behaviour tested without
waiting in real time — a sleep by another name. Deadline propagation is a
correctness requirement here, so it has to be as controllable as every other
timer.

There is deliberately no context-free `Sleep`.

## Fake clock semantics worth pinning

These are the choices that make a test reproducible, and each is covered by a
named test:

- **Ordering is `(deadline, creation sequence)`.** Two timers due at the same
  instant fire in the order they were created or last reset. Without this,
  firing order depends on heap layout and adding an unrelated timer breaks an
  unrelated test.
- **`Now()` inside a callback reads that callback's own deadline**, not the end
  of the advance window. A real timer callback cannot see the future either.
- **A callback may arm a timer inside the same advance**, and it fires if it
  falls in the window.
- **`Advance` is serialised** against other advances, so a sloppy test does not
  become a nondeterministic one.
- **Ticks are dropped, not queued**, matching `time.Ticker`.
- **Misuse is recorded, not panicked.** CLAUDE.md forbids panics in library
  paths. `time.NewTicker(0)` panics; ours records a misuse, returns a ticker
  that never fires, and the harness fails the test at teardown.
- **`Advance` fires at most 2^20 timers** before recording misuse and stopping.
  A callback that re-arms itself inside the window would otherwise hang CI for
  the full test timeout, and a hang is much harder to diagnose than a failure.

## Synchronising without sleeping

`Advance` delivers to every due timer and returns. It cannot know whether the
goroutine waiting on a timer has made progress. The answer is *not* to sleep;
it is `BlockUntil(n)`, which parks until *n* timers are registered:

```go
go worker(fc)          // worker calls fc.After(time.Minute)
fc.BlockUntil(1)       // worker is parked on its timer
fc.Advance(time.Minute)
<-workerDidTheThing    // explicit signal
```

## Reversal

Removing a method later is a breaking change for implementers, but the only
implementers are in this repository and the test harness. Low cost.
