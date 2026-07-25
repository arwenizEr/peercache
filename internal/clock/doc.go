// Package clock is the single source of time for PeerCache.
//
// Nothing else in this repository may call [time.Now], [time.Sleep],
// [time.After], [time.NewTimer], [time.AfterFunc] or [time.NewTicker].
// Every component that needs to observe or wait on time takes a [Clock] at
// construction. Production code is given [Real]; tests are given [NewFake].
//
// This is not stylistic. A distributed cache is mostly timeouts — activation
// delays, idle deactivation, failure detection, lease expiry, retry backoff —
// and a test suite that reaches those code paths by sleeping is both slow and
// nondeterministic. With an injected clock, an hour of simulated time costs
// microseconds and always produces the same interleaving.
//
// # Determinism
//
// [Fake] fires timers in (deadline, creation sequence) order. Two timers due at
// the same instant always fire in the order they were created or last reset, so
// a test that passes once passes every time. While a timer's callback runs, the
// clock reads exactly that timer's deadline, not the final time of the
// [Fake.Advance] call, which is what a real timer callback would observe.
//
// # Synchronising with goroutines
//
// [Fake.Advance] returns as soon as it has delivered to every due timer. It does
// not and cannot know whether the goroutine that was waiting on a timer has made
// progress yet. Tests must not paper over this with a sleep. Instead, use
// [Fake.BlockUntil] to wait until the code under test has registered the timers
// you are about to fire, and use an explicit channel or [sync.WaitGroup] to
// observe the effect afterwards:
//
//	fc := clock.NewFake(clock.Epoch)
//	go worker(fc)          // worker calls fc.After(time.Minute)
//	fc.BlockUntil(1)       // worker is now parked on its timer
//	fc.Advance(time.Minute)
//	<-workerDidTheThing    // explicit signal, never a sleep
package clock
