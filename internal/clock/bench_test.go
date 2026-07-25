package clock_test

import (
	"testing"
	"time"

	"github.com/peercache/peercache/internal/clock"
)

// BenchmarkFakeAdvanceIdleHour is the claim the whole harness rests on: with no
// timers armed, skipping an hour costs nothing. If this ever becomes
// proportional to the duration, every timeout test in the repository silently
// becomes a slow test.
func BenchmarkFakeAdvanceIdleHour(b *testing.B) {
	fc := clock.NewFake(clock.Epoch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fc.Advance(time.Hour)
	}
}

// BenchmarkFakeAdvanceWithArmedTimers measures an advance that fires nothing
// but has to inspect the heap, which is the common case during a test.
func BenchmarkFakeAdvanceWithArmedTimers(b *testing.B) {
	fc := clock.NewFake(clock.Epoch)
	for i := 0; i < 1000; i++ {
		fc.AfterFunc(time.Duration(i+1)*time.Hour, func() {})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fc.Advance(time.Millisecond)
	}
}

// BenchmarkFakeTimerChurn measures arm-then-cancel, the shape of a per-request
// deadline.
func BenchmarkFakeTimerChurn(b *testing.B) {
	fc := clock.NewFake(clock.Epoch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := fc.AfterFunc(time.Minute, func() {})
		t.Stop()
	}
}

// BenchmarkFakeFireTimers measures the firing path itself.
func BenchmarkFakeFireTimers(b *testing.B) {
	const batch = 1000
	fc := clock.NewFake(clock.Epoch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < batch; j++ {
			fc.AfterFunc(time.Duration(j+1)*time.Millisecond, func() {})
		}
		b.StartTimer()
		fc.Advance(2 * batch * time.Millisecond)
	}
}

// BenchmarkRealNow is the baseline the fake is compared against.
func BenchmarkRealNow(b *testing.B) {
	c := clock.Real()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Now()
	}
}

// BenchmarkFakeNow measures the lock the fake takes on every read, which is the
// price of injectability on a hot path.
func BenchmarkFakeNow(b *testing.B) {
	fc := clock.NewFake(clock.Epoch)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fc.Now()
	}
}
