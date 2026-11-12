package codec_test

import (
	"bytes"
	"testing"

	"github.com/arwenizEr/peercache/internal/codec"
)

type benchValue struct {
	ID     string            `json:"id"`
	Score  int               `json:"score"`
	Tags   []string          `json:"tags"`
	Labels map[string]string `json:"labels"`
}

func sampleValue() benchValue {
	return benchValue{
		ID:     "user:123456",
		Score:  4242,
		Tags:   []string{"eu", "premium", "beta"},
		Labels: map[string]string{"region": "eu-west-1", "tier": "gold"},
	}
}

// BenchmarkMarshal is the comparison that decides which codec a namespace
// should name.
func BenchmarkMarshal(b *testing.B) {
	value := sampleValue()
	for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
		c, err := codec.Get(name)
		if err != nil {
			b.Fatalf("Get(%q): %v", name, err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := c.Marshal(value)
				if err != nil {
					b.Fatalf("Marshal: %v", err)
				}
				if i == 0 {
					b.ReportMetric(float64(len(out)), "bytes")
				}
			}
		})
	}
}

func BenchmarkUnmarshal(b *testing.B) {
	value := sampleValue()
	for _, name := range []string{codec.NameJSON, codec.NameMsgPack} {
		c, err := codec.Get(name)
		if err != nil {
			b.Fatalf("Get(%q): %v", name, err)
		}
		encoded, err := c.Marshal(value)
		if err != nil {
			b.Fatalf("Marshal: %v", err)
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out benchValue
				if err := c.Unmarshal(encoded, &out); err != nil {
					b.Fatalf("Unmarshal: %v", err)
				}
			}
		})
	}
}

// BenchmarkFrame measures what compression costs on the write path, at sizes
// on both sides of a typical threshold.
func BenchmarkFrame(b *testing.B) {
	framer, err := codec.NewFramer(codec.NameLZ4, 128)
	if err != nil {
		b.Fatalf("NewFramer: %v", err)
	}

	payloads := map[string][]byte{
		"64B below threshold": bytes.Repeat([]byte("x"), 64),
		"1KiB compressible":   bytes.Repeat([]byte("peercache-"), 103)[:1024],
		"64KiB compressible":  bytes.Repeat([]byte("peercache-"), 6554)[:65536],
	}
	for name, payload := range payloads {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out := framer.Frame(payload)
				if i == 0 {
					b.ReportMetric(float64(len(out))/float64(len(payload))*100, "%size")
				}
			}
		})
	}
}

// BenchmarkUnframe measures the read path, which is the one that matters:
// compression is written once and read many times, and this is why it is off
// by default.
func BenchmarkUnframe(b *testing.B) {
	framer, err := codec.NewFramer(codec.NameLZ4, 128)
	if err != nil {
		b.Fatalf("NewFramer: %v", err)
	}

	payloads := map[string][]byte{
		"64B stored raw":   bytes.Repeat([]byte("x"), 64),
		"1KiB compressed":  bytes.Repeat([]byte("peercache-"), 103)[:1024],
		"64KiB compressed": bytes.Repeat([]byte("peercache-"), 6554)[:65536],
	}
	for name, payload := range payloads {
		framed := framer.Frame(payload)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := codec.Unframe(framed); err != nil {
					b.Fatalf("Unframe: %v", err)
				}
			}
		})
	}
}
