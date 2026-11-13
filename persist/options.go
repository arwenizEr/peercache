package persist

import "time"

// Option configures how a namespace persists; pass it to
// peercache.WithPersistence.
type Option func(*Config)

// Config is what Options set. The zero value is the default.
type Config struct {
	// FlushInterval is how often owners flush pending writes: the main term
	// of the loss window. Zero means the default, 100 ms; anything below
	// zero is refused when the namespace is opened.
	FlushInterval time.Duration
}

// WithFlushInterval sets how often a namespace's owners flush.
//
// Each flush is one object written per node (decision 0030), so the interval
// is a trade between the loss window and the request rate. A local disk
// suits the default 100 ms. An object store, which charges per request,
// suits a second or more: one PUT a second per node. A non-positive d is
// refused, with ErrInvalidOption, when the namespace is opened.
func WithFlushInterval(d time.Duration) Option {
	return func(c *Config) {
		if d <= 0 {
			d = -1
		}
		c.FlushInterval = d
	}
}
