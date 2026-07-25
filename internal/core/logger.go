package core

// Logger is the logging surface of the library.
//
// CLAUDE.md forbids the log package: a library that writes to a global logger
// takes a decision that belongs to the application embedding it. The default is
// [NopLogger], which discards everything, so an embedding application that
// configures nothing pays nothing.
//
// The kv arguments are alternating keys and values, as in [log/slog]. An odd
// number of arguments is a caller bug; implementations must tolerate it rather
// than panic.
type Logger interface {
	Debug(msg string, kv ...any)
	Info(msg string, kv ...any)
	Warn(msg string, kv ...any)
	Error(msg string, kv ...any)
}

// NopLogger discards every message. It is the default.
type NopLogger struct{}

// Debug implements [Logger].
func (NopLogger) Debug(string, ...any) {}

// Info implements [Logger].
func (NopLogger) Info(string, ...any) {}

// Warn implements [Logger].
func (NopLogger) Warn(string, ...any) {}

// Error implements [Logger].
func (NopLogger) Error(string, ...any) {}

var _ Logger = NopLogger{}
