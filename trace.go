package peercache

import (
	"context"
	"strconv"

	"github.com/arwenizEr/peercache/internal/core"
)

// tracing reports whether a real tracer is set, so the default path builds no
// attributes and no carrier.
func (c *cache) tracing() bool {
	_, nop := c.opts.Tracer.(core.NopTracer)
	return !nop
}

// traceRPC starts the calling node's span around one peer request and attaches
// its context to ctx for the transport to send.
func (c *cache) traceRPC(ctx context.Context, method string, peer core.Node) (context.Context, core.Span) {
	if !c.tracing() {
		return core.NopTracer{}.Start(ctx, "")
	}
	ctx, span := c.opts.Tracer.Start(ctx, "peercache.rpc "+method,
		core.Attr{Key: "peercache.peer", Value: peer.ID},
		core.Attr{Key: "peercache.node", Value: c.self().ID})
	carrier := make(map[string]string, 2)
	c.opts.Tracer.Inject(ctx, carrier)
	if len(carrier) > 0 {
		ctx = core.WithCarrier(ctx, carrier)
	}
	return ctx, span
}

// traceServe continues the caller's trace, if its request carried one, and
// starts the serving node's span around handling it.
func (c *cache) traceServe(ctx context.Context, method string, from core.Node) (context.Context, core.Span) {
	if !c.tracing() {
		return core.NopTracer{}.Start(ctx, "")
	}
	if carrier := core.CarrierFrom(ctx); len(carrier) > 0 {
		ctx = c.opts.Tracer.Extract(ctx, carrier)
	}
	return c.opts.Tracer.Start(ctx, "peercache.serve "+method,
		core.Attr{Key: "peercache.peer", Value: from.ID},
		core.Attr{Key: "peercache.node", Value: c.self().ID})
}

// traceLoad starts the span around one loader call of n keys.
func (c *cache) traceLoad(ctx context.Context, namespace string, n int) (context.Context, core.Span) {
	if !c.tracing() {
		return core.NopTracer{}.Start(ctx, "")
	}
	return c.opts.Tracer.Start(ctx, "peercache.load",
		core.Attr{Key: "peercache.namespace", Value: namespace},
		core.Attr{Key: "peercache.keys", Value: strconv.Itoa(n)},
		core.Attr{Key: "peercache.node", Value: c.self().ID})
}
