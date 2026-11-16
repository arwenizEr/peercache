package peercache

import "strings"

// warnIgnoredOptions logs, once per namespace, a call to Namespace whose
// options would have changed an already open namespace. The first call still
// wins, as documented; this only makes the losing call visible. Open question
// 26 records the afternoon it cost when it was silent.
//
// The options are applied to a copy of the namespace's own configuration and
// the copy compared with the original, so only what the call actually sets can
// differ: a call that names a subset of the original options, or none, is
// silent. See [core.NamespaceOptions.Differences] for what counts as a
// difference.
func (c *cache) warnIgnoredOptions(ns *namespace, opts []NamespaceOption) {
	if len(opts) == 0 || ns.warnedOptions.Load() {
		return
	}
	asked := ns.opts
	for _, opt := range opts {
		if opt != nil {
			opt(&asked)
		}
	}
	differ := ns.opts.Differences(&asked)
	if len(differ) == 0 || !ns.warnedOptions.CompareAndSwap(false, true) {
		return
	}
	c.opts.Logger.Warn("namespace options ignored: the namespace is already open",
		"namespace", ns.name, "options", strings.Join(differ, ","))
}
