package repo

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// TestImplementedInterfacesAreFrozen holds decision 0042's third promise: the
// interfaces an application implements and hands to the library keep their
// method sets until v2. A method added to one breaks every implementation of
// it, in code this repository never sees.
//
// The expected sets are written out here. A change to one of them fails this
// test, and the fix is not to update the list: it is an optional interface
// found by type assertion, which leaves existing implementations compiling.
// Updating the list is for a v2.
func TestImplementedInterfacesAreFrozen(t *testing.T) {
	frozen := map[string]struct {
		typ  reflect.Type
		want []string
	}{
		"Transport": {reflect.TypeOf((*peercache.Transport)(nil)).Elem(), []string{
			"Close() error",
			"Dial(core.Node) (core.PeerClient, error)",
			"Serve(net.Listener, core.Handler) error",
		}},
		"PeerClient": {reflect.TypeOf((*peercache.PeerClient)(nil)).Elem(), []string{
			"Close() error",
			"Invoke(context.Context, string, []uint8) ([]uint8, error)",
		}},
		"Handler": {reflect.TypeOf((*peercache.Handler)(nil)).Elem(), []string{
			"Handle(context.Context, core.Node, string, []uint8) ([]uint8, error)",
		}},
		"MembershipAuthority": {reflect.TypeOf((*peercache.MembershipAuthority)(nil)).Elem(), []string{
			"Join([]string) error",
			"Leave(context.Context) error",
			"Members() ([]core.Node, core.Epoch)",
			"Self() core.Node",
			"Watch() <-chan core.MembershipChange",
		}},
		"Logger": {reflect.TypeOf((*peercache.Logger)(nil)).Elem(), []string{
			"Debug(string, ...interface {})",
			"Error(string, ...interface {})",
			"Info(string, ...interface {})",
			"Warn(string, ...interface {})",
		}},
		"Tracer": {reflect.TypeOf((*peercache.Tracer)(nil)).Elem(), []string{
			"Extract(context.Context, map[string]string) context.Context",
			"Inject(context.Context, map[string]string)",
			"Start(context.Context, string, ...core.Attr) (context.Context, core.Span)",
		}},
		"Span": {reflect.TypeOf((*peercache.Span)(nil)).Elem(), []string{
			"End(error)",
		}},
		"Partitioner": {reflect.TypeOf((*peercache.Partitioner)(nil)).Elem(), []string{
			"GroupKey(string) string",
		}},
		"persist.BlobStore": {reflect.TypeOf((*persist.BlobStore)(nil)).Elem(), []string{
			"Delete(context.Context, string) error",
			"Get(context.Context, string) ([]uint8, error)",
			"List(context.Context, string) ([]string, error)",
			"Put(context.Context, string, []uint8) error",
		}},
		"Clock": {reflect.TypeOf((*peercache.Clock)(nil)).Elem(), []string{
			"After(time.Duration) <-chan time.Time",
			"AfterFunc(time.Duration, func()) clock.Timer",
			"NewTicker(time.Duration) clock.Ticker",
			"NewTimer(time.Duration) clock.Timer",
			"Now() time.Time",
			"Since(time.Time) time.Duration",
			"Sleep(context.Context, time.Duration) error",
		}},
		// A Clock's timers and tickers are implemented along with it.
		"Timer": {reflect.TypeOf((*peercache.Timer)(nil)).Elem(), []string{
			"C() <-chan time.Time",
			"Reset(time.Duration) bool",
			"Stop() bool",
		}},
		"Ticker": {reflect.TypeOf((*peercache.Ticker)(nil)).Elem(), []string{
			"C() <-chan time.Time",
			"Reset(time.Duration)",
			"Stop()",
		}},
	}

	for name, f := range frozen {
		got := methodSet(f.typ)
		if strings.Join(got, "\n") != strings.Join(f.want, "\n") {
			t.Errorf("%s's method set changed (decision 0042: add an optional interface instead)\n got:\n\t%s\nwant:\n\t%s",
				name, strings.Join(got, "\n\t"), strings.Join(f.want, "\n\t"))
		}
	}
}

// methodSet renders an interface's methods as sorted signature strings.
func methodSet(typ reflect.Type) []string {
	out := make([]string, 0, typ.NumMethod())
	for i := 0; i < typ.NumMethod(); i++ {
		m := typ.Method(i)
		sig := m.Type.String() // "func(args) results"
		out = append(out, m.Name+strings.TrimPrefix(sig, "func"))
	}
	sort.Strings(out)
	return out
}
