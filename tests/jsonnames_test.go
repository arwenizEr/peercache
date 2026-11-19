package tests

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unicode"

	peercache "github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/internal/debugui"
)

// TestDebugHandlerJSONNamesAreCamelCase keeps the debug handler's responses
// spelled one way.
//
// The handler serialises Stats, Description, ClusterDescription and
// KeyPlacement as they are. A field without a json tag goes out under its Go
// name, so one forgotten tag puts a PascalCase key beside camelCase ones — and
// once a release ships it, renaming the key breaks whoever reads it. ReadyState
// and then Stats each shipped that way before anyone noticed.
func TestDebugHandlerJSONNamesAreCamelCase(t *testing.T) {
	roots := []reflect.Type{
		reflect.TypeOf(peercache.Stats{}),
		reflect.TypeOf(peercache.Description{}),
		reflect.TypeOf(peercache.ClusterDescription{}),
		reflect.TypeOf(peercache.KeyPlacement{}),
		reflect.TypeOf(peercache.KeyEvent{}),
	}
	seen := map[reflect.Type]bool{}
	for _, root := range roots {
		checkJSONNames(t, root, root.Name(), seen)
	}
}

func checkJSONNames(t *testing.T, typ reflect.Type, path string, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice ||
		typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || seen[typ] {
		return
	}
	// A type that marshals itself chooses its own encoding.
	if typ.Implements(jsonMarshaler) || reflect.PointerTo(typ).Implements(jsonMarshaler) {
		return
	}
	// Standard-library structs such as time.Time encode themselves.
	if typ.PkgPath() != "" && !strings.HasPrefix(typ.PkgPath(), "github.com/arwenizEr/peercache") {
		return
	}
	seen[typ] = true

	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		where := path + "." + f.Name
		tag, ok := f.Tag.Lookup("json")
		if !ok {
			t.Errorf("%s (%s) has no json tag, so it is encoded as %q", where, typ, f.Name)
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if name == "" || !unicode.IsLower(rune(name[0])) || strings.ContainsAny(name, "_-") ||
			(len(name) > 1 && unicode.IsUpper(rune(name[1]))) {
			t.Errorf("%s (%s) is encoded as %q, want a camelCase name", where, typ, name)
		}
		checkJSONNames(t, f.Type, where, seen)
	}
}

var jsonMarshaler = reflect.TypeOf((*interface{ MarshalJSON() ([]byte, error) })(nil)).Elem()

// TestDashboardTilesNameStatsFields keeps the dashboard reading fields that
// exist. It skips a tile whose key is missing from /stats rather than failing,
// so a renamed field would quietly take its tile off the page.
func TestDashboardTilesNameStatsFields(t *testing.T) {
	names := map[string]bool{}
	st := reflect.TypeOf(peercache.Stats{})
	for i := 0; i < st.NumField(); i++ {
		name, _, _ := strings.Cut(st.Field(i).Tag.Get("json"), ",")
		names[name] = true
	}

	page := string(debugui.Page)
	start := strings.Index(page, "const TILES = [")
	if start < 0 {
		t.Fatal("dashboard has no TILES table")
	}
	end := strings.Index(page[start:], "];")
	keys := regexp.MustCompile(`\["([^"]+)",`).FindAllStringSubmatch(page[start:start+end], -1)
	if len(keys) == 0 {
		t.Fatal("TILES table names no fields")
	}
	for _, k := range keys {
		if !names[k[1]] {
			t.Errorf("dashboard tile reads %q, which /stats does not have", k[1])
		}
	}
}
