package core_test

import (
	"reflect"
	"testing"

	"github.com/arwenizEr/peercache/internal/core"
)

// TestDifferencesCoversEveryField sets each field of NamespaceOptions in turn
// and requires Differences to name it, and only it. A field added to the struct
// and forgotten in Differences fails here, which is what the hand-written
// comparison gives up by not using reflection itself.
func TestDifferencesCoversEveryField(t *testing.T) {
	base := core.DefaultNamespaceOptions()
	typ := reflect.TypeOf(base)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		asked := base
		v := reflect.ValueOf(&asked).Elem().Field(i)
		if !setToSomethingElse(v) {
			t.Fatalf("field %s has kind %s, which this test cannot change; teach it", f.Name, v.Kind())
		}
		got := base.Differences(&asked)
		if len(got) != 1 || got[0] != f.Name {
			t.Errorf("changing %s: Differences = %v, want [%s]", f.Name, got, f.Name)
		}
	}
	if got := base.Differences(&base); got != nil {
		t.Errorf("identical options: Differences = %v, want nil", got)
	}
}

// setToSomethingElse changes v to a value different from the default's, or
// reports that it cannot.
func setToSomethingElse(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.Int, reflect.Int64:
		v.SetInt(v.Int() + 7)
	case reflect.Float64:
		v.SetFloat(v.Float() + 0.25)
	case reflect.String:
		v.SetString(v.String() + "x")
	case reflect.Func, reflect.Interface:
		if !v.IsNil() {
			v.SetZero()
			return true
		}
		if v.Kind() == reflect.Func {
			v.Set(reflect.MakeFunc(v.Type(), func([]reflect.Value) []reflect.Value {
				panic("not called")
			}))
			return true
		}
		if !reflect.TypeOf(everything{}).Implements(v.Type()) {
			return false
		}
		v.Set(reflect.ValueOf(everything{}))
	default:
		return false
	}
	return true
}

// everything satisfies every interface-typed option. Its methods are never
// called: Differences only asks whether a field is set.
type everything struct {
	core.Partitioner
	core.BlobStore
	core.TypedLoaderSpec
}
