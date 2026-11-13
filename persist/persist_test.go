package persist_test

import (
	"testing"

	"github.com/arwenizEr/peercache/persist"
	"github.com/arwenizEr/peercache/persist/persisttest"
)

func TestMemoryMeetsTheContract(t *testing.T) {
	persisttest.Run(t, func(*testing.T) persist.BlobStore { return persist.Memory() })
}

func TestDirMeetsTheContract(t *testing.T) {
	persisttest.Run(t, func(t *testing.T) persist.BlobStore {
		s, err := persist.Dir(t.TempDir())
		if err != nil {
			t.Fatalf("Dir: %v", err)
		}
		return s
	})
}
