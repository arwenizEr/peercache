package persist_test

import (
	"testing"

	"github.com/arwenizEr/peercache/persist"
	"github.com/arwenizEr/peercache/persist/persisttest"
)

func TestMemoryMeetsTheContract(t *testing.T) {
	persisttest.Run(t, func(*testing.T) persist.BlobStore { return persist.Memory() })
}
