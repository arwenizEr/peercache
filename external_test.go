package peercache_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// externalApp is what docs/api.md tells an application to write to run a
// cluster: a gRPC transport, gossip membership, and the cache wired to both.
const externalApp = `package main

import (
	"context"
	"net"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/membership"
	"github.com/arwenizEr/peercache/transport"
)

func main() {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	tr, err := transport.New(transport.Options{
		Self: peercache.Node{ID: "pod-0", Addr: l.Addr().String()},
	})
	if err != nil {
		panic(err)
	}
	gossip, err := membership.New(membership.Options{
		Self:     peercache.Node{ID: "pod-0", Addr: l.Addr().String()},
		BindAddr: "127.0.0.1:0",
	})
	if err != nil {
		panic(err)
	}
	cache, err := peercache.New(
		peercache.WithNodeID("pod-0"),
		peercache.WithTransport(tr),
		peercache.WithListener(l),
		peercache.WithMembership(gossip),
		peercache.WithSeeds("10.0.0.2:7946"),
		peercache.WithActivationDelay(3*time.Second),
	)
	if err != nil {
		panic(err)
	}
	_ = cache.Close(context.Background())
}
`

// TestAnExternalModuleCanBuildACluster compiles that program as a separate
// module, the way an application depending on this library is compiled.
//
// Every test and example in this repository is inside the module, where Go
// allows importing internal packages. An application is not, and until this
// test existed the gossip membership and the gRPC transport lived under
// internal/ — so the documented way to build a cluster did not compile for
// anyone but this repository, with "use of internal package ... not allowed".
func TestAnExternalModuleCanBuildACluster(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles a module")
	}
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("no go binary on PATH to compile an external module with: %v", err)
	}
	repo, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	sum, err := os.ReadFile(filepath.Join(repo, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	goMod := "module example.com/app\n\ngo 1.25\n\n" +
		"require github.com/arwenizEr/peercache v0.0.0\n\n" +
		"replace github.com/arwenizEr/peercache => " + filepath.ToSlash(repo) + "\n"
	for name, body := range map[string]string{
		"go.mod":  goMod,
		"go.sum":  string(sum),
		"main.go": externalApp,
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(goBin, args...)
		cmd.Dir = dir
		// Offline: every dependency is already in the module cache, because
		// this repository's own build put it there.
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off", "GOWORK=off")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go %s in an external module failed: %v\n%s",
				strings.Join(args, " "), err, out)
		}
	}
	run("mod", "tidy")
	run("build", "-o", filepath.Join(dir, "app"+exeSuffix()), ".")
}

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}
