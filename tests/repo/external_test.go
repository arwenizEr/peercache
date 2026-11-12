package repo

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

	goEnv := func(name string) string {
		out, err := exec.Command(goBin, "env", name).Output()
		if err != nil {
			t.Fatalf("go env %s: %v", name, err)
		}
		return strings.TrimSpace(string(out))
	}
	cacheURL := "file:///" + strings.TrimPrefix(
		filepath.ToSlash(filepath.Join(goEnv("GOMODCACHE"), "cache", "download")), "/")
	proxy := cacheURL
	if upstream := goEnv("GOPROXY"); upstream != "" && upstream != "off" {
		proxy += "," + upstream
	}

	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command(goBin, args...)
		cmd.Dir = dir
		// The local module cache first, then whatever proxy this machine
		// uses. Resolving the external module's graph needs go.mod files
		// that no build in this repository reads — memberlist's go-metrics
		// predates graph pruning, so tidy walks to datadog-go and beyond —
		// and a fresh cache, as on CI, does not have them. This was
		// GOPROXY=off, which passed only where a warm cache hid that.
		cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOWORK=off", "GOPROXY="+proxy)
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
