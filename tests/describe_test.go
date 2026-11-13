package tests

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arwenizEr/peercache"
	"github.com/arwenizEr/peercache/persist"
)

// Introspection, decision 0031.

func TestDescribeReportsTheNodeAndItsNamespaces(t *testing.T) {
	c, _ := newCache(t)
	ctx := context.Background()
	_ = c.Namespace("b-plain", peercache.WithL1TTL(3*time.Second)).Set(ctx, "k", []byte("v"), 0)
	_ = c.Namespace("a-durable", peercache.WithPersistence(persist.Memory(),
		persist.WithFlushInterval(time.Second))).Set(ctx, "k", []byte("v"), 0)
	c.Namespace("c-broken", peercache.WithReplicationFactor(9))

	d := c.Describe()
	if d.Node.ID != "n0" || len(d.Members) != 1 || d.Members[0].ID != "n0" {
		t.Fatalf("node %+v, members %+v; a node alone is its own view", d.Node, d.Members)
	}
	if len(d.Namespaces) != 3 || d.Namespaces[0].Name != "a-durable" || d.Namespaces[2].Name != "c-broken" {
		t.Fatalf("namespaces %+v; want all three, sorted by name", d.Namespaces)
	}
	durable, plain, broken := d.Namespaces[0], d.Namespaces[1], d.Namespaces[2]
	if durable.Persistence == nil || durable.Persistence.FlushInterval != time.Second || durable.Persistence.Pending != 1 {
		t.Fatalf("persistent namespace described as %+v / %+v", durable, durable.Persistence)
	}
	if plain.Persistence != nil || plain.L1TTL != 3*time.Second || plain.OwnedKeys != 1 {
		t.Fatalf("plain namespace described as %+v", plain)
	}
	if broken.Error == "" {
		t.Fatalf("a misconfigured namespace carries no error: %+v", broken)
	}
}

// TestEveryNodeDescribesAKeyTheSameWay: owner and holders agree everywhere, and
// each node's role and copies are what it actually holds.
func TestEveryNodeDescribesAKeyTheSameWay(t *testing.T) {
	f := newFabric(t, 3)
	for _, id := range f.ids {
		f.caches[id].Namespace("repl", peercache.WithReplicationFactor(2),
			peercache.WithSyncReplication(), peercache.WithL1TTL(time.Minute))
	}
	const key = "placed"
	owner := f.ownerOf(t, key)
	if err := f.caches[owner].Namespace("repl").Set(context.Background(), key, []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	var reader string
	var first peercache.KeyPlacement
	for i, id := range f.ids {
		p, err := f.caches[id].DescribeKey("repl", key)
		if err != nil {
			t.Fatalf("DescribeKey on %s: %v", id, err)
		}
		if i == 0 {
			first = p
		} else if p.Owner != first.Owner || strings.Join(p.Holders, ",") != strings.Join(first.Holders, ",") {
			t.Fatalf("%s places the key at %s %v, %s at %s %v", id, p.Owner, p.Holders, f.ids[0], first.Owner, first.Holders)
		}
		switch {
		case id == owner:
			if p.Role != "owner" || !p.Owned {
				t.Fatalf("owner %s describes itself as %+v", id, p)
			}
		case id == p.Holders[1]:
			if p.Role != "replica" || !p.ReplicaCopy || p.Owned {
				t.Fatalf("replica %s describes itself as %+v", id, p)
			}
		default:
			reader = id
			if p.Role != "none" || p.Owned || p.ReplicaCopy || p.L1Copy {
				t.Fatalf("bystander %s describes itself as %+v", id, p)
			}
		}
	}
	if first.Owner != owner || len(first.Holders) != 2 {
		t.Fatalf("placement %+v, owner %s", first, owner)
	}

	if _, err := f.caches[reader].Namespace("repl").Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if p, _ := f.caches[reader].DescribeKey("repl", key); !p.L1Copy {
		t.Fatalf("after a remote read %s reports no L1 copy: %+v", reader, p)
	}
}

func TestDescribeKeyOfAnUnopenedNamespaceIsNotFound(t *testing.T) {
	c, _ := newCache(t)
	if _, err := c.DescribeKey("never", "k"); !errors.Is(err, peercache.ErrNotFound) {
		t.Fatalf("DescribeKey of an unopened namespace = %v, want ErrNotFound", err)
	}
}

// TestADescriptionNeverContainsAValue: whatever is described, the value is
// not in it.
func TestADescriptionNeverContainsAValue(t *testing.T) {
	c, _ := newCache(t)
	const secret = "s3cr3t-value-7f1c"
	_ = c.Namespace("vault", peercache.WithPersistence(persist.Memory())).Set(context.Background(), "token", []byte(secret), 0)

	for name, v := range map[string]any{"Describe": c.Describe()} {
		b, _ := json.Marshal(v)
		if strings.Contains(string(b), secret) {
			t.Fatalf("%s's JSON contains the value", name)
		}
	}
	p, err := c.DescribeKey("vault", "token")
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := json.Marshal(p); strings.Contains(string(b), secret) {
		t.Fatal("DescribeKey's JSON contains the value")
	}
	srv := httptest.NewServer(c.DebugHandler())
	defer srv.Close()
	for _, path := range []string{"/", "/namespaces", "/key?namespace=vault&key=token"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if strings.Contains(string(body), secret) {
			t.Fatalf("GET %s served the value", path)
		}
	}
}

func TestTheDebugHandlerServesJSONAndRefusesTheRest(t *testing.T) {
	c, _ := newCache(t)
	c.Namespace("orders")
	srv := httptest.NewServer(c.DebugHandler())
	defer srv.Close()

	cases := []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/", http.StatusOK},
		{http.MethodGet, "/namespaces", http.StatusOK},
		{http.MethodGet, "/key?namespace=orders&key=o:1", http.StatusOK},
		{http.MethodGet, "/key?namespace=orders&key=", http.StatusOK}, // an empty key is a key
		{http.MethodGet, "/key?namespace=orders", http.StatusBadRequest},
		{http.MethodGet, "/key?namespace=never&key=k", http.StatusNotFound},
		{http.MethodGet, "/nowhere", http.StatusNotFound},
		{http.MethodPost, "/", http.StatusMethodNotAllowed},
		{http.MethodDelete, "/key?namespace=orders&key=k", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, srv.URL+tc.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Errorf("%s %s = %d, want %d (%s)", tc.method, tc.path, resp.StatusCode, tc.status, body)
			continue
		}
		if tc.status == http.StatusOK || tc.status == http.StatusBadRequest || tc.status == http.StatusNotFound && strings.HasPrefix(tc.path, "/key") {
			if !json.Valid(body) {
				t.Errorf("%s %s answered %d with a body that is not JSON: %s", tc.method, tc.path, tc.status, body)
			}
		}
	}

	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var d peercache.Description
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil || d.Node.ID != "n0" {
		t.Fatalf("GET / decoded to %+v, %v", d, err)
	}
}
