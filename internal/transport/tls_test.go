package transport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/peercache/peercache/internal/core"
	"github.com/peercache/peercache/internal/transport"
)

// SPEC.md §4 calls mTLS "optional but implemented". Optional is easy to test by
// omission; implemented is not, and an mTLS path that is never exercised is an
// mTLS path that does not work. These tests generate a throwaway CA and two
// node certificates, and complete a real TLS 1.3 handshake over loopback.
//
// The interesting assertion is the negative one: a peer holding a certificate
// from a different CA must be rejected. Without it, "mTLS is on" would be
// indistinguishable from "TLS is on and the client certificate is ignored",
// which is the failure mode that makes mutual authentication theatre.

// certAuthority is a throwaway CA that issues node certificates.
type certAuthority struct {
	cert   *x509.Certificate
	key    *ecdsa.PrivateKey
	pemDER []byte
	dir    string
}

// newCA creates a CA whose files live in the test's temporary directory.
func newCA(t *testing.T, name string) *certAuthority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	ca := &certAuthority{
		cert:   cert,
		key:    key,
		pemDER: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		dir:    t.TempDir(),
	}
	if err := os.WriteFile(ca.caFile(), ca.pemDER, 0o600); err != nil {
		t.Fatalf("writing CA file: %v", err)
	}
	return ca
}

func (ca *certAuthority) caFile() string { return filepath.Join(ca.dir, "ca.pem") }

// issue writes a key pair for one node and returns a TLSConfig using it.
//
// The certificate carries the node's ID as both its common name and a DNS SAN,
// because that is what clientConfig verifies against: identity in this cluster
// is the node ID, not the pod IP, which changes on every restart.
func (ca *certAuthority) issue(t *testing.T, nodeID string) *transport.TLSConfig {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key for %s: %v", nodeID, err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: nodeID},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		DNSNames:    []string{nodeID},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("signing certificate for %s: %v", nodeID, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling key for %s: %v", nodeID, err)
	}

	certPath := filepath.Join(ca.dir, nodeID+".pem")
	keyPath := filepath.Join(ca.dir, nodeID+"-key.pem")
	writeFile(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	writeFile(t, keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))

	return &transport.TLSConfig{
		CertFile: certPath,
		KeyFile:  keyPath,
		CAFile:   ca.caFile(),
	}
}

func writeFile(t *testing.T, path string, body []byte) {
	t.Helper()
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// TestMutualTLSRoundTrip is the positive case: two nodes holding certificates
// from the same CA complete a handshake and exchange a message.
func TestMutualTLSRoundTrip(t *testing.T) {
	ca := newCA(t, "peercache-test-ca")

	a := newPeer(t, "node-a", ca.issue(t, "node-a"))
	b := newPeer(t, "node-b", ca.issue(t, "node-b"))

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	payload := []byte("over TLS")
	got, err := dial(t, a, b).Invoke(ctx, transport.MethodGet, payload)
	if err != nil {
		t.Fatalf("Invoke over mTLS: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("Invoke returned %q, want %q", got, payload)
	}
	if calls, from, _, _, _ := b.handler.snapshot(); calls != 1 || from.ID != "node-a" {
		t.Fatalf("handler saw %d calls from %q, want 1 from node-a", calls, from.ID)
	}
}

// TestMutualTLSRejectsAnImpostorCA is the assertion that makes the positive case
// mean something: it isolates *server-side verification of the client
// certificate*.
//
// Getting this test to test that took a mutation to discover. The first version
// used a rogue CA with its own name, and it passed even with the server
// downgraded to RequireAnyClientCert — because the failure was happening
// somewhere else entirely. In TLS 1.3 the server's CertificateRequest advertises
// the subject names of its ClientCAs, and Go's client will not send a
// certificate that cannot chain to one of them. The handshake died with "client
// didn't provide a certificate" before the server ever had an opinion.
//
// So the impostor CA here carries the *same subject name* as the real one and a
// different key. The client matches on the advertised name and sends its
// certificate; the server then has to reject it on the signature. That is also
// the realistic attack, since a CA's distinguished name is not a secret.
func TestMutualTLSRejectsAnImpostorCA(t *testing.T) {
	const caName = "peercache-test-ca"
	ca := newCA(t, caName)
	rogue := newCA(t, caName) // same name, different key

	b := newPeer(t, "node-b", ca.issue(t, "node-b"))

	// The intruder trusts the real CA — so it will accept the server — but
	// presents a certificate the server cannot verify.
	intruderTLS := rogue.issue(t, "node-a")
	intruderTLS.CAFile = ca.caFile()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	intruder, err := transport.New(transport.Options{
		Self: core.Node{ID: "node-a", Addr: l.Addr().String()},
		TLS:  intruderTLS,
	})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { _ = intruder.Close() })

	c, err := intruder.Dial(b.node)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = c.Invoke(ctx, transport.MethodGet, []byte("let me in"))
	if err == nil {
		t.Fatal("a peer holding a certificate from an impostor CA was served; the server is " +
			"not verifying the client certificate it was sent, which makes mutual TLS " +
			"encryption without authentication")
	}
	if !errors.Is(err, core.ErrNodeUnreachable) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the rejected peer got %v; want a transport-level failure", err)
	}
	if calls, _, _, _, _ := b.handler.snapshot(); calls != 0 {
		t.Fatalf("the handler ran %d times for an unauthenticated peer, want 0", calls)
	}
}

// TestAClientWithNoAcceptableCertificateSendsNone pins the behaviour that made
// the test above vacuous, so that it is documented rather than merely observed.
//
// A client whose certificate chains to a CA the server does not advertise sends
// no certificate at all, and the connection fails on the server's requirement
// that one be present. This is a second, independent barrier — useful, but not
// the same barrier as verification, and not a substitute for it.
func TestAClientWithNoAcceptableCertificateSendsNone(t *testing.T) {
	ca := newCA(t, "peercache-test-ca")
	unrelated := newCA(t, "some-other-org-ca")

	b := newPeer(t, "node-b", ca.issue(t, "node-b"))

	clientTLS := unrelated.issue(t, "node-a")
	clientTLS.CAFile = ca.caFile() // trusts the server, but cannot prove itself

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	tr, err := transport.New(transport.Options{
		Self: core.Node{ID: "node-a", Addr: l.Addr().String()},
		TLS:  clientTLS,
	})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { _ = tr.Close() })

	c, err := tr.Dial(b.node)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := c.Invoke(ctx, transport.MethodGet, []byte("k")); err == nil {
		t.Fatal("a client that never sent a certificate was served")
	}
	if calls, _, _, _, _ := b.handler.snapshot(); calls != 0 {
		t.Fatalf("the handler ran %d times for an unauthenticated peer, want 0", calls)
	}
}

// TestMutualTLSRejectsAnUntrustedServer is the same check in the other
// direction: a client must not talk to a server it cannot verify. Otherwise a
// misrouted or hijacked address would be served data.
func TestMutualTLSRejectsAnUntrustedServer(t *testing.T) {
	ca := newCA(t, "peercache-test-ca")
	rogue := newCA(t, "rogue-ca")

	// The impostor serves with a certificate from a CA the client does not
	// trust, but accepts any client so the failure is the client's decision.
	impostorTLS := rogue.issue(t, "node-b")
	impostorTLS.InsecureSkipVerify = true
	impostor := newPeer(t, "node-b", impostorTLS)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	honest, err := transport.New(transport.Options{
		Self: core.Node{ID: "node-a", Addr: l.Addr().String()},
		TLS:  ca.issue(t, "node-a"),
	})
	if err != nil {
		t.Fatalf("transport.New: %v", err)
	}
	t.Cleanup(func() { _ = honest.Close() })

	c, err := honest.Dial(impostor.node)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := c.Invoke(ctx, transport.MethodGet, []byte("hello")); err == nil {
		t.Fatal("a client accepted a server certificate from an untrusted CA")
	}
}

// TestPlaintextAndTLSDoNotInteroperate pins the boundary between the two modes.
// A cluster half-configured for TLS must fail loudly at the first request
// rather than silently running with some connections unencrypted.
func TestPlaintextAndTLSDoNotInteroperate(t *testing.T) {
	ca := newCA(t, "peercache-test-ca")

	secure := newPeer(t, "node-b", ca.issue(t, "node-b"))
	plain := newPeer(t, "node-a", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := dial(t, plain, secure).Invoke(ctx, transport.MethodGet, []byte("k")); err == nil {
		t.Fatal("a plaintext client reached a TLS server; a half-configured cluster must fail " +
			"loudly rather than run partly unencrypted")
	}
}

// TestTLSFloorIsEnforced proves the MinVersion setting is load-bearing rather
// than decorative.
//
// It is checked with a raw handshake rather than through the transport, because
// two peers built from this package both prefer TLS 1.3 and would negotiate it
// whatever the floor said — lowering MinVersion changes nothing observable
// between them. A client that *cannot* speak 1.3 is the only thing that can
// tell the difference, and it is also the thing the floor exists to turn away.
func TestTLSFloorIsEnforced(t *testing.T) {
	ca := newCA(t, "peercache-test-ca")
	b := newPeer(t, "node-b", ca.issue(t, "node-b"))

	clientTLS := ca.issue(t, "node-a")
	cert, err := tls.LoadX509KeyPair(clientTLS.CertFile, clientTLS.KeyFile)
	if err != nil {
		t.Fatalf("loading the client key pair: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca.pemDER) {
		t.Fatal("the CA bundle contains no certificate")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", b.node.Addr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   "node-b",
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	})
	if err == nil {
		state := conn.ConnectionState()
		_ = conn.Close()
		t.Fatalf("a TLS 1.2 client completed a handshake (negotiated version 0x%04x); the "+
			"1.3 floor is not being enforced", state.Version)
	}
}

func TestTLSConfigValidates(t *testing.T) {
	cases := []struct {
		name string
		cfg  *transport.TLSConfig
		ok   bool
	}{
		{"nil is plaintext", nil, true},
		{"cert without key", &transport.TLSConfig{CertFile: "c.pem"}, false},
		{"key without cert", &transport.TLSConfig{KeyFile: "k.pem"}, false},
		{"no CA and verification on", &transport.TLSConfig{CertFile: "c.pem", KeyFile: "k.pem"}, false},
		{"no CA but verification off", &transport.TLSConfig{
			CertFile: "c.pem", KeyFile: "k.pem", InsecureSkipVerify: true,
		}, true},
		{"complete", &transport.TLSConfig{CertFile: "c.pem", KeyFile: "k.pem", CAFile: "ca.pem"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.ok && err != nil {
				t.Fatalf("Validate() = %v, want nil", err)
			}
			if !tc.ok && !errors.Is(err, core.ErrInvalidOption) {
				t.Fatalf("Validate() = %v, want ErrInvalidOption", err)
			}
		})
	}
}
