package transport

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/keepalive"

	"github.com/peercache/peercache/internal/core"
)

// TLSConfig configures mutual TLS between peers.
//
// SPEC.md §4 (M4) calls mTLS "optional but implemented". Optional means the
// zero value is plaintext, which is the honest default for a single Kubernetes
// cluster where the pod network is already private and a mesh may be handling
// transport security. Implemented means it is real when switched on: both sides
// present a certificate and both sides verify one, because server-only TLS in a
// peer-to-peer system authenticates half of each connection and calls it done.
type TLSConfig struct {
	// CertFile and KeyFile are this node's identity, used both when
	// serving and when dialling. One identity per node, not one per role:
	// every pod is both a client and a server.
	CertFile string
	KeyFile  string
	// CAFile verifies peers. Required — a TLSConfig without it would be
	// encryption without authentication, which is the failure mode this
	// type exists to prevent.
	CAFile string
	// ServerName overrides the name verified against a peer's
	// certificate. Empty verifies against the node's ID, which is what a
	// certificate issued per pod should carry.
	ServerName string
	// InsecureSkipVerify disables peer verification. It exists for tests
	// that generate throwaway certificates and is rejected outside them by
	// nothing at all — which is why it is named the way it is.
	InsecureSkipVerify bool
}

// Validate reports whether the configuration is usable.
func (c *TLSConfig) Validate() error {
	if c == nil {
		return nil
	}
	if c.CertFile == "" || c.KeyFile == "" {
		return fmt.Errorf("transport: TLS needs both CertFile and KeyFile: %w", core.ErrInvalidOption)
	}
	if c.CAFile == "" && !c.InsecureSkipVerify {
		return fmt.Errorf("transport: TLS needs a CAFile to verify peers: %w", core.ErrInvalidOption)
	}
	return nil
}

// serverConfig builds the server side: present our certificate, require and
// verify the client's.
func (c *TLSConfig) serverConfig() (*tls.Config, error) {
	cert, pool, err := c.load()
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
	if c.InsecureSkipVerify {
		// Still ask for a certificate, just do not verify it. Dropping
		// the request entirely would make the test configuration
		// exercise a different handshake from the real one.
		cfg.ClientAuth = tls.RequireAnyClientCert
	}
	return cfg, nil
}

// clientConfig builds the client side: present our certificate, verify the
// server's against the peer's expected name.
func (c *TLSConfig) clientConfig(node core.Node) (*tls.Config, error) {
	cert, pool, err := c.load()
	if err != nil {
		return nil, err
	}
	name := c.ServerName
	if name == "" {
		name = node.ID
	}
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		ServerName:         name,
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // documented, test-only
	}, nil
}

// load reads the key pair and the CA bundle from disk.
func (c *TLSConfig) load() (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("transport: loading key pair: %w", err)
	}
	if c.CAFile == "" {
		return cert, nil, nil
	}
	pem, err := os.ReadFile(c.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("transport: reading CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return tls.Certificate{}, nil, fmt.Errorf("transport: CA file %s contains no certificate: %w",
			c.CAFile, core.ErrInvalidOption)
	}
	return cert, pool, nil
}

// KeepaliveConfig tunes connection health checking.
//
// The defaults assume a pod network, not the internet: failures are detected in
// single-digit seconds because a peer that has gone away is holding up
// ownership decisions, and the cost of an occasional extra ping inside a
// cluster is nothing.
type KeepaliveConfig struct {
	// Time is how long a connection may be idle before a ping is sent.
	Time time.Duration
	// Timeout is how long to wait for the ping's reply before declaring
	// the connection dead.
	Timeout time.Duration
	// MaxConnectionIdle closes a server-side connection that has been idle
	// this long. Zero leaves it open forever, which is correct for a
	// fixed peer set.
	MaxConnectionIdle time.Duration
}

// Default keepalive timings.
const (
	DefaultKeepaliveTime    = 10 * time.Second
	DefaultKeepaliveTimeout = 3 * time.Second
)

func (k KeepaliveConfig) withDefaults() KeepaliveConfig {
	if k.Time == 0 {
		k.Time = DefaultKeepaliveTime
	}
	if k.Timeout == 0 {
		k.Timeout = DefaultKeepaliveTimeout
	}
	return k
}

func (k KeepaliveConfig) clientParams() keepalive.ClientParameters {
	return keepalive.ClientParameters{
		Time:    k.Time,
		Timeout: k.Timeout,
		// Peers gossip continuously, so a connection with no active RPC
		// is normal and still worth keeping warm: the next ownership
		// decision should not pay a handshake.
		PermitWithoutStream: true,
	}
}

func (k KeepaliveConfig) serverParams() keepalive.ServerParameters {
	return keepalive.ServerParameters{
		Time:              k.Time,
		Timeout:           k.Timeout,
		MaxConnectionIdle: k.MaxConnectionIdle,
	}
}

// enforcement stops the server rejecting its own client's keepalive profile.
//
// gRPC servers default to killing clients that ping "too often", where too
// often is 5 minutes — far stricter than the 10-second profile above. Left
// alone, two peers running this code would ping each other into a
// disconnect loop.
func (k KeepaliveConfig) enforcement() keepalive.EnforcementPolicy {
	return keepalive.EnforcementPolicy{
		MinTime:             k.Time / 2,
		PermitWithoutStream: true,
	}
}
