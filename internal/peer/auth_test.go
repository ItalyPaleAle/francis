package peer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"log/slog"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/peerauth"
	"github.com/italypaleale/francis/protocol"
)

// startEchoServer starts a peer server with the given TLS config and authenticator, returning its address
func startEchoServer(t *testing.T, ctx context.Context, tlsConfig *tls.Config, auth Authenticator) string {
	t.Helper()
	addr := freeUDPAddr(t)
	ps := NewServer(ServerConfig{
		Bind:      addr,
		TLSConfig: tlsConfig,
		HostID:    func() string { return "host-b" },
		Handler:   echoHandler,
		Auth:      auth,
		Log:       slog.New(slog.DiscardHandler),
	})
	go func() {
		_ = ps.Run(ctx)
	}()
	return addr
}

// invokeOnce performs a single object invocation against the peer at address
func invokeOnce(ctx context.Context, pc *Client, address string) *protocol.Error {
	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_, perr := pc.InvokeObject(reqCtx, address, protocol.InvokeActorRequest{
		TargetHostID: "host-b",
		ActorType:    "T",
		ActorID:      "a1",
		Method:       "echo",
		Data:         []byte("hi"),
	})
	return perr
}

// waitUntilUp invokes with a valid client until the server accepts the connection, confirming it is serving
func waitUntilUp(t *testing.T, ctx context.Context, pc *Client, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		perr := invokeOnce(ctx, pc, address)
		if perr == nil {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("peer server did not come up: %v", perr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestPeerAuthSharedKey(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)
	auth := &peerauth.PeerAuthenticationSharedKey{
		Key: "correct-shared-key-1234567890",
	}
	addr := startEchoServer(t, ctx, srvTLS, auth)

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)

	// A client with the correct key authenticates at the session upgrade and succeeds
	good := NewClient(ClientConfig{
		TLSConfig: cliTLS,
		Auth:      auth,
		Log:       slog.New(slog.DiscardHandler),
	})
	defer good.Close()
	waitUntilUp(t, ctx, good, addr)

	// A client with the wrong key is rejected at the session upgrade
	wrong := NewClient(ClientConfig{
		TLSConfig: cliTLS,
		Auth:      &peerauth.PeerAuthenticationSharedKey{Key: "wrong-shared-key-0987654321"},
		Log:       slog.New(slog.DiscardHandler),
	})
	defer wrong.Close()
	perr := invokeOnce(ctx, wrong, addr)
	require.NotNil(t, perr, "an invocation with the wrong shared key must be rejected")
}

func TestPeerAuthMTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ca, leaf := generateMTLSCerts(t)

	// Build the server and client TLS configs through the production mTLS path
	mtls := &peerauth.PeerAuthenticationMTLS{
		CA: ca, Certificate: leaf,
	}
	err := mtls.Validate()
	require.NoError(t, err)
	var opts hosttls.HostTLSOptions
	mtls.SetTLSOptions(&opts)
	srvTLS, cliTLS, err := opts.GetTLSConfig()
	require.NoError(t, err)

	addr := startEchoServer(t, ctx, srvTLS, mtls)

	// A client presenting the mTLS client certificate succeeds
	good := NewClient(ClientConfig{
		TLSConfig: cliTLS,
		Auth:      mtls,
		Log:       slog.New(slog.DiscardHandler),
	})
	defer good.Close()
	waitUntilUp(t, ctx, good, addr)

	// A client that trusts the CA but presents no client certificate is rejected at the TLS handshake
	_, noCertTLS, err := hosttls.HostTLSOptions{
		CACertificate: ca,
	}.GetTLSConfig()
	require.NoError(t, err)
	bad := NewClient(ClientConfig{
		TLSConfig: noCertTLS,
		Log:       slog.New(slog.DiscardHandler),
	})
	defer bad.Close()
	perr := invokeOnce(ctx, bad, addr)
	require.NotNil(t, perr, "an invocation without a client certificate must be rejected under mTLS")
}

func TestPeerSessionReuse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	srvTLS, _, err := hosttls.HostTLSOptions{}.GetTLSConfig()
	require.NoError(t, err)
	addr := startEchoServer(t, ctx, srvTLS, nil)

	_, cliTLS, err := hosttls.HostTLSOptions{
		InsecureSkipTLSValidation: true,
	}.GetTLSConfig()
	require.NoError(t, err)
	pc := NewClient(ClientConfig{
		TLSConfig: cliTLS,
		Log:       slog.New(slog.DiscardHandler),
	})
	defer pc.Close()

	// Drive the first call until the server is up, which pools one session
	waitUntilUp(t, ctx, pc, addr)

	// Several more calls to the same peer must reuse the pooled session rather than dialing again
	for range 5 {
		perr := invokeOnce(ctx, pc, addr)
		require.Nil(t, perr)
	}
	assert.Equal(t, uintptr(1), pc.sessions.Len(), "repeated calls to the same peer reuse a single pooled session")
}

// generateMTLSCerts returns a CA certificate and a leaf certificate-and-key suitable for mTLS, with both server and client extended key usages and a localhost SAN
func generateMTLSCerts(t *testing.T) (*x509.Certificate, *tls.Certificate) {
	t.Helper()

	// Certificate authority
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	sub := pkix.Name{Organization: []string{"Test CA"}}
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               sub,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	// Leaf certificate signed by the CA, valid for both server and client authentication
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	leafTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{Organization: []string{"Test Leaf"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTpl, caCert, &leafKey.PublicKey, caKey)
	require.NoError(t, err)

	leaf := &tls.Certificate{
		Certificate: [][]byte{leafDER},
		PrivateKey:  leafKey,
	}
	return caCert, leaf
}
