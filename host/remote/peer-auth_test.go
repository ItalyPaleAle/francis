package remote

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newHostWithPeerAuth builds a remote host with the given extra options, returning the construction error
func newHostWithPeerAuth(extra ...HostOption) (*Host, error) {
	opts := append([]HostOption{
		WithAddress("127.0.0.1:7000"),
		WithRuntimeAddresses("127.0.0.1:9000"),
	}, extra...)
	return NewHost(opts...)
}

func TestRemoteHostPeerAuthValidation(t *testing.T) {
	t.Run("peer auth is required", func(t *testing.T) {
		_, err := newHostWithPeerAuth()
		require.Error(t, err, "a remote host must configure peer authentication")
	})

	t.Run("a valid shared key is accepted", func(t *testing.T) {
		h, err := newHostWithPeerAuth(WithPeerAuthenticationSharedKey("a-sufficiently-long-key"))
		require.NoError(t, err)
		require.NotNil(t, h)
	})

	t.Run("a too-short shared key is rejected", func(t *testing.T) {
		_, err := newHostWithPeerAuth(WithPeerAuthenticationSharedKey("short"))
		require.Error(t, err)
	})

	t.Run("valid mTLS on its own is accepted", func(t *testing.T) {
		ca, leaf := generateMTLSCerts(t)
		h, err := newHostWithPeerAuth(WithPeerAuthenticationMTLS(leaf, ca))
		require.NoError(t, err)
		require.NotNil(t, h)
	})

	t.Run("mTLS cannot be combined with insecure TLS validation", func(t *testing.T) {
		ca, leaf := generateMTLSCerts(t)
		_, err := newHostWithPeerAuth(
			WithPeerAuthenticationMTLS(leaf, ca),
			WithServerTLSInsecureSkipTLSValidation(),
		)
		require.Error(t, err)
	})

	t.Run("mTLS cannot be combined with a server certificate", func(t *testing.T) {
		ca, leaf := generateMTLSCerts(t)
		_, err := newHostWithPeerAuth(
			WithPeerAuthenticationMTLS(leaf, ca),
			WithServerTLSCertificate(leaf),
		)
		require.Error(t, err)
	})

	t.Run("mTLS cannot be combined with a CA certificate", func(t *testing.T) {
		ca, leaf := generateMTLSCerts(t)
		_, err := newHostWithPeerAuth(
			WithPeerAuthenticationMTLS(leaf, ca),
			WithServerTLSCA(ca),
		)
		require.Error(t, err)
	})
}

// generateMTLSCerts returns a CA and a leaf certificate valid for both server and client authentication
func generateMTLSCerts(t *testing.T) (*x509.Certificate, *tls.Certificate) {
	t.Helper()

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
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
