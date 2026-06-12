package runtime

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/ca"
)

// runtimeServerCertTTL is the lifetime of the runtime's own server certificate
// It is minted once at startup and re-minted on restart, so it is long enough to outlive a typical runtime process
const runtimeServerCertTTL = 90 * 24 * time.Hour

// resolveBootstrap validates that exactly one host bootstrap method is configured and builds the PSK validator when that method is selected
// The JWT validator is built later in Run so its refresh goroutine is bound to the run context
func resolveBootstrap(options *runtimeOptions) (method string, psk *bootstrapauth.PSK, err error) {
	hasPSK := len(options.hostBootstrapPSK) > 0
	hasJWT := options.hostBootstrapJWT != nil
	switch {
	case hasPSK && hasJWT:
		return "", nil, errors.New("only one host bootstrap method may be configured")
	case hasPSK:
		psk, err = bootstrapauth.NewPSK(options.hostBootstrapPSK)
		if err != nil {
			return "", nil, fmt.Errorf("invalid host bootstrap PSK: %w", err)
		}
		return bootstrapauth.MethodPSK, psk, nil
	case hasJWT:
		return bootstrapauth.MethodJWT, nil, nil
	default:
		return "", nil, errors.New("a host bootstrap method is required: configure either a host PSK or JWT")
	}
}

// CABundlePEM derives the cluster CA bundle from the given runtime PSKs and returns the PEM-encoded certificates
// It backs the print-ca subcommand so operators can pin the CA out-of-band before hosts connect
func CABundlePEM(psks ...[]byte) ([][]byte, error) {
	cas, err := ca.CABundle(psks)
	if err != nil {
		return nil, err
	}

	out := make([][]byte, len(cas))
	for i, c := range cas {
		out[i] = c.CertPEM()
	}
	return out, nil
}

// mintServerCert generates a key pair for the runtime server and signs a workload certificate for it with the primary CA
func mintServerCert(primary *ca.CA, runtimeID string) (tls.Certificate, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate runtime key: %w", err)
	}

	der, err := primary.IssueWorkloadCert(ca.RuntimeURI(runtimeID), pub, runtimeServerCertTTL)
	if err != nil {
		return tls.Certificate{}, err
	}

	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}, nil
}

// issueWorkloadCert signs a workload certificate for a host with the given public key from the primary CA
func (rt *Runtime) issueWorkloadCert(hostID string, pub []byte) (der []byte, notAfter time.Time, err error) {
	if len(pub) == 0 {
		return nil, time.Time{}, errors.New("workload public key is required")
	}

	// The reported expiry mirrors the template the CA uses, so the host can schedule renewal off it
	notAfter = time.Now().Add(rt.workloadCertTTL)
	der, err = rt.cas[0].IssueWorkloadCert(ca.HostURI(hostID), ed25519.PublicKey(pub), rt.workloadCertTTL)
	if err != nil {
		return nil, time.Time{}, err
	}
	return der, notAfter, nil
}

// caBundlePEM returns the PEM-encoded trust anchors, carrying more than one entry during a root rotation
func (rt *Runtime) caBundlePEM() [][]byte {
	out := make([][]byte, len(rt.cas))
	for i, c := range rt.cas {
		out[i] = c.CertPEM()
	}
	return out
}

// verifyHostClientCert validates the client certificate presented on an mTLS reconnect and returns the host ID from its SPIFFE identity
func (rt *Runtime) verifyHostClientCert(c *hostConn) (string, error) {
	state := c.session.SessionState().ConnectionState.TLS
	if len(state.PeerCertificates) == 0 {
		return "", errors.New("no client certificate presented")
	}

	// Verify the leaf chains to one of the trust anchors, which may include the old CA during a root rotation
	leaf := state.PeerCertificates[0]
	pool := ca.NewCertPool(rt.cas)
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	if err != nil {
		return "", fmt.Errorf("client certificate verification failed: %w", err)
	}

	// The identity must be a host, and we return the host ID portion to drive reattachment
	id, err := ca.SPIFFEIDFromCert(leaf)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(id.Path, ca.HostPrefix) {
		return "", fmt.Errorf("client identity %q is not a host", id.String())
	}
	return strings.TrimPrefix(id.Path, ca.HostPrefix), nil
}
