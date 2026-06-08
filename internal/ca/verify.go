package ca

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// HostPrefix is the SPIFFE path prefix for host identities
const HostPrefix = "/host/"

// RuntimePrefix is the SPIFFE path prefix for runtime identities
const RuntimePrefix = "/runtime/"

// NewCertPool builds an x509.CertPool from a CA bundle, which may contain more than one anchor during a root rotation
func NewCertPool(cas []*CA) *x509.CertPool {
	pool := x509.NewCertPool()
	for _, c := range cas {
		pool.AddCert(c.cert)
	}
	return pool
}

// PoolFromPEM builds an x509.CertPool from PEM-encoded CA certificates, used by hosts to turn the trust bundle the runtime sends, or a pinned bundle, into a trust pool
func PoolFromPEM(pems [][]byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, p := range pems {
		if !pool.AppendCertsFromPEM(p) {
			return nil, errors.New("failed to parse CA certificate PEM")
		}
	}
	return pool, nil
}

// SPIFFEIDFromCert extracts the single spiffe:// URI SAN from a certificate
func SPIFFEIDFromCert(cert *x509.Certificate) (*url.URL, error) {
	var found *url.URL
	for _, u := range cert.URIs {
		if u.Scheme != "spiffe" {
			continue
		}
		if found != nil {
			return nil, errors.New("certificate has multiple SPIFFE URIs")
		}
		found = u
	}
	if found == nil {
		return nil, errors.New("certificate has no SPIFFE URI")
	}
	return found, nil
}

// VerifyPeerSPIFFE returns a tls.Config.VerifyPeerCertificate function that verifies the peer chain against the current trust anchors and asserts the leaf SPIFFE identity
// The roots are read through rootsFn on every handshake so a root rotation can swap the trust bundle without rebuilding any tls.Config
// wantPrefix restricts the identity to a namespace, and expectedID, when it returns a non-empty value, pins the identity to one specific peer chosen by placement
func VerifyPeerSPIFFE(rootsFn func() *x509.CertPool, wantPrefix string, expectedID func() string) func([][]byte, [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("peer presented no certificate")
		}

		// Parse the leaf and any intermediates ourselves because this verifier runs with InsecureSkipVerify, which leaves verifiedChains empty
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("failed to parse peer leaf certificate: %w", err)
		}
		intermediates := x509.NewCertPool()
		for _, raw := range rawCerts[1:] {
			ic, parseErr := x509.ParseCertificate(raw)
			if parseErr != nil {
				return fmt.Errorf("failed to parse peer intermediate certificate: %w", parseErr)
			}
			intermediates.AddCert(ic)
		}

		// Verify the chain against the live trust anchors, which may include both the old and new CA during a root rotation
		opts := x509.VerifyOptions{
			Roots:         rootsFn(),
			Intermediates: intermediates,
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		_, err = leaf.Verify(opts)
		if err != nil {
			return fmt.Errorf("peer certificate verification failed: %w", err)
		}

		// Assert the SPIFFE identity is in the expected namespace
		id, err := SPIFFEIDFromCert(leaf)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(id.Path, wantPrefix) {
			return fmt.Errorf("peer SPIFFE identity %q is not in namespace %q", id.String(), wantPrefix)
		}

		// Optionally pin the identity to the exact peer placement told us to dial, which stops a different valid host from impersonating the target
		if expectedID != nil {
			want := expectedID()
			if want != "" && id.Path != wantPrefix+want {
				return fmt.Errorf("peer SPIFFE identity %q does not match expected %q", id.String(), wantPrefix+want)
			}
		}
		return nil
	}
}
