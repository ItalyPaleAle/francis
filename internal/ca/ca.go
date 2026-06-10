package ca

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/url"
	"time"
)

// TrustDomain is the SPIFFE trust domain for all Francis identities
const TrustDomain = "francis"

// clockSkew is how far in the past a freshly issued certificate's NotBefore is set, to tolerate small clock differences between nodes
const clockSkew = 5 * time.Minute

// caHKDFInfo domain-separates the CA key derivation from any other use of the runtime PSK
const caHKDFInfo = "francis-ca-v1"

// caHKDFSalt is a fixed salt for the CA key derivation
// A constant salt keeps the derivation deterministic across every runtime that shares the PSK
var caHKDFSalt = []byte("francis-ca-salt-v1")

// caEpoch is a fixed validity start for the deterministic CA certificate
// Using a constant rather than time.Now keeps the CA certificate byte-identical across runtimes and restarts
var caEpoch = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

// caNotAfter is a fixed, far-future expiry for the deterministic CA certificate
var caNotAfter = time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)

// CA is a deterministic Ed25519 certificate authority derived from a runtime PSK
// Every runtime provisioned with the same PSK derives a byte-identical CA, which lets the small set of runtimes act as one issuer without coordination
type CA struct {
	priv    ed25519.PrivateKey
	cert    *x509.Certificate
	certDER []byte
}

// DeriveCA derives the deterministic CA from a single runtime PSK
func DeriveCA(psk []byte) (*CA, error) {
	if len(psk) == 0 {
		return nil, errors.New("runtime PSK is empty")
	}

	// Derive a deterministic Ed25519 seed from the PSK so every runtime with the same PSK produces the same CA key
	seed, err := hkdf.Key(sha256.New, psk, caHKDFSalt, caHKDFInfo, ed25519.SeedSize)
	if err != nil {
		return nil, fmt.Errorf("failed to derive CA seed: %w", err)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("failed to obtain CA public key")
	}

	// Build a fully deterministic self-signed CA certificate
	// Every field is constant so the encoded certificate is byte-identical across runtimes and restarts, which is what lets the public cert be pinned out-of-band
	trustURI := &url.URL{Scheme: "spiffe", Host: TrustDomain}
	tpl := &x509.Certificate{
		SerialNumber:          serialFromKey(pub),
		Subject:               pkix.Name{CommonName: "Francis CA"},
		NotBefore:             caEpoch,
		NotAfter:              caNotAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
		URIs:                  []*url.URL{trustURI},
	}

	// Ed25519 signing ignores the random reader and is itself deterministic, so the resulting DER is stable
	certDER, err := x509.CreateCertificate(rand.Reader, tpl, tpl, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("failed to create CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	return &CA{priv: priv, cert: cert, certDER: certDER}, nil
}

// CABundle derives one CA per PSK, preserving order so index 0 is the primary used to mint new certificates
func CABundle(psks [][]byte) ([]*CA, error) {
	if len(psks) == 0 {
		return nil, errors.New("at least one runtime PSK is required")
	}

	out := make([]*CA, len(psks))
	for i, psk := range psks {
		cur, err := DeriveCA(psk)
		if err != nil {
			return nil, fmt.Errorf("failed to derive CA for PSK at index %d: %w", i, err)
		}
		out[i] = cur
	}
	return out, nil
}

// Certificate returns the self-signed CA certificate
func (c *CA) Certificate() *x509.Certificate {
	return c.cert
}

// CertPEM returns the PEM-encoded CA certificate, used by the pinning subcommand and handed to hosts in-band
func (c *CA) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.certDER})
}

// IssueWorkloadCert signs a short-lived leaf certificate for the given SPIFFE URI and public key
// The leaf carries both server and client auth so a host can act as a peer server and a peer client with one certificate
func (c *CA) IssueWorkloadCert(spiffeURI *url.URL, pub ed25519.PublicKey, ttl time.Duration) ([]byte, error) {
	if spiffeURI == nil {
		return nil, errors.New("SPIFFE URI is required")
	}
	if len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("invalid Ed25519 public key")
	}
	if ttl <= 0 {
		return nil, errors.New("certificate TTL must be positive")
	}

	// Generate a random serial because issuance is distributed across runtimes and the certificates are never persisted, so a counter could collide
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial number: %w", err)
	}

	now := time.Now()
	tpl := &x509.Certificate{
		SerialNumber:          serial,
		NotBefore:             now.Add(-clockSkew),
		NotAfter:              now.Add(ttl),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		URIs:                  []*url.URL{spiffeURI},
	}

	der, err := x509.CreateCertificate(rand.Reader, tpl, c.cert, pub, c.priv)
	if err != nil {
		return nil, fmt.Errorf("failed to create workload certificate: %w", err)
	}
	return der, nil
}

// RuntimeURI returns the SPIFFE identity for a runtime
func RuntimeURI(runtimeID string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: TrustDomain, Path: "/runtime/" + runtimeID}
}

// HostURI returns the SPIFFE identity for a host
func HostURI(hostID string) *url.URL {
	return &url.URL{Scheme: "spiffe", Host: TrustDomain, Path: "/host/" + hostID}
}

// serialFromKey derives a stable, positive serial number from a public key so the CA certificate stays deterministic
func serialFromKey(pub ed25519.PublicKey) *big.Int {
	sum := sha256.Sum256(pub)
	// The low 16 bytes are interpreted as an unsigned integer, which is always positive
	return new(big.Int).SetBytes(sum[:16])
}
