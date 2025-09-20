package peerauth

import (
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
)

// PeerAuthenticationMTLS configures peer authentication to use mTLS.
type PeerAuthenticationMTLS struct {
	// Certification Authority certificate, PEM-encoded
	CA []byte
	// Certificate, PEM-encoded
	Certificate []byte
	// Private key, PEM-encoded
	Key []byte

	// Parsed objects
	parsedCA   *x509.Certificate
	parsedCert *x509.Certificate
	parsedKey  crypto.PrivateKey
}

func (p *PeerAuthenticationMTLS) Validate() (err error) {
	// Parse CA certificate
	p.parsedCA, err = parsePEMCert(p.CA)
	if err != nil {
		return err
	}

	// Parse certificate
	p.parsedCert, err = parsePEMCert(p.Certificate)
	if err != nil {
		return err
	}

	// Parse private key
	keyBlock, _ := pem.Decode(p.Key)
	if keyBlock == nil {
		return errors.New("invalid key PEM")
	}
	switch keyBlock.Type {
	case "RSA PRIVATE KEY":
		p.parsedKey, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	case "EC PRIVATE KEY":
		p.parsedKey, err = x509.ParseECPrivateKey(keyBlock.Bytes)
	case "PRIVATE KEY":
		p.parsedKey, err = x509.ParsePKCS8PrivateKey(keyBlock.Bytes)
	default:
		return fmt.Errorf("unsupported private key type: %s", keyBlock.Type)
	}
	if err != nil {
		return fmt.Errorf("invalid private key: %w", err)
	}

	return nil
}

func (p *PeerAuthenticationMTLS) UpdateRequest(r *http.Request) error {
	// No-op in this implementation
	return nil
}

func (p *PeerAuthenticationMTLS) ValidateIncomingRequest(r *http.Request) (bool, error) {
	// No-op in this implementation
	return true, nil
}

func parsePEMCert(data []byte) (cert *x509.Certificate, err error) {
	certBlock, _ := pem.Decode(data)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, errors.New("invalid certificate PEM")
	}
	cert, err = x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("invalid certificate: %w", err)
	}
	return cert, nil
}
