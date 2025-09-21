package peerauth

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"

	"github.com/italypaleale/francis/internal/hosttls"
)

// PeerAuthenticationMTLS configures peer authentication to use mTLS.
type PeerAuthenticationMTLS struct {
	// Certification Authority certificate
	CA *x509.Certificate
	// Certificate and key
	Certificate *tls.Certificate
}

func (p *PeerAuthenticationMTLS) Validate() (err error) {
	if p.CA == nil {
		return errors.New("property CA certificate is empty")
	}
	if p.Certificate == nil {
		return errors.New("property Certificate is empty")
	}
	if len(p.Certificate.Certificate) == 0 || p.Certificate.PrivateKey == nil {
		return errors.New("property Certificate is not valid: must contain both a certificate and private key")
	}

	// Parse the certificate to validate its extended key usage
	cert, err := x509.ParseCertificate(p.Certificate.Certificate[0])
	if err != nil {
		return errors.New("failed to parse certificate: " + err.Error())
	}

	// Check if certificate can be used for both server auth and client auth
	var hasServerAuth, hasClientAuth bool
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			hasServerAuth = true
		}
		if usage == x509.ExtKeyUsageClientAuth {
			hasClientAuth = true
		}
	}

	if !hasServerAuth {
		return errors.New("certificate does not have server authentication extended key usage")
	}
	if !hasClientAuth {
		return errors.New("certificate does not have client authentication extended key usage")
	}

	return nil
}

func (p *PeerAuthenticationMTLS) SetTLSOptions(opts *hosttls.HostTLSOptions) {
	opts.InsecureSkipTLSValidation = false
	opts.CACertificate = p.CA
	opts.ClientCertificate = p.Certificate
	opts.ServerCertificate = p.Certificate
	opts.ClientAuth = tls.RequireAndVerifyClientCert
}

func (p *PeerAuthenticationMTLS) UpdateRequest(r *http.Request) error {
	// No-op in this implementation
	return nil
}

func (p *PeerAuthenticationMTLS) ValidateIncomingRequest(r *http.Request) (bool, error) {
	// No-op in this implementation
	return true, nil
}
