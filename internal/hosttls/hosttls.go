package hosttls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/quic-go/quic-go/http3"
)

const minTLSVersion = tls.VersionTLS13

// HostTLSOptions contains the options for the host's TLS configuration.
// All fields are optional
type HostTLSOptions struct {
	// CA Certificate, used by all nodes in the cluster
	CACertificate *x509.Certificate
	// TLS certificate and key for the server
	// If empty, uses a self-signed certificate
	ServerCertificate *tls.Certificate
	// If true, skips validating TLS certificates presented by other hosts
	// This is required when using self-signed certificates
	InsecureSkipTLSValidation bool
}

func (opts HostTLSOptions) GetTLSConfig() (serverConfig *tls.Config, clientConfig *tls.Config, err error) {
	// TLS configuration for the server
	serverConfig = &tls.Config{
		MinVersion: minTLSVersion,
	}

	// TLS configuration for the client
	clientConfig = &tls.Config{
		MinVersion: minTLSVersion,
		// Set the ALPN for HTTP/3
		NextProtos: []string{http3.NextProtoH3},
	}

	// Set the CA certificate if present
	if opts.CACertificate != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AddCert(opts.CACertificate)

		// Set the cert pool on the server and client
		serverConfig.RootCAs = caCertPool
		clientConfig.RootCAs = caCertPool
	}

	// Disable TLS validation for the client if configured
	if opts.InsecureSkipTLSValidation {
		clientConfig.InsecureSkipVerify = true
	}

	// If a certificate was passed as input, use that
	if opts.ServerCertificate != nil {
		if len(opts.ServerCertificate.Certificate) == 0 || opts.ServerCertificate.PrivateKey == nil {
			return nil, nil, errors.New("option TLSOptions.ServerCertificate is not valid: must contain both a certificate and private key")
		}

		// Set the certificate in the object
		serverConfig.Certificates = []tls.Certificate{*opts.ServerCertificate}

		return serverConfig, clientConfig, nil
	}

	// Generate a self-signed certificate
	cert, err := generateSelfSignedServerCert()
	if err != nil {
		return nil, nil, err
	}

	serverConfig.Certificates = []tls.Certificate{*cert}

	return serverConfig, clientConfig, nil
}

func generateSelfSignedServerCert() (*tls.Certificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key for the self-signed certificate: %w", err)
	}

	now := time.Now()
	tpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Actor Host"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(180 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("failed to create self-signed certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal self-signed private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create self-signed key pair: %w", err)
	}

	return &cert, nil
}
