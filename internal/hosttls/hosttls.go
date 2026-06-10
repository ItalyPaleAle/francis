// Package hosttls builds the TLS configurations used by hosts for their runtime and peer connections
// Certificates and trust anchors are read live from a certholder, so a workload-cert renewal or a root rotation takes effect without rebuilding any tls.Config
package hosttls

import (
	"crypto/tls"
	"crypto/x509"

	"github.com/quic-go/quic-go/http3"

	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/certholder"
)

// minTLSVersion is the minimum TLS version for every host connection
const minTLSVersion = tls.VersionTLS13

// RuntimeClientTLSConfig builds the client TLS config for host-to-runtime connections
// It presents the host's workload certificate once one has been issued and verifies the runtime's certificate against the live trust bundle
// During the very first bootstrap there is no trust anchor yet: when the CA is not pinned the runtime certificate is accepted, since the PSK challenge-response still protects the credential and a JWT cluster is expected to pin the CA
func RuntimeClientTLSConfig(holder *certholder.Holder) *tls.Config {
	verifyRuntime := ca.VerifyPeerSPIFFE(holder.Roots, ca.RuntimePrefix, nil)
	verify := func(rawCerts [][]byte, chains [][]*x509.Certificate) error {
		// With no trust anchor yet this is a first bootstrap to an unpinned runtime, so accept the certificate
		if holder.Roots() == nil {
			return nil
		}

		return verifyRuntime(rawCerts, chains)
	}

	return &tls.Config{
		MinVersion:           minTLSVersion,
		NextProtos:           []string{http3.NextProtoH3},
		GetClientCertificate: holder.GetClientCertificate,
		// #nosec G402 -- VerifyPeerCertificate performs all verification against the live trust bundle
		InsecureSkipVerify: true,
		// #nosec G123 -- Session resumption reuses a session whose cert was already verified - the QUIC transport is the session boundary
		VerifyPeerCertificate: verify,
	}
}

// PeerClientTLSConfig builds the client TLS config for host-to-host peer connections
// It presents the host's workload certificate and verifies the peer is a host signed by a current trust anchor
func PeerClientTLSConfig(holder *certholder.Holder) *tls.Config {
	return &tls.Config{
		MinVersion:           minTLSVersion,
		NextProtos:           []string{http3.NextProtoH3},
		GetClientCertificate: holder.GetClientCertificate,
		// #nosec G402 -- VerifyPeerCertificate performs all verification against the live trust bundle
		InsecureSkipVerify: true,
		// #nosec G123 -- see RuntimeClientTLSConfig
		VerifyPeerCertificate: ca.VerifyPeerSPIFFE(holder.Roots, ca.HostPrefix, nil),
	}
}

// PeerServerTLSConfig builds the server TLS config for the host's peer server
// It serves the host's workload certificate and requires every connecting peer to present a host certificate signed by a current trust anchor
func PeerServerTLSConfig(holder *certholder.Holder) *tls.Config {
	return &tls.Config{
		MinVersion:     minTLSVersion,
		GetCertificate: holder.GetCertificate,
		ClientAuth:     tls.RequireAnyClientCert,
		// #nosec G123 -- see RuntimeClientTLSConfig
		VerifyPeerCertificate: ca.VerifyPeerSPIFFE(holder.Roots, ca.HostPrefix, nil),
	}
}
