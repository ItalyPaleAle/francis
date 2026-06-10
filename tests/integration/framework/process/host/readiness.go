//go:build integration

package host

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"net/http"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/tests/integration/framework/process/clustersecret"
)

const (
	peerReadyTimeout  = 30 * time.Second
	peerProbeInterval = 50 * time.Millisecond
	peerProbeTimeout  = 2 * time.Second
)

// waitPeerServer blocks until the host's peer WebTransport server answers its health endpoint
//
// A host's Ready channel only covers registration with the provider or runtime, not its peer server, which starts concurrently
// Without this gate a very fast scenario can register and then shut down while the peer server is still starting, which crashes quic-go during accept
// Probing the plain HTTP/3 /healthz endpoint confirms the peer server is actually serving before the scenario proceeds
func waitPeerServer(t *testing.T, address string) {
	t.Helper()

	// The peer server requires a client certificate signed by the cluster CA, so the probe presents one derived from the shared runtime PSK
	transport := &http3.Transport{
		//nolint:gosec
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify:   true,
			NextProtos:           []string{http3.NextProtoH3},
			GetClientCertificate: probeClientCert(t),
		},
		QUICConfig: &quic.Config{},
	}
	defer transport.Close()

	client := &http.Client{Transport: transport}
	url := "https://" + address + "/healthz"

	// Poll until the endpoint responds or the deadline passes
	deadline := time.Now().Add(peerReadyTimeout)
	for {
		probeCtx, probeCancel := context.WithDeadline(t.Context(), deadline)
		ok := probeHealthz(probeCtx, client, url)
		probeCancel()
		if ok {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("peer server %s did not become ready within %s", address, peerReadyTimeout)
		}
		time.Sleep(peerProbeInterval)
	}
}

// probeClientCert returns a GetClientCertificate callback that presents a host workload certificate signed by the cluster CA
// The peer server verifies it against the same CA, so the readiness probe can complete the mTLS handshake
func probeClientCert(t *testing.T) func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	t.Helper()
	cas, err := ca.CABundle([][]byte{clustersecret.RuntimePSK})
	require.NoError(t, err)
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	der, err := cas[0].IssueWorkloadCert(ca.HostURI("readiness-probe"), pub, time.Hour)
	require.NoError(t, err)

	cert := &tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		return cert, nil
	}
}

// probeHealthz reports whether a single health request to the peer server succeeds
func probeHealthz(ctx context.Context, client *http.Client, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, peerProbeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	// Any response means the HTTP/3 server is up and serving
	return resp.StatusCode == http.StatusNoContent
}
