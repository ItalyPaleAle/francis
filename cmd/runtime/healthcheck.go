package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/quic-go/quic-go/http3"

	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
	"github.com/italypaleale/francis/runtime"
)

// defaultHealthcheckTimeout is the default time bound for a single healthcheck probe
// The container runtime wraps the command with its own HEALTHCHECK timeout, but an internal bound stops a half-open QUIC connection from hanging the probe
const defaultHealthcheckTimeout = 5 * time.Second

// runHealthcheck dials the runtime over WebTransport and reports whether it accepted a session
// It is intended as the HEALTHCHECK for containers, so it needs no extra binaries and defaults to the locally-running server
func runHealthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ExitOnError)

	var (
		addr               string
		insecureSkipVerify bool
		timeout            time.Duration
		verbose            bool
	)
	fs.StringVar(&addr, "addr", "", "Runtime address (host:port) to healthcheck; defaults to 127.0.0.1 on the configured bind port")
	fs.BoolVar(&insecureSkipVerify, "insecure-skip-verify", false, "Skip TLS certificate verification against the cluster CA")
	fs.DurationVar(&timeout, "timeout", defaultHealthcheckTimeout, "Maximum time to wait for the healthcheck to complete")
	fs.BoolVar(&verbose, "verbose", false, "Print a message when the healthcheck succeeds")
	_ = fs.Parse(args)

	if timeout <= 0 {
		timeout = defaultHealthcheckTimeout
	}

	// The config resolves the bind address (unless -addr is set) and the CA used to verify the runtime (unless -insecure-skip-verify is set)
	var cfg *config
	if addr == "" || !insecureSkipVerify {
		// Resolve the config file from the FRANCIS_CONFIG env var or the well-known paths
		configPath, err := resolveConfigPath()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
			return 1
		}

		cfg, err = loadConfig(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
			return 1
		}
	}

	// Default to the locally-running server: the IPv4 loopback on the runtime's configured bind port
	if addr == "" {
		var err error
		addr, err = loopbackBindAddr(cfg.Bind)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error resolving bind address: %v\n", err)
			return 1
		}
	}

	tlsConfig, err := buildHealthcheckTLSConfig(cfg, insecureSkipVerify)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	// Dial the runtime over WebTransport using the shared dialer, which sets the QUIC settings the WebTransport handshake requires
	dialer := wt.NewDialer(tlsConfig)
	defer func() {
		_ = dialer.Close()
	}()

	start := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Dialing the runtime connect path performs the full QUIC + TLS 1.3 + HTTP/3 + WebTransport handshake
	// A successful upgrade (2xx) proves the runtime is serving and accepting sessions, which is all a liveness probe needs
	url := "https://" + addr + protocol.RuntimeConnectPath
	rsp, session, err := dialer.Dial(ctx, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Healthcheck failed: could not dial runtime at %s: %v (took %dms)\n", addr, err, time.Since(start).Milliseconds())
		return 1
	}
	defer func() { _ = session.CloseWithError(0, "") }()

	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		fmt.Fprintf(os.Stderr, "❌ Healthcheck failed: runtime at %s returned status %d (took %dms)\n", addr, rsp.StatusCode, time.Since(start).Milliseconds())
		return 1
	}

	if verbose {
		fmt.Fprintf(os.Stdout, "✅ Healthcheck succeeded: runtime at %s returned status %d (took %dms)\n", addr, rsp.StatusCode, time.Since(start).Milliseconds())
	}
	return 0
}

// buildHealthcheckTLSConfig builds the client TLS config for dialing the runtime over WebTransport
// By default it verifies the runtime's SPIFFE certificate against the CA derived from the configured runtime PSKs, mirroring how hosts verify the runtime
// The runtime serves a SPIFFE certificate with no DNS SANs, so hostname verification is skipped in favor of verifying the chain and asserting the identity against the trust anchors
// With insecureSkipVerify set it skips verification entirely, matching a plain loopback probe
func buildHealthcheckTLSConfig(cfg *config, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{http3.NextProtoH3},
		// #nosec G402 -- verification is either performed via VerifyPeerCertificate against the cluster CA, or explicitly skipped by the operator
		InsecureSkipVerify: true,
	}

	if insecureSkipVerify {
		return tlsConfig, nil
	}

	if cfg == nil {
		return nil, errors.New("configuration is required to verify the runtime certificate")
	}

	// Derive the same CA the runtime uses
	psks, err := parsePSKs(cfg.RuntimePSKs)
	if err != nil {
		return nil, fmt.Errorf("error parsing runtime PSKs: %w", err)
	}
	bundle, err := runtime.CABundlePEM(psks...)
	if err != nil {
		return nil, fmt.Errorf("error deriving CA: %w", err)
	}
	pool, err := ca.PoolFromPEM(bundle)
	if err != nil {
		return nil, fmt.Errorf("error parsing CA bundle: %w", err)
	}

	roots := func() *x509.CertPool { return pool }

	// #nosec G402 G123 -- VerifyPeerCertificate verifies the runtime cert against the CA derived from the runtime PSKs. The probe makes a single fresh dial with no session resumption, and the QUIC transport is the session boundary.
	tlsConfig.VerifyPeerCertificate = ca.VerifyPeerSPIFFE(roots, ca.RuntimePrefix, nil)

	return tlsConfig, nil
}

// loopbackBindAddr rewrites a runtime bind address as an IPv4 loopback dial target, keeping the port
// The healthcheck runs alongside the runtime, so it always probes the loopback regardless of the bound host
// It uses 127.0.0.1 rather than "localhost" because QUIC dials a single resolved address with no fallback: if "localhost" resolved to ::1 but the runtime binds to an IPv4 address, the probe would fail and needlessly mark the container unhealthy
func loopbackBindAddr(bind string) (string, error) {
	_, port, err := net.SplitHostPort(bind)
	if err != nil {
		return "", fmt.Errorf("invalid bind address '%s': %w", bind, err)
	}
	if port == "" {
		return "", fmt.Errorf("bind address '%s' has no port", bind)
	}

	return net.JoinHostPort("127.0.0.1", port), nil
}
