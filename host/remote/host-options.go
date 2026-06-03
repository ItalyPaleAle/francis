package remote

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/internal/hosttls"
)

type HostOption func(*newHostOptions)

// WithAddress sets the peer address where this host can be reached by other hosts and that it advertises to the runtime
func WithAddress(addr string) HostOption {
	return func(o *newHostOptions) { o.Address = addr }
}

// WithBindPort sets the port for the peer server to listen on
// If unset, it is extracted from the address
func WithBindPort(port int) HostOption {
	return func(o *newHostOptions) { o.BindPort = port }
}

// WithBindAddress sets the address to bind the peer server to
// If unset, it is extracted from the address
func WithBindAddress(addr string) HostOption {
	return func(o *newHostOptions) { o.BindAddress = addr }
}

// WithRuntimeAddresses sets the addresses of the runtime replicas the host connects to
// The host connects to one at a time and rolls over to another on failure
func WithRuntimeAddresses(addresses ...string) HostOption {
	return func(o *newHostOptions) { o.RuntimeAddresses = addresses }
}

// WithServerTLSCertificate sets the TLS certificate for the host
// If empty, uses a self-signed certificate
func WithServerTLSCertificate(cert *tls.Certificate) HostOption {
	return func(o *newHostOptions) { o.TLSOptions.ServerCertificate = cert }
}

// WithServerTLSCA sets the TLS CA certificate used by all hosts and runtimes in the cluster
func WithServerTLSCA(ca *x509.Certificate) HostOption {
	return func(o *newHostOptions) { o.TLSOptions.CACertificate = ca }
}

// WithServerTLSInsecureSkipTLSValidation configures the node to skip validating TLS certificates when connecting to runtimes and other hosts
// This is automatically set when using self-signed certificates
func WithServerTLSInsecureSkipTLSValidation() HostOption {
	return func(o *newHostOptions) { o.TLSOptions.InsecureSkipTLSValidation = true }
}

// WithLogger sets the instance of the slog logger
func WithLogger(logger *slog.Logger) HostOption {
	return func(o *newHostOptions) { o.Logger = logger }
}

// WithShutdownGracePeriod sets the grace period for shutting down
func WithShutdownGracePeriod(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.ShutdownGracePeriod = d }
}

// WithRequestTimeout sets the timeout for individual requests sent to the runtime
func WithRequestTimeout(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.RequestTimeout = d }
}

type newHostOptions struct {
	Address             string
	BindPort            int
	BindAddress         string
	RuntimeAddresses    []string
	TLSOptions          hosttls.HostTLSOptions
	Logger              *slog.Logger
	ShutdownGracePeriod time.Duration
	RequestTimeout      time.Duration

	// Allows setting a clock for testing
	clock clock.WithTicker
}
