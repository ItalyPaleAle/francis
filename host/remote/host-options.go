package remote

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/peerauth"
)

// Default values
const (
	defaultShutdownGracePeriod   = 30 * time.Second
	defaultRequestTimeout        = 15 * time.Second
	defaultHealthCheckInterval   = 5 * time.Second
	defaultReconnectInterval     = 5 * time.Second
	defaultMaxReconnectAttempts  = 10
	defaultPlacementCacheMaxTTL  = 5 * time.Second
)

// HostOption configures the remote host
type HostOption func(*hostOptions)

// hostOptions holds configuration for the remote host
type hostOptions struct {
	// FrancisAddress is the address of the Francis server
	FrancisAddress string
	// ClientAddress is this client's address for peer-to-peer communication
	ClientAddress string
	// BindPort is the port to bind the peer-to-peer server to
	BindPort int
	// BindAddress is the address to bind the peer-to-peer server to
	BindAddress string
	// TLS options for peer-to-peer and Francis connection
	TLSOptions hosttls.HostTLSOptions
	// Peer authentication method
	PeerAuthentication peerauth.PeerAuthenticationMethod
	// Logger instance
	Logger *slog.Logger
	// Shutdown grace period
	ShutdownGracePeriod time.Duration
	// Request timeout for Francis and peer requests
	RequestTimeout time.Duration
	// Health check interval for Francis connection
	HealthCheckInterval time.Duration
	// Reconnect interval on connection loss
	ReconnectInterval time.Duration
	// Maximum reconnect attempts before giving up
	MaxReconnectAttempts int
	// TLS configuration for connecting to Francis
	FrancisTLSConfig *tls.Config
	// Clock for testing
	clock clock.WithTicker
}

// WithFrancisAddress sets the address of the Francis server
func WithFrancisAddress(addr string) HostOption {
	return func(o *hostOptions) { o.FrancisAddress = addr }
}

// WithClientAddress sets this client's address for peer-to-peer communication
func WithClientAddress(addr string) HostOption {
	return func(o *hostOptions) { o.ClientAddress = addr }
}

// WithBindPort sets the port for the peer-to-peer server to listen on
func WithBindPort(port int) HostOption {
	return func(o *hostOptions) { o.BindPort = port }
}

// WithBindAddress sets the address to bind the peer-to-peer server to
func WithBindAddress(addr string) HostOption {
	return func(o *hostOptions) { o.BindAddress = addr }
}

// WithLogger sets the logger instance
func WithLogger(logger *slog.Logger) HostOption {
	return func(o *hostOptions) { o.Logger = logger }
}

// WithServerTLSCertificate sets the TLS certificate for the peer-to-peer server
func WithServerTLSCertificate(cert *tls.Certificate) HostOption {
	return func(o *hostOptions) { o.TLSOptions.ServerCertificate = cert }
}

// WithServerTLSCA sets the TLS CA certificate for peer-to-peer communication
func WithServerTLSCA(ca *x509.Certificate) HostOption {
	return func(o *hostOptions) { o.TLSOptions.CACertificate = ca }
}

// WithServerTLSInsecureSkipTLSValidation disables TLS validation for peer-to-peer communication
func WithServerTLSInsecureSkipTLSValidation() HostOption {
	return func(o *hostOptions) { o.TLSOptions.InsecureSkipTLSValidation = true }
}

// WithPeerAuthenticationSharedKey configures peer authentication with a shared key
func WithPeerAuthenticationSharedKey(key string) HostOption {
	return func(o *hostOptions) {
		o.PeerAuthentication = &peerauth.PeerAuthenticationSharedKey{
			Key: key,
		}
	}
}

// WithPeerAuthenticationMTLS configures peer authentication with mTLS
func WithPeerAuthenticationMTLS(certificate *tls.Certificate, ca *x509.Certificate) HostOption {
	return func(o *hostOptions) {
		o.PeerAuthentication = &peerauth.PeerAuthenticationMTLS{
			CA:          ca,
			Certificate: certificate,
		}
	}
}

// WithShutdownGracePeriod sets the shutdown grace period
func WithShutdownGracePeriod(d time.Duration) HostOption {
	return func(o *hostOptions) { o.ShutdownGracePeriod = d }
}

// WithRequestTimeout sets the request timeout
func WithRequestTimeout(d time.Duration) HostOption {
	return func(o *hostOptions) { o.RequestTimeout = d }
}

// WithHealthCheckInterval sets the health check interval for Francis connection
func WithHealthCheckInterval(d time.Duration) HostOption {
	return func(o *hostOptions) { o.HealthCheckInterval = d }
}

// WithReconnectInterval sets the reconnect interval on connection loss
func WithReconnectInterval(d time.Duration) HostOption {
	return func(o *hostOptions) { o.ReconnectInterval = d }
}

// WithMaxReconnectAttempts sets the maximum reconnect attempts
func WithMaxReconnectAttempts(n int) HostOption {
	return func(o *hostOptions) { o.MaxReconnectAttempts = n }
}

// WithFrancisTLSConfig sets the TLS configuration for connecting to Francis
func WithFrancisTLSConfig(cfg *tls.Config) HostOption {
	return func(o *hostOptions) { o.FrancisTLSConfig = cfg }
}

// withClock sets the clock for testing (internal use only)
func withClock(c clock.WithTicker) HostOption {
	return func(o *hostOptions) { o.clock = c }
}
