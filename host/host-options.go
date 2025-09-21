package host

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/hosttls"
	"github.com/italypaleale/francis/internal/peerauth"
	"k8s.io/utils/clock"
)

type HostOption func(*newHostOptions)

// WithAddress sets the address where the host can be reached at
func WithAddress(addr string) HostOption {
	return func(o *newHostOptions) { o.Address = addr }
}

// WithBindPort sets the port for the server to listen on
// If unset, will be extracted from the address
func WithBindPort(port int) HostOption {
	return func(o *newHostOptions) { o.BindPort = port }
}

// WithBindAddress sets the address to bind the server to
// If unset, will be extracted from the address
func WithBindAddress(addr string) HostOption {
	return func(o *newHostOptions) { o.BindAddress = addr }
}

// WithServerTLSCertificate sets the TLS certificate for the host
// If empty, uses a self-signed certificate
func WithServerTLSCertificate(cert *tls.Certificate) HostOption {
	return func(o *newHostOptions) { o.TLSOptions.ServerCertificate = cert }
}

// WithServerTLSCA sets the TLS CA certificate used by all hosts in the cluster
func WithServerTLSCA(ca *x509.Certificate) HostOption {
	return func(o *newHostOptions) { o.TLSOptions.CACertificate = ca }
}

// WithServerTLSInsecureSkipTLSValidation configures the node to skip validating TLS certificates when communicating with other hosts
// This is automatically set when using self-signed certificates
func WithServerTLSInsecureSkipTLSValidation() HostOption {
	return func(o *newHostOptions) { o.TLSOptions.InsecureSkipTLSValidation = true }
}

// WithLogger sets the instance of the slog logger
func WithLogger(logger *slog.Logger) HostOption {
	return func(o *newHostOptions) { o.Logger = logger }
}

// WithSQLiteProvider sets the SQLite provider
func WithSQLiteProvider(opts sqlite.SQLiteProviderOptions) HostOption {
	return func(o *newHostOptions) { o.ProviderOptions = opts }
}

// WithPostgresProvider sets the Postgres provider
func WithPostgresProvider(opts postgres.PostgresProviderOptions) HostOption {
	return func(o *newHostOptions) { o.ProviderOptions = opts }
}

// WithPeerAuthenticationSharedKey configures peer authentication to use a pre-shared key
func WithPeerAuthenticationSharedKey(key string) HostOption {
	return func(o *newHostOptions) {
		o.PeerAuthentication = &peerauth.PeerAuthenticationSharedKey{
			Key: key,
		}
	}
}

// WithPeerAuthenticationMTLS configures peer authentication to use mTLS
func WithPeerAuthenticationMTLS(certificate *tls.Certificate, ca *x509.Certificate) HostOption {
	return func(o *newHostOptions) {
		o.PeerAuthentication = &peerauth.PeerAuthenticationMTLS{
			CA:          ca,
			Certificate: certificate,
		}
	}
}

// WithHostHealthCheckDeadline sets the maximum interval between pings received from an actor host
func WithHostHealthCheckDeadline(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.HostHealthCheckDeadline = d }
}

// WithAlarmsPollInterval sets the interval for polling alarms
func WithAlarmsPollInterval(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.AlarmsPollInterval = d }
}

// WithAlarmsLeaseDuration sets the alarm lease duration
func WithAlarmsLeaseDuration(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.AlarmsLeaseDuration = d }
}

// WithAlarmsFetchAheadInterval sets the pre-fetch interval for alarms
func WithAlarmsFetchAheadInterval(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.AlarmsFetchAheadInterval = d }
}

// WithAlarmsFetchAheadBatchSize sets the batch size for pre-fetching alarms
func WithAlarmsFetchAheadBatchSize(n int) HostOption {
	return func(o *newHostOptions) { o.AlarmsFetchAheadBatchSize = n }
}

// WithShutdownGracePeriod sets the grace period for shutting down
func WithShutdownGracePeriod(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.ShutdownGracePeriod = d }
}

// WithProviderRequestTimeout sets the timeout for requests to the provider
func WithProviderRequestTimeout(d time.Duration) HostOption {
	return func(o *newHostOptions) { o.ProviderRequestTimeout = d }
}

type newHostOptions struct {
	Address                   string
	BindPort                  int
	BindAddress               string
	TLSOptions                hosttls.HostTLSOptions
	Logger                    *slog.Logger
	ProviderOptions           components.ProviderOptions
	PeerAuthentication        peerauth.PeerAuthenticationMethod
	HostHealthCheckDeadline   time.Duration
	AlarmsPollInterval        time.Duration
	AlarmsLeaseDuration       time.Duration
	AlarmsFetchAheadInterval  time.Duration
	AlarmsFetchAheadBatchSize int
	ShutdownGracePeriod       time.Duration
	ProviderRequestTimeout    time.Duration

	// Allows setting a clock for testing
	clock clock.WithTicker
}
