package runtime

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/internal/hosttls"
)

// RuntimeOption configures a Runtime
type RuntimeOption func(*runtimeOptions)

// newRuntimeOptions returns a runtimeOptions pre-populated with default values
func newRuntimeOptions() *runtimeOptions {
	return &runtimeOptions{
		logger:                    slog.New(slog.DiscardHandler),
		hostHealthCheckDeadline:   20 * time.Second,
		alarmsPollInterval:        1500 * time.Millisecond,
		alarmsLeaseDuration:       20 * time.Second,
		alarmsFetchAheadInterval:  2500 * time.Millisecond,
		alarmsFetchAheadBatchSize: 25,
		providerRequestTimeout:    15 * time.Second,
		alarmExecutionTimeout:     60 * time.Second,
		shutdownGracePeriod:       30 * time.Second,
		clock:                     &clock.RealClock{},
	}
}

type runtimeOptions struct {
	bind                      string
	tlsOptions                hosttls.HostTLSOptions
	logger                    *slog.Logger
	hostHealthCheckDeadline   time.Duration
	alarmsPollInterval        time.Duration
	alarmsLeaseDuration       time.Duration
	alarmsFetchAheadInterval  time.Duration
	alarmsFetchAheadBatchSize int
	providerRequestTimeout    time.Duration
	alarmExecutionTimeout     time.Duration
	shutdownGracePeriod       time.Duration

	// Allows setting a clock for testing
	clock clock.WithTicker
}

// WithBind sets the address and port the runtime WebTransport server listens on
func WithBind(bind string) RuntimeOption {
	return func(o *runtimeOptions) { o.bind = bind }
}

// WithServerTLSCertificate sets the TLS certificate for the runtime server
// If empty, a self-signed certificate is generated
func WithServerTLSCertificate(cert *tls.Certificate) RuntimeOption {
	return func(o *runtimeOptions) { o.tlsOptions.ServerCertificate = cert }
}

// WithServerTLSCA sets the TLS CA certificate used to validate hosts in the cluster
func WithServerTLSCA(ca *x509.Certificate) RuntimeOption {
	return func(o *runtimeOptions) { o.tlsOptions.CACertificate = ca }
}

// WithServerTLSClientAuth sets the client authentication mode, used to enable mTLS
func WithServerTLSClientAuth(clientAuth tls.ClientAuthType) RuntimeOption {
	return func(o *runtimeOptions) { o.tlsOptions.ClientAuth = clientAuth }
}

// WithLogger sets the slog logger
func WithLogger(logger *slog.Logger) RuntimeOption {
	return func(o *runtimeOptions) { o.logger = logger }
}

// WithHostHealthCheckDeadline sets the maximum interval between health checks received from a host
func WithHostHealthCheckDeadline(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.hostHealthCheckDeadline = d }
}

// WithAlarmsPollInterval sets the interval for polling alarms
func WithAlarmsPollInterval(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmsPollInterval = d }
}

// WithAlarmsLeaseDuration sets the alarm lease duration
func WithAlarmsLeaseDuration(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmsLeaseDuration = d }
}

// WithAlarmsFetchAheadInterval sets the pre-fetch interval for alarms
func WithAlarmsFetchAheadInterval(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmsFetchAheadInterval = d }
}

// WithAlarmsFetchAheadBatchSize sets the batch size for pre-fetching alarms
func WithAlarmsFetchAheadBatchSize(n int) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmsFetchAheadBatchSize = n }
}

// WithProviderRequestTimeout sets the timeout for requests to the provider
func WithProviderRequestTimeout(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.providerRequestTimeout = d }
}

// WithAlarmExecutionTimeout sets the maximum time to wait for a host to execute a dispatched alarm
func WithAlarmExecutionTimeout(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmExecutionTimeout = d }
}

// WithShutdownGracePeriod sets the grace period for shutting down
func WithShutdownGracePeriod(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.shutdownGracePeriod = d }
}
