package host

import (
	"log/slog"
	"time"

	"github.com/italypaleale/actors/components"
	"github.com/italypaleale/actors/components/postgres"
	"github.com/italypaleale/actors/components/sqlite"
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

// WithTLSOptions sets TLS options
func WithTLSOptions(tls *HostTLSOptions) HostOption {
	return func(o *newHostOptions) { o.TLSOptions = tls }
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
		o.PeerAuthentication = &PeerAuthenticationSharedKey{
			Key: key,
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
	TLSOptions                *HostTLSOptions
	Logger                    *slog.Logger
	ProviderOptions           components.ProviderOptions
	PeerAuthentication        peerAuthenticationMethod
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
