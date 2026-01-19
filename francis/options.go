package francis

import (
	"crypto/tls"
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
)

// Default values for server configuration
const (
	defaultBindAddress            = ":8443"
	defaultShutdownGracePeriod    = 30 * time.Second
	defaultRequestTimeout         = 15 * time.Second
	defaultHostHealthCheckDeadline = 20 * time.Second
	defaultAlarmsPollInterval     = 1500 * time.Millisecond
	defaultAlarmsLeaseDuration    = 20 * time.Second
	defaultAlarmsFetchAheadInterval = 2500 * time.Millisecond
	defaultAlarmsFetchAheadBatch  = 25
)

// ServerOption is a function that configures the server
type ServerOption func(*serverOptions)

// serverOptions holds configuration for the Francis server
type serverOptions struct {
	// Bind address for the server
	BindAddress string
	// TLS configuration for the server
	TLSConfig *tls.Config
	// Logger instance
	Logger *slog.Logger
	// Provider options (SQLite, Postgres, or Standalone)
	ProviderOptions components.ProviderOptions
	// Health check deadline for connected clients
	HostHealthCheckDeadline time.Duration
	// Alarm poll interval
	AlarmsPollInterval time.Duration
	// Alarm lease duration
	AlarmsLeaseDuration time.Duration
	// Alarm fetch-ahead interval
	AlarmsFetchAheadInterval time.Duration
	// Alarm fetch-ahead batch size
	AlarmsFetchAheadBatchSize int
	// Shutdown grace period
	ShutdownGracePeriod time.Duration
	// Request timeout
	RequestTimeout time.Duration
	// Clock for testing
	clock clock.WithTicker
}

func (o serverOptions) getProviderConfig() components.ProviderConfig {
	return components.ProviderConfig{
		HostHealthCheckDeadline:   o.HostHealthCheckDeadline,
		AlarmsLeaseDuration:       o.AlarmsLeaseDuration,
		AlarmsFetchAheadInterval:  o.AlarmsFetchAheadInterval,
		AlarmsFetchAheadBatchSize: o.AlarmsFetchAheadBatchSize,
	}
}

// WithBindAddress sets the address to bind the server to
func WithBindAddress(addr string) ServerOption {
	return func(o *serverOptions) { o.BindAddress = addr }
}

// WithTLSConfig sets the TLS configuration for the server
func WithTLSConfig(cfg *tls.Config) ServerOption {
	return func(o *serverOptions) { o.TLSConfig = cfg }
}

// WithLogger sets the logger instance
func WithLogger(logger *slog.Logger) ServerOption {
	return func(o *serverOptions) { o.Logger = logger }
}

// WithSQLiteProvider sets the SQLite provider options
func WithSQLiteProvider(opts sqlite.SQLiteProviderOptions) ServerOption {
	return func(o *serverOptions) { o.ProviderOptions = opts }
}

// WithPostgresProvider sets the Postgres provider options
func WithPostgresProvider(opts postgres.PostgresProviderOptions) ServerOption {
	return func(o *serverOptions) { o.ProviderOptions = opts }
}

// WithStandaloneMemoryProvider sets up an in-memory standalone provider
func WithStandaloneMemoryProvider() ServerOption {
	return func(o *serverOptions) { o.ProviderOptions = standalone.StandaloneMemoryProviderOptions{} }
}

// WithStandaloneSQLiteProvider sets up a standalone provider with SQLite persistence
func WithStandaloneSQLiteProvider(opts standalone.StandaloneSQLiteProviderOptions) ServerOption {
	return func(o *serverOptions) { o.ProviderOptions = opts }
}

// WithStandalonePostgresProvider sets up a standalone provider with Postgres persistence
func WithStandalonePostgresProvider(opts standalone.StandalonePostgresProviderOptions) ServerOption {
	return func(o *serverOptions) { o.ProviderOptions = opts }
}

// WithHostHealthCheckDeadline sets the health check deadline for connected clients
func WithHostHealthCheckDeadline(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.HostHealthCheckDeadline = d }
}

// WithAlarmsPollInterval sets the interval for polling alarms
func WithAlarmsPollInterval(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.AlarmsPollInterval = d }
}

// WithAlarmsLeaseDuration sets the alarm lease duration
func WithAlarmsLeaseDuration(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.AlarmsLeaseDuration = d }
}

// WithAlarmsFetchAheadInterval sets the alarm fetch-ahead interval
func WithAlarmsFetchAheadInterval(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.AlarmsFetchAheadInterval = d }
}

// WithAlarmsFetchAheadBatchSize sets the alarm fetch-ahead batch size
func WithAlarmsFetchAheadBatchSize(n int) ServerOption {
	return func(o *serverOptions) { o.AlarmsFetchAheadBatchSize = n }
}

// WithShutdownGracePeriod sets the shutdown grace period
func WithShutdownGracePeriod(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.ShutdownGracePeriod = d }
}

// WithRequestTimeout sets the request timeout
func WithRequestTimeout(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.RequestTimeout = d }
}

// withClock sets the clock for testing (internal use only)
func withClock(c clock.WithTicker) ServerOption {
	return func(o *serverOptions) { o.clock = c }
}
