package local

import (
	"log/slog"
	"time"

	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
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

// WithRuntimePSKs sets the runtime pre-shared keys from which the cluster CA is derived
// In local mode every host self-issues its workload certificate from this CA, so hosts that share the PSKs authenticate each other with mTLS
// The first key is the primary used to sign this host's certificate, and additional keys are trusted during a rolling root rotation
func WithRuntimePSKs(psks ...[]byte) HostOption {
	return func(o *newHostOptions) { o.RuntimePSKs = psks }
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

// WithStandaloneMemoryProvider sets the standalone in-memory provider
func WithStandaloneMemoryProvider(opts standalone.StandaloneMemoryOptions) HostOption {
	return func(o *newHostOptions) { o.ProviderOptions = opts }
}

// WithStandaloneSQLiteProvider sets the standalone SQLite-backed provider
func WithStandaloneSQLiteProvider(opts standalone.StandaloneSQLiteOptions) HostOption {
	return func(o *newHostOptions) { o.ProviderOptions = opts }
}

// WithStandalonePostgresProvider sets the standalone Postgres-backed provider
func WithStandalonePostgresProvider(opts standalone.StandalonePostgresOptions) HostOption {
	return func(o *newHostOptions) { o.ProviderOptions = opts }
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

// WithMaxInFlightRequests sets how many peer invocations this host's peer server processes concurrently per session
// Invocations past the limit are rejected with a retryable overloaded error so callers back off and re-resolve
func WithMaxInFlightRequests(n int) HostOption {
	return func(o *newHostOptions) { o.MaxInFlightRequests = n }
}

// WithMaxHosts caps the number of hosts allowed in the cluster at the same time
// A value of 0 (the default) means no limit
// The limit is enforced when a host registers, so a host that would exceed it fails to start with components.ErrClusterFull
// All hosts in a cluster must be configured with the same value; changing it requires shutting the whole cluster down first
func WithMaxHosts(n int) HostOption {
	return func(o *newHostOptions) { o.MaxHosts = n }
}

// WithMaxRequestBodySize caps the size of a streamed peer invocation request body this host will accept, in bytes
func WithMaxRequestBodySize(n int64) HostOption {
	return func(o *newHostOptions) { o.MaxRequestBodySize = n }
}

type newHostOptions struct {
	Address                   string
	BindPort                  int
	BindAddress               string
	RuntimePSKs               [][]byte
	Logger                    *slog.Logger
	ProviderOptions           components.ProviderOptions
	HostHealthCheckDeadline   time.Duration
	AlarmsPollInterval        time.Duration
	AlarmsLeaseDuration       time.Duration
	AlarmsFetchAheadInterval  time.Duration
	AlarmsFetchAheadBatchSize int
	ShutdownGracePeriod       time.Duration
	ProviderRequestTimeout    time.Duration
	MaxInFlightRequests       int
	MaxRequestBodySize        int64
	MaxHosts                  int

	// Allows setting a clock for testing
	clock clock.WithTicker
}
