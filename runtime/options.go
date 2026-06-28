package runtime

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/internal/bootstrapauth"
)

// defaultWorkloadCertTTL is the default lifetime of a host workload certificate
// A short lifetime keeps the renewal path exercised and bounds how long a revoked host stays trusted
const defaultWorkloadCertTTL = time.Hour

// RuntimeOption configures a Runtime
type RuntimeOption func(*runtimeOptions)

// newRuntimeOptions returns a runtimeOptions pre-populated with default values
func newRuntimeOptions() *runtimeOptions {
	return &runtimeOptions{
		logger:                  slog.New(slog.DiscardHandler),
		meter:                   noop.NewMeterProvider().Meter(""),
		workloadCertTTL:         defaultWorkloadCertTTL,
		hostHealthCheckDeadline: 20 * time.Second,
		alarmsPollInterval:      1500 * time.Millisecond,
		providerRequestTimeout:  15 * time.Second,
		alarmExecutionTimeout:   60 * time.Second,
		shutdownGracePeriod:     30 * time.Second,
		clock:                   &clock.RealClock{},
	}
}

type runtimeOptions struct {
	bind string
	// runtimePSKs is the ordered list of runtime pre-shared keys, each deriving one CA, with index 0 being the primary used to mint new certificates
	// More than one entry is used only during a rolling root rotation
	runtimePSKs [][]byte
	// runtimeID identifies this runtime in its server certificate SPIFFE SAN
	runtimeID string
	// hostBootstrapPSK, when set, configures the cluster to bootstrap hosts with a host pre-shared key
	hostBootstrapPSK []byte
	// hostBootstrapJWT, when set, configures the cluster to bootstrap hosts with a JWT validated against a JWKS
	hostBootstrapJWT *bootstrapauth.JWTConfig
	// workloadCertTTL is the lifetime of issued host workload certificates
	workloadCertTTL         time.Duration
	logger                  *slog.Logger
	meter                   metric.Meter
	hostHealthCheckDeadline time.Duration
	alarmsPollInterval      time.Duration
	providerRequestTimeout  time.Duration
	alarmExecutionTimeout   time.Duration
	shutdownGracePeriod     time.Duration

	// Allows setting a clock for testing
	clock clock.WithTicker
}

// WithBind sets the address and port the runtime WebTransport server listens on
func WithBind(bind string) RuntimeOption {
	return func(o *runtimeOptions) { o.bind = bind }
}

// WithRuntimePSKs sets the runtime pre-shared keys from which the cluster CA is derived
// The first key is the primary used to mint new certificates, and additional keys are trusted during a rolling root rotation
func WithRuntimePSKs(psks ...[]byte) RuntimeOption {
	return func(o *runtimeOptions) { o.runtimePSKs = psks }
}

// WithRuntimeID sets the identity placed in the runtime server certificate
func WithRuntimeID(id string) RuntimeOption {
	return func(o *runtimeOptions) { o.runtimeID = id }
}

// WithHostBootstrapPSK configures the cluster to authenticate joining hosts with a host pre-shared key via a channel-bound challenge-response
func WithHostBootstrapPSK(hostPSK []byte) RuntimeOption {
	return func(o *runtimeOptions) { o.hostBootstrapPSK = hostPSK }
}

// WithHostBootstrapJWT configures the cluster to authenticate joining hosts with a JWT validated against the given configuration
func WithHostBootstrapJWT(cfg bootstrapauth.JWTConfig) RuntimeOption {
	return func(o *runtimeOptions) { o.hostBootstrapJWT = &cfg }
}

// WithWorkloadCertTTL sets the lifetime of issued host workload certificates
func WithWorkloadCertTTL(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.workloadCertTTL = d }
}

// WithLogger sets the slog logger
func WithLogger(logger *slog.Logger) RuntimeOption {
	return func(o *runtimeOptions) { o.logger = logger }
}

// WithMeter sets the OpenTelemetry meter used to record runtime metrics
func WithMeter(meter metric.Meter) RuntimeOption {
	return func(o *runtimeOptions) {
		if meter != nil {
			o.meter = meter
		}
	}
}

// WithHostHealthCheckDeadline sets the maximum interval between health checks received from a host
func WithHostHealthCheckDeadline(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.hostHealthCheckDeadline = d }
}

// WithAlarmsPollInterval sets the interval for polling alarms
func WithAlarmsPollInterval(d time.Duration) RuntimeOption {
	return func(o *runtimeOptions) { o.alarmsPollInterval = d }
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
