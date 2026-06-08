package remote

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"k8s.io/utils/clock"
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

// WithHostBootstrapPSK configures the host to bootstrap with a host pre-shared key, proven to the runtime via a channel-bound challenge-response
func WithHostBootstrapPSK(psk []byte) HostOption {
	return func(o *newHostOptions) { o.BootstrapPSK = psk }
}

// WithHostBootstrapJWT configures the host to bootstrap with a static JWT
// This is primarily useful for tests; production deployments usually use WithHostBootstrapJWTFile so a rotated token is re-read
func WithHostBootstrapJWT(token string) HostOption {
	return func(o *newHostOptions) {
		o.BootstrapTokenFn = func() (string, error) { return token, nil }
	}
}

// WithHostBootstrapJWTFile configures the host to bootstrap with a JWT read from a file
// The file is read fresh on every bootstrap so a rotated token, such as a Kubernetes projected service-account token, is picked up
func WithHostBootstrapJWTFile(path string) HostOption {
	return func(o *newHostOptions) {
		o.BootstrapTokenFn = func() (string, error) {
			b, err := os.ReadFile(path)
			if err != nil {
				return "", fmt.Errorf("failed to read bootstrap token file: %w", err)
			}
			return strings.TrimSpace(string(b)), nil
		}
	}
}

// WithPinnedCA pins one or more PEM-encoded cluster CA certificates the host trusts before its first connection
// Pinning closes the bootstrap trust gap, so the host verifies the runtime from the very first connection
// Exactly one of WithPinnedCA or WithUnsafeNoPinnedCA must be set, forcing the trust decision to be explicit
func WithPinnedCA(caPEM ...[]byte) HostOption {
	return func(o *newHostOptions) { o.PinnedCAPEM = caPEM }
}

// WithUnsafeNoPinnedCA opts out of CA pinning, trusting the runtime's certificate on the first connection
// This is unsafe: a man-in-the-middle on the first connection can impersonate the runtime, which is especially dangerous for JWT bootstrap where a bearer token would be exposed
// Exactly one of WithPinnedCA or WithUnsafeNoPinnedCA must be set, forcing the trust decision to be explicit
func WithUnsafeNoPinnedCA() HostOption {
	return func(o *newHostOptions) { o.UnsafeNoPinnedCA = true }
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

// WithMaxInFlightRequests sets how many peer invocations this host's peer server processes concurrently per session
// Invocations past the limit are rejected with a retryable overloaded error so callers back off and re-resolve
func WithMaxInFlightRequests(n int) HostOption {
	return func(o *newHostOptions) { o.MaxInFlightRequests = n }
}

// WithMaxRequestBodySize caps the size of a streamed peer invocation request body this host will accept, in bytes
func WithMaxRequestBodySize(n int64) HostOption {
	return func(o *newHostOptions) { o.MaxRequestBodySize = n }
}

type newHostOptions struct {
	Address          string
	BindPort         int
	BindAddress      string
	RuntimeAddresses []string
	// BootstrapPSK is the host pre-shared key, set when bootstrapping with PSK
	BootstrapPSK []byte
	// BootstrapTokenFn returns a fresh bootstrap JWT, set when bootstrapping with JWT
	BootstrapTokenFn func() (string, error)
	// PinnedCAPEM holds pinned cluster CA certificates trusted before the first connection
	PinnedCAPEM [][]byte
	// UnsafeNoPinnedCA opts out of pinning, trusting the runtime on first connection
	UnsafeNoPinnedCA    bool
	Logger              *slog.Logger
	ShutdownGracePeriod time.Duration
	RequestTimeout      time.Duration
	MaxInFlightRequests int
	MaxRequestBodySize  int64

	// Allows setting a clock for testing
	clock clock.WithTicker
}
