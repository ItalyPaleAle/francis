package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"github.com/quic-go/webtransport-go"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/bootstrapauth"
	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// Runtime is a standalone Francis runtime
// It coordinates database access on behalf of the application hosts connected to it: host registration, placement lookup, state, alarms, health checks, and alarm dispatch
type Runtime struct {
	provider components.ActorProvider
	hosts    *HostManager

	bind            string
	serverTLSConfig *tls.Config

	// cas is the CA bundle derived from the runtime PSKs, index 0 being the primary used to mint new certificates
	cas []*ca.CA
	// runtimeID identifies this runtime in its server certificate
	runtimeID string
	// workloadCertTTL is the lifetime of issued host workload certificates
	workloadCertTTL time.Duration
	// bootstrapMethod is the cluster-wide host bootstrap method, either "psk" or "jwt"
	bootstrapMethod string
	// bootstrapPSK validates host PSK challenge-responses, set when bootstrapMethod is "psk"
	bootstrapPSK *bootstrapauth.PSK
	// bootstrapJWTConfig configures the JWT validator, built in Run so its JWKS refresh goroutine is tied to the run context
	bootstrapJWTConfig *bootstrapauth.JWTConfig
	// bootstrapJWT validates host bootstrap tokens, set when bootstrapMethod is "jwt"
	bootstrapJWT *bootstrapauth.JWTValidator

	hostHealthCheckDeadline time.Duration
	alarmsPollInterval      time.Duration
	providerRequestTimeout  time.Duration
	alarmExecutionTimeout   time.Duration
	shutdownGracePeriod     time.Duration

	// placementCache holds short-lived actor placement lookups to reduce provider load
	// A nil cache disables runtime-side caching
	placementCache *ttlcache.Cache[string, *cachedPlacement]

	// alarmProcessor schedules leased alarms for dispatch at their due time
	alarmProcessor *eventqueue.Processor[string, *ref.AlarmLease]
	// activeAlarms and retryingAlarms track in-flight and retrying alarms by key, guarded by activeAlarmsLock
	activeAlarmsLock sync.Mutex
	activeAlarms     map[string]struct{}
	retryingAlarms   map[string]struct{}
	// alarmsDraining is set during shutdown to stop new executions from starting, also guarded by activeAlarmsLock
	alarmsDraining bool
	// alarmWg counts in-flight alarm executions so shutdown can wait for them to finish
	alarmWg sync.WaitGroup

	// sendToHost dispatches a request to a connected host and returns its response
	// Tests can substitute a fake host transport
	sendToHost func(ctx context.Context, c *hostConn, env *protocol.Envelope) (*protocol.Envelope, error)

	log   *slog.Logger
	clock clock.WithTicker
}

// NewRuntime returns a new Runtime backed by the given actor provider
func NewRuntime(provider components.ActorProvider, opts ...RuntimeOption) (*Runtime, error) {
	if provider == nil {
		return nil, errors.New("provider is required")
	}

	options := newRuntimeOptions()
	for _, opt := range opts {
		opt(options)
	}

	if options.bind == "" {
		return nil, errors.New("option Bind is required")
	}
	if len(options.runtimePSKs) == 0 {
		return nil, errors.New("option RuntimePSKs is required")
	}

	// Derive the CA bundle from the runtime PSKs so every runtime sharing the PSKs acts as the same issuer without coordination
	cas, err := ca.CABundle(options.runtimePSKs)
	if err != nil {
		return nil, fmt.Errorf("failed to derive cluster CA: %w", err)
	}

	// Default the runtime ID to a random value so its server certificate has a stable identity for the process lifetime
	runtimeID := options.runtimeID
	if runtimeID == "" {
		runtimeID = uuid.NewString()
	}

	// Mint the runtime server certificate from the primary CA, which hosts verify against the pinned or in-band CA bundle
	serverCert, err := mintServerCert(cas[0], runtimeID)
	if err != nil {
		return nil, fmt.Errorf("failed to mint runtime server certificate: %w", err)
	}

	// Resolve the cluster-wide host bootstrap method, which must be exactly one of PSK or JWT
	bootstrapMethod, bootstrapPSK, err := resolveBootstrap(options)
	if err != nil {
		return nil, err
	}

	// The runtime requests but does not require a client certificate, so a first-time bootstrap that presents no certificate and an mTLS reconnect that does both reach the registration handler, which is the authority on whether a session is authenticated
	serverTLSConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequestClientCert,
	}

	rt := &Runtime{
		provider:                provider,
		hosts:                   NewHostManager(),
		bind:                    options.bind,
		serverTLSConfig:         serverTLSConfig,
		cas:                     cas,
		runtimeID:               runtimeID,
		workloadCertTTL:         options.workloadCertTTL,
		bootstrapMethod:         bootstrapMethod,
		bootstrapPSK:            bootstrapPSK,
		bootstrapJWTConfig:      options.hostBootstrapJWT,
		hostHealthCheckDeadline: options.hostHealthCheckDeadline,
		alarmsPollInterval:      options.alarmsPollInterval,
		providerRequestTimeout:  options.providerRequestTimeout,
		alarmExecutionTimeout:   options.alarmExecutionTimeout,
		shutdownGracePeriod:     options.shutdownGracePeriod,
		activeAlarms:            make(map[string]struct{}),
		retryingAlarms:          make(map[string]struct{}),
		log:                     options.logger,
		clock:                   options.clock,
	}

	// By default, alarms are dispatched to hosts over their WebTransport session
	rt.sendToHost = func(ctx context.Context, c *hostConn, env *protocol.Envelope) (*protocol.Envelope, error) {
		return c.sendRequest(ctx, env)
	}
	return rt, nil
}

// Run starts the runtime and blocks until the context is canceled
func (rt *Runtime) Run(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Initialize the provider
	initCtx, initCancel := context.WithTimeout(ctx, rt.providerRequestTimeout)
	err := rt.provider.Init(initCtx)
	initCancel()
	if err != nil {
		return fmt.Errorf("failed to init provider: %w", err)
	}

	// Build the JWT validator here so its background JWKS refresh goroutine is bound to the run context
	if rt.bootstrapJWTConfig != nil {
		rt.bootstrapJWT, err = bootstrapauth.NewJWTValidator(ctx, *rt.bootstrapJWTConfig)
		if err != nil {
			return fmt.Errorf("failed to build JWT validator: %w", err)
		}
	}

	// Create the short-lived placement cache
	// The max TTL is bounded by the host health check deadline so a dead host is never served past it
	rt.placementCache = ttlcache.NewCache[string, *cachedPlacement](&ttlcache.CacheOptions{
		MaxTTL: rt.placementCacheTTL(),
	})
	defer rt.placementCache.Stop()

	rt.log.InfoContext(ctx, "Starting Francis runtime", slog.String("bind", rt.bind))

	// Run all background services
	// This blocks until the context is canceled or one of the services returns
	return servicerunner.
		NewServiceRunner(
			// Run the WebTransport server that accepts host sessions
			rt.runServer,

			// Fetch and dispatch alarms for the hosts connected to this runtime
			rt.runAlarmFetcher,

			// Renew leases for the alarms owned by connected hosts
			rt.runLeaseRenewal,

			// Run the actor provider
			rt.provider.Run,
		).
		Run(ctx)
}

// runServer runs the WebTransport server that accepts host sessions
func (rt *Runtime) runServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// Create the WebTransport server with the shared HTTP/3 and QUIC settings
	wtServer := wt.NewServer(rt.bind, rt.serverTLSConfig, mux)

	// Health endpoint for liveness probes over plain HTTP/3
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// WebTransport endpoint for host sessions
	mux.HandleFunc(protocol.RuntimeConnectPath, func(w http.ResponseWriter, r *http.Request) {
		session, rErr := wtServer.Upgrade(w, r)
		if rErr != nil {
			rt.log.WarnContext(r.Context(), "Failed to upgrade WebTransport session", slog.Any("error", rErr))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Each session is served on its own goroutine for the lifetime of the connection
		go rt.serveSession(ctx, session)
	})

	rt.log.InfoContext(ctx, "Runtime WebTransport server started", slog.String("bind", rt.bind))

	srvErr := make(chan error, 1)
	go func() {
		// Blocks until the server is closed
		rErr := wtServer.ListenAndServe()
		if wt.IsServeError(rErr) {
			srvErr <- fmt.Errorf("error running WebTransport server: %w", rErr)
			return
		}
		srvErr <- nil
	}()

	select {
	case err := <-srvErr:
		return err
	case <-ctx.Done():
		// Fall through to graceful shutdown
	}

	// Close the server - this terminates all active sessions
	err := wtServer.Close()
	if err != nil {
		rt.log.WarnContext(ctx, "Runtime WebTransport server shutdown error", slog.Any("error", err))
	}
	return nil
}

// serveSession serves a single host's WebTransport session
// The first stream must carry the registration handshake, while subsequent streams are dispatched concurrently
func (rt *Runtime) serveSession(parentCtx context.Context, session *webtransport.Session) {
	sessCtx := session.Context()

	// Close the session if the runtime shuts down
	go func() {
		select {
		case <-parentCtx.Done():
			_ = session.CloseWithError(sessionErrorShutdown, "runtime shutting down")
		case <-sessCtx.Done():
		}
	}()

	c := &hostConn{
		session: session,
		log:     rt.log,
	}

	// Perform the registration handshake on the first stream
	// This sets the host identity on c and registers it in the HostManager before any other stream is handled, so concurrent handlers never observe a half-registered connection
	ok := rt.handleRegistration(sessCtx, c)
	if !ok {
		_ = session.CloseWithError(sessionErrorSuperseded, "registration failed")
		return
	}

	rt.log.InfoContext(sessCtx, "Host connected",
		slog.String("hostId", c.hostID),
		slog.String("sessionId", c.sessionID),
		slog.String("address", c.address),
	)

	// On session teardown, remove the host from the manager and clean up the provider for a host that drained gracefully
	defer rt.handleHostDisconnect(c)

	// Accept and dispatch subsequent streams until the session ends
	for {
		stream, err := session.AcceptStream(sessCtx)
		if err != nil {
			// The session has ended (clean close, error, or runtime shutdown)
			return
		}

		go rt.handleStream(sessCtx, c, stream)
	}
}

// handleHostDisconnect removes a disconnected host from the manager and, for a host that drained gracefully, from the provider
// It runs on session teardown, only if we still own the host's current session
func (rt *Runtime) handleHostDisconnect(c *hostConn) {
	// Only act if the session being torn down is still the one tracked for this host
	// A superseded or stale session must not evict the host's newer session
	removed := rt.hosts.Remove(c.hostID, c.sessionID)
	if !removed {
		return
	}

	rt.log.Info("Host disconnected", slog.String("hostId", c.hostID))

	// A host that did not drain disconnected ungracefully: leave its provider record to the health deadline, since it may reconnect and reattach
	if !c.IsDraining() {
		return
	}

	// A draining host whose session has now closed is completely done, so remove it from the provider
	// This also reaps any active-actor placements the host did not clear while draining
	// Use a fresh context since the session context is already canceled
	ctx, cancel := context.WithTimeout(context.Background(), rt.providerRequestTimeout)
	defer cancel()
	err := rt.provider.UnregisterHost(ctx, c.hostID)
	if err != nil && !errors.Is(err, components.ErrHostUnregistered) {
		rt.log.Warn("Error unregistering drained host from provider",
			slog.String("hostId", c.hostID),
			slog.Any("error", err),
		)
	}
}

// requestReadTimeout bounds how long an inbound request frame may take to arrive on an accepted stream before it is abandoned
const requestReadTimeout = 30 * time.Second

// handleStream reads a single request from a stream, dispatches it, and writes the response
func (rt *Runtime) handleStream(ctx context.Context, c *hostConn, stream *webtransport.Stream) {
	defer stream.Close()

	req, err := protocol.ReadMessageWithTimeout(stream, requestReadTimeout)
	if err != nil {
		// Nothing we can usefully respond with if we cannot even read the request
		return
	}

	// Base the handler context on the stream's context rather than the session's
	// The stream context is canceled when this stream's write side closes, which covers both a per-request cancellation (the caller abandons the request and resets the stream) and session or connection teardown
	// The caller bounds the operation with its own context timeout, so no deadline travels on the wire

	resp := rt.dispatch(stream.Context(), c, req)
	err = protocol.WriteMessage(stream, resp)
	if err != nil {
		rt.log.WarnContext(ctx, "Failed to write response to host",
			slog.String("hostId", c.hostID),
			slog.String("kind", req.Kind),
			slog.Any("error", err),
		)
	}
}
