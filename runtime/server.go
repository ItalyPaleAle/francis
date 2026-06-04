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

	"github.com/italypaleale/go-kit/eventqueue"
	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"github.com/quic-go/webtransport-go"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// Runtime is a standalone Francis runtime
// It coordinates database access on behalf of the application hosts connected to it: host registration, placement lookup, state, alarms, health checks, and alarm dispatch
type Runtime struct {
	provider components.ActorProvider
	hosts    *HostManager

	bind                    string
	serverTLSConfig         *tls.Config
	hostHealthCheckDeadline time.Duration
	alarmsPollInterval      time.Duration
	providerRequestTimeout  time.Duration
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

	// Build the server TLS configuration
	// The WebTransport server sets the HTTP/3 ALPN on it
	serverTLSConfig, _, err := options.tlsOptions.GetTLSConfig()
	if err != nil {
		return nil, err
	}

	rt := &Runtime{
		provider:                provider,
		hosts:                   NewHostManager(),
		bind:                    options.bind,
		serverTLSConfig:         serverTLSConfig,
		hostHealthCheckDeadline: options.hostHealthCheckDeadline,
		alarmsPollInterval:      options.alarmsPollInterval,
		providerRequestTimeout:  options.providerRequestTimeout,
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

	// On session teardown, remove the host from the manager, but only if we still own its current session
	defer func() {
		removed := rt.hosts.Remove(c.hostID, c.sessionID)
		if removed {
			rt.log.InfoContext(sessCtx, "Host disconnected", slog.String("hostId", c.hostID))
		}
	}()

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

// handleStream reads a single request from a stream, dispatches it, and writes the response
func (rt *Runtime) handleStream(ctx context.Context, c *hostConn, stream *webtransport.Stream) {
	defer stream.Close()

	req, err := protocol.ReadMessage(stream)
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
