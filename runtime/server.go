package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/italypaleale/go-kit/ttlcache"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// runtimePath is the HTTP/3 path hosts use to establish a WebTransport session with the runtime
const runtimePath = "/runtime/v1/connect"

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
		log:                     options.logger,
		clock:                   options.clock,
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

			// Run the actor provider
			rt.provider.Run,
		).
		Run(ctx)
}

// runServer runs the WebTransport server that accepts host sessions
func (rt *Runtime) runServer(ctx context.Context) error {
	mux := http.NewServeMux()

	// Create the WebTransport server
	wtServer := &webtransport.Server{
		H3: &http3.Server{
			Addr:            rt.bind,
			TLSConfig:       rt.serverTLSConfig,
			QUICConfig:      &quic.Config{},
			EnableDatagrams: true,
			Handler:         mux,
		},
	}

	// Health endpoint for liveness probes over plain HTTP/3
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// WebTransport endpoint for host sessions
	mux.HandleFunc(runtimePath, func(w http.ResponseWriter, r *http.Request) {
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
		listenErr := wtServer.ListenAndServe()
		if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) && !errors.Is(listenErr, quic.ErrServerClosed) {
			srvErr <- fmt.Errorf("error running WebTransport server: %w", listenErr)
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
// The first stream must carry the registration handshake; subsequent streams are dispatched concurrently
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
	// The stream context is canceled when this stream's write side closes, which covers both a per-request cancellation (the peer sends STOP_SENDING for our response) and session or connection teardown
	// This gives finer-grained cancellation than the session context without firing during normal dispatch
	reqCtx := stream.Context()
	deadline, hasDeadline := req.Deadline()
	if hasDeadline {
		_ = stream.SetDeadline(deadline)
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithDeadline(reqCtx, deadline)
		defer cancel()
	}

	resp := rt.dispatch(reqCtx, c, req)
	err = protocol.WriteMessage(stream, resp)
	if err != nil {
		rt.log.WarnContext(ctx, "Failed to write response to host",
			slog.String("hostId", c.hostID),
			slog.String("kind", req.Kind),
			slog.Any("error", err),
		)
	}
}
