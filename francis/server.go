package francis

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/italypaleale/go-kit/servicerunner"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/postgres"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/components/standalone"
)

// Server is the Francis coordination server
type Server struct {
	running atomic.Bool

	provider       components.ActorProvider
	clientManager  *ClientManager
	alarmRunner    *AlarmRunner
	handlers       *Handlers

	bindAddress         string
	tlsConfig           interface{} // *tls.Config - using interface to avoid nil comparison issues
	shutdownGracePeriod time.Duration
	requestTimeout      time.Duration
	alarmsPollInterval  time.Duration

	logSource *slog.Logger
	log       *slog.Logger
	clock     clock.WithTicker
}

// NewServer creates a new Francis server
func NewServer(opts ...ServerOption) (*Server, error) {
	options := &serverOptions{}
	for _, opt := range opts {
		opt(options)
	}

	// Set defaults
	if options.BindAddress == "" {
		options.BindAddress = defaultBindAddress
	}
	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}
	if options.HostHealthCheckDeadline < time.Second {
		options.HostHealthCheckDeadline = defaultHostHealthCheckDeadline
	}
	if options.AlarmsPollInterval <= 100*time.Millisecond {
		options.AlarmsPollInterval = defaultAlarmsPollInterval
	}
	if options.AlarmsLeaseDuration < time.Second {
		options.AlarmsLeaseDuration = defaultAlarmsLeaseDuration
	}
	if options.AlarmsFetchAheadInterval < 100*time.Millisecond {
		options.AlarmsFetchAheadInterval = defaultAlarmsFetchAheadInterval
	}
	if options.AlarmsFetchAheadBatchSize <= 0 {
		options.AlarmsFetchAheadBatchSize = defaultAlarmsFetchAheadBatch
	}
	if options.ShutdownGracePeriod <= 0 {
		options.ShutdownGracePeriod = defaultShutdownGracePeriod
	}
	if options.RequestTimeout <= 0 {
		options.RequestTimeout = defaultRequestTimeout
	}
	if options.clock == nil {
		options.clock = &clock.RealClock{}
	}

	// Validate TLS config
	if options.TLSConfig == nil {
		return nil, ErrTLSRequired
	}

	// Create the provider
	var provider components.ActorProvider
	var err error
	switch x := options.ProviderOptions.(type) {
	case sqlite.SQLiteProviderOptions:
		provider, err = sqlite.NewSQLiteProvider(options.Logger, x, options.getProviderConfig())
	case *sqlite.SQLiteProviderOptions:
		provider, err = sqlite.NewSQLiteProvider(options.Logger, *x, options.getProviderConfig())
	case postgres.PostgresProviderOptions:
		provider, err = postgres.NewPostgresProvider(options.Logger, x, options.getProviderConfig())
	case *postgres.PostgresProviderOptions:
		provider, err = postgres.NewPostgresProvider(options.Logger, *x, options.getProviderConfig())
	case standalone.StandaloneMemoryProviderOptions:
		provider, err = standalone.NewStandaloneMemoryProvider(options.Logger, options.getProviderConfig())
	case *standalone.StandaloneMemoryProviderOptions:
		provider, err = standalone.NewStandaloneMemoryProvider(options.Logger, options.getProviderConfig())
	case standalone.StandaloneSQLiteProviderOptions:
		provider, err = standalone.NewStandaloneSQLiteProvider(options.Logger, x, options.getProviderConfig())
	case *standalone.StandaloneSQLiteProviderOptions:
		provider, err = standalone.NewStandaloneSQLiteProvider(options.Logger, *x, options.getProviderConfig())
	case standalone.StandalonePostgresProviderOptions:
		provider, err = standalone.NewStandalonePostgresProvider(options.Logger, x, options.getProviderConfig())
	case *standalone.StandalonePostgresProviderOptions:
		provider, err = standalone.NewStandalonePostgresProvider(options.Logger, *x, options.getProviderConfig())
	case nil:
		return nil, ErrProviderRequired
	default:
		return nil, fmt.Errorf("unsupported provider type: %T", options.ProviderOptions)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create provider: %w", err)
	}

	// Create server
	s := &Server{
		provider:            provider,
		bindAddress:         options.BindAddress,
		tlsConfig:           options.TLSConfig,
		shutdownGracePeriod: options.ShutdownGracePeriod,
		requestTimeout:      options.RequestTimeout,
		alarmsPollInterval:  options.AlarmsPollInterval,
		logSource:           options.Logger,
		log:                 options.Logger.With(slog.String("component", "francis-server")),
		clock:               options.clock,
	}

	// Create client manager
	s.clientManager = NewClientManager(s.log)

	// Create handlers
	s.handlers = NewHandlers(s.provider, s.clientManager, s.requestTimeout, s.log)

	// Create alarm runner
	s.alarmRunner = NewAlarmRunner(s.provider, s.clientManager, options.AlarmsPollInterval, s.requestTimeout, s.clock, s.log)

	return s, nil
}

// Run starts the Francis server
func (s *Server) Run(parentCtx context.Context) error {
	if !s.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer s.running.Store(false)

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// Initialize provider
	initCtx, initCancel := context.WithTimeout(parentCtx, s.requestTimeout)
	err := s.provider.Init(initCtx)
	initCancel()
	if err != nil {
		return fmt.Errorf("failed to init provider: %w", err)
	}

	s.log.InfoContext(ctx, "Francis server starting", slog.String("bind", s.bindAddress))

	// Run all services
	return servicerunner.
		NewServiceRunner(
			// Run the HTTP/3 (WebTransport) server
			s.runServer,
			// Run the alarm fetcher
			s.alarmRunner.Run,
			// Run the provider
			s.provider.Run,
			// Run client health check cleanup
			s.runClientHealthCheck,
		).
		Run(ctx)
}

func (s *Server) runServer(ctx context.Context) error {
	// Create HTTP/3 server with WebTransport support
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// WebTransport endpoint for client connections
	mux.HandleFunc("/connect", s.handleWebTransportConnection)

	server := &http3.Server{
		Addr:            s.bindAddress,
		Handler:         mux,
		EnableWebTransport: true,
		QUICConfig: &quic.Config{
			MaxIdleTimeout: 30 * time.Second,
		},
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		s.log.InfoContext(ctx, "Starting WebTransport server", slog.String("address", s.bindAddress))
		// Note: ListenAndServeTLS requires TLS config, but http3 server uses the QUIC-level TLS
		// For HTTP/3, we need to pass the cert and key files or use ListenAndServe with TLSConfig set on the server
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		s.log.InfoContext(ctx, "Shutting down WebTransport server")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), s.shutdownGracePeriod)
		defer shutdownCancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleWebTransportConnection(w http.ResponseWriter, r *http.Request) {
	// Upgrade to WebTransport
	conn, ok := w.(http3.WebTransporter)
	if !ok {
		http.Error(w, "WebTransport not supported", http.StatusUpgradeRequired)
		return
	}

	session, err := conn.WebTransport()
	if err != nil {
		s.log.Error("Failed to upgrade to WebTransport", slog.Any("error", err))
		http.Error(w, "Failed to upgrade to WebTransport", http.StatusInternalServerError)
		return
	}

	// Handle the session in a goroutine
	go s.handleClientSession(r.Context(), session)
}

func (s *Server) handleClientSession(ctx context.Context, session *http3.WebTransportSession) {
	defer session.CloseWithError(0, "session closed")

	s.log.Debug("New WebTransport session established")

	// Read messages from the client
	for {
		// Accept a bidirectional stream
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			s.log.Debug("Error accepting stream", slog.Any("error", err))
			return
		}

		// Handle the stream in a goroutine
		go s.handleStream(ctx, session, stream)
	}
}

func (s *Server) handleStream(ctx context.Context, session *http3.WebTransportSession, stream http3.Stream) {
	defer stream.Close()

	// Read the message
	msg, err := s.handlers.ReadMessage(stream)
	if err != nil {
		s.log.Debug("Error reading message", slog.Any("error", err))
		return
	}

	// Handle the message and get response
	response := s.handlers.HandleMessage(ctx, session, msg)

	// Send response
	err = response.Encode(stream)
	if err != nil {
		s.log.Debug("Error sending response", slog.Any("error", err))
	}
}

func (s *Server) runClientHealthCheck(ctx context.Context) error {
	interval := s.provider.HealthCheckInterval()
	deadline := interval * 2 // Allow 2 missed health checks before cleanup

	s.log.DebugContext(ctx, "Starting client health check cleanup", slog.Any("interval", interval))

	t := s.clock.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			stale := s.clientManager.CleanupStaleClients(deadline)
			for _, clientID := range stale {
				s.log.WarnContext(ctx, "Removed stale client", slog.String("clientId", clientID))
				// Unregister from provider
				unregCtx, unregCancel := context.WithTimeout(ctx, s.requestTimeout)
				err := s.provider.UnregisterHost(unregCtx, clientID)
				unregCancel()
				if err != nil && !errors.Is(err, components.ErrHostUnregistered) {
					s.log.ErrorContext(ctx, "Error unregistering stale client", slog.String("clientId", clientID), slog.Any("error", err))
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
