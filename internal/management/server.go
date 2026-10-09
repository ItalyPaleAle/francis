package management

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/dashboardserver"
	"github.com/italypaleale/francis/internal/netutils"
	"k8s.io/utils/clock"
)

const (
	// maxRequestBodySize limits the body of action requests
	maxRequestBodySize = 64 << 10 // 64KB
	// requestTimeout limits the work done for a single request
	requestTimeout = 60 * time.Second
)

// Server serves the management API
type Server struct {
	cfg     Config
	backend Backend
	auth    *authenticator
	cors    corsPolicy
	log     *slog.Logger
	audit   *slog.Logger
	clock   clock.PassiveClock
	handler http.Handler

	// dashboard serves the compiled dashboard, nil when none is configured
	dashboard *dashboardserver.Dashboard

	// hostTimeout limits each request sent to a host
	hostTimeout time.Duration
	// fanOutConcurrency limits the number of hosts queried concurrently
	fanOutConcurrency int
}

// ServerOptions contains the options for NewServer
type ServerOptions struct {
	// Config is the management API configuration, which NewServer validates
	Config Config
	// Backend is the process that runs the server, such as the standalone runtime or a local host
	Backend Backend
	// Logger is the slog logger
	// If nil, logs are discarded
	Logger *slog.Logger
	// Clock timestamps responses and scheduled work, and tests replace it with a fake clock
	// If nil, the real clock is used
	Clock clock.PassiveClock
}

// NewServer returns a management server for the backend
func NewServer(opts ServerOptions) (*Server, error) {
	err := opts.Config.Validate()
	if err != nil {
		return nil, err
	}
	if opts.Backend == nil {
		return nil, errors.New("management backend is required")
	}

	// Fill in the optional dependencies
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Clock == nil {
		opts.Clock = clock.RealClock{}
	}

	s := &Server{
		cfg:               opts.Config,
		backend:           opts.Backend,
		auth:              newAuthenticator(opts.Config),
		cors:              newCORSPolicy(opts.Config.AllowedOrigins),
		log:               opts.Logger,
		audit:             opts.Logger.With(slog.String("audit", "management")),
		clock:             opts.Clock,
		hostTimeout:       10 * time.Second,
		fanOutConcurrency: 16,
	}

	// Load the dashboard's files
	if opts.Config.Dashboard != nil {
		s.dashboard, err = dashboardserver.New(dashboardserver.Options{
			Files: opts.Config.Dashboard,
			Mode:  dashboardserver.ModeEmbedded,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load the management dashboard: %w", err)
		}
	}

	s.handler = s.routes()

	return s, nil
}

// Handler returns the HTTP handler, used by tests
func (s *Server) Handler() http.Handler {
	return s.handler
}

// Run listens on the configured address and serves until the context is canceled
func (s *Server) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.cfg.Bind)
	if err != nil {
		return fmt.Errorf("error listening for management API connections: %w", err)
	}

	return s.Serve(ctx, ln)
}

// Serve serves on the listener until the context is canceled
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s.handler,
		TLSConfig:         s.cfg.TLSConfig,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      90 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    16 << 10, // 16KB
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
		// Requests keep the values of ctx but not its cancellation, so the shutdown below gives in-flight requests time to finish instead of canceling them all at once
		BaseContext: func(net.Listener) context.Context {
			return context.WithoutCancel(ctx)
		},
	}

	// Actor state and workflow payloads are readable through the API, so a plaintext listener reachable from other machines deserves a warning
	if s.cfg.TLSConfig == nil && !netutils.IsLoopbackBind(ln.Addr().String()) {
		s.log.WarnContext(ctx,
			"Management API is listening on a non-loopback address without TLS, terminate TLS in a proxy or configure TLS",
			slog.String("bind", ln.Addr().String()),
		)
	}

	// Stop the server when the context is canceled
	errCh := make(chan error, 1)
	go func() {
		if s.cfg.TLSConfig != nil {
			errCh <- srv.ServeTLS(ln, "", "")
		} else {
			errCh <- srv.Serve(ln)
		}
	}()

	s.log.InfoContext(ctx, "Management API server started", slog.String("bind", ln.Addr().String()), slog.Bool("tls", s.cfg.TLSConfig != nil))

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return fmt.Errorf("error running management API server: %w", err)
	case <-ctx.Done():
		// Fallthrough
	}

	// Give in-flight requests a limited time to finish
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	if err != nil {
		s.log.WarnContext(ctx, "Management API server shutdown error", slog.Any("error", err))
		_ = srv.Close()
	}
	<-errCh

	return nil
}

// fail converts an unexpected error into a response, logging it when it is an internal error
func (s *Server) fail(r *http.Request, msg string, err error) *apiError {
	apiErr := backendError(err)
	if apiErr != nil {
		return apiErr
	}

	ae, ok := errors.AsType[*apiError](err)
	if ok {
		return ae
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return newAPIErrorf(http.StatusGatewayTimeout, CodeTimeout, "%s: the request timed out", msg).retryable()
	}

	// The provider refused the action's write because an exclusive-access lease is held, which it checks atomically with the write
	// The lease's holder isn't read again for the error, since GET /api/v1/cluster/summary reports it
	if errors.Is(err, components.ErrClusterLocked) {
		return newAPIError(http.StatusConflict, CodeExclusiveLeaseHeld, "an exclusive-access lease is held on the cluster, so the action was refused").retryable()
	}

	s.log.ErrorContext(r.Context(),
		"Management API request failed",
		slog.String("operation", msg),
		slog.String("requestId", requestIDFromContext(r.Context())),
		slog.Any("error", err),
	)

	return newAPIError(http.StatusInternalServerError, CodeInternal, msg)
}

// listAllHosts returns every host with a live registration, paging through the provider
func (s *Server) listAllHosts(ctx context.Context) ([]components.HostDetails, error) {
	var (
		res   []components.HostDetails
		after string
	)
	for {
		page, err := s.backend.Provider().ListHostDetails(ctx, components.ListHostDetailsReq{
			After: after,
			Limit: components.MaxManagementListLimit,
		})
		if err != nil {
			return nil, err
		}
		res = append(res, page.Hosts...)
		if !page.HasMore || len(page.Hosts) == 0 {
			return res, nil
		}
		after = page.Hosts[len(page.Hosts)-1].HostID
	}
}
