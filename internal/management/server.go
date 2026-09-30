package management

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"time"
	"uuid"

	"github.com/italypaleale/francis/components"
	"k8s.io/utils/clock"
)

const (
	readHeaderTimeout = 10 * time.Second
	readTimeout       = 30 * time.Second
	writeTimeout      = 90 * time.Second
	idleTimeout       = 120 * time.Second
	maxHeaderBytes    = 16 << 10
	// maxRequestBodySize bounds the body of action requests
	maxRequestBodySize = 64 << 10
	// requestTimeout bounds the work done for a single request
	requestTimeout = 60 * time.Second
	// shutdownTimeout bounds how long in-flight requests are given to finish on shutdown
	shutdownTimeout = 10 * time.Second
)

// Server serves the management API
type Server struct {
	cfg     Config
	backend Backend
	auth    *authenticator
	log     *slog.Logger
	audit   *slog.Logger
	clock   clock.PassiveClock
	handler http.Handler

	// hostTimeout bounds each request sent to a host
	hostTimeout time.Duration
	// fanOutConcurrency bounds the number of hosts queried concurrently
	fanOutConcurrency int
}

// Option customizes a Server
type Option func(*Server)

// WithLogger sets the logger
func WithLogger(log *slog.Logger) Option {
	return func(s *Server) {
		s.log = log
	}
}

// WithClock sets the clock, used by tests
func WithClock(c clock.PassiveClock) Option {
	return func(s *Server) {
		s.clock = c
	}
}

// NewServer returns a management server for the backend
// The configuration must have been validated
func NewServer(cfg Config, backend Backend, opts ...Option) (*Server, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}
	if backend == nil {
		return nil, errors.New("management backend is required")
	}

	s := &Server{
		cfg:               cfg,
		backend:           backend,
		auth:              newAuthenticator(cfg),
		hostTimeout:       10 * time.Second,
		fanOutConcurrency: 16,
	}
	for _, o := range opts {
		o(s)
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.clock == nil {
		s.clock = clock.RealClock{}
	}
	s.audit = s.log.With(slog.String("audit", "management"))
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
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		MaxHeaderBytes:    maxHeaderBytes,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
		// Requests keep the values of ctx but not its cancellation, so the shutdown below gives in-flight requests time to finish instead of canceling them all at once
		BaseContext: func(net.Listener) context.Context {
			return context.WithoutCancel(ctx)
		},
	}

	// Actor state and workflow payloads are readable through the API, so a plaintext listener reachable from other machines deserves a warning
	if s.cfg.TLSConfig == nil && !isLoopbackBind(ln.Addr().String()) {
		s.log.WarnContext(ctx, "Management API is listening on a non-loopback address without TLS; terminate TLS in a proxy or configure TLS", slog.String("bind", ln.Addr().String()))
	}

	// Stop the server when the context is canceled
	errCh := make(chan error, 1)
	go func() {
		var serveErr error
		if s.cfg.TLSConfig != nil {
			serveErr = srv.ServeTLS(ln, "", "")
		} else {
			serveErr = srv.Serve(ln)
		}
		errCh <- serveErr
	}()

	s.log.InfoContext(ctx, "Management API server started", slog.String("bind", ln.Addr().String()), slog.Bool("tls", s.cfg.TLSConfig != nil))

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("error running management API server: %w", err)
	case <-ctx.Done():
	}

	// Give in-flight requests a bounded time to finish
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	err := srv.Shutdown(shutdownCtx)
	if err != nil {
		s.log.WarnContext(ctx, "Management API server shutdown error", slog.Any("error", err))
		_ = srv.Close()
	}
	<-errCh

	return nil
}

type requestIDCtxKey struct{}

// requestIDFromContext returns the request ID stored in the context
func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDCtxKey{}).(string)
	return id
}

// handlerFunc is a route handler that returns an error response instead of writing it
type handlerFunc func(w http.ResponseWriter, r *http.Request) *apiError

// route wraps a handler with request ID, authentication, authorization, timeouts and panic recovery
// scope is the scope the route requires, and an empty scope makes the route public
func (s *Server) route(scope Scope, h handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Assign a request ID, used in error responses and audit records
		reqID := uuid.NewV7().String()
		w.Header().Set("X-Request-Id", reqID)
		ctx := context.WithValue(r.Context(), requestIDCtxKey{}, reqID)

		ctx, cancel := context.WithTimeout(ctx, requestTimeout)
		defer cancel()
		r = r.WithContext(ctx)

		defer func() {
			rec := recover()
			if rec != nil {
				s.log.ErrorContext(ctx, "Panic in management API handler", slog.Any("panic", rec), slog.String("stack", string(debug.Stack())))
				writeError(w, r, newAPIError(http.StatusInternalServerError, CodeInternal, "internal error"))
			}
		}()

		if scope != "" {
			// Authenticate the caller first, then authorize the route from its declared scope
			caller := s.auth.authenticate(r)
			if caller == nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="francis-management"`)
				writeError(w, r, newAPIError(http.StatusUnauthorized, CodeUnauthorized, "a valid bearer token is required"))
				return
			}
			if !caller.Has(scope) {
				writeError(w, r, newAPIErrorf(http.StatusForbidden, CodeForbidden, "the token does not grant the '%s' scope", scope))
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), callerCtxKey{}, caller))
		}

		// Bound the body of every request, since no route takes a large one
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
		}

		apiErr := h(w, r)
		if apiErr != nil {
			writeError(w, r, apiErr)
		}
	})
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

	// The provider refused a write because an exclusive-access lease was taken after checkExclusiveLease
	if errors.Is(err, components.ErrClusterLocked) {
		return s.clusterLockedError(r)
	}

	s.log.ErrorContext(r.Context(), "Management API request failed", slog.String("operation", msg), slog.String("requestId", requestIDFromContext(r.Context())), slog.Any("error", err))
	return newAPIError(http.StatusInternalServerError, CodeInternal, msg)
}

// checkExclusiveLease refuses actions while a clusteradmin exclusive-access lease is held, since hosts may be being evicted for a restore
// A lease taken after this check is caught by the provider write the action makes, which then fails with components.ErrClusterLocked
func (s *Server) checkExclusiveLease(r *http.Request) *apiError {
	lease, err := s.backend.Provider().GetExclusiveLease(r.Context())
	if err != nil {
		return s.fail(r, "failed to read the exclusive-access lease", err)
	}
	if lease.IsHeld() {
		return exclusiveLeaseHeldError(lease)
	}
	return nil
}

// clusterLockedError is the error of an action whose provider write was refused because an exclusive-access lease is held
func (s *Server) clusterLockedError(r *http.Request) *apiError {
	// The lease is read again for its owner and expiry, which are left out if it was released in the meantime
	lease, err := s.backend.Provider().GetExclusiveLease(r.Context())
	if err != nil {
		lease = components.ExclusiveLeaseInfo{}
	}
	return exclusiveLeaseHeldError(lease)
}

// exclusiveLeaseHeldError is the error of an action refused while an exclusive-access lease is held, with the lease's owner and expiry when known
func exclusiveLeaseHeldError(lease components.ExclusiveLeaseInfo) *apiError {
	apiErr := newAPIError(http.StatusConflict, CodeExclusiveLeaseHeld, "an exclusive-access lease is held on the cluster, so actions are refused").retryable()
	if !lease.IsHeld() {
		return apiErr
	}
	return apiErr.withDetails(map[string]any{
		"owner":     lease.Owner,
		"expiresAt": lease.ExpiresAt.UTC(),
	})
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
