package management

import (
	"context"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"
	"uuid"

	"github.com/italypaleale/go-kit/httpserver"
)

type requestIDCtxKey struct{}

// requestIDFromContext returns the request ID stored in the context
func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(requestIDCtxKey{}).(string)
	return id
}

// handlerFunc is a route handler that returns an error response instead of writing it
type handlerFunc func(w http.ResponseWriter, r *http.Request) *apiError

// ServeHTTP runs the handler and writes the error it returns, which makes handlerFunc an http.Handler
func (h handlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiErr := h(w, r)
	if apiErr != nil {
		writeError(w, r, apiErr)
	}
}

// middlewareRequestID assigns every request an ID, which error responses and audit records carry
func middlewareRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := uuid.NewV7().String()
		w.Header().Set("X-Request-ID", reqID)

		r = r.WithContext(context.WithValue(r.Context(), requestIDCtxKey{}, reqID))
		next.ServeHTTP(w, r)
	})
}

// middlewareRecover turns a panic in a handler into a JSON 500 response instead of a dropped connection
func (s *Server) middlewareRecover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			rec := recover()
			if rec != nil {
				s.log.ErrorContext(r.Context(),
					"Panic in management API handler",
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
				writeError(w, r, newAPIError(http.StatusInternalServerError, CodeInternal, "internal error"))
			}
		}()

		next.ServeHTTP(w, r)
	})
}

// middlewareTimeout limits the work done for a single request
func middlewareTimeout(timeout time.Duration) httpserver.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// middlewareAuthenticate authenticates the caller from its bearer token, without requiring any scope
// The caller is stored in the request context, where handlers and audit records read it
func (s *Server) middlewareAuthenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller := s.auth.authenticate(r)
		if caller == nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="francis-management"`)
			writeError(w, r, newAPIError(http.StatusUnauthorized, CodeUnauthorized, "a valid bearer token is required"))
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), callerCtxKey{}, caller)))
	})
}

// middlewareRequireScope authenticates the caller from its bearer token and authorizes it for the scope the route requires
func (s *Server) middlewareRequireScope(scope Scope) httpserver.Middleware {
	return func(next http.Handler) http.Handler {
		// Authentication runs first and stores the caller, which is then authorized from the route's declared scope
		authorize := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !callerFromContext(r.Context()).Has(scope) {
				writeError(w, r, newAPIErrorf(http.StatusForbidden, CodeForbidden, "the token does not grant the '%s' scope", scope))
				return
			}

			next.ServeHTTP(w, r)
		})
		return s.middlewareAuthenticate(authorize)
	}
}
