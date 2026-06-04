package peer

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// InvokeHandler executes an object invocation for an actor owned by this host
// It returns the encoded result, or a structured error such as actor_halted or invoke_failed
type InvokeHandler func(ctx context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error)

// StreamHandler executes a stream invocation for an actor owned by this host
// It reads the request body from body and writes the response to w, returning a structured error if the invocation fails
type StreamHandler func(ctx context.Context, req protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error

// ServerConfig configures a Server
type ServerConfig struct {
	// Bind is the address and port the peer server listens on
	Bind string
	// TLSConfig is the server TLS configuration
	TLSConfig *tls.Config
	// IdleTimeout closes a session after it has been idle this long
	IdleTimeout time.Duration
	// Handler runs object invocations
	// If nil, object invocation is unsupported
	Handler InvokeHandler
	// StreamHandler runs stream invocations
	// If nil, stream invocation is unsupported
	StreamHandler StreamHandler
	// Auth authorizes incoming sessions
	// If nil, there's no authnorization (we still use TLS for transport-level encryption)
	Auth Authenticator
	// Log is the slog logger
	Log *slog.Logger

	// HostID returns this host's current runtime-assigned ID, used to reject invocations aimed at a stale placement
	HostID func() string
}

// Server accepts host-to-host actor invocations over WebTransport
type Server struct {
	cfg ServerConfig
}

// NewServer returns a Server with defaults filled in
func NewServer(cfg ServerConfig) *Server {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	return &Server{
		cfg: cfg,
	}
}

// Run starts the peer WebTransport server and blocks until the context is canceled
func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	srv := wt.NewServer(s.cfg.Bind, s.cfg.TLSConfig, mux, wt.WithMaxIdleTimeout(s.cfg.IdleTimeout))

	// Health endpoint for liveness probes over plain HTTP/3
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	// WebTransport endpoint that other hosts dial to invoke actors
	mux.HandleFunc(protocol.PeerConnectPath, func(w http.ResponseWriter, r *http.Request) {
		// Authorize the session before upgrading it
		// A nil authenticator means transport-level TLS is the only check
		if s.cfg.Auth != nil {
			ok, rErr := s.cfg.Auth.ValidateIncomingRequest(r)
			if rErr != nil {
				s.cfg.Log.WarnContext(r.Context(), "Error authorizing peer session", slog.Any("error", rErr))
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if !ok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		session, rErr := srv.Upgrade(w, r)
		if rErr != nil {
			s.cfg.Log.WarnContext(r.Context(), "Failed to upgrade peer WebTransport session", slog.Any("error", rErr))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		go s.serveSession(ctx, session)
	})

	s.cfg.Log.InfoContext(ctx, "Peer WebTransport server started", slog.String("bind", s.cfg.Bind))

	// Start the server in a goroutine
	srvErrCh := make(chan error, 1)
	go func() {
		rErr := srv.ListenAndServe()
		if wt.IsServeError(rErr) {
			srvErrCh <- fmt.Errorf("error running peer WebTransport server: %w", rErr)
			return
		}
		srvErrCh <- nil
	}()

	select {
	case err := <-srvErrCh:
		return err
	case <-ctx.Done():
		// Fall through to graceful shutdown
	}

	// Close the server, terminating all peer sessions
	err := srv.Close()
	if err != nil {
		s.cfg.Log.WarnContext(ctx, "Peer WebTransport server shutdown error", slog.Any("error", err))
	}

	return nil
}

// serveSession dispatches each stream of a peer session as an independent invocation
func (s *Server) serveSession(ctx context.Context, session *webtransport.Session) {
	sessCtx := session.Context()

	// Close the session if the host shuts down
	go func() {
		select {
		case <-ctx.Done():
			_ = session.CloseWithError(0, "host shutting down")
		case <-sessCtx.Done():
			// Noop
		}
	}()

	// Each invocation arrives on its own stream and is handled concurrently
	for {
		stream, err := session.AcceptStream(sessCtx)
		if err != nil {
			return
		}

		go s.handleStream(sessCtx, stream)
	}
}

// handleStream reads one invocation from a stream, validates it, and dispatches by invocation mode
func (s *Server) handleStream(ctx context.Context, stream *webtransport.Stream) {
	defer stream.Close()

	// Read the invocation metadata frame
	// For stream invocation the request body follows it on the same stream
	req, err := protocol.ReadMessage(stream)
	if err != nil {
		return
	}

	// The only message a peer sends is an actor invocation
	if req.Kind != protocol.KindInvokeActor {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeBadRequest, "unexpected message kind %q", req.Kind)))
		return
	}

	// Decode the invocation metadata
	var payload protocol.InvokeActorRequest
	err = req.DecodePayload(&payload)
	if err != nil {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode invocation request")))
		return
	}

	// Reject invocations meant for a different host, which indicate the caller's placement is stale
	if payload.TargetHostID != "" && payload.TargetHostID != s.cfg.HostID() {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeHostMismatch, "invocation is for a different host")))
		return
	}

	// Dispatch by invocation mode
	// Object responses are a single frame, stream responses carry a trailing body
	switch payload.Mode {
	case protocol.InvocationModeObject:
		_ = protocol.WriteMessage(stream, s.handleObject(ctx, req, payload))
	case protocol.InvocationModeStream:
		s.handleStreamInvoke(ctx, stream, req, payload)
	default:
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeInvokeModeUnsupported, "unsupported invocation mode %d", payload.Mode)))
	}
}

// handleObject runs an object invocation and returns the response envelope
func (s *Server) handleObject(ctx context.Context, req *protocol.Envelope, payload protocol.InvokeActorRequest) *protocol.Envelope {
	// An actor type that does not implement object invocation cannot be called this way
	if s.cfg.Handler == nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, "object invocation is not supported"))
	}

	// Run the actor locally and relay any structured failure back to the caller
	out, perr := s.cfg.Handler(ctx, payload)
	if perr != nil {
		return req.ErrorReply(perr)
	}

	// Return the encoded result
	resp, err := req.ReplyWith(protocol.KindInvokeActorResponse, out)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode invocation response"))
	}

	return resp
}

// handleStreamInvoke runs a stream invocation, reading the request body from the stream and writing the response metadata followed by the response body
func (s *Server) handleStreamInvoke(ctx context.Context, stream *webtransport.Stream, req *protocol.Envelope, payload protocol.InvokeActorRequest) {
	// An actor type that does not implement stream invocation cannot be called this way
	if s.cfg.StreamHandler == nil {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, "stream invocation is not supported")))
		return
	}

	// The remaining bytes on the stream are the request body
	// The handler writes the response through w
	// w emits the response metadata frame on its first Write, so the content type can be set before any body bytes are produced
	w := &peerStreamWriter{stream: stream, req: req}
	perr := s.cfg.StreamHandler(ctx, payload, stream, w)
	if perr != nil {
		// We can only report a structured error if the response metadata frame has not been sent yet
		// Once the body has started, a mid-stream failure surfaces as a truncated body when the stream closes
		if !w.flushed {
			_ = protocol.WriteMessage(stream, req.ErrorReply(perr))
		}
		return
	}

	// A handler that returned without writing anything produces an empty (no-content) response
	if !w.flushed {
		respEnv, err := req.ReplyWith(protocol.KindInvokeActorResponse, protocol.InvokeActorResponse{NoContent: true})
		if err != nil {
			_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode invocation response")))
			return
		}

		_ = protocol.WriteMessage(stream, respEnv)
	}
}
