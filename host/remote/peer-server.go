package remote

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// peerInvokeHandler executes an object invocation for an actor owned by this host
// It returns the encoded result, or a structured error such as actor_halted or invoke_failed
type peerInvokeHandler func(ctx context.Context, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error)

// peerStreamHandler executes a stream invocation for an actor owned by this host
// It reads the request body from body and writes the response to w, returning a structured error if the invocation fails
type peerStreamHandler func(ctx context.Context, req protocol.InvokeActorRequest, body io.Reader, w actor.StreamResponseWriter) *protocol.Error

// peerServerConfig configures a peerServer
type peerServerConfig struct {
	bind          string
	tlsConfig     *tls.Config
	handler       peerInvokeHandler
	streamHandler peerStreamHandler
	log           *slog.Logger

	// hostID returns this host's current runtime-assigned ID, used to reject invocations aimed at a stale placement
	hostID func() string
}

// peerServer accepts host-to-host actor invocations over WebTransport
type peerServer struct {
	cfg peerServerConfig
}

// newPeerServer returns a peerServer with defaults filled in
func newPeerServer(cfg peerServerConfig) *peerServer {
	if cfg.log == nil {
		cfg.log = slog.New(slog.DiscardHandler)
	}

	return &peerServer{
		cfg: cfg,
	}
}

// Run starts the peer WebTransport server and blocks until the context is canceled
func (ps *peerServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	srv := wt.NewServer(ps.cfg.bind, ps.cfg.tlsConfig, mux)

	// WebTransport endpoint that other hosts dial to invoke actors
	mux.HandleFunc(protocol.PeerConnectPath, func(w http.ResponseWriter, r *http.Request) {
		session, rErr := srv.Upgrade(w, r)
		if rErr != nil {
			ps.cfg.log.WarnContext(r.Context(), "Failed to upgrade peer WebTransport session", slog.Any("error", rErr))
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		go ps.serveSession(ctx, session)
	})

	ps.cfg.log.InfoContext(ctx, "Peer WebTransport server started", slog.String("bind", ps.cfg.bind))

	// Serve in the background so we can shut down when the context is canceled
	srvErr := make(chan error, 1)
	go func() {
		rErr := srv.ListenAndServe()
		if wt.IsServeError(rErr) {
			srvErr <- fmt.Errorf("error running peer WebTransport server: %w", rErr)
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

	// Close the server, terminating all peer sessions
	err := srv.Close()
	if err != nil {
		ps.cfg.log.WarnContext(ctx, "Peer WebTransport server shutdown error", slog.Any("error", err))
	}

	return nil
}

// serveSession dispatches each stream of a peer session as an independent invocation
func (ps *peerServer) serveSession(ctx context.Context, session *webtransport.Session) {
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

		go ps.handleStream(sessCtx, stream)
	}
}

// handleStream reads one invocation from a stream, validates it, and dispatches by invocation mode
func (ps *peerServer) handleStream(ctx context.Context, stream *webtransport.Stream) {
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
	if payload.TargetHostID != "" && payload.TargetHostID != ps.cfg.hostID() {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeHostMismatch, "invocation is for a different host")))
		return
	}

	// Dispatch by invocation mode
	// Object responses are a single frame, stream responses carry a trailing body
	switch payload.Mode {
	case protocol.InvocationModeObject:
		_ = protocol.WriteMessage(stream, ps.handleObject(ctx, req, payload))
	case protocol.InvocationModeStream:
		ps.handleStreamInvoke(ctx, stream, req, payload)
	default:
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeInvokeModeUnsupported, "unsupported invocation mode %d", payload.Mode)))
	}
}

// handleObject runs an object invocation and returns the response envelope
func (ps *peerServer) handleObject(ctx context.Context, req *protocol.Envelope, payload protocol.InvokeActorRequest) *protocol.Envelope {
	// An actor type that does not implement object invocation cannot be called this way
	if ps.cfg.handler == nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, "object invocation is not supported"))
	}

	// Run the actor locally and relay any structured failure back to the caller
	out, perr := ps.cfg.handler(ctx, payload)
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
func (ps *peerServer) handleStreamInvoke(ctx context.Context, stream *webtransport.Stream, req *protocol.Envelope, payload protocol.InvokeActorRequest) {
	// An actor type that does not implement stream invocation cannot be called this way
	if ps.cfg.streamHandler == nil {
		_ = protocol.WriteMessage(stream, req.ErrorReply(protocol.NewError(protocol.ErrCodeInvokeModeUnsupported, "stream invocation is not supported")))
		return
	}

	// The remaining bytes on the stream are the request body
	// The handler writes the response through w
	// w emits the response metadata frame on its first Write, so the content type can be set before any body bytes are produced
	w := &peerStreamWriter{stream: stream, req: req}
	perr := ps.cfg.streamHandler(ctx, payload, stream, w)
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

// peerStreamWriter is a StreamResponseWriter that writes a peer stream response: a metadata frame followed by the raw response body
type peerStreamWriter struct {
	stream      *webtransport.Stream
	req         *protocol.Envelope
	contentType string
	flushed     bool
}

// SetContentType records the content type, which is sent in the metadata frame on the first Write
func (w *peerStreamWriter) SetContentType(contentType string) {
	if !w.flushed {
		w.contentType = contentType
	}
}

// Write sends the response metadata frame on the first call, then writes the raw body bytes to the stream
func (w *peerStreamWriter) Write(p []byte) (int, error) {
	if !w.flushed {
		w.flushed = true
		env, err := w.req.ReplyWith(protocol.KindInvokeActorResponse, protocol.InvokeActorResponse{ContentType: w.contentType})
		if err != nil {
			return 0, err
		}

		err = protocol.WriteMessage(w.stream, env)
		if err != nil {
			return 0, err
		}
	}

	return w.stream.Write(p)
}
