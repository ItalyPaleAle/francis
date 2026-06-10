package peer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/internal/ca"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// defaultDialTimeout bounds how long a single peer dial may take
const defaultDialTimeout = 15 * time.Second

// defaultIdleTimeout closes a pooled peer session after it has been idle this long
// A session stays open across repeated calls and is reclaimed once it goes quiet
const defaultIdleTimeout = 60 * time.Second

// errClientClosed is returned by session acquisition once the client has been closed
var errClientClosed = errors.New("peer client is closed")

// ClientConfig configures a Client
type ClientConfig struct {
	// TLSConfig is the client TLS configuration, which must advertise the HTTP/3 ALPN and present the host's workload certificate for mutual authentication
	TLSConfig *tls.Config
	// DialTimeout bounds a single peer dial
	DialTimeout time.Duration
	// IdleTimeout closes a pooled session after it has been idle this long
	IdleTimeout time.Duration
	// Log is the slog logger
	Log *slog.Logger
}

// Client invokes actors on other hosts over WebTransport, pooling one session per peer address
type Client struct {
	closed      atomic.Bool
	dialed      atomic.Bool
	dialer      *webtransport.Dialer
	dialTimeout time.Duration
	log         *slog.Logger

	// sessions pool is a lock-free map: dead sessions are detected via their context and atomically replaced, never deleted, so a concurrent redial can never be clobbered
	sessions *haxmap.Map[string, *webtransport.Session]
}

// NewClient returns a Client that dials peers using the given configuration
func NewClient(cfg ClientConfig) *Client {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	return &Client{
		// The dialer's QUIC idle timeout reclaims a session once it stops carrying traffic, while an active stream keeps it alive
		dialer:      wt.NewDialer(cfg.TLSConfig, wt.WithMaxIdleTimeout(cfg.IdleTimeout)),
		dialTimeout: cfg.DialTimeout,
		log:         cfg.Log,
		sessions:    haxmap.New[string, *webtransport.Session](),
	}
}

// InvokeObject performs an object invocation against the actor on the peer at address
// A returned protocol error that is Retryable signals the caller to invalidate its cached placement and retry a fresh lookup
func (c *Client) InvokeObject(ctx context.Context, address string, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	req.Mode = protocol.InvocationModeObject

	// Reuse a live pooled session to the peer, dialing one if necessary
	session, err := c.session(ctx, address)
	if err != nil {
		// A connection that cannot be established is treated as stale placement so the caller re-resolves it
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to connect to peer %s: %v", address, err)
	}

	// Pin the session to the host identity placement told us to dial before sending any payload to it
	pinErr := c.verifyPeerHostID(session, req.TargetHostID)
	if pinErr != nil {
		return protocol.InvokeActorResponse{}, pinErr
	}

	// Open a fresh stream for this invocation (which WebTransport multiplexes over the peer connection)
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		// The session may have died, in which case the next invocation's session() will detect and replace it
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to open stream to peer %s: %v", address, err)
	}
	defer stream.Close()

	// Build the invocation envelope carrying the encoded argument and target identity
	env, err := protocol.NewRequest(protocol.KindInvokeActor, req)
	if err != nil {
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to encode invocation: %v", err)
	}

	// Send the request and wait for the peer's response
	respEnv, err := protocol.RoundTrip(ctx, stream, env)
	if err != nil {
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeRetryLater, "invocation transport to peer %s failed: %v", address, err)
	}

	// Surface a structured error from the peer
	perr, isErr := respEnv.AsError()
	if isErr {
		return protocol.InvokeActorResponse{}, perr
	}

	// Decode the result of the invocation
	var out protocol.InvokeActorResponse
	err = respEnv.DecodePayload(&out)
	if err != nil {
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to decode invocation response: %v", err)
	}

	return out, nil
}

// InvokeStream performs a stream invocation against the actor on the peer at address
// The request body is streamed from body
// On success the returned reader carries the streamed response body and must be closed by the caller
// A returned protocol error that is Retryable signals the caller to invalidate its cached placement and retry a fresh lookup
func (c *Client) InvokeStream(ctx context.Context, address string, req protocol.InvokeActorRequest, body io.Reader) (string, io.ReadCloser, *protocol.Error) {
	req.Mode = protocol.InvocationModeStream

	// Reuse a live pooled session to the peer, dialing one if necessary
	session, err := c.session(ctx, address)
	if err != nil {
		return "", nil, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to connect to peer %s: %v", address, err)
	}

	// Pin the session to the host identity placement told us to dial before streaming any payload to it
	pinErr := c.verifyPeerHostID(session, req.TargetHostID)
	if pinErr != nil {
		return "", nil, pinErr
	}

	// Open a fresh stream for this invocation
	// The read side stays open to carry the response body
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return "", nil, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to open stream to peer %s: %v", address, err)
	}

	// Bound the request send and response-metadata read by the caller's context
	// QUIC streams are not context-aware, so a watcher forces the blocking calls to unblock until we hand the body reader to the caller
	stopWatch := make(chan struct{})
	watchStopped := false
	stop := func() {
		if !watchStopped {
			watchStopped = true
			close(stopWatch)
		}
	}
	defer stop()
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.SetDeadline(time.Now())
		case <-stopWatch:
			// Noop
		}
	}()

	// Send the invocation metadata frame
	env, err := protocol.NewRequest(protocol.KindInvokeActor, req)
	if err != nil {
		_ = stream.Close()
		return "", nil, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to encode invocation: %v", err)
	}

	err = protocol.WriteMessage(stream, env)
	if err != nil {
		_ = stream.Close()
		return "", nil, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to send invocation to peer %s: %v", address, err)
	}

	// Stream the request body, then close the write side to signal its end
	if body != nil {
		_, err = io.Copy(stream, body)
		if err != nil {
			_ = stream.Close()
			return "", nil, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to stream request body to peer %s: %v", address, err)
		}
	}
	_ = stream.Close()

	// Read the response metadata frame
	respEnv, err := protocol.ReadMessage(stream)
	if err != nil {
		stream.CancelRead(0)
		return "", nil, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to read response from peer %s: %v", address, err)
	}

	// Surface a structured error from the peer (host mismatch, halted actor, invocation failure, and so on)
	perr, isErr := respEnv.AsError()
	if isErr {
		stream.CancelRead(0)
		return "", nil, perr
	}

	var meta protocol.InvokeActorResponse
	err = respEnv.DecodePayload(&meta)
	if err != nil {
		stream.CancelRead(0)
		return "", nil, protocol.NewErrorf(protocol.ErrCodeInternal, "failed to decode invocation response: %v", err)
	}

	// From here the caller owns the stream's read side, so stop the context watcher
	stop()

	// The remaining bytes on the stream are the response body, which the caller reads and then closes
	return meta.ContentType, &streamBody{stream: stream}, nil
}

// verifyPeerHostID pins an established session to the host identity placement told us to dial
// The peer certificate was already verified against the cluster CA during the handshake, so this only asserts it is the intended host, which stops a different but cluster-valid host at the same address from receiving the invocation
// The pool is keyed by address, so this also guards against a pooled session for one host silently satisfying a call meant for another after an address is reused
// An empty expected ID skips the check, since the caller did not pin a specific host
func (c *Client) verifyPeerHostID(session *webtransport.Session, expectedHostID string) *protocol.Error {
	if expectedHostID == "" {
		return nil
	}

	// Read the peer's authenticated certificate from the completed handshake
	certs := session.SessionState().ConnectionState.TLS.PeerCertificates
	if len(certs) == 0 {
		return protocol.NewError(protocol.ErrCodeHostMismatch, "peer presented no certificate")
	}

	// A mismatch means the host at this address is not the one placement selected, so the caller should re-resolve
	gotHostID, err := ca.HostIDFromCert(certs[0])
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeHostMismatch, "peer identity is invalid: %v", err)
	}
	if gotHostID != expectedHostID {
		return protocol.NewErrorf(protocol.ErrCodeHostMismatch, "peer is host %q, not the expected %q", gotHostID, expectedHostID)
	}

	return nil
}

// session returns a live pooled session for the peer address, dialing and pooling a new one when there is none or the pooled one has died
func (c *Client) session(ctx context.Context, address string) (*webtransport.Session, error) {
	// Reject early once the client is closed
	if c.closed.Load() {
		return nil, errClientClosed
	}

	// Fast path: reuse the pooled session while it is still alive
	existing, ok := c.sessions.Get(address)
	if ok && existing.Context().Err() == nil {
		return c.guard(address, existing)
	}

	// Either nothing is pooled or the pooled session is dead, so dial a replacement
	created, err := c.dial(ctx, address)
	if err != nil {
		return nil, err
	}

	// Replace the specific dead session we observed
	// If another goroutine already replaced it, use theirs
	if ok {
		swapped := c.sessions.CompareAndSwap(address, existing, created)
		if swapped {
			_ = existing.CloseWithError(0, "")
			return c.guard(address, created)
		}

		_ = created.CloseWithError(0, "")
		current, _ := c.sessions.Get(address)
		return c.guard(address, current)
	}

	// Nothing was pooled, so store ours unless another goroutine won the race
	actual, loaded := c.sessions.GetOrSet(address, created)
	if loaded {
		_ = created.CloseWithError(0, "")
		return c.guard(address, actual)
	}

	return c.guard(address, created)
}

// guard reclaims a session that was pooled concurrently with Close, so Close can never leak a connection
// If the client closed while we were dialing or storing, the just-pooled session is removed and closed here, and exactly one of guard or Close ends up closing it because both go through GetAndDel
func (c *Client) guard(address string, session *webtransport.Session) (*webtransport.Session, error) {
	if c.closed.Load() {
		s, ok := c.sessions.GetAndDel(address)
		if ok {
			_ = s.CloseWithError(0, "")
		}

		return nil, errClientClosed
	}

	if session == nil {
		return nil, fmt.Errorf("no session available for peer %s", address)
	}

	return session, nil
}

// dial opens a new WebTransport session to the peer at address
// Mutual authentication is performed by the TLS layer: the dialer presents the host's workload certificate and verifies the peer's
func (c *Client) dial(ctx context.Context, address string) (*webtransport.Session, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.dialTimeout)
	defer cancel()

	rsp, session, err := c.dialer.Dial(dialCtx, "https://"+address+protocol.PeerConnectPath, nil)
	// The dialer lazily initializes its transport on the first Dial, so record that it is now safe to close
	c.dialed.Store(true)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		_ = session.CloseWithError(0, "")
		return nil, fmt.Errorf("peer returned unexpected status %d", rsp.StatusCode)
	}

	return session, nil
}

// Close tears down all pooled sessions and the dialer
func (c *Client) Close() {
	// Flag the client closed so session() pools nothing more and reclaims any in-flight session via guard
	c.closed.Store(true)

	// Close the dialer so no new connection can be established
	// The dialer's transport is created lazily on the first Dial, and closing it before then panics, so only close it if we ever dialed
	if c.dialed.Load() {
		_ = c.dialer.Close()
	}

	// Snapshot the pooled addresses, then remove and close each session
	// A session pooled concurrently is reclaimed by exactly one of this drain or its own guard, since both use GetAndDel
	addresses := make([]string, 0, c.sessions.Len())
	c.sessions.ForEach(func(address string, _ *webtransport.Session) bool {
		addresses = append(addresses, address)
		return true
	})

	for _, address := range addresses {
		session, ok := c.sessions.GetAndDel(address)
		if ok {
			_ = session.CloseWithError(0, "")
		}
	}
}
