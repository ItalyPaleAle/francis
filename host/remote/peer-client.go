package remote

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/alphadose/haxmap"
	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// errPeerClientClosed is returned by session acquisition once the client has been closed
var errPeerClientClosed = errors.New("peer client is closed")

// peerClient invokes actors on other hosts over WebTransport, pooling one session per peer address
type peerClient struct {
	closed      atomic.Bool
	dialer      *webtransport.Dialer
	dialTimeout time.Duration
	log         *slog.Logger

	// sessions pool is a lock-free map: dead sessions are detected via their context and atomically replaced, never deleted, so a concurrent redial can never be clobbered
	sessions *haxmap.Map[string, *webtransport.Session]
}

// newPeerClient returns a peerClient that dials peers using the given client TLS configuration
func newPeerClient(tlsConfig *tls.Config, dialTimeout time.Duration, log *slog.Logger) *peerClient {
	// Set default options
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if dialTimeout <= 0 {
		dialTimeout = 15 * time.Second
	}

	return &peerClient{
		dialer:      wt.NewDialer(tlsConfig),
		dialTimeout: dialTimeout,
		log:         log,
		sessions:    haxmap.New[string, *webtransport.Session](),
	}
}

// InvokeObject performs an object invocation against the actor on the peer at address
// A returned protocol error that is Retryable signals the caller to invalidate its cached placement and retry a fresh lookup
func (pc *peerClient) InvokeObject(ctx context.Context, address string, req protocol.InvokeActorRequest) (protocol.InvokeActorResponse, *protocol.Error) {
	req.Mode = protocol.InvocationModeObject

	// Reuse a live pooled session to the peer, dialing one if necessary
	session, err := pc.session(ctx, address)
	if err != nil {
		// A connection that cannot be established is treated as stale placement so the caller re-resolves it
		return protocol.InvokeActorResponse{}, protocol.NewErrorf(protocol.ErrCodeRetryLater, "failed to connect to peer %s: %v", address, err)
	}

	// Open a fresh stream for this invocation, which WebTransport multiplexes over the peer connection
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

	// Surface a structured error from the peer (host mismatch, halted actor, invocation failure, and so on)
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

// session returns a live pooled session for the peer address, dialing and pooling a new one when there is none or the pooled one has died
func (pc *peerClient) session(ctx context.Context, address string) (*webtransport.Session, error) {
	// Reject early once the client is closed
	if pc.closed.Load() {
		return nil, errPeerClientClosed
	}

	// Fast path: reuse the pooled session while it is still alive
	existing, ok := pc.sessions.Get(address)
	if ok && existing.Context().Err() == nil {
		return pc.guard(address, existing)
	}

	// Either nothing is pooled or the pooled session is dead, so dial a replacement
	created, err := pc.dial(ctx, address)
	if err != nil {
		return nil, err
	}

	// Replace the specific dead session we observed
	// If another goroutine already replaced it, use theirs
	if ok {
		swapped := pc.sessions.CompareAndSwap(address, existing, created)
		if swapped {
			_ = existing.CloseWithError(0, "")
			return pc.guard(address, created)
		}

		_ = created.CloseWithError(0, "")
		current, _ := pc.sessions.Get(address)
		return pc.guard(address, current)
	}

	// Nothing was pooled, so store ours unless another goroutine won the race
	actual, loaded := pc.sessions.GetOrSet(address, created)
	if loaded {
		_ = created.CloseWithError(0, "")
		return pc.guard(address, actual)
	}

	return pc.guard(address, created)
}

// guard reclaims a session that was pooled concurrently with Close, so Close can never leak a connection
// If the client closed while we were dialing or storing, the just-pooled session is removed and closed here, and exactly one of guard or Close ends up closing it because both go through GetAndDel
func (pc *peerClient) guard(address string, session *webtransport.Session) (*webtransport.Session, error) {
	if pc.closed.Load() {
		s, ok := pc.sessions.GetAndDel(address)
		if ok {
			_ = s.CloseWithError(0, "")
		}

		return nil, errPeerClientClosed
	}

	if session == nil {
		return nil, fmt.Errorf("no session available for peer %s", address)
	}

	return session, nil
}

// dial opens a new WebTransport session to the peer at address
func (pc *peerClient) dial(ctx context.Context, address string) (*webtransport.Session, error) {
	dialCtx, cancel := context.WithTimeout(ctx, pc.dialTimeout)
	defer cancel()

	rsp, session, err := pc.dialer.Dial(dialCtx, "https://"+address+protocol.PeerConnectPath, nil)
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
func (pc *peerClient) Close() {
	// Flag the client closed so session() pools nothing more and reclaims any in-flight session via guard
	pc.closed.Store(true)

	// Close the dialer so no new connection can be established
	_ = pc.dialer.Close()

	// Snapshot the pooled addresses, then remove and close each session
	// A session pooled concurrently is reclaimed by exactly one of this drain or its own guard, since both use GetAndDel
	addresses := make([]string, 0, pc.sessions.Len())
	pc.sessions.ForEach(func(address string, _ *webtransport.Session) bool {
		addresses = append(addresses, address)
		return true
	})

	for _, address := range addresses {
		session, ok := pc.sessions.GetAndDel(address)
		if ok {
			_ = session.CloseWithError(0, "")
		}
	}
}
