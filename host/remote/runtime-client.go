package remote

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/quic-go/webtransport-go"
	"k8s.io/utils/clock"

	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// errNotConnected is returned by host-to-runtime requests when there is no active runtime session
var errNotConnected = errors.New("not connected to a runtime")

// errFatalRegistration wraps a registration rejection that will never succeed on retry, such as a protocol version mismatch
var errFatalRegistration = errors.New("runtime permanently rejected registration")

// isFatalRegistrationError reports whether a registration rejection is permanent, so reconnecting to any replica would fail the same way
func isFatalRegistrationError(err error) bool {
	if err == nil {
		return false
	}

	// A protocol version mismatch is decided against a constant compiled into both sides, so every replica rejects it identically
	return isProtocolErrorCode(err, protocol.ErrCodeProtocolVersion)
}

// runtimeHandlers are the callbacks invoked for runtime-initiated requests
type runtimeHandlers struct {
	// executeAlarm runs an alarm for an actor owned by this host and returns the result
	executeAlarm func(ctx context.Context, req protocol.ExecuteAlarmRequest) (protocol.ExecuteAlarmResponse, *protocol.Error)
	// terminateActor halts an actor active on this host
	terminateActor func(ctx context.Context, req protocol.TerminateActorRequest) *protocol.Error
}

// runtimeClientConfig configures a runtimeClient
type runtimeClientConfig struct {
	addresses      []string
	peerAddress    string
	actorTypes     []protocol.ActorHostType
	tlsConfig      *tls.Config
	requestTimeout time.Duration
	minBackoff     time.Duration
	maxBackoff     time.Duration
	handlers       runtimeHandlers

	// onDrainStart is called once at the very start of graceful shutdown, before anything else, so the host can mark itself draining and begin rejecting new peer invocations
	onDrainStart func()

	// onDrain is called once during graceful shutdown, while the runtime session is still alive, to drain local actors after the runtime has been told we are draining
	onDrain func()

	log   *slog.Logger
	clock clock.WithTicker
}

// runtimeClient maintains a persistent WebTransport session to one runtime replica at a time
// It registers the host, sends periodic health checks, serves runtime-initiated requests, and reconnects
// to another runtime address when the current session fails, reattaching to the same host registration
type runtimeClient struct {
	cfg    runtimeClientConfig
	dialer *webtransport.Dialer

	mu        sync.RWMutex
	session   *webtransport.Session
	hostID    string
	sessionID string

	readyOnce sync.Once
	ready     chan struct{}
}

// newRuntimeClient returns a runtimeClient with defaults filled in
func newRuntimeClient(cfg runtimeClientConfig) *runtimeClient {
	if cfg.log == nil {
		cfg.log = slog.New(slog.DiscardHandler)
	}
	if cfg.clock == nil {
		cfg.clock = &clock.RealClock{}
	}
	if cfg.requestTimeout <= 0 {
		cfg.requestTimeout = 15 * time.Second
	}
	if cfg.minBackoff <= 0 {
		cfg.minBackoff = 500 * time.Millisecond
	}
	if cfg.maxBackoff <= 0 {
		cfg.maxBackoff = 10 * time.Second
	}

	return &runtimeClient{
		cfg:    cfg,
		dialer: wt.NewDialer(cfg.tlsConfig),
		ready:  make(chan struct{}),
	}
}

// Ready returns a channel that is closed once the client has registered with a runtime for the first time
func (rc *runtimeClient) Ready() <-chan struct{} {
	return rc.ready
}

// HostID returns the current provider-backed host ID, or empty if not yet registered
func (rc *runtimeClient) HostID() string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.hostID
}

// Run connects to a runtime and keeps the session alive, reconnecting on failure until the context is canceled
func (rc *runtimeClient) Run(ctx context.Context) error {
	defer func() { _ = rc.dialer.Close() }()

	// Start at a random address so replicas spread the initial connections
	// #nosec G404 -- not security-sensitive
	idx := rand.IntN(len(rc.cfg.addresses))
	attempt := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Move to the next runtime address each attempt so failures roll over to other replicas
		addr := rc.cfg.addresses[idx%len(rc.cfg.addresses)]
		idx++

		// Connect and serve this address until the session ends or the context is canceled
		established, err := rc.connectAndServe(ctx, addr)
		if ctx.Err() != nil {
			// The context was canceled: this is a graceful shutdown
			return nil
		}
		if errors.Is(err, errFatalRegistration) {
			// The runtime rejected our registration in a way that no reconnect can fix, so stop and report it rather than spinning and never closing Ready
			rc.cfg.log.ErrorContext(ctx, "Runtime permanently rejected registration; giving up", slog.String("address", addr), slog.Any("error", err))
			return err
		}
		if err != nil {
			rc.cfg.log.WarnContext(ctx, "Runtime connection failed, will reconnect", slog.String("address", addr), slog.Any("error", err))
		} else {
			rc.cfg.log.InfoContext(ctx, "Runtime session ended, will reconnect", slog.String("address", addr))
		}

		// Reset the backoff after a session that actually connected, so a long-lived session reconnects quickly
		if established {
			attempt = 0
		}
		attempt++

		// Wait out the backoff before retrying, unless we are shutting down
		delay := rc.backoffDelay(attempt)
		t := rc.cfg.clock.NewTimer(delay)
		select {
		case <-t.C():
		case <-ctx.Done():
			t.Stop()
			return nil
		}
	}
}

// connectAndServe dials a runtime, registers, then serves the session until it ends or the context is canceled
// The bool return is true if a session was successfully established before it ended
func (rc *runtimeClient) connectAndServe(ctx context.Context, addr string) (bool, error) {
	// Open the QUIC/WebTransport connection to this runtime address
	dialCtx, cancel := context.WithTimeout(ctx, rc.cfg.requestTimeout)
	session, err := rc.dial(dialCtx, addr)
	cancel()
	if err != nil {
		return false, fmt.Errorf("failed to dial runtime: %w", err)
	}

	defer func() {
		// Always tear the session down when we stop serving it
		_ = session.CloseWithError(0, "")
	}()

	// Perform the registration handshake, which (re)claims our host identity with the runtime
	regCtx, cancel := context.WithTimeout(ctx, rc.cfg.requestTimeout)
	resp, err := rc.register(regCtx, session)
	cancel()
	if isFatalRegistrationError(err) {
		// A permanent rejection (such as a protocol version mismatch) will never succeed on retry, so surface it as fatal to stop the reconnect loop
		return false, fmt.Errorf("%w: %w", errFatalRegistration, err)
	} else if err != nil {
		return false, fmt.Errorf("failed to register with runtime: %w", err)
	}

	// Publish the live session so host-to-runtime requests can use it, and clear it again on the way out
	rc.setSession(session, resp.HostID, resp.SessionID)
	defer rc.clearSession()

	// Signal first-time readiness so callers waiting on Ready can proceed
	rc.readyOnce.Do(func() { close(rc.ready) })

	rc.cfg.log.InfoContext(ctx, "Connected to runtime",
		slog.String("address", addr),
		slog.String("hostId", resp.HostID),
		slog.Bool("reattached", resp.Reattached),
	)

	// Run health checks alongside the inbound listener, tying both to a context we cancel on return
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	healthInterval := time.Duration(resp.HealthCheckIntervalMs) * time.Millisecond
	go rc.runHealthChecks(serveCtx, session, healthInterval)

	// Serve runtime-initiated requests until the session ends or the context is canceled
	rc.serveInbound(serveCtx, session)

	// A canceled context means we are shutting down gracefully
	// Order matters: mark ourselves draining, tell the runtime, then drain local actors, all while the session is still alive
	if ctx.Err() != nil {
		// Mark the host draining so the peer server rejects new invocations with retry-later before any actors are halted
		if rc.cfg.onDrainStart != nil {
			rc.cfg.onDrainStart()
		}

		// Tell the runtime we are draining so it stops selecting us for new placement and alarm work
		rc.sendUnregister(session, resp.HostID, resp.SessionID)

		// Drain local actors so their deactivation can persist state and clear placement through the still-open runtime session
		if rc.cfg.onDrain != nil {
			rc.cfg.onDrain()
		}
	}
	return true, nil
}

// dial establishes a WebTransport session with the runtime at addr
func (rc *runtimeClient) dial(ctx context.Context, addr string) (*webtransport.Session, error) {
	url := "https://" + addr + protocol.RuntimeConnectPath
	rsp, session, err := rc.dialer.Dial(ctx, url, nil)
	if err != nil {
		return nil, err
	}

	if rsp.StatusCode < 200 || rsp.StatusCode >= 300 {
		_ = session.CloseWithError(0, "")
		return nil, fmt.Errorf("runtime returned unexpected status %d", rsp.StatusCode)
	}

	return session, nil
}

// register performs the registration handshake on the first stream of a new session
func (rc *runtimeClient) register(ctx context.Context, session *webtransport.Session) (protocol.RegisterHostResponse, error) {
	// Registration is the first exchange on a new session, so it gets its own dedicated stream
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return protocol.RegisterHostResponse{}, fmt.Errorf("failed to open registration stream: %w", err)
	}
	defer stream.Close()

	// Advertise our actor types and peer address; our protocol version travels in the request envelope
	// PreviousHostID is our last known host ID so the runtime reattaches us rather than minting a new identity on reconnect
	req, err := protocol.NewRequest(protocol.KindRegisterHost, protocol.RegisterHostRequest{
		PreviousHostID: rc.HostID(),
		Address:        rc.cfg.peerAddress,
		ActorTypes:     rc.cfg.actorTypes,
	})
	if err != nil {
		return protocol.RegisterHostResponse{}, err
	}

	// Send the request and wait for the runtime's reply
	resp, err := protocol.RoundTrip(ctx, stream, req)
	if err != nil {
		return protocol.RegisterHostResponse{}, err
	}

	// A structured error here means the runtime rejected the registration (for example a protocol version mismatch)
	perr, isErr := resp.AsError()
	if isErr {
		return protocol.RegisterHostResponse{}, perr
	}

	// Decode the assigned host ID, session ID, and health check interval
	var out protocol.RegisterHostResponse
	err = resp.DecodePayload(&out)
	if err != nil {
		return protocol.RegisterHostResponse{}, fmt.Errorf("failed to decode registration response: %w", err)
	}
	return out, nil
}

// runHealthChecks sends periodic health checks and closes the session if one fails, triggering a reconnect
func (rc *runtimeClient) runHealthChecks(ctx context.Context, session *webtransport.Session, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}

	t := rc.cfg.clock.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-t.C():
			// Send a health check, bounding it by the per-request timeout
			reqCtx, cancel := context.WithTimeout(ctx, rc.cfg.requestTimeout)
			err := rc.doRequest(reqCtx, protocol.KindHealthCheck, protocol.HealthCheckRequest{}, nil)
			cancel()

			// A failed health check on a live context means the session is unhealthy
			// Close it so the connect loop reconnects, ignoring failures that are just our own shutdown
			if err != nil && ctx.Err() == nil {
				rc.cfg.log.WarnContext(ctx, "Health check failed; closing session to reconnect", slog.Any("error", err))
				_ = session.CloseWithError(0, "health check failed")
				return
			}
		case <-ctx.Done():
			// The session ended or we are shutting down
			return
		}
	}
}

// serveInbound accepts and dispatches runtime-initiated streams until the session ends
func (rc *runtimeClient) serveInbound(ctx context.Context, session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			// The session has ended or the context was canceled
			return
		}
		go rc.handleInbound(ctx, stream)
	}
}

// inboundReadTimeout bounds how long a runtime-initiated request frame may take to arrive on an accepted stream before it is abandoned
const inboundReadTimeout = 30 * time.Second

// handleInbound reads one runtime request from a stream, dispatches it, and writes the response
func (rc *runtimeClient) handleInbound(ctx context.Context, stream *webtransport.Stream) {
	defer stream.Close()

	// Read the runtime's request off the stream
	req, err := protocol.ReadMessageWithTimeout(stream, inboundReadTimeout)
	if err != nil {
		// We cannot respond if we could not even read the request
		return
	}

	// Dispatch to the matching handler and write its response back on the same stream
	resp := rc.dispatchInbound(ctx, req)
	_ = protocol.WriteMessage(stream, resp)
}

// dispatchInbound routes a runtime-initiated request to its handler
func (rc *runtimeClient) dispatchInbound(ctx context.Context, req *protocol.Envelope) *protocol.Envelope {
	switch req.Kind {
	case protocol.KindExecuteAlarm:
		return rc.handleExecuteAlarm(ctx, req)
	case protocol.KindTerminateActor:
		return rc.handleTerminateActor(ctx, req)
	default:
		return req.ErrorReply(protocol.NewErrorf(protocol.ErrCodeBadRequest, "unknown message kind %q", req.Kind))
	}
}

func (rc *runtimeClient) handleExecuteAlarm(ctx context.Context, req *protocol.Envelope) *protocol.Envelope {
	// A host that registered no alarm handler cannot run alarms
	if rc.cfg.handlers.executeAlarm == nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "host does not handle alarms"))
	}

	// Decode the alarm to execute
	var payload protocol.ExecuteAlarmRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode execute alarm request"))
	}

	// Run the alarm locally
	// A structured error is relayed back so the runtime can decide to retry or drop it
	out, perr := rc.cfg.handlers.executeAlarm(ctx, payload)
	if perr != nil {
		return req.ErrorReply(perr)
	}

	// Report the execution result (such as the execution time) back to the runtime
	resp, err := req.ReplyWith(protocol.KindExecuteAlarmResponse, out)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "failed to encode execute alarm response"))
	}
	return resp
}

func (rc *runtimeClient) handleTerminateActor(ctx context.Context, req *protocol.Envelope) *protocol.Envelope {
	// A host with no termination handler cannot honor the request
	if rc.cfg.handlers.terminateActor == nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeInternal, "host does not handle actor termination"))
	}

	// Decode which actor to terminate
	var payload protocol.TerminateActorRequest
	err := req.DecodePayload(&payload)
	if err != nil {
		return req.ErrorReply(protocol.NewError(protocol.ErrCodeBadRequest, "failed to decode terminate actor request"))
	}

	// Halt the actor locally and acknowledge, relaying any structured failure
	perr := rc.cfg.handlers.terminateActor(ctx, payload)
	if perr != nil {
		return req.ErrorReply(perr)
	}

	return req.Reply(protocol.KindTerminateActorResponse, nil)
}

// doRequest sends a host-to-runtime request on a new stream and decodes the response into out
func (rc *runtimeClient) doRequest(ctx context.Context, kind string, payload any, out any) error {
	// Snapshot the live session and identity
	// Without a session there is nowhere to send the request
	session, hostID, sessionID := rc.snapshot()
	if session == nil {
		return errNotConnected
	}

	// Build the request and stamp our identity so the runtime can reject it if our session was superseded
	req, err := protocol.NewRequest(kind, payload)
	if err != nil {
		return err
	}
	req.HostID = hostID
	req.SessionID = sessionID

	// Each request gets its own stream, which WebTransport multiplexes over the connection
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream to runtime: %w", err)
	}
	defer stream.Close()

	// Send the request and wait for the correlated response
	resp, err := protocol.RoundTrip(ctx, stream, req)
	if err != nil {
		return err
	}

	// Surface a structured runtime error as the returned error
	perr, isErr := resp.AsError()
	if isErr {
		return perr
	}

	// Decode the success payload when the caller wants it
	if out != nil {
		err = resp.DecodePayload(out)
		if err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}
	return nil
}

// sendUnregister sends a best-effort graceful unregister on the given session during shutdown
func (rc *runtimeClient) sendUnregister(session *webtransport.Session, hostID string, sessionID string) {
	ctx, cancel := context.WithTimeout(context.Background(), rc.cfg.requestTimeout)
	defer cancel()

	req, err := protocol.NewRequest(protocol.KindUnregisterHost, protocol.UnregisterHostRequest{})
	if err != nil {
		return
	}
	req.HostID = hostID
	req.SessionID = sessionID

	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return
	}
	defer stream.Close()

	_, _ = protocol.RoundTrip(ctx, stream, req)
}

// LookupActor resolves the placement of an actor through the runtime
func (rc *runtimeClient) LookupActor(ctx context.Context, req protocol.LookupActorRequest) (protocol.LookupActorResponse, error) {
	var out protocol.LookupActorResponse
	err := rc.doRequest(ctx, protocol.KindLookupActor, req, &out)
	return out, err
}

// RemoveActor notifies the runtime that an actor has been deactivated on this host
func (rc *runtimeClient) RemoveActor(ctx context.Context, req protocol.RemoveActorRequest) error {
	return rc.doRequest(ctx, protocol.KindRemoveActor, req, nil)
}

// GetState retrieves an actor's persistent state through the runtime
func (rc *runtimeClient) GetState(ctx context.Context, req protocol.GetStateRequest) (protocol.GetStateResponse, error) {
	var out protocol.GetStateResponse
	err := rc.doRequest(ctx, protocol.KindGetState, req, &out)
	return out, err
}

// SetState stores an actor's persistent state through the runtime
func (rc *runtimeClient) SetState(ctx context.Context, req protocol.SetStateRequest) error {
	return rc.doRequest(ctx, protocol.KindSetState, req, nil)
}

// DeleteState removes an actor's persistent state through the runtime
func (rc *runtimeClient) DeleteState(ctx context.Context, req protocol.DeleteStateRequest) error {
	return rc.doRequest(ctx, protocol.KindDeleteState, req, nil)
}

// GetAlarm retrieves an alarm through the runtime
func (rc *runtimeClient) GetAlarm(ctx context.Context, req protocol.GetAlarmRequest) (protocol.GetAlarmResponse, error) {
	var out protocol.GetAlarmResponse
	err := rc.doRequest(ctx, protocol.KindGetAlarm, req, &out)
	return out, err
}

// SetAlarm creates or replaces an alarm through the runtime
func (rc *runtimeClient) SetAlarm(ctx context.Context, req protocol.SetAlarmRequest) error {
	return rc.doRequest(ctx, protocol.KindSetAlarm, req, nil)
}

// DeleteAlarm removes an alarm through the runtime
func (rc *runtimeClient) DeleteAlarm(ctx context.Context, req protocol.DeleteAlarmRequest) error {
	return rc.doRequest(ctx, protocol.KindDeleteAlarm, req, nil)
}

// setSession records the active session and its negotiated identity
func (rc *runtimeClient) setSession(session *webtransport.Session, hostID string, sessionID string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.session = session
	rc.hostID = hostID
	rc.sessionID = sessionID
}

// clearSession clears the active session but keeps the host ID so the next connection can reattach
func (rc *runtimeClient) clearSession() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.session = nil
	rc.sessionID = ""
}

// snapshot returns the current session and identity under the lock
func (rc *runtimeClient) snapshot() (*webtransport.Session, string, string) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.session, rc.hostID, rc.sessionID
}

// backoffDelay returns an exponential backoff with jitter, capped at the configured maximum
func (rc *runtimeClient) backoffDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	// Exponential growth capped at maxBackoff, bounding the shift to avoid overflow
	shift := min(attempt-1, 16)
	delay := rc.cfg.minBackoff << shift
	if delay <= 0 || delay > rc.cfg.maxBackoff {
		delay = rc.cfg.maxBackoff
	}

	// Apply jitter in the range [0.8, 1.2)
	// #nosec G404 -- not security-sensitive
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(delay) * jitter)
}
