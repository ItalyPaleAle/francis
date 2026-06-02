package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/protocol"
)

// Session error codes used when the runtime closes a host's WebTransport session
const (
	// sessionErrorSuperseded is sent when a session is superseded by a newer one for the same host
	sessionErrorSuperseded webtransport.SessionErrorCode = 1
	// sessionErrorShutdown is sent when the runtime is shutting down
	sessionErrorShutdown webtransport.SessionErrorCode = 2
)

// hostConn represents a single connected host and its active WebTransport session
// One hostConn corresponds to one QUIC connection
// A reconnect produces a new hostConn that supersedes the previous one for the same host ID
type hostConn struct {
	session   *webtransport.Session
	hostID    string
	sessionID string
	address   string

	protocolVersion uint16
	actorTypes      []protocol.ActorHostType

	// draining is set when a graceful UnregisterHost is in progress
	// While draining the host must not be selected for new placement or alarm work
	draining atomic.Bool

	log *slog.Logger
}

// ID returns the stable host ID
func (c *hostConn) ID() string {
	return c.hostID
}

// SessionID returns the runtime-generated session ID used to detect superseded sessions
func (c *hostConn) SessionID() string {
	return c.sessionID
}

// IsDraining reports whether the host is gracefully draining
func (c *hostConn) IsDraining() bool {
	return c.draining.Load()
}

// setDraining marks the host as draining
func (c *hostConn) setDraining() {
	c.draining.Store(true)
}

// close terminates the host's WebTransport session with the given error code
func (c *hostConn) close(code webtransport.SessionErrorCode, msg string) {
	if c.session == nil {
		return
	}

	_ = c.session.CloseWithError(code, msg)
}

// actorTypeConfig returns the host's advertised configuration for an actor type
func (c *hostConn) actorTypeConfig(actorType string) (protocol.ActorHostType, bool) {
	for _, at := range c.actorTypes {
		if at.ActorType == actorType {
			return at, true
		}
	}

	return protocol.ActorHostType{}, false
}

// sendRequest opens a new bi-directional stream to the host, writes the request, and reads the response
// It is used for runtime-initiated operations such as ExecuteAlarm
// The caller's context bounds the round-trip
func (c *hostConn) sendRequest(ctx context.Context, env *protocol.Envelope) (*protocol.Envelope, error) {
	// Stamp the host identity and session so the host can reject a request meant for a superseded session
	env.HostID = c.hostID
	env.SessionID = c.sessionID

	stream, err := c.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream to host: %w", err)
	}
	defer stream.Close()

	return protocol.RoundTrip(ctx, stream, env)
}
