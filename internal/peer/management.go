package peer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/internal/tracing"
	"github.com/italypaleale/francis/internal/wt"
	"github.com/italypaleale/francis/protocol"
)

// MaxManagementResponseSize bounds the reply to a management request, which is far smaller than a general peer message even for a full snapshot page
const MaxManagementResponseSize = 4 << 20

// SendManagement sends one management request of the given kind to the host at address, and decodes the reply payload into out
// The peer must present the certificate of expectedHostID, like the target of an invocation, and its reply is bounded to MaxManagementResponseSize
// A peer that cannot be reached returns ErrCodeHostUnavailable, and structured failures reported by the peer are returned as-is
// out may be nil when the reply carries no payload of interest
func (c *Client) SendManagement(ctx context.Context, address string, expectedHostID string, kind string, payload any, out any) (perr *protocol.Error) {
	// Span the management call as a client span, the parent of the peer's server-side span
	ctx, span := tracing.Start(ctx, "rpc.peer.mgmt",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			tracing.PeerAddress(address),
			tracing.HostID(expectedHostID),
		),
	)
	defer func() {
		if perr != nil {
			tracing.End(span, perr)
			return
		}
		span.End()
	}()

	// Reuse a live pooled session to the peer, dialing one if necessary
	session, err := c.session(ctx, address)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeHostUnavailable, "failed to connect to host %s at %s: %v", expectedHostID, address, err)
	}

	// Management requests are always aimed at a specific host, so the peer identity must match
	if expectedHostID == "" {
		return protocol.NewError(protocol.ErrCodeBadRequest, "management requests require the target host ID")
	}
	pinErr := c.verifyPeerHostID(session, expectedHostID)
	if pinErr != nil {
		return pinErr
	}

	// Build the request envelope
	env, err := protocol.NewRequest(kind, payload)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to encode management request: %v", err)
	}
	protocol.InjectTraceContext(ctx, env)

	// Open a fresh stream for this request
	stream, err := session.OpenStreamSync(ctx)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeHostUnavailable, "failed to open stream to host %s at %s: %v", expectedHostID, address, err)
	}
	defer wt.CloseStream(stream)

	// Unblock the stream's blocking calls when the context is done, since QUIC streams are not context-aware
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.SetDeadline(time.Now())
		case <-stop:
			// Nop
		}
	}()

	// Send the request
	err = protocol.WriteMessage(stream, env)
	if err != nil {
		ctxErr := contextError(ctx, "before the management request reached host "+expectedHostID)
		if ctxErr != nil {
			return ctxErr
		}
		return protocol.NewErrorf(protocol.ErrCodeHostUnavailable, "failed to send management request to host %s at %s: %v", expectedHostID, address, err)
	}

	// Read the bounded reply
	respEnv, err := readBoundedMessage(stream, MaxManagementResponseSize)
	if err != nil {
		ctxErr := contextError(ctx, "while waiting for the management response from host "+expectedHostID)
		if ctxErr != nil {
			return ctxErr
		}
		return protocol.NewErrorf(protocol.ErrCodeTransportFailure, "failed to read management response from host %s at %s: %v", expectedHostID, address, err)
	}

	// Surface a structured error from the peer
	perr, isErr := respEnv.AsError()
	if isErr {
		return perr
	}

	// Decode the reply payload
	if out == nil {
		return nil
	}
	err = respEnv.DecodePayload(out)
	if err != nil {
		return protocol.NewErrorf(protocol.ErrCodeInternal, "failed to decode management response: %v", err)
	}

	return nil
}

// contextError returns the protocol error for a canceled or expired context, or nil if the context is still live
func contextError(ctx context.Context, when string) *protocol.Error {
	ctxErr := ctx.Err()
	switch {
	case errors.Is(ctxErr, context.DeadlineExceeded):
		return protocol.NewError(protocol.ErrCodeDeadlineExceeded, "deadline exceeded "+when)
	case ctxErr != nil:
		return protocol.NewError(protocol.ErrCodeCanceled, "canceled "+when)
	default:
		return nil
	}
}

// readBoundedMessage reads one length-prefixed envelope like protocol.ReadMessage, but rejects a message larger than maxSize before buffering it
func readBoundedMessage(r io.Reader, maxSize uint32) (*protocol.Envelope, error) {
	// Read the length prefix to check it against the bound
	var lenBuf [4]byte
	_, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(lenBuf[:])
	if size > maxSize {
		return nil, fmt.Errorf("message size %d exceeds maximum %d", size, maxSize)
	}

	// Hand the prefix back to the regular reader
	return protocol.ReadMessage(io.MultiReader(bytes.NewReader(lenBuf[:]), r))
}
