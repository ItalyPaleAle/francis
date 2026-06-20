package protocol

import (
	"context"
	"io"
	"time"
)

// RuntimeConnectPath is the HTTP/3 path a host uses to establish a WebTransport session with a runtime
const RuntimeConnectPath = "/runtime/v1/connect"

// PeerConnectPath is the HTTP/3 path a host uses to establish a WebTransport session with another host for actor invocation
const PeerConnectPath = "/peer/v1/invoke"

// Stream is a bidirectional, length-framed transport stream that supports deadlines
// It is satisfied by a WebTransport stream
type Stream interface {
	io.ReadWriteCloser
	SetDeadline(t time.Time) error
}

// ReadMessageWithTimeout reads a single inbound envelope from a freshly accepted stream, bounding the read with a deadline
// This stops a stalled or trickling peer from pinning a goroutine and read buffer on an inbound frame that never completes
// The deadline is cleared once the frame is read, so it does not bound handler execution or a trailing streamed body on the same stream
func ReadMessageWithTimeout(stream Stream, timeout time.Duration) (*Envelope, error) {
	err := stream.SetDeadline(time.Now().Add(timeout))
	if err != nil {
		return nil, err
	}

	e, err := ReadMessage(stream)
	if err != nil {
		return nil, err
	}

	// Clear the deadline so the handler and any trailing body stream are not bounded by it
	_ = stream.SetDeadline(time.Time{})
	return e, nil
}

// RoundTrip writes req to the stream, then reads and returns the response
// The caller's context bounds the exchange: when it is canceled or times out, the blocking read or write is unblocked and the context error is returned
// quic streams are not context-aware, so a watcher forces the blocking call to return by setting a past deadline
// On any error return the stream may carry a past deadline and must be discarded - callers must not reuse a stream after RoundTrip returns an error
func RoundTrip(ctx context.Context, stream Stream, req *Envelope) (*Envelope, error) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.SetDeadline(time.Now())
		case <-stop:
		}
	}()

	err := WriteMessage(stream, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	resp, err := ReadMessage(stream)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}

	return resp, nil
}
