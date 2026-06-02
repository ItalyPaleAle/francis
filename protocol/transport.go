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

// RoundTrip writes req to the stream, then reads and returns the response
// The caller's context bounds the exchange: when it is canceled or times out, the blocking read or write is unblocked and the context error is returned
// quic streams are not context-aware, so a watcher forces the blocking call to return by setting a past deadline
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
