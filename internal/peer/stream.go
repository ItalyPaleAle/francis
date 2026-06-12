package peer

import (
	"errors"

	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/protocol"
)

// streamErrorHandlerFailed is the stream error code the server resets the response stream with when an actor's stream invocation fails after it has started writing the body
const streamErrorHandlerFailed webtransport.StreamErrorCode = 1

// ErrStreamReset is returned by a streamed response body when the remote host reset the stream because the actor's invocation failed mid-stream
// It lets the caller tell a failed, truncated body apart from a complete one, which a clean io.EOF cannot
var ErrStreamReset = errors.New("peer reset the response stream before the invocation completed")

// streamBody adapts a WebTransport stream's read side to an io.ReadCloser for a streamed response body
type streamBody struct {
	stream *webtransport.Stream
}

func (s *streamBody) Read(p []byte) (int, error) {
	n, err := s.stream.Read(p)
	if err == nil {
		return n, nil
	}

	// A reset carrying our handler-failed code means the actor's stream invocation failed after it began writing
	// Surface it as ErrStreamReset so the caller does not mistake a truncated body for a complete response
	streamErr, ok := errors.AsType[*webtransport.StreamError](err)
	if ok && streamErr.ErrorCode == streamErrorHandlerFailed {
		return n, ErrStreamReset
	}

	return n, err
}

func (s *streamBody) Close() error {
	// Releasing the body cancels the read side, which is harmless once the body has been fully read
	s.stream.CancelRead(0)
	return nil
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
