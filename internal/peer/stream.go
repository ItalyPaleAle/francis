package peer

import (
	"github.com/quic-go/webtransport-go"

	"github.com/italypaleale/francis/protocol"
)

// streamBody adapts a WebTransport stream's read side to an io.ReadCloser for a streamed response body
type streamBody struct {
	stream *webtransport.Stream
}

func (s *streamBody) Read(p []byte) (int, error) {
	return s.stream.Read(p)
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
