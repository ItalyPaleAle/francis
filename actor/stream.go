package actor

import (
	"context"
	"io"
)

// StreamResponseWriter is given to ActorStream.InvokeStream to write the streamed response.
type StreamResponseWriter interface {
	// SetContentType sets the content type of the response.
	// It must be called before the first Write and is ignored afterward.
	SetContentType(contentType string)

	// Write writes response body bytes.
	// The first Write flushes the response metadata (including the content type) to the caller.
	io.Writer
}

// ActorStream can be implemented by actors that offer the InvokeStream method.
type ActorStream interface {
	// InvokeStream is called for a streamed invocation.
	// It reads the request body from body and writes the response to w.
	// The actor holds its turn-based lock for the entire duration of the call.
	InvokeStream(ctx context.Context, method string, reqContentType string, body io.Reader, w StreamResponseWriter) error
}

// ActorPeekStream can be implemented by actors that offer the read-only PeekStream method.
// PeekStream is the streaming, read-side analogue of InvokeStream: the actor holds its shared (read) lock for the entire duration of the call, so multiple PeekStream calls can run concurrently with each other, while still excluding any in-flight InvokeStream.
type ActorPeekStream interface {
	// PeekStream is called for a streamed, read-only invocation.
	// It reads the request body from body and writes the response to w.
	PeekStream(ctx context.Context, method string, reqContentType string, body io.Reader, w StreamResponseWriter) error
}
