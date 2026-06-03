package actorcore

import (
	"context"
	"io"
	"sync"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/ref"
)

// LockAndStream runs a streamed invocation against a local actor and bridges the actor's writer to a reader for the caller.
// fn must call the actor's InvokeStream with the provided writer; it runs under the actor's turn-based lock for the whole call.
// The returned reader carries the response body and must be closed by the caller, which also unblocks the actor if the caller stops reading early.
func (m *Manager) LockAndStream(parentCtx context.Context, r ref.ActorRef, activeOnly bool, fn func(ctx context.Context, act *ActiveActor, w actor.StreamResponseWriter) error) (contentType string, resp io.ReadCloser, err error) {
	// The actor writes the response into the pipe; the caller reads it from the other end
	pr, pw := io.Pipe()
	w := &pipeStreamWriter{pw: pw, ready: make(chan string, 1)}

	// finished receives the result when the actor method returns without ever writing a body
	finished := make(chan error, 1)

	go func() {
		// Run the actor under its turn-based lock; the lock is held for the entire streamed call
		run := m.LockAndInvoke
		if activeOnly {
			run = m.LockAndInvokeActive
		}
		_, rErr := run(parentCtx, r, func(ctx context.Context, act *ActiveActor) (any, error) {
			return nil, fn(ctx, act, w)
		})

		// Closing the pipe surfaces EOF (or the error) to the caller's reader
		_ = w.pw.CloseWithError(rErr)

		// If the actor never wrote a body, unblock the waiter below since the content type was never signaled
		w.mu.Lock()
		flushed := w.flushed
		w.mu.Unlock()
		if !flushed {
			finished <- rErr
		}
	}()

	// Wait until the actor flushes its first bytes (content type known) or returns without writing
	select {
	case ct := <-w.ready:
		return ct, pr, nil
	case err := <-finished:
		if err != nil {
			_ = pr.Close()
			return "", nil, err
		}

		// The actor returned successfully without writing a body, so the response is empty
		return "", pr, nil
	}
}

// pipeStreamWriter is a StreamResponseWriter that writes into an io.Pipe and signals the content type on the first write
type pipeStreamWriter struct {
	pw *io.PipeWriter

	mu          sync.Mutex
	contentType string
	flushed     bool
	ready       chan string
}

// SetContentType records the content type, which is sent to the caller on the first Write
func (w *pipeStreamWriter) SetContentType(contentType string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.flushed {
		w.contentType = contentType
	}
}

// Write flushes the content type on the first call, then forwards the bytes to the pipe
func (w *pipeStreamWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	if !w.flushed {
		w.flushed = true
		w.ready <- w.contentType
	}
	w.mu.Unlock()

	return w.pw.Write(p)
}
