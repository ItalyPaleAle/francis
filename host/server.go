package host

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	sloghttp "github.com/samber/slog-http"
)

func (h *Host) runServer(ctx context.Context) (err error) {
	// Create the HTTP/3 server (QUIC)
	srv := http3.Server{
		Handler:        h.getServerHandler(),
		Addr:           h.bind,
		MaxHeaderBytes: 1 << 20,
		TLSConfig:      h.serverTLSConfig,
		QUICConfig:     &quic.Config{},
	}

	h.log.InfoContext(ctx, "Actor host server started", slog.String("bind", h.bind))

	srvErr := make(chan error, 1)
	go func() {
		// Next call blocks until the server is shut down
		err := srv.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			srvErr <- fmt.Errorf("error running HTTP3 server: %w", err)
		}
		srvErr <- nil
	}()

	select {
	case err = <-srvErr:
		// Error running the server
		return err
	case <-ctx.Done():
		// Block until the context is canceled
		// Fallthrough
	}

	// Handle graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = srv.Shutdown(shutdownCtx)
	shutdownCancel()
	if err != nil {
		// Log the error only (could be context canceled)
		h.log.WarnContext(ctx, "Actor host server shutdown error", slog.Any("error", err))
	}

	return nil
}

func (h *Host) getServerHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /v1/invoke/{actorType}/{actorID}/{method}", h.handleMessageRequest)

	handler := Use(mux,
		// Recover from panics
		sloghttp.Recovery,
		// Add the middleware adding the host ID to each response
		middlewareHostIDHeader(h.hostID),
		// Log requests
		sloghttp.New(h.log),
	)

	return handler
}
