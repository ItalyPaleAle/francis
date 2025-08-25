package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/host"
	"github.com/italypaleale/actors/internal/servicerunner"
	"github.com/italypaleale/actors/internal/signals"
)

func main() {
	ctx := signals.SignalContext(context.Background())

	err := runWorker(ctx)
	if err != nil {
		slog.Error("Error running worker", slog.Any("error", err))
	}
}

func runWorker(ctx context.Context) error {
	log := slog.Default()

	// Create a new actor host
	h, err := host.NewHost(host.NewHostOptions{
		Address: "todo",
		Logger:  log,
		ProviderOptions: host.SQLiteProviderOptions{
			ConnectionString: "data.db",
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register all supported actors
	err = h.RegisterActor("myactor", NewMyActor, host.RegisterActorOptions{})
	if err != nil {
		return fmt.Errorf("failed to register actor 'myactor': %w", err)
	}

	// Get the service
	actorService := h.Service()

	err = servicerunner.
		NewServiceRunner(
			h.Run,
			runControlServer(actorService),
		).
		Run(ctx)
	if err != nil {
		return fmt.Errorf("error running services: %w", err)
	}

	return nil
}

func runControlServer(actorService *actor.Service) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		mux := http.NewServeMux()
		mux.HandleFunc("POST /invoke/{actorType}/{actorID}/{method}", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()

			body, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "failed to read body")
				return
			}

			resp, err := actorService.Invoke(r.Context(), r.PathValue("actorType"), r.PathValue("actorID"), r.PathValue("method"), body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err.Error())
				return
			}

			if resp != nil {
				w.WriteHeader(http.StatusOK)
				switch v := resp.(type) {
				case []byte:
					_, _ = w.Write(v)
				case string:
					_, _ = w.Write([]byte(v))
				default:
					// ignore unsupported types; could add encoding here if needed
				}
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})

		server := &http.Server{
			Addr:    "127.0.0.1:8081",
			Handler: mux,
		}
		serveErrCh := make(chan error, 1)

		go func() {
			slog.Info("Control server listening", slog.String("addr", server.Addr))
			rErr := server.ListenAndServe()
			if rErr != nil && rErr != http.ErrServerClosed {
				serveErrCh <- rErr
			} else {
				serveErrCh <- nil
			}
		}()

		select {
		case <-ctx.Done():
			shutdownCtx, shutdownErr := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownErr()
			err := server.Shutdown(shutdownCtx)
			if err != nil {
				slog.Warn("Error shutting down server", slog.Any("error", err))
			}
		case err := <-serveErrCh:
			return err
		}
		return <-serveErrCh
	}
}
