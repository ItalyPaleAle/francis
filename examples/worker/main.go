package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

	"github.com/italypaleale/actors/actor"
	"github.com/italypaleale/actors/host"
	"github.com/italypaleale/actors/internal/servicerunner"
	"github.com/italypaleale/actors/internal/signals"
)

var log *slog.Logger

func main() {
	log = initLogger(slog.LevelDebug)

	ctx := signals.SignalContext(context.Background(), log)

	err := runWorker(ctx)
	if err != nil {
		log.Error("Error running worker", slog.Any("error", err))
		os.Exit(1)
	}
}

func initLogger(level slog.Level) *slog.Logger {
	var handler slog.Handler
	if isatty.IsTerminal(os.Stdout.Fd()) {
		// Enable colors if we have a TTY
		handler = tint.NewHandler(os.Stdout, &tint.Options{
			TimeFormat: time.StampMilli,
			Level:      level,
		})
	} else {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: level,
		})
	}

	return slog.New(handler)
}

func runWorker(ctx context.Context) error {
	// Create a new actor host
	h, err := host.NewHost(host.NewHostOptions{
		Address: "todo",
		Logger:  log.With("scope", "actor-host"),
		ProviderOptions: host.SQLiteProviderOptions{
			ConnectionString: "data.db",
		},
		ShutdownGracePeriod: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("failed to create actor host: %w", err)
	}

	// Register all supported actors
	err = h.RegisterActor("myactor", NewMyActor, host.RegisterActorOptions{
		IdleTimeout: 5 * time.Second,
	})
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
				log.ErrorContext(r.Context(), "Error invoking actor", slog.Any("error", err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err.Error())
				return
			}

			if resp != nil {
				w.WriteHeader(http.StatusOK)
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				enc.Encode(resp)
			} else {
				w.WriteHeader(http.StatusNoContent)
			}
		})

		mux.HandleFunc("POST /halt/{actorType}/{actorId}", func(w http.ResponseWriter, r *http.Request) {
			err := actorService.Halt(r.PathValue("actorType"), r.PathValue("actorID"))
			if err != nil {
				log.ErrorContext(r.Context(), "Error halting actor", slog.Any("error", err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err.Error())
				return
			}
		})

		mux.HandleFunc("POST /halt-all", func(w http.ResponseWriter, r *http.Request) {
			err := actorService.HaltAll()
			if err != nil {
				log.ErrorContext(r.Context(), "Error halting all actors", slog.Any("error", err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err.Error())
				return
			}
		})

		server := &http.Server{
			Addr:    "127.0.0.1:8081",
			Handler: mux,
		}
		serveErrCh := make(chan error, 1)

		go func() {
			log.Info("Control server listening", slog.String("addr", server.Addr))
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
				log.Warn("Error shutting down server", slog.Any("error", err))
			}
		case err := <-serveErrCh:
			return err
		}
		return <-serveErrCh
	}
}
