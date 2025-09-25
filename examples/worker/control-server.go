package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/italypaleale/francis/actor"
)

func runControlServer(actorService *actor.Service) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		mux := http.NewServeMux()

		mux.HandleFunc("POST /invoke/{actorType}/{actorID}/{method}", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()

			var body struct {
				In int64
			}
			err := json.NewDecoder(r.Body).Decode(&body)
			if err != nil && !errors.Is(err, io.EOF) {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprintf(w, "failed to read body as JSON: %v", err)
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
				var data struct {
					Out int64
				}
				err = resp.Decode(&data)
				if err != nil {
					log.ErrorContext(r.Context(), "Error decoding response envelope", slog.Any("error", err))
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprint(w, err.Error())
					return
				}

				w.WriteHeader(http.StatusOK)
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				_ = enc.Encode(data) //nolint:errcheck
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

		mux.HandleFunc("POST /alarm/{actorType}/{actorID}/{name}", func(w http.ResponseWriter, r *http.Request) {
			defer r.Body.Close()

			properties := actor.AlarmProperties{}
			err := json.NewDecoder(r.Body).Decode(&properties)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				fmt.Fprint(w, "failed to read body as JSON")
				return
			}

			err = actorService.SetAlarm(r.Context(), r.PathValue("actorType"), r.PathValue("actorID"), r.PathValue("name"), properties)
			if err != nil {
				log.ErrorContext(r.Context(), "Error invoking actor", slog.Any("error", err))
				w.WriteHeader(http.StatusInternalServerError)
				fmt.Fprint(w, err.Error())
				return
			}

			w.WriteHeader(http.StatusNoContent)
		})

		server := &http.Server{
			Addr:              workerAddress,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
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
