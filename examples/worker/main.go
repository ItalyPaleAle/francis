package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/host"
	"github.com/italypaleale/francis/internal/servicerunner"
	"github.com/italypaleale/francis/internal/signals"
)

var (
	log              *slog.Logger
	actorHostAddress string
	workerAddress    string
	certName         string
)

const peerAuthKey = "test-auth-key-1234567890"

func main() {
	flag.StringVar(&actorHostAddress, "actor-host-address", "127.0.0.1:7571", "Address and port for the actor host to bind to")
	flag.StringVar(&workerAddress, "worker-address", "127.0.0.1:8081", "Address and port for the example worker to bind to")
	flag.StringVar(&certName, "cert", "", "Name of the certificate in the 'certs' folder (e.g. 'node-1')")
	flag.Parse()

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

func getMTLSHostOption() (host.HostOption, error) {
	// Load the certificate and key
	cert, err := tls.LoadX509KeyPair("certs/"+certName+".crt", "certs/"+certName+".key")
	if err != nil {
		return nil, fmt.Errorf("failed to load certificate and key from 'certs/%s.crt' and 'certs/%s.key': %w", certName, certName, err)
	}

	// Load the CA certificate
	caData, err := os.ReadFile("certs/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate from certs/ca.crt: %w", err)
	}

	// Parse the CA certificate
	caBlock, _ := pem.Decode(caData)
	if caBlock == nil {
		return nil, errors.New("failed to parse PEM block from CA certificate")
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse CA certificate: %w", err)
	}

	return host.WithPeerAuthenticationMTLS(&cert, caCert), nil
}

func runWorker(ctx context.Context) error {
	// Options for the host
	opts := []host.HostOption{
		host.WithAddress(actorHostAddress),
		host.WithLogger(log.With("scope", "actor-host")),
		host.WithSQLiteProvider(host.SQLiteProviderOptions{
			ConnectionString: "data.db",
		}),
		host.WithShutdownGracePeriod(10 * time.Second),
	}

	// Check if we're using mTLS
	if certName != "" {
		mtlsOpt, err := getMTLSHostOption()
		if err != nil {
			return err
		}
		opts = append(opts, mtlsOpt)
	} else {
		opts = append(opts,
			// Use shared key for auth
			host.WithPeerAuthenticationSharedKey(peerAuthKey),
			// Use self-signed certs
			host.WithServerTLSInsecureSkipTLSValidation(),
		)
	}

	// Create a new actor host
	h, err := host.NewHost(opts...)
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
