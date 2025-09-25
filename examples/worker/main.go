package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

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
		IdleTimeout: 10 * time.Second,
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
