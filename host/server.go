package host

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

func (h *Host) runServer(ctx context.Context) (err error) {
	// Create the HTTP/3 server (QUIC)
	srv := http3.Server{
		Handler:        h.getServerMux(),
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
		if err != http.ErrServerClosed {
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

func (h *Host) getServerMux() *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /v1/invoke/{actorType}/{actorID}/{method}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var (
			apiErr           *apiError
			err              error
			reqData, outData any
		)

		// Read the request body
		ct := r.Header.Get(headerContentType)
		switch ct {
		case contentTypeMsgpack:
			dec := msgpack.GetDecoder()
			defer msgpack.PutDecoder(dec)
			dec.Reset(r.Body)
			err = dec.Decode(&reqData)
			if err != nil {
				apiErr = newApiErrorf(http.StatusBadRequest, "req_invoke_body", "Failed to parse request body: %v", err)
				apiErr.WriteResponse(w)
				return
			}
		case "":
			// Ignore the body if the content type is unsupported
		default:
			apiErr = newApiErrorf(http.StatusBadRequest, "req_invoke_content_type", "Unsupported content type: %s", ct)
			apiErr.WriteResponse(w)
			return
		}

		// Invoke the actor
		err = h.InvokeLocal(r.Context(), r.PathValue("actorType"), r.PathValue("actorID"), r.PathValue("method"), reqData, &outData)
		switch {
		case errors.Is(err, ErrActorNotHosted):
			apiErr = newApiError(http.StatusNotFound, "actor_not_hosted", "Actor is not active on the current host")
		case errors.Is(err, ErrActorHalted):
			apiErr = newApiError(http.StatusConflict, "actor_halted", "Actor is halted")
		case err != nil:
			apiErr = newApiErrorf(http.StatusInternalServerError, "invoke_error", "Actor invocation error: %v", err)
		}
		if apiErr != nil {
			apiErr.WriteResponse(w)
			return
		}

		// If there's no output data, respond with 204 No Content
		if outData == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Respond with the body
		w.WriteHeader(http.StatusOK)
		enc := msgpack.GetEncoder()
		defer msgpack.PutEncoder(enc)
		enc.Reset(w)
		err = enc.Encode(outData)
		if err != nil {
			// At this point all we can do is log the error
			h.log.ErrorContext(r.Context(), "Error writing response body", slog.Any("error", err))
			return
		}
	})

	return mux
}

func (h *Host) initTLS(opts *HostTLSOptions) (clientTLSConfig *tls.Config, err error) {
	if opts == nil {
		opts = &HostTLSOptions{}
	}

	// Init the TLS configuration object for the server
	h.serverTLSConfig = &tls.Config{
		MinVersion: minTLSVersion,
	}

	// TLS configuration for the client
	clientTLSConfig = &tls.Config{
		MinVersion: minTLSVersion,
		// Set the ALPN for HTTP/3
		NextProtos: []string{http3.NextProtoH3},
	}

	// Set the CA certificate if present
	if opts.CACertificate != nil {
		caCertPool := x509.NewCertPool()
		caCertPool.AddCert(opts.CACertificate)

		// Set the cert pool on the server and client
		h.serverTLSConfig.RootCAs = caCertPool
		clientTLSConfig.RootCAs = caCertPool
	}

	// Disable TLS validation for the client if configured
	if opts.InsecureSkipTLSValidation {
		clientTLSConfig.InsecureSkipVerify = true
	}

	// If a certificate was passed as input, use that
	if opts.ServerCertificate != nil {
		if len(opts.ServerCertificate.Certificate) == 0 || opts.ServerCertificate.PrivateKey == nil {
			return nil, errors.New("option TLSOptions.ServerCertificate is not valid: must contain both a certificate and private key")
		}

		// Set the certificate in the object
		h.serverTLSConfig.Certificates = []tls.Certificate{*opts.ServerCertificate}

		return clientTLSConfig, nil
	}

	// Generate a self-signed certificate
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate private key for the self-signed certificate: %w", err)
	}

	now := time.Now()
	tpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Actor Host"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(180 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, fmt.Errorf("failed to create self-signed certificate: %w", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal self-signed private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to create self-signed key pair: %w", err)
	}

	h.serverTLSConfig.Certificates = []tls.Certificate{cert}

	return clientTLSConfig, nil
}
