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
	"strconv"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/actors/actor"
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

func (h *Host) getServerHandler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("POST /v1/invoke/{actorType}/{actorID}/{method}", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var (
			err     error
			reqData any
		)

		// Validate the request is for the correct host
		// It can happen that clients make calls to incorrect hosts if the app just (re-)started and their cached data is stale
		reqHostID := r.Header.Get(headerXHostID)
		if reqHostID == "" {
			errApiReqInvokeHostIdEmpty.WriteResponse(w)
			return
		} else if reqHostID != h.hostID {
			errApiReqInvokeHostIdMismatch.WriteResponse(w)
			return
		}

		// Read the request body
		ct := r.Header.Get(headerContentType)
		switch ct {
		case contentTypeMsgpack:
			dec := msgpack.GetDecoder()
			defer msgpack.PutDecoder(dec)
			dec.Reset(r.Body)
			err = dec.Decode(&reqData)
			if err != nil {
				errApiReqInvokeBody.
					Clone(withInnerError(err)).
					WriteResponse(w)
				return
			}
		case "":
			// Ignore the body if the content type is unsupported
		default:
			errApiReqInvokeContentType.WriteResponse(w)
			return
		}

		// Invoke the actor
		actorType := r.PathValue("actorType")
		outData, err := h.InvokeLocal(r.Context(), actorType, r.PathValue("actorID"), r.PathValue("method"), reqData)
		switch {
		case errors.Is(err, actor.ErrActorNotHosted):
			errApiActorNotHosted.WriteResponse(w)
			return
		case errors.Is(err, actor.ErrActorHalted):
			// Get the deactivation timeout for the actor type (in ms), and include the ActorDeactivationTimeout metadata key to aid the caller in deciding how long to wait
			dt := h.deactivationTimeoutForActorType(actorType).Milliseconds()
			errApiActorHalted.
				Clone(withMetadata(map[string]string{
					errMetadataActorDeactivationTimeout: strconv.FormatInt(dt, 10),
				})).
				WriteResponse(w)
			return
		case err != nil:
			errApiInvokeFailed.
				Clone(withInnerError(err)).
				WriteResponse(w)
			return
		}

		// If there's no output data, respond with 204 No Content
		if outData == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Respond with the body
		// Set the content type for msgpack if we have a body
		w.Header().Set(headerContentType, contentTypeMsgpack)
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

	// Add the middleware adding the host ID to each response
	handler := h.hostIdHeaderServerMiddleware(mux)

	return handler
}

func (h *Host) hostIdHeaderServerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Add(headerXHostID, h.hostID)
		next.ServeHTTP(w, req)
	})
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
