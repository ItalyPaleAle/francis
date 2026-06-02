// Package wt centralizes the WebTransport server and client configuration shared by the runtime and hosts
package wt

import (
	"crypto/tls"
	"errors"
	"net/http"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// NewServer builds a WebTransport server bound to addr that serves handler
// It advertises the HTTP/3 ALPN and the WebTransport SETTINGS so clients can negotiate sessions
func NewServer(addr string, tlsConfig *tls.Config, handler http.Handler) *webtransport.Server {
	// WebTransport runs only over HTTP/3, so the server offers just the h3 ALPN
	tlsConfig.NextProtos = []string{http3.NextProtoH3}

	srv := &webtransport.Server{
		H3: &http3.Server{
			Addr:       addr,
			TLSConfig:  tlsConfig,
			QUICConfig: quicConfig(),
			Handler:    handler,
		},
	}

	// Enable the WebTransport SETTINGS and make the QUIC connection available to Upgrade
	webtransport.ConfigureHTTP3Server(srv.H3)
	return srv
}

// NewDialer builds a WebTransport client dialer with the required QUIC settings
// The provided tlsConfig must already advertise the HTTP/3 ALPN
func NewDialer(tlsConfig *tls.Config) *webtransport.Dialer {
	return &webtransport.Dialer{
		TLSClientConfig: tlsConfig,
		QUICConfig:      quicConfig(),
	}
}

// IsServeError returns true if an error returned by webtransport.Server's ListenAndServe is a serve error to report, usually during initialization
// It ignores the ErrServerClosed errors
func IsServeError(err error) bool {
	return err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, quic.ErrServerClosed)
}

// quicConfig returns the QUIC configuration required for WebTransport
// Both datagram support and stream-reset partial delivery are mandatory for the WebTransport handshake
func quicConfig() *quic.Config {
	return &quic.Config{
		EnableDatagrams:                  true,
		EnableStreamResetPartialDelivery: true,
	}
}
