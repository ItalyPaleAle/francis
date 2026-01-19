package francis

import "errors"

var (
	// ErrClientNotFound is returned when a client is not found
	ErrClientNotFound = errors.New("client not found")
	// ErrClientNotConnected is returned when trying to send to a disconnected client
	ErrClientNotConnected = errors.New("client not connected")
	// ErrAlreadyRunning is returned when the server is already running
	ErrAlreadyRunning = errors.New("server is already running")
	// ErrProviderRequired is returned when no provider is configured
	ErrProviderRequired = errors.New("provider is required")
	// ErrTLSRequired is returned when TLS configuration is missing
	ErrTLSRequired = errors.New("TLS configuration is required for WebTransport")
)
