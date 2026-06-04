package peer

import (
	"net/http"
)

// Authenticator authenticates peer WebTransport sessions at establishment
// A nil Authenticator means the session relies on transport-level (TLS) authentication only
type Authenticator interface {
	// UpdateHeader sets authentication headers on the outgoing session dial request
	UpdateHeader(h http.Header) error
	// ValidateIncomingRequest authorizes an incoming session upgrade request
	ValidateIncomingRequest(r *http.Request) (bool, error)
}
