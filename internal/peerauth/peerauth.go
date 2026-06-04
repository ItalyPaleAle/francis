package peerauth

import (
	"net/http"
)

// PeerAuthenticationMethod is the interface implemented by all peer authentication methods
type PeerAuthenticationMethod interface {
	// Validate the peer authentication method
	Validate() error

	// UpdateHeader stamps authentication credentials on the headers of an outgoing peer session
	UpdateHeader(h http.Header) error

	// ValidateIncomingRequest checks if the incoming peer session request is authorized
	ValidateIncomingRequest(r *http.Request) (bool, error)
}
