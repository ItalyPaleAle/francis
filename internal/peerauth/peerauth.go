package peerauth

import (
	"net/http"
)

// Interface implemented by all peer authentication methods
type PeerAuthenticationMethod interface {
	// Validate the peer authentication method
	Validate() error

	// UpdateRequest updates a request object while messaging another host
	UpdateRequest(r *http.Request) error

	// ValidateIncomingRequest checks if the incoming request is authorized
	ValidateIncomingRequest(r *http.Request) (bool, error)
}
