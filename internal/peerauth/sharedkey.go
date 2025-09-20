package peerauth

import (
	"errors"
	"net/http"
	"strings"
)

const (
	headerAuthorization          = "Authorization"
	authorizationHeaderSharedKey = "PSK"
)

// PeerAuthenticationSharedKey configures peer authentication to use a shared key
type PeerAuthenticationSharedKey struct {
	// Shared key
	Key string
}

func (p *PeerAuthenticationSharedKey) Validate() error {
	if p.Key == "" {
		return errors.New("key is empty")
	}
	if len(p.Key) < 16 {
		return errors.New("key must be at least 16-characters long")
	}
	return nil
}

func (p *PeerAuthenticationSharedKey) UpdateRequest(r *http.Request) error {
	r.Header.Set(headerAuthorization, authorizationHeaderSharedKey+" "+p.Key)
	return nil
}

func (p *PeerAuthenticationSharedKey) ValidateIncomingRequest(r *http.Request) (bool, error) {
	prefix, value, ok := strings.Cut(r.Header.Get(headerAuthorization), " ")
	if !ok || prefix != authorizationHeaderSharedKey {
		return false, nil
	}

	return value == p.Key, nil
}
