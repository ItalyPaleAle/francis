// Package bootstrapauth implements the registration-time authentication used when a host first joins a runtime
// A host proves it is allowed to join either with a host pre-shared key, via a channel-bound challenge-response, or with a JWT validated against a JWKS
// Once a host has bootstrapped it receives a workload certificate and all later connections use mTLS, so these methods run only at the first registration
package bootstrapauth

import (
	"crypto/rand"
)

// Method identifies which bootstrap method a cluster is configured to use
const (
	MethodPSK = "psk"
	MethodJWT = "jwt"
)

// NonceSize is the exact length in bytes of the random nonces exchanged during the PSK challenge-response
const NonceSize = 32

// Nonce returns a fresh random nonce for the PSK challenge-response
func Nonce() ([]byte, error) {
	b := make([]byte, NonceSize)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}
