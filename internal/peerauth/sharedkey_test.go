package peerauth

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerAuthenticationSharedKeyValidate(t *testing.T) {
	t.Run("accepts a sufficiently long key", func(t *testing.T) {
		require.NoError(t, (&PeerAuthenticationSharedKey{Key: "this-is-a-shared-key"}).Validate())
	})

	t.Run("rejects an empty key", func(t *testing.T) {
		require.Error(t, (&PeerAuthenticationSharedKey{}).Validate())
	})

	t.Run("rejects a too-short key", func(t *testing.T) {
		require.Error(t, (&PeerAuthenticationSharedKey{Key: "short"}).Validate())
	})
}

func TestPeerAuthenticationSharedKeyValidateIncomingRequest(t *testing.T) {
	// This test cannot assert the comparison is constant-time, only that the header is parsed and matched correctly
	auth := &PeerAuthenticationSharedKey{Key: "this-is-a-shared-key"}

	t.Run("accepts the header written by UpdateHeader", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		require.NoError(t, auth.UpdateHeader(r.Header))

		ok, err := auth.ValidateIncomingRequest(r)
		require.NoError(t, err)
		assert.True(t, ok, "a request carrying the correct shared key must be authorized")
	})

	t.Run("rejects a wrong key", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(headerAuthorization, authorizationHeaderSharedKey+" wrong-key-value-here")

		ok, err := auth.ValidateIncomingRequest(r)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("rejects a missing authorization header", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}

		ok, err := auth.ValidateIncomingRequest(r)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("rejects a different authorization scheme", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(headerAuthorization, "Bearer this-is-a-shared-key")

		ok, err := auth.ValidateIncomingRequest(r)
		require.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("rejects a value with no scheme separator", func(t *testing.T) {
		r := &http.Request{Header: http.Header{}}
		r.Header.Set(headerAuthorization, "this-is-a-shared-key")

		ok, err := auth.ValidateIncomingRequest(r)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}
