package bootstrapauth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v4/jwa"
	"github.com/lestrrat-go/jwx/v4/jwk"
	"github.com/lestrrat-go/jwx/v4/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJWTValidator(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	// Build a static JWKS holding the Ed25519 public key
	jwks := fmt.Sprintf(`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k1","x":%q}]}`, base64.RawURLEncoding.EncodeToString(pub))

	v, err := NewJWTValidator(context.Background(), JWTConfig{
		Issuer:     "https://issuer.example",
		Audience:   "francis",
		StaticJWKS: json.RawMessage(jwks),
	})
	require.NoError(t, err)

	// Signing with a jwk.Key that carries a kid makes jwx stamp the same kid on the token header
	signingKey := newTestSigningKey(t, priv, "k1")
	sign := func(b *jwt.Builder) string {
		return signTestToken(t, b, jwa.EdDSA(), signingKey)
	}

	// A valid token with a jti validates and returns its subject and join token
	sub, jt, jtExp, err := v.Validate(sign(jwt.NewBuilder().
		JwtID("unique-bootstrap-token-1").
		Issuer("https://issuer.example").
		Subject("spiffe://platform/host/abc").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Hour)),
	))
	require.NoError(t, err)
	assert.Equal(t, "spiffe://platform/host/abc", sub)
	assert.Equal(t, "unique-bootstrap-token-1", jt)
	assert.False(t, jtExp.IsZero())

	// A token without a jti is accepted
	// The returned join token is empty because there is nothing to track
	sub, jt, jtExp, err = v.Validate(sign(jwt.NewBuilder().
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Minute)),
	))
	require.NoError(t, err)
	assert.Equal(t, "x", sub)
	assert.Empty(t, jt)
	assert.True(t, jtExp.IsZero())

	// A token whose remaining lifetime exceeds the maximum is rejected
	_, _, _, err = v.Validate(sign(jwt.NewBuilder().
		JwtID("too-long-lived").
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(2 * time.Hour)),
	))
	require.Error(t, err)

	// A token with a jti but no exp is accepted
	// The join token is empty because we cannot bound the replay window
	_, jt, jtExp, err = v.Validate(sign(jwt.NewBuilder().
		JwtID("jti-no-exp").
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}),
	))
	require.NoError(t, err)
	assert.Empty(t, jt)
	assert.True(t, jtExp.IsZero())

	// A wrong audience is rejected
	_, _, _, err = v.Validate(sign(jwt.NewBuilder().
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"other"}).
		Expiration(time.Now().Add(time.Hour)),
	))
	require.Error(t, err)

	// A wrong issuer is rejected
	_, _, _, err = v.Validate(sign(jwt.NewBuilder().
		Issuer("https://evil.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Hour)),
	))
	require.Error(t, err)

	// An expired token is rejected
	_, _, _, err = v.Validate(sign(jwt.NewBuilder().
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(-time.Hour)),
	))
	require.Error(t, err)

	// A token signed by a different key is rejected
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherSigned := signTestToken(t, jwt.NewBuilder().
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Hour)),
		jwa.EdDSA(), newTestSigningKey(t, otherPriv, "k1"),
	)
	_, _, _, err = v.Validate(otherSigned)
	require.Error(t, err)

	// A token signed with an algorithm outside the allowlist is rejected before its signature is even considered
	hmacKey, err := jwk.Import[jwk.Key]([]byte("not-a-public-key-at-all-but-long-enough"))
	require.NoError(t, err)
	err = hmacKey.Set(jwk.KeyIDKey, "k1")
	require.NoError(t, err)
	hmacSigned := signTestToken(t, jwt.NewBuilder().
		Issuer("https://issuer.example").
		Subject("x").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Hour)),
		jwa.HS256(), hmacKey,
	)
	_, _, _, err = v.Validate(hmacSigned)
	require.ErrorContains(t, err, "not allowed")
}

func TestJWTValidatorWithJWKSURL(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	jwks := fmt.Sprintf(`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k1","x":%q}]}`, base64.RawURLEncoding.EncodeToString(pub))

	// Serving the JWKS from a local endpoint exercises the remote key source rather than the inline one
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwks))
	}))
	t.Cleanup(srv.Close)

	// The context bounds the background refresh workers, so cancelling it releases them when the test ends
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	v, err := NewJWTValidator(ctx, JWTConfig{
		Issuer:   "https://issuer.example",
		Audience: "francis",
		JWKSURL:  srv.URL,
	})
	require.NoError(t, err)

	// Registering the endpoint waits for the first fetch, so the keys are already loaded here
	token := signTestToken(t, jwt.NewBuilder().
		JwtID("remote-jwks-token").
		Issuer("https://issuer.example").
		Subject("spiffe://platform/host/remote").
		Audience([]string{"francis"}).
		Expiration(time.Now().Add(time.Minute)),
		jwa.EdDSA(), newTestSigningKey(t, priv, "k1"),
	)

	sub, jt, jtExp, err := v.Validate(token)
	require.NoError(t, err)
	assert.Equal(t, "spiffe://platform/host/remote", sub)
	assert.Equal(t, "remote-jwks-token", jt)
	assert.False(t, jtExp.IsZero())
}

func TestNewJWTValidatorRejectsBadConfig(t *testing.T) {
	// Issuer and audience are required
	_, err := NewJWTValidator(context.Background(), JWTConfig{Audience: "francis", StaticJWKS: json.RawMessage(`{"keys":[]}`)})
	require.Error(t, err)

	// Exactly one key source must be configured
	_, err = NewJWTValidator(context.Background(), JWTConfig{Issuer: "i", Audience: "a"})
	require.Error(t, err)

	_, err = NewJWTValidator(context.Background(), JWTConfig{Issuer: "i", Audience: "a", JWKSURL: "https://x", StaticJWKS: json.RawMessage(`{"keys":[]}`)})
	require.Error(t, err)
}

// newTestSigningKey wraps a raw key as a jwk.Key carrying the given key ID
func newTestSigningKey(t *testing.T, raw any, kid string) jwk.Key {
	t.Helper()

	key, err := jwk.Import[jwk.Key](raw)
	require.NoError(t, err)
	err = key.Set(jwk.KeyIDKey, kid)
	require.NoError(t, err)

	return key
}

// signTestToken builds and signs the token described by the builder
func signTestToken(t *testing.T, b *jwt.Builder, alg jwa.SignatureAlgorithm, key jwk.Key) string {
	t.Helper()

	tok, err := b.Build()
	require.NoError(t, err)
	signed, err := jwt.Sign(tok, jwt.WithKey(alg, key))
	require.NoError(t, err)

	return string(signed)
}
