package bootstrapauth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
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

	sign := func(claims jwt.RegisteredClaims) string {
		tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		tok.Header["kid"] = "k1"
		signed, signErr := tok.SignedString(priv)
		require.NoError(t, signErr)
		return signed
	}

	// A valid token with a jti validates and returns its subject
	sub, err := v.Validate(sign(jwt.RegisteredClaims{
		ID:        "unique-bootstrap-token-1",
		Issuer:    "https://issuer.example",
		Subject:   "spiffe://platform/host/abc",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}))
	require.NoError(t, err)
	assert.Equal(t, "spiffe://platform/host/abc", sub)

	// A token missing a jti is rejected
	_, err = v.Validate(sign(jwt.RegisteredClaims{
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}))
	require.Error(t, err)

	// A token whose remaining lifetime exceeds the maximum is rejected
	_, err = v.Validate(sign(jwt.RegisteredClaims{
		ID:        "too-long-lived",
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(2 * time.Hour)),
	}))
	require.Error(t, err)

	// The same valid token cannot be presented twice (replay protection)
	replayToken := sign(jwt.RegisteredClaims{
		ID:        "replay-me",
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	})
	_, err = v.Validate(replayToken)
	require.NoError(t, err)
	_, err = v.Validate(replayToken)
	require.Error(t, err)

	// A wrong audience is rejected
	_, err = v.Validate(sign(jwt.RegisteredClaims{
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"other"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}))
	require.Error(t, err)

	// A wrong issuer is rejected
	_, err = v.Validate(sign(jwt.RegisteredClaims{
		Issuer:    "https://evil.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}))
	require.Error(t, err)

	// An expired token is rejected
	_, err = v.Validate(sign(jwt.RegisteredClaims{
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
	}))
	require.Error(t, err)

	// A token signed by a different key is rejected
	_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	otherTok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{
		Issuer:    "https://issuer.example",
		Subject:   "x",
		Audience:  jwt.ClaimStrings{"francis"},
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	})
	otherTok.Header["kid"] = "k1"
	otherSigned, err := otherTok.SignedString(otherPriv)
	require.NoError(t, err)
	_, err = v.Validate(otherSigned)
	require.Error(t, err)
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
