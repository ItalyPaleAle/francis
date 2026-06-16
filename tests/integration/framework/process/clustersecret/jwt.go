//go:build integration

package clustersecret

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/italypaleale/francis/internal/bootstrapauth"
)

// JWTBootstrap is a self-contained host-bootstrap JWT authority for the remote topology
// It mints an Ed25519 keypair and the matching JWKS, so the runtime can validate tokens against the inline key set while each joining host presents a token signed by the same key
type JWTBootstrap struct {
	// Issuer and Audience are the iss and aud claims the runtime enforces and the tokens carry
	Issuer   string
	Audience string

	priv ed25519.PrivateKey
	jwks json.RawMessage
}

// NewJWTBootstrap builds a JWT authority with a freshly generated signing key and its inline JWKS
func NewJWTBootstrap() (*JWTBootstrap, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate Ed25519 key: %w", err)
	}

	jwks := fmt.Sprintf(`{"keys":[{"kty":"OKP","crv":"Ed25519","kid":"k1","x":%q}]}`, base64.RawURLEncoding.EncodeToString(pub))

	return &JWTBootstrap{
		Issuer:   "francis-integration-issuer",
		Audience: "francis-integration-runtime",
		priv:     priv,
		jwks:     json.RawMessage(jwks),
	}, nil
}

// RuntimeConfig returns the JWT validation config the runtime is configured with, validating tokens against the inline JWKS
func (j *JWTBootstrap) RuntimeConfig() bootstrapauth.JWTConfig {
	return bootstrapauth.JWTConfig{
		Issuer:     j.Issuer,
		Audience:   j.Audience,
		StaticJWKS: j.jwks,
	}
}

// Token signs a host bootstrap token with the given subject, valid for the given duration
// The subject becomes the host's platform identity that the runtime reads from the validated token
func (j *JWTBootstrap) Token(subject string, ttl time.Duration) (string, error) {
	now := time.Now()
	tok := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.RegisteredClaims{
		Issuer:    j.Issuer,
		Subject:   subject,
		Audience:  jwt.ClaimStrings{j.Audience},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
	})

	signed, err := tok.SignedString(j.priv)
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}
	return signed, nil
}
