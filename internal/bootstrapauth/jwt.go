package bootstrapauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// jwtValidMethods is the allowlist of signing algorithms, chosen to exclude "none" and symmetric algorithms that would be unsafe with public keys
var jwtValidMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}

// JWTConfig configures validation of host bootstrap tokens
// Exactly one of JWKSURL or StaticJWKS must be set, which is how the pluggable key source is selected
type JWTConfig struct {
	// Issuer is the expected iss claim
	Issuer string
	// Audience is the expected aud claim
	Audience string
	// JWKSURL is a remote JWKS endpoint whose keys are fetched and refreshed in the background
	JWKSURL string
	// StaticJWKS is an inline JWKS document (JSON-encoded), used for tests or air-gapped clusters
	StaticJWKS json.RawMessage
}

// JWTValidator validates host bootstrap tokens against a configured key source
type JWTValidator struct {
	parser  *jwt.Parser
	keyfunc jwt.Keyfunc
}

// NewJWTValidator builds a validator from the given config
// The context bounds the lifetime of the background JWKS refresh goroutine when a remote URL is used
func NewJWTValidator(ctx context.Context, cfg JWTConfig) (*JWTValidator, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("JWT issuer is required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("JWT audience is required")
	}

	// Build the key source from either a remote JWKS URL or a static, inline JWKS document
	var kf keyfunc.Keyfunc
	var err error
	switch {
	case cfg.JWKSURL != "" && len(cfg.StaticJWKS) > 0:
		return nil, errors.New("only one of JWKS URL or static JWKS may be set")
	case cfg.JWKSURL != "":
		kf, err = keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURL})
	case len(cfg.StaticJWKS) > 0:
		kf, err = keyfunc.NewJWKSetJSON(cfg.StaticJWKS)
	default:
		return nil, errors.New("one of JWKS URL or static JWKS is required")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build JWT key source: %w", err)
	}

	// Validate the standard claims as part of parsing so a malformed issuer, audience, or expiry is rejected centrally
	parser := jwt.NewParser(
		jwt.WithValidMethods(jwtValidMethods),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(time.Minute),
	)

	return &JWTValidator{parser: parser, keyfunc: kf.Keyfunc}, nil
}

// Validate checks the token's signature and standard claims and returns the subject, which is the host's platform identity
func (v *JWTValidator) Validate(token string) (string, error) {
	parsed, err := v.parser.Parse(token, v.keyfunc)
	if err != nil {
		return "", fmt.Errorf("token validation failed: %w", err)
	}
	if !parsed.Valid {
		return "", errors.New("token is invalid")
	}

	sub, err := parsed.Claims.GetSubject()
	if err != nil {
		return "", fmt.Errorf("failed to read token subject: %w", err)
	}
	return sub, nil
}
