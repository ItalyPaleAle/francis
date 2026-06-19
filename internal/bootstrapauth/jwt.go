package bootstrapauth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

// jwtValidMethods is the allowlist of signing algorithms, chosen to exclude "none" and symmetric algorithms that would be unsafe with public keys
var jwtValidMethods = []string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "PS256", "PS384", "PS512", "EdDSA"}

// maxJWTLifetime caps how far in the future the exp claim may be set to bound the window a captured token remains usable
const maxJWTLifetime = time.Hour

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
	parser   *jwt.Parser
	keyfunc  jwt.Keyfunc
	mu       sync.Mutex
	usedJTIs map[string]time.Time // jti → expiry with leeway; guards replay across concurrent registrations
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

	return &JWTValidator{
		parser:   parser,
		keyfunc:  kf.Keyfunc,
		usedJTIs: make(map[string]time.Time),
	}, nil
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

	// Require a jti claim so each token can be individually tracked to block replay
	mapClaims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return "", errors.New("unexpected claims type")
	}
	jti, _ := mapClaims["jti"].(string)
	if jti == "" {
		return "", errors.New("token is missing required jti claim")
	}

	// Reject tokens whose remaining lifetime exceeds the maximum to limit how long a captured token can be replayed
	exp, expErr := parsed.Claims.GetExpirationTime()
	if expErr != nil || exp == nil {
		return "", errors.New("token is missing expiration")
	}
	if time.Until(exp.Time) > maxJWTLifetime {
		return "", fmt.Errorf("token lifetime exceeds maximum of %v", maxJWTLifetime)
	}

	// Record the jti to prevent the same token from being accepted twice; prune expired entries on each call to avoid unbounded growth
	v.mu.Lock()
	_, seen := v.usedJTIs[jti]
	if !seen {
		now := time.Now()
		for k, expAt := range v.usedJTIs {
			if now.After(expAt) {
				delete(v.usedJTIs, k)
			}
		}
		// Retain the entry until the token can no longer be accepted even with the parser's leeway applied
		v.usedJTIs[jti] = exp.Time.Add(time.Minute)
	}
	v.mu.Unlock()

	if seen {
		return "", errors.New("token jti has already been used")
	}

	sub, err := parsed.Claims.GetSubject()
	if err != nil {
		return "", fmt.Errorf("failed to read token subject: %w", err)
	}
	return sub, nil
}
