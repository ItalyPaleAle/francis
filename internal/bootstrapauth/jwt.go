package bootstrapauth

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jwx-go/jwkfetch/v4"
	"github.com/lestrrat-go/httprc/v3"
	"github.com/lestrrat-go/jwx/v4/jwa"
	"github.com/lestrrat-go/jwx/v4/jwk"
	"github.com/lestrrat-go/jwx/v4/jws"
	"github.com/lestrrat-go/jwx/v4/jwt"
)

// maxJWTLifetime caps how far in the future the exp claim may be set to bound the window a captured token remains usable
const maxJWTLifetime = time.Hour

// jwksInitialFetchTimeout bounds how long startup waits for the first fetch of a remote JWKS before letting the cache finish it in the background
const jwksInitialFetchTimeout = 10 * time.Second

// jwksShutdownTimeout bounds how long we wait for the background JWKS refresh workers to drain once the validator's context is done
const jwksShutdownTimeout = 10 * time.Second

// jwtChannelBindDomain domain-separates the Ed25519 proof-of-possession signature that binds a JWT bootstrap registration to its TLS session
var jwtChannelBindDomain = []byte("francis-bootstrap-jwt-v1")

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
	// AllowedSubjects restricts which sub claims are accepted, supporting exact matches and '*' wildcards (such as "system:serviceaccount:my-ns:*")
	// When empty, any subject is accepted
	AllowedSubjects []string
}

// JWTValidator validates host bootstrap tokens against a configured key source
type JWTValidator struct {
	parseOpts       []jwt.ParseOption
	allowedSubjects []string
}

// NewJWTValidator builds a validator from the given config
// The context bounds the lifetime of the background JWKS refresh goroutines when a remote URL is used
func NewJWTValidator(ctx context.Context, cfg JWTConfig) (*JWTValidator, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("JWT issuer is required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("JWT audience is required")
	}
	if slices.Contains(cfg.AllowedSubjects, "") {
		return nil, errors.New("JWT allowed subjects must not contain empty entries")
	}

	// Build the key source from either a remote JWKS URL or a static, inline JWKS document
	var (
		keySet jwk.Set
		err    error
	)
	switch {
	case cfg.JWKSURL != "" && len(cfg.StaticJWKS) > 0:
		return nil, errors.New("only one of JWKS URL or static JWKS may be set")
	case cfg.JWKSURL != "":
		keySet, err = newCachedJWKS(ctx, cfg.JWKSURL)
	case len(cfg.StaticJWKS) > 0:
		keySet, err = jwk.Parse(cfg.StaticJWKS)
	default:
		return nil, errors.New("one of JWKS URL or static JWKS is required")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build JWT key source: %w", err)
	}

	// Validate the standard claims as part of parsing so a malformed issuer, audience, or missing expiry is rejected centrally
	parseOpts := []jwt.ParseOption{
		// Publishing a JWKS whose keys carry no "alg" is common, so the algorithm is inferred from the key type instead of the key being skipped
		// Falling back to the only key in the set keeps tokens that carry no "kid" working, which is what a single-key JWKS relies on
		jwt.WithKeySet(keySet,
			jws.WithInferAlgorithmFromKey(true),
			jws.WithUseDefault(true),
		),
		jwt.WithIssuer(cfg.Issuer),
		jwt.WithAudience(cfg.Audience),
		jwt.WithRequiredClaim(jwt.ExpirationKey),
		jwt.WithAcceptableSkew(time.Minute),
	}

	return &JWTValidator{
		parseOpts:       parseOpts,
		allowedSubjects: slices.Clone(cfg.AllowedSubjects),
	}, nil
}

// Validate checks the token's signature and standard claims and returns the subject and, when present, the join token (jti) and its expiry
// The join token is empty when the token carries no jti
func (v *JWTValidator) Validate(token string) (subject, joinToken string, expiresAt time.Time, err error) {
	raw := []byte(token)

	// Reject disallowed signing algorithms up front, before any signature is verified
	err = checkSigningAlgorithm(raw)
	if err != nil {
		return "", "", time.Time{}, err
	}

	parsed, err := jwt.Parse(raw, v.parseOpts...)
	if err != nil {
		return "", "", time.Time{}, fmt.Errorf("token validation failed: %w", err)
	}

	// Require an exp of at most maxJWTLifetime on every token
	exp, ok := parsed.Expiration()
	if !ok {
		return "", "", time.Time{}, errors.New("token validation failed: missing exp claim")
	}
	if time.Until(exp) > maxJWTLifetime {
		return "", "", time.Time{}, fmt.Errorf("token lifetime exceeds maximum of %v", maxJWTLifetime)
	}

	// The subject is the host's platform identity, checked against the allowlist when one is configured
	subject, _ = parsed.Subject()
	if len(v.allowedSubjects) > 0 && !matchesAllowedSubject(v.allowedSubjects, subject) {
		return "", "", time.Time{}, errors.New("token subject is not allowed")
	}

	// A missing jti means there is nothing to track for replay protection
	jti, ok := parsed.JwtID()
	if !ok || jti == "" {
		return subject, "", time.Time{}, nil
	}

	return subject, jti, exp, nil
}

// SignJWTChannelBinding signs the TLS channel-binding value and bootstrap token with the host's workload private key
// This binds the registration request and workload public key to one specific TLS session
func SignJWTChannelBinding(priv ed25519.PrivateKey, cb []byte, token string) []byte {
	return ed25519.Sign(priv, jwtChannelBindTranscript(cb, token))
}

// VerifyJWTChannelBinding verifies that proof was signed by pub over the given TLS channel-binding value and bootstrap token
func VerifyJWTChannelBinding(pub []byte, cb []byte, token string, proof []byte) bool {
	if len(pub) != ed25519.PublicKeySize || len(proof) != ed25519.SignatureSize || len(cb) == 0 {
		return false
	}

	return ed25519.Verify(ed25519.PublicKey(pub), jwtChannelBindTranscript(cb, token), proof)
}

func jwtChannelBindTranscript(cb []byte, token string) []byte {
	out := make([]byte, 0, 12+len(jwtChannelBindDomain)+len(cb)+len(token))
	out = appendField(out, jwtChannelBindDomain)
	out = appendField(out, cb)
	out = appendField(out, []byte(token))
	return out
}

// matchesAllowedSubject reports whether subject is non-empty and matches at least one configured pattern
func matchesAllowedSubject(patterns []string, subject string) bool {
	if subject == "" {
		return false
	}

	for _, pattern := range patterns {
		if matchWildcard(pattern, subject) {
			return true
		}
	}

	return false
}

// matchWildcard matches value against pattern in linear time, where '*' matches any sequence of characters (including '/' and ':')
func matchWildcard(pattern string, value string) bool {
	var pi, vi, matchIdx int
	starIdx := -1
	for vi < len(value) {
		switch {
		case pi < len(pattern) && pattern[pi] == '*':
			starIdx = pi
			matchIdx = vi
			pi++
		case pi < len(pattern) && pattern[pi] == value[vi]:
			pi++
			vi++
		case starIdx != -1:
			pi = starIdx + 1
			matchIdx++
			vi = matchIdx
		default:
			return false
		}
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern)
}

// checkSigningAlgorithm rejects tokens signed with an algorithm we do not accept
// It is a separate step because jwx takes the verification algorithm from the key in the JWK set rather than from the token, so this is the only place the token's own choice is constrained
func checkSigningAlgorithm(raw []byte) error {
	msg, err := jws.Parse(raw)
	if err != nil {
		return fmt.Errorf("token validation failed: %w", err)
	}

	// A host bootstrap token is a compact JWS, which carries exactly one signature
	sigs := msg.Signatures()
	if len(sigs) != 1 {
		return fmt.Errorf("token validation failed: expected exactly one signature, got %d", len(sigs))
	}

	// The algorithm must come from the protected header, since an unprotected one is not covered by the signature
	hdr := sigs[0].ProtectedHeaders()
	if hdr == nil {
		return errors.New("token validation failed: token has no protected header")
	}
	alg, ok := hdr.Algorithm()
	if !ok {
		return errors.New("token validation failed: token does not declare a signing algorithm")
	}

	// The accepted algorithms are the asymmetric ones, which leaves out "none" and the symmetric algorithms that would be unsafe against a set of public keys
	// Both EdDSA and Ed25519 are accepted because RFC 9864 made them distinct identifiers for the same Ed25519 signatures and issuers are split between the two
	switch alg {
	case jwa.RS256(), jwa.RS384(), jwa.RS512(),
		jwa.PS256(), jwa.PS384(), jwa.PS512(),
		jwa.ES256(), jwa.ES384(), jwa.ES512(),
		jwa.EdDSA(), jwa.EdDSAEd25519():
		return nil
	default:
		return fmt.Errorf("token validation failed: signing algorithm %q is not allowed", alg)
	}
}

// newCachedJWKS returns a key set backed by a remote JWKS endpoint that is refreshed in the background
// The returned set always reflects the latest successful fetch, so the validator can hold on to it for its whole lifetime
func newCachedJWKS(ctx context.Context, url string) (jwk.Set, error) {
	cache, err := jwkfetch.NewCache(ctx, httprc.NewClient())
	if err != nil {
		return nil, fmt.Errorf("failed to start the JWKS cache: %w", err)
	}

	// Registering waits for the first fetch so a reachable endpoint has its keys loaded before the first host tries to join
	// The wait is bounded and a first fetch that does not land is not fatal, because an endpoint that is briefly unreachable must not stop the runtime from starting
	registerCtx, cancel := context.WithTimeout(ctx, jwksInitialFetchTimeout)
	defer cancel()
	err = cache.Register(registerCtx, url)
	if err != nil && !errors.Is(err, httprc.ErrNotReady()) {
		shutdownJWKS(ctx, cache)
		return nil, fmt.Errorf("failed to register JWKS endpoint %q: %w", url, err)
	}

	set, err := cache.CachedSet(url)
	if err != nil {
		shutdownJWKS(ctx, cache)
		return nil, fmt.Errorf("failed to read the cached JWKS for %q: %w", url, err)
	}

	// The cache owns background workers that must be released explicitly, and the caller's context is what bounds their lifetime
	go func() {
		<-ctx.Done()
		shutdownJWKS(ctx, cache)
	}()

	return set, nil
}

// shutdownJWKS releases the background workers owned by a JWKS cache
// The parent context is usually already done by the time this runs, so the shutdown gets a fresh deadline of its own
func shutdownJWKS(ctx context.Context, cache *jwkfetch.Cache) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), jwksShutdownTimeout)
	defer cancel()
	_ = cache.Shutdown(shutdownCtx)
}
