package management

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// Scope is an authorization scope granted to a caller
type Scope string

const (
	ScopeClusterRead       Scope = "cluster:read"
	ScopeActorsRead        Scope = "actors:read"
	ScopeActorsStateRead   Scope = "actors:state:read"
	ScopeWorkflowsRead     Scope = "workflows:read"
	ScopeWorkflowsDataRead Scope = "workflows:data:read"
	ScopeJobsRead          Scope = "jobs:read"
	ScopeActorsManage      Scope = "actors:manage"
	ScopeHostsManage       Scope = "hosts:manage"
	ScopeWorkflowsManage   Scope = "workflows:manage"
)

// Caller is an authenticated caller and the scopes it was granted
type Caller struct {
	// TokenSuffix is the last characters of the token, used to identify it in audit logs
	TokenSuffix string
	scopes      map[Scope]struct{}
}

// Has reports whether the caller was granted the scope
func (c *Caller) Has(s Scope) bool {
	if c == nil {
		return false
	}

	_, ok := c.scopes[s]
	return ok
}

// tokenEntry is a configured token, stored as a hash so every comparison has the same length
type tokenEntry struct {
	hash   [sha256.Size]byte
	suffix string
	scopes map[Scope]struct{}
}

// authenticator validates bearer tokens against the configured lists
type authenticator struct {
	tokens []tokenEntry
}

func newAuthenticator(cfg Config) *authenticator {
	// Read-only tokens receive every scope except those ending in ":manage", which authorize actions
	// TestAuthenticatorScopes checks both lists against every declared scope, so a new scope can't be left out
	ro := map[Scope]struct{}{
		ScopeClusterRead:       {},
		ScopeActorsRead:        {},
		ScopeActorsStateRead:   {},
		ScopeWorkflowsRead:     {},
		ScopeWorkflowsDataRead: {},
		ScopeJobsRead:          {},
	}

	// Management tokens receive every scope
	mgmt := map[Scope]struct{}{
		ScopeClusterRead:       {},
		ScopeActorsRead:        {},
		ScopeActorsStateRead:   {},
		ScopeWorkflowsRead:     {},
		ScopeWorkflowsDataRead: {},
		ScopeJobsRead:          {},
		ScopeActorsManage:      {},
		ScopeHostsManage:       {},
		ScopeWorkflowsManage:   {},
	}

	a := &authenticator{
		tokens: make([]tokenEntry, 0, len(cfg.ReadOnlyTokens)+len(cfg.ManagementTokens)),
	}
	for _, t := range cfg.ReadOnlyTokens {
		a.tokens = append(a.tokens, tokenEntry{hash: sha256.Sum256([]byte(t)), suffix: tokenSuffix(t), scopes: ro})
	}
	for _, t := range cfg.ManagementTokens {
		a.tokens = append(a.tokens, tokenEntry{hash: sha256.Sum256([]byte(t)), suffix: tokenSuffix(t), scopes: mgmt})
	}

	return a
}

// authenticate returns the caller for the request's bearer token, or nil when it is missing or unknown
// Tokens are only accepted in the Authorization header, never in the query string
func (a *authenticator) authenticate(r *http.Request) *Caller {
	header := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(header) <= len(prefix) || strings.ToLower(header[:len(prefix)]) != prefix {
		return nil
	}

	token := strings.TrimSpace(header[len(prefix):])
	if token == "" {
		return nil
	}

	// Compare against every configured token in constant time, without stopping at the first match
	h := sha256.Sum256([]byte(token))
	var match *tokenEntry
	for i := range a.tokens {
		if subtle.ConstantTimeCompare(h[:], a.tokens[i].hash[:]) == 1 {
			match = &a.tokens[i]
		}
	}
	if match == nil {
		return nil
	}

	return &Caller{
		TokenSuffix: match.suffix,
		scopes:      match.scopes,
	}
}

// tokenSuffix returns the last characters of a token
func tokenSuffix(token string) string {
	if len(token) <= tokenSuffixLength {
		return token
	}

	return token[len(token)-tokenSuffixLength:]
}

type callerCtxKey struct{}

// callerFromContext returns the authenticated caller stored in the context
func callerFromContext(ctx context.Context) *Caller {
	c, _ := ctx.Value(callerCtxKey{}).(*Caller)
	return c
}
