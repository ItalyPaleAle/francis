package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testReadOnlyToken   = "ro-token-0123456789abcdefghijklmnopqrsXYZ12"
	testManagementToken = "mg-token-0123456789abcdefghijklmnopqrsABC34"
)

func newTestAuthenticator() *authenticator {
	return newAuthenticator(Config{
		ReadOnlyTokens:   []string{testReadOnlyToken},
		ManagementTokens: []string{testManagementToken},
	})
}

func TestAuthenticate(t *testing.T) {
	a := newTestAuthenticator()

	tests := []struct {
		name    string
		header  string
		query   string
		wantNil bool
		suffix  string
	}{
		{name: "read-only token", header: "Bearer " + testReadOnlyToken, suffix: "XYZ12"},
		{name: "management token", header: "Bearer " + testManagementToken, suffix: "ABC34"},
		{name: "lowercase scheme", header: "bearer " + testReadOnlyToken, suffix: "XYZ12"},
		{name: "uppercase scheme", header: "BEARER " + testReadOnlyToken, suffix: "XYZ12"},
		{name: "mixed case scheme", header: "bEaReR " + testManagementToken, suffix: "ABC34"},
		{name: "surrounding whitespace around the token", header: "Bearer   " + testReadOnlyToken + "  ", suffix: "XYZ12"},
		{name: "missing header", wantNil: true},
		{name: "scheme only", header: "Bearer", wantNil: true},
		{name: "scheme with trailing space", header: "Bearer ", wantNil: true},
		{name: "scheme with only whitespace", header: "Bearer    ", wantNil: true},
		{name: "no space after scheme", header: "Bearer" + testReadOnlyToken, wantNil: true},
		{name: "basic scheme", header: "Basic " + testReadOnlyToken, wantNil: true},
		{name: "token without scheme", header: testReadOnlyToken, wantNil: true},
		{name: "unknown token", header: "Bearer " + strings.Repeat("z", 40), wantNil: true},
		{name: "prefix of a valid token", header: "Bearer " + testReadOnlyToken[:len(testReadOnlyToken)-1], wantNil: true},
		{name: "token in query string", query: "token=" + testManagementToken, wantNil: true},
		{name: "access_token in query string", query: "access_token=" + testManagementToken, wantNil: true},
		{name: "authorization in query string", query: "authorization=Bearer+" + testManagementToken, wantNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := "/api/v1/hosts"
			if tc.query != "" {
				target += "?" + tc.query
			}
			r := httptest.NewRequest(http.MethodGet, target, nil)
			if tc.header != "" {
				r.Header.Set("Authorization", tc.header)
			}

			c := a.authenticate(r)
			if tc.wantNil {
				assert.Nil(t, c)
				return
			}
			require.NotNil(t, c)
			assert.Equal(t, tc.suffix, c.TokenSuffix)
		})
	}
}

func TestScopes(t *testing.T) {
	a := newTestAuthenticator()

	authAs := func(token string) *Caller {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		c := a.authenticate(r)
		require.NotNil(t, c)
		return c
	}

	t.Run("read-only token receives every non-manage scope", func(t *testing.T) {
		c := authAs(testReadOnlyToken)
		for _, s := range allScopes {
			assert.Equal(t, !s.IsManage(), c.Has(s), "scope %s", s)
		}
		assert.True(t, c.Has(ScopeWorkflowsDataRead))
		assert.True(t, c.Has(ScopeActorsStateRead))
		assert.False(t, c.Has(ScopeHostsManage))
		assert.False(t, c.Has(ScopeActorsManage))
		assert.False(t, c.Has(ScopeWorkflowsManage))
	})

	t.Run("management token receives every scope", func(t *testing.T) {
		c := authAs(testManagementToken)
		for _, s := range allScopes {
			assert.True(t, c.Has(s), "scope %s", s)
		}
	})

	t.Run("unknown scope is never granted", func(t *testing.T) {
		c := authAs(testManagementToken)
		assert.False(t, c.Has(Scope("secrets:read")))
	})

	t.Run("nil caller has no scope", func(t *testing.T) {
		var c *Caller
		assert.False(t, c.Has(ScopeClusterRead))
	})

	t.Run("caller without scopes has none", func(t *testing.T) {
		c := &Caller{}
		assert.False(t, c.Has(ScopeClusterRead))
		assert.False(t, c.Has(ScopeWorkflowsDataRead))
	})

	t.Run("caller missing the data scope", func(t *testing.T) {
		// Neither configured token lacks workflows:data:read, so a hand-built caller covers the redaction branch's check
		c := &Caller{scopes: map[Scope]struct{}{ScopeWorkflowsRead: {}}}
		assert.True(t, c.Has(ScopeWorkflowsRead))
		assert.False(t, c.Has(ScopeWorkflowsDataRead))
	})
}

func TestScopeIsManage(t *testing.T) {
	manage := map[Scope]bool{
		ScopeActorsManage:    true,
		ScopeHostsManage:     true,
		ScopeWorkflowsManage: true,
	}
	for _, s := range allScopes {
		assert.Equal(t, manage[s], s.IsManage(), "scope %s", s)
	}
}

func TestTokenSuffix(t *testing.T) {
	assert.Equal(t, "vwxyz", tokenSuffix("abcdefghijklmnopqrstuvwxyz"))
	assert.Equal(t, "12345", tokenSuffix("12345"))
	assert.Equal(t, "abc", tokenSuffix("abc"))
	assert.Empty(t, tokenSuffix(""))
	assert.Len(t, tokenSuffix(testReadOnlyToken), tokenSuffixLength)
}
