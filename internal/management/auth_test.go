package management

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
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

	t.Run("read-only token receives the read scopes", func(t *testing.T) {
		c := authAs(testReadOnlyToken)
		assert.True(t, c.Has(ScopeWorkflowsDataRead))
		assert.True(t, c.Has(ScopeActorsStateRead))
		assert.False(t, c.Has(ScopeHostsManage))
		assert.False(t, c.Has(ScopeActorsManage))
		assert.False(t, c.Has(ScopeWorkflowsManage))
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

// TestAuthenticatorScopes checks the scopes newAuthenticator grants against every Scope constant declared in the package
// newAuthenticator lists the scopes by hand, so this is what catches a new scope that was not added to it
func TestAuthenticatorScopes(t *testing.T) {
	declared := declaredScopes(t)

	// Management tokens receive every declared scope, and read-only tokens every one except those ending in ":manage"
	wantManagement := make(map[Scope]struct{}, len(declared))
	wantReadOnly := make(map[Scope]struct{}, len(declared))
	for _, s := range declared {
		wantManagement[s] = struct{}{}
		if !strings.HasSuffix(string(s), ":manage") {
			wantReadOnly[s] = struct{}{}
		}
	}

	// Comparing the whole sets also fails on a scope granted by newAuthenticator but never declared
	a := newTestAuthenticator()
	for _, tc := range []struct {
		name  string
		token string
		want  map[Scope]struct{}
	}{
		{name: "read-only", token: testReadOnlyToken, want: wantReadOnly},
		{name: "management", token: testManagementToken, want: wantManagement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Authorization", "Bearer "+tc.token)
			c := a.authenticate(r)
			require.NotNil(t, c)
			assert.Equal(t, tc.want, c.scopes)
		})
	}
}

// declaredScopes returns every Scope constant declared in the package's source files
// It reads the source rather than a list kept in code, so adding a constant is enough for TestAuthenticatorScopes to require it
func declaredScopes(t *testing.T) []Scope {
	t.Helper()

	files, err := filepath.Glob("*.go")
	require.NoError(t, err)

	var scopes []Scope
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}

		f, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		// Collect the constants declared with the Scope type, or as a conversion to it
		for _, decl := range f.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.CONST {
				continue
			}
			for _, spec := range gen.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, v := range vs.Values {
					lit := scopeLiteral(vs.Type, v)
					if lit == nil {
						continue
					}
					val, err := strconv.Unquote(lit.Value)
					require.NoError(t, err)
					scopes = append(scopes, Scope(val))
				}
			}
		}
	}

	// Guard against the parsing silently finding nothing, which would make the check above pass vacuously
	require.Contains(t, scopes, ScopeClusterRead)
	require.Contains(t, scopes, ScopeWorkflowsManage)

	return scopes
}

// scopeLiteral returns the string literal of a constant declared as `X Scope = "..."` or `X = Scope("...")`, or nil for any other constant
func scopeLiteral(typ ast.Expr, value ast.Expr) *ast.BasicLit {
	ident, ok := typ.(*ast.Ident)
	if ok && ident.Name == "Scope" {
		lit, _ := value.(*ast.BasicLit)
		return lit
	}

	call, ok := value.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil
	}

	fn, ok := call.Fun.(*ast.Ident)
	if !ok || fn.Name != "Scope" {
		return nil
	}

	lit, _ := call.Args[0].(*ast.BasicLit)
	return lit
}

func TestTokenSuffix(t *testing.T) {
	assert.Equal(t, "vwxyz", tokenSuffix("abcdefghijklmnopqrstuvwxyz"))
	assert.Equal(t, "12345", tokenSuffix("12345"))
	assert.Equal(t, "abc", tokenSuffix("abc"))
	assert.Empty(t, tokenSuffix(""))
	assert.Len(t, tokenSuffix(testReadOnlyToken), tokenSuffixLength)
}
