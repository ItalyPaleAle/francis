package management

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeOrigin(t *testing.T) {
	valid := map[string]string{
		"*":                         "*",
		"http://localhost:7402":     "http://localhost:7402",
		"HTTP://LocalHost:7402":     "http://localhost:7402",
		"https://example.com/":      "https://example.com",
		"https://example.com:443":   "https://example.com",
		"http://example.com:80":     "http://example.com",
		"https://example.com:8443":  "https://example.com:8443",
		"http://[::1]:7402":         "http://[::1]:7402",
		"http://[::1]":              "http://[::1]",
		"http://10.0.0.5:7402":      "http://10.0.0.5:7402",
		"https://dash.example.com":  "https://dash.example.com",
		"http://127.0.0.1:7402/":    "http://127.0.0.1:7402",
		"https://EXAMPLE.com:08443": "https://example.com:08443",
	}
	for in, want := range valid {
		t.Run(in, func(t *testing.T) {
			got, err := normalizeOrigin(in)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}

	t.Run("trims surrounding whitespace", func(t *testing.T) {
		got, err := normalizeOrigin("  http://10.0.0.5:7402\n")
		require.NoError(t, err)
		assert.Equal(t, "http://10.0.0.5:7402", got)
	})

	invalid := []string{
		"",
		"localhost:7402",
		"ftp://example.com",
		"https://example.com/dashboard",
		"https://example.com?x=1",
		"https://example.com#x",
		"https://user@example.com",
		"null",
		"://",
	}
	for _, in := range invalid {
		t.Run("invalid "+in, func(t *testing.T) {
			_, err := normalizeOrigin(in)
			require.Error(t, err)
		})
	}
}

func TestCORS(t *testing.T) {
	const allowed = "http://localhost:7402"
	srv := newTestDashboardServer(t, "HTTP://LOCALHOST:7402", "https://dash.example.com:443")

	t.Run("answers a preflight from an allowed origin", func(t *testing.T) {
		w := send(srv, http.MethodOptions, "/api/v1/hosts/h1/drain",
			"Origin", allowed,
			"Access-Control-Request-Method", "POST",
			"Access-Control-Request-Headers", "authorization, content-type",
		)
		require.Equal(t, http.StatusNoContent, w.Code)
		assert.Equal(t, allowed, w.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "GET, POST", w.Header().Get("Access-Control-Allow-Methods"))
		assert.Equal(t, "Authorization, Content-Type", w.Header().Get("Access-Control-Allow-Headers"))
		assert.Equal(t, "600", w.Header().Get("Access-Control-Max-Age"))
		assert.Contains(t, w.Header().Values("Vary"), "Origin")
		assert.Empty(t, w.Body.Bytes())
	})

	t.Run("matches origins after normalizing them", func(t *testing.T) {
		w := send(srv, http.MethodOptions, "/api/v1/token", "Origin", "https://dash.example.com", "Access-Control-Request-Method", "GET")
		require.Equal(t, http.StatusNoContent, w.Code)
		assert.Equal(t, "https://dash.example.com", w.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("refuses a preflight from another origin", func(t *testing.T) {
		w := send(srv, http.MethodOptions, "/api/v1/token", "Origin", "http://evil.example.com", "Access-Control-Request-Method", "GET")
		decodeError(t, w, http.StatusForbidden, CodeForbidden)
		assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("adds the headers to requests from an allowed origin", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/api/v1/token", "Origin", allowed, "Authorization", "Bearer "+testReadOnlyToken)
		decodeJSON(t, w, http.StatusOK)
		assert.Equal(t, allowed, w.Header().Get("Access-Control-Allow-Origin"))
		assert.Equal(t, "X-Request-Id", w.Header().Get("Access-Control-Expose-Headers"))

		// Error responses carry them too, so the page can read why a request failed
		w = send(srv, http.MethodGet, "/api/v1/token", "Origin", allowed)
		decodeError(t, w, http.StatusUnauthorized, CodeUnauthorized)
		assert.Equal(t, allowed, w.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("serves requests from other origins without the headers", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/api/v1/token", "Origin", "http://evil.example.com", "Authorization", "Bearer "+testReadOnlyToken)
		decodeJSON(t, w, http.StatusOK)
		assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	})

	t.Run("requests without an origin are unchanged", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/api/v1/token", "Authorization", "Bearer "+testReadOnlyToken)
		decodeJSON(t, w, http.StatusOK)
		assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
		assert.Empty(t, w.Header().Values("Vary"))
	})

	t.Run("an OPTIONS request that isn't a preflight is routed", func(t *testing.T) {
		w := send(srv, http.MethodOptions, "/api/v1/token", "Origin", allowed)
		decodeError(t, w, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
	})
}

func TestCORSDisabled(t *testing.T) {
	srv := newTestDashboardServer(t)

	t.Run("refuses every preflight", func(t *testing.T) {
		w := send(srv, http.MethodOptions, "/api/v1/token", "Origin", "http://localhost:7402", "Access-Control-Request-Method", "GET")
		decodeError(t, w, http.StatusForbidden, CodeForbidden)
	})

	t.Run("same-origin requests with an Origin header still work", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/api/v1/token", "Origin", "http://example.com", "Authorization", "Bearer "+testReadOnlyToken)
		decodeJSON(t, w, http.StatusOK)
		assert.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
	})
}

func TestCORSAnyOrigin(t *testing.T) {
	srv := newTestDashboardServer(t, "*")

	w := send(srv, http.MethodOptions, "/api/v1/token", "Origin", "http://anything.example:1234", "Access-Control-Request-Method", "GET")
	require.Equal(t, http.StatusNoContent, w.Code)
	assert.Equal(t, "http://anything.example:1234", w.Header().Get("Access-Control-Allow-Origin"))
}
