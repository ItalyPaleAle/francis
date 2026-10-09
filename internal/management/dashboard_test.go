package management

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testDashboardPage = `<!doctype html><html><head><meta name="francis-dashboard-mode" content="embedded" /></head><body></body></html>`

// newTestDashboardServer returns a server with a dashboard and the given allowed origins
func newTestDashboardServer(t *testing.T, allowedOrigins ...string) *Server {
	t.Helper()

	srv, err := NewServer(ServerOptions{
		Config: Config{
			ReadOnlyTokens: []string{testReadOnlyToken},
			Dashboard: fstest.MapFS{
				"index.html":      {Data: []byte(testDashboardPage)},
				"assets/index.js": {Data: []byte("console.log(1)")},
			},
			AllowedOrigins: allowedOrigins,
		},
		Backend: &fakeBackend{},
		Logger:  slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)

	return srv
}

// send sends a request to the server, with headers given as name and value pairs
func send(srv *Server, method string, target string, headers ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}

	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, r)
	return w
}

func TestDashboard(t *testing.T) {
	srv := newTestDashboardServer(t)

	t.Run("serves the page in embedded mode", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, testDashboardPage, w.Body.String())
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "connect-src 'self';")
		assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
	})

	t.Run("serves the page for the dashboard's routes", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/hosts/h1", "Accept", "text/html")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, testDashboardPage, w.Body.String())
	})

	t.Run("unknown API paths are never the page", func(t *testing.T) {
		for _, target := range []string{"/api", "/api/", "/api/v1/nope", "/api/v2/hosts"} {
			w := send(srv, http.MethodGet, target, "Accept", "text/html")
			decodeError(t, w, http.StatusNotFound, CodeNotFound)
		}
	})

	t.Run("missing files are a JSON 404", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/assets/missing.js")
		decodeError(t, w, http.StatusNotFound, CodeNotFound)
	})

	t.Run("other methods are a JSON 405", func(t *testing.T) {
		w := send(srv, http.MethodPost, "/hosts")
		decodeError(t, w, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
		assert.Equal(t, "GET, HEAD", w.Header().Get("Allow"))
	})

	t.Run("API routes still work", func(t *testing.T) {
		w := send(srv, http.MethodGet, "/healthz")
		res := decodeJSON(t, w, http.StatusOK)
		assert.Equal(t, "ok", res["status"])

		w = send(srv, http.MethodGet, "/api/v1/token", "Authorization", "Bearer "+testReadOnlyToken)
		decodeJSON(t, w, http.StatusOK)
	})
}
