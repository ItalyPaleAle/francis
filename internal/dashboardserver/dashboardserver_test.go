package dashboardserver

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPage   = `<!doctype html><html><head><meta name="francis-dashboard-mode" content="embedded" /><script type="module" src="/assets/index.abc123.js"></script></head><body></body></html>`
	testScript = `console.log("the dashboard bundle, long enough to be worth compressing, the dashboard bundle, the dashboard bundle")`
)

func testFiles() fstest.MapFS {
	return fstest.MapFS{
		"index.html":             {Data: []byte(testPage)},
		"favicon-light.svg":      {Data: []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`)},
		"assets/index.abc123.js": {Data: []byte(testScript)},
		"assets/geist.woff2":     {Data: []byte("wOF2")},
		".gitkeep":               {Data: []byte{}},
	}
}

func newTestDashboard(t *testing.T, mode Mode) *Dashboard {
	t.Helper()

	d, err := New(Options{Files: testFiles(), Mode: mode})
	require.NoError(t, err)
	return d
}

// get sends a request to the dashboard, with headers given as name and value pairs
func get(d *Dashboard, method string, target string, headers ...string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}

	w := httptest.NewRecorder()
	d.ServeHTTP(w, r)
	return w
}

func TestNew(t *testing.T) {
	t.Run("requires the files", func(t *testing.T) {
		_, err := New(Options{Mode: ModeEmbedded})
		require.Error(t, err)
	})

	t.Run("requires a valid mode", func(t *testing.T) {
		_, err := New(Options{Files: testFiles(), Mode: "other"})
		require.ErrorContains(t, err, "invalid dashboard mode")
	})

	t.Run("requires the page", func(t *testing.T) {
		_, err := New(Options{Files: fstest.MapFS{"assets/index.js": {Data: []byte("x")}}, Mode: ModeEmbedded})
		require.ErrorContains(t, err, "index.html")
	})

	t.Run("requires the mode tag in the page", func(t *testing.T) {
		_, err := New(Options{Files: fstest.MapFS{"index.html": {Data: []byte("<html></html>")}}, Mode: ModeStandalone})
		require.ErrorContains(t, err, "francis-dashboard-mode")
	})
}

func TestModes(t *testing.T) {
	t.Run("embedded", func(t *testing.T) {
		w := get(newTestDashboard(t, ModeEmbedded), http.MethodGet, "/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), `<meta name="francis-dashboard-mode" content="embedded" />`)
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "connect-src 'self';")
	})

	t.Run("standalone", func(t *testing.T) {
		w := get(newTestDashboard(t, ModeStandalone), http.MethodGet, "/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), `<meta name="francis-dashboard-mode" content="standalone" />`)
		assert.NotContains(t, w.Body.String(), "embedded")
		assert.Contains(t, w.Header().Get("Content-Security-Policy"), "connect-src 'self' http: https:;")
	})
}

func TestServe(t *testing.T) {
	d := newTestDashboard(t, ModeEmbedded)

	t.Run("serves the page at the root", func(t *testing.T) {
		w := get(d, http.MethodGet, "/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, testPage, w.Body.String())
		assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
		assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
		assert.Equal(t, pageCSP(ModeEmbedded), w.Header().Get("Content-Security-Policy"))
		assert.Equal(t, "DENY", w.Header().Get("X-Frame-Options"))
		assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
		assert.NotEmpty(t, w.Header().Get("ETag"))
	})

	t.Run("serves the page for the dashboard's routes", func(t *testing.T) {
		for _, target := range []string{"/hosts", "/hosts/h1", "/actors/my.type/actor.1", "/workflows/wf/instances/i1?tab=events"} {
			w := get(d, http.MethodGet, target, "Accept", "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8")
			require.Equal(t, http.StatusOK, w.Code, target)
			assert.Equal(t, testPage, w.Body.String(), target)
			assert.Equal(t, pageCSP(ModeEmbedded), w.Header().Get("Content-Security-Policy"), target)
		}
	})

	t.Run("serves assets with a long cache lifetime", func(t *testing.T) {
		w := get(d, http.MethodGet, "/assets/index.abc123.js")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, testScript, w.Body.String())
		assert.Contains(t, "text/javascript", w.Header().Get("Content-Type"))
		assert.Equal(t, "public, max-age=31536000, immutable", w.Header().Get("Cache-Control"))
		assert.Empty(t, w.Header().Get("Content-Security-Policy"))

		w = get(d, http.MethodGet, "/assets/geist.woff2")
		require.Equal(t, http.StatusOK, w.Code)
		assert.NotEmpty(t, w.Header().Get("Content-Type"))
	})

	t.Run("serves other files at the root", func(t *testing.T) {
		w := get(d, http.MethodGet, "/favicon-light.svg")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "image/svg+xml", w.Header().Get("Content-Type"))
		assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	})

	t.Run("compresses text files when the client accepts gzip", func(t *testing.T) {
		w := get(d, http.MethodGet, "/assets/index.abc123.js", "Accept-Encoding", "br, gzip")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "gzip", w.Header().Get("Content-Encoding"))
		assert.Equal(t, "Accept-Encoding", w.Header().Get("Vary"))

		gz, err := gzip.NewReader(bytes.NewReader(w.Body.Bytes()))
		require.NoError(t, err)
		data, err := io.ReadAll(gz)
		require.NoError(t, err)
		assert.Equal(t, testScript, string(data))

		// A quality of zero refuses the coding
		w = get(d, http.MethodGet, "/assets/index.abc123.js", "Accept-Encoding", "gzip;q=0")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, w.Header().Get("Content-Encoding"))
		assert.Equal(t, testScript, w.Body.String())
	})

	t.Run("answers a conditional request for an unchanged file with 304", func(t *testing.T) {
		w := get(d, http.MethodGet, "/assets/index.abc123.js")
		etag := w.Header().Get("ETag")
		require.NotEmpty(t, etag)

		w = get(d, http.MethodGet, "/assets/index.abc123.js", "If-None-Match", `"other", W/`+etag)
		assert.Equal(t, http.StatusNotModified, w.Code)
		assert.Empty(t, w.Body.Bytes())

		// The compressed representation has its own ETag
		w = get(d, http.MethodGet, "/assets/index.abc123.js", "If-None-Match", etag, "Accept-Encoding", "gzip")
		assert.Equal(t, http.StatusOK, w.Code)
		assert.NotEqual(t, etag, w.Header().Get("ETag"))
	})

	t.Run("answers HEAD without a body", func(t *testing.T) {
		w := get(d, http.MethodHead, "/")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Empty(t, w.Body.Bytes())
		assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
	})

	t.Run("missing assets are not the page", func(t *testing.T) {
		w := get(d, http.MethodGet, "/assets/missing.js", "Accept", "text/html")
		assert.Equal(t, http.StatusNotFound, w.Code)

		// A request that isn't a navigation does not get the page either
		w = get(d, http.MethodGet, "/favicon.ico", "Accept", "image/*")
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("hidden files are not served", func(t *testing.T) {
		w := get(d, http.MethodGet, "/.gitkeep")
		assert.Equal(t, http.StatusNotFound, w.Code)
	})

	t.Run("other methods are not allowed", func(t *testing.T) {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
			w := get(d, method, "/hosts")
			assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
			assert.Equal(t, "GET, HEAD", w.Header().Get("Allow"))
		}
	})

	t.Run("paths can't escape the dashboard", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.URL.Path = "/assets/../../index.html"
		w := httptest.NewRecorder()
		err := d.Serve(w, r)
		require.NoError(t, err)
		assert.Equal(t, testPage, w.Body.String())
	})
}

func TestAcceptsGzip(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{header: "", want: false},
		{header: "gzip", want: true},
		{header: "GZIP", want: true},
		{header: "deflate, gzip;q=1.0, *;q=0.5", want: true},
		{header: "br", want: false},
		{header: "gzip;q=0", want: false},
		{header: "gzip; q=0.0", want: false},
		{header: "gzip;q=0.1", want: true},
		{header: "xgzip", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				r.Header.Set("Accept-Encoding", tc.header)
			}
			assert.Equal(t, tc.want, acceptsGzip(r))
		})
	}
}

func TestAcceptsHTML(t *testing.T) {
	tests := []struct {
		header string
		want   bool
	}{
		{header: "", want: false},
		{header: "text/html", want: true},
		{header: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", want: true},
		{header: "application/json", want: false},
		{header: "*/*", want: false},
		{header: "image/avif,image/webp,*/*", want: false},
		{header: "text/html-ish", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				r.Header.Set("Accept", tc.header)
			}
			assert.Equal(t, tc.want, acceptsHTML(r))
		})
	}
}

func TestEtagMatches(t *testing.T) {
	assert.False(t, etagMatches("", `"a"`))
	assert.True(t, etagMatches(`"a"`, `"a"`))
	assert.True(t, etagMatches(`W/"a"`, `"a"`))
	assert.True(t, etagMatches(`"b", "a"`, `"a"`))
	assert.True(t, etagMatches(`*`, `"a"`))
	assert.False(t, etagMatches(`"b"`, `"a"`))
	assert.False(t, etagMatches(strings.Repeat(`"b",`, 3), `"a"`))
}
