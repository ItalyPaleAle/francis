package management

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

func TestNewServer(t *testing.T) {
	t.Run("rejects an invalid configuration", func(t *testing.T) {
		_, err := NewServer(ServerOptions{Config: Config{}, Backend: &fakeBackend{}})
		require.Error(t, err)
	})

	t.Run("requires a backend", func(t *testing.T) {
		_, err := NewServer(ServerOptions{Config: Config{ReadOnlyTokens: []string{testReadOnlyToken}}})
		require.Error(t, err)
		assert.ErrorContains(t, err, "backend is required")
	})
}

// protectedRoutes lists a request for every route that requires a token
var protectedRoutes = []struct {
	method string
	path   string
}{
	{http.MethodGet, "/api/v1/token"},
	{http.MethodGet, "/api/v1/cluster/summary"},
	{http.MethodGet, "/api/v1/runtimes"},
	{http.MethodGet, "/api/v1/hosts"},
	{http.MethodGet, "/api/v1/hosts/h1"},
	{http.MethodGet, "/api/v1/hosts/h1/activations"},
	{http.MethodPost, "/api/v1/hosts/h1/drain"},
	{http.MethodGet, "/api/v1/activations"},
	{http.MethodGet, "/api/v1/placements"},
	{http.MethodGet, "/api/v1/actor-types"},
	{http.MethodGet, "/api/v1/actor-states?type=t"},
	{http.MethodGet, "/api/v1/actor-states/t/i"},
	{http.MethodPost, "/api/v1/actors/t/i/deactivate"},
	{http.MethodGet, "/api/v1/jobs"},
	{http.MethodGet, "/api/v1/jobs/j1"},
	{http.MethodGet, "/api/v1/alarms"},
	{http.MethodGet, "/api/v1/workflows"},
	{http.MethodGet, "/api/v1/workflows/wf/instances"},
	{http.MethodGet, "/api/v1/workflows/wf/instances/i1"},
	{http.MethodGet, "/api/v1/workflows/wf/instances/i1/events"},
	{http.MethodPost, "/api/v1/workflows/wf/instances/i1/cancel"},
	{http.MethodPost, "/api/v1/workflows/wf/instances/i1/suspend"},
	{http.MethodPost, "/api/v1/workflows/wf/instances/i1/resume"},
}

func TestAuthentication(t *testing.T) {
	// The mock provider has no expectations, so any route that got past authentication would fail the test
	ts := newTestServer(t)

	for _, rt := range protectedRoutes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			t.Run("without a token", func(t *testing.T) {
				w := ts.do(t, rt.method, rt.path, "", "")
				decodeError(t, w, http.StatusUnauthorized, CodeUnauthorized)
				assert.Equal(t, `Bearer realm="francis-management"`, w.Header().Get("WWW-Authenticate"))
			})

			t.Run("with an unknown token", func(t *testing.T) {
				w := ts.do(t, rt.method, rt.path, strings.Repeat("x", 40), "")
				decodeError(t, w, http.StatusUnauthorized, CodeUnauthorized)
				assert.Equal(t, `Bearer realm="francis-management"`, w.Header().Get("WWW-Authenticate"))
			})

			t.Run("with the token in the query string", func(t *testing.T) {
				sep := "?"
				if strings.Contains(rt.path, "?") {
					sep = "&"
				}
				w := ts.do(t, rt.method, rt.path+sep+"token="+testManagementToken+"&access_token="+testManagementToken, "", "")
				decodeError(t, w, http.StatusUnauthorized, CodeUnauthorized)
			})

			t.Run("with a malformed header", func(t *testing.T) {
				w := ts.do(t, rt.method, rt.path, "", "", "Authorization", "Basic "+testManagementToken)
				decodeError(t, w, http.StatusUnauthorized, CodeUnauthorized)
			})
		})
	}
}

func TestReadOnlyTokenForbiddenOnActions(t *testing.T) {
	// The mock provider has no expectations, so a handler that ran would fail the test
	ts := newTestServer(t)

	var actions int
	for _, rt := range protectedRoutes {
		if rt.method != http.MethodPost {
			continue
		}
		actions++
		t.Run(rt.path, func(t *testing.T) {
			w := ts.do(t, rt.method, rt.path, testReadOnlyToken, `{"reason":"x","force":true}`)
			e := decodeError(t, w, http.StatusForbidden, CodeForbidden)
			assert.Contains(t, e.Message, ":manage")
			assert.Empty(t, w.Header().Get("WWW-Authenticate"))
		})
	}
	assert.Equal(t, 5, actions)
	assert.Empty(t, ts.backend.drains)
	assert.Empty(t, ts.backend.deactivates)
	assert.Empty(t, ts.backend.dispatches)
}

func TestRequestID(t *testing.T) {
	ts := newTestServer(t)

	ids := map[string]struct{}{}
	for _, target := range []string{"/healthz", "/api/v1/openapi.yaml", "/api/v1/hosts", "/api/v1/nope"} {
		w := ts.do(t, http.MethodGet, target, "", "")
		id := w.Header().Get("X-Request-ID")
		require.NotEmpty(t, id, target)
		ids[id] = struct{}{}
	}
	assert.Len(t, ids, 4, "every request gets its own ID")
}

func TestPublicRoutes(t *testing.T) {
	ts := newTestServer(t)

	t.Run("healthz", func(t *testing.T) {
		w := ts.do(t, http.MethodGet, "/healthz", "", "")
		res := decodeJSON(t, w, http.StatusOK)
		assert.Equal(t, "ok", res["status"])
		assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	})

	t.Run("openapi", func(t *testing.T) {
		w := ts.do(t, http.MethodGet, "/api/v1/openapi.yaml", "", "")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, "application/yaml", w.Header().Get("Content-Type"))
		assert.Equal(t, openAPISpec, w.Body.Bytes())
	})

	t.Run("openapi ignores a bad token", func(t *testing.T) {
		w := ts.do(t, http.MethodGet, "/api/v1/openapi.yaml", "bad", "")
		require.Equal(t, http.StatusOK, w.Code)
	})
}

func TestRoutingErrors(t *testing.T) {
	ts := newTestServer(t)

	t.Run("unknown route", func(t *testing.T) {
		for _, target := range []string{"/api/v1/nope", "/", "/api/v2/hosts", "/api/v1/hosts/h1/unknown"} {
			w := ts.do(t, http.MethodGet, target, testManagementToken, "")
			decodeError(t, w, http.StatusNotFound, CodeNotFound)
		}
	})

	t.Run("unknown route without a token is still 404", func(t *testing.T) {
		w := ts.do(t, http.MethodGet, "/api/v1/nope", "", "")
		decodeError(t, w, http.StatusNotFound, CodeNotFound)
	})

	tests := []struct {
		method string
		path   string
		allow  string
	}{
		{http.MethodPost, "/healthz", "GET"},
		{http.MethodPost, "/api/v1/hosts", "GET"},
		{http.MethodDelete, "/api/v1/hosts/h1", "GET"},
		{http.MethodGet, "/api/v1/hosts/h1/drain", "POST"},
		{http.MethodPut, "/api/v1/actors/t/i/deactivate", "POST"},
		{http.MethodGet, "/api/v1/workflows/wf/instances/i1/cancel", "POST"},
		{http.MethodPost, "/api/v1/openapi.yaml", "GET"},
	}
	for _, tc := range tests {
		t.Run("method not allowed "+tc.method+" "+tc.path, func(t *testing.T) {
			w := ts.do(t, tc.method, tc.path, testManagementToken, "")
			e := decodeError(t, w, http.StatusMethodNotAllowed, CodeMethodNotAllowed)
			assert.Equal(t, tc.allow, w.Header().Get("Allow"))
			assert.Contains(t, e.Message, tc.method)
		})
	}
}

func TestErrorEnvelope(t *testing.T) {
	ts := newTestServer(t)
	ts.provider.EXPECT().GetHostDetails(mock.Anything, "missing").Return(components.HostDetails{}, components.ErrHostUnregistered)

	w := ts.do(t, http.MethodGet, "/api/v1/hosts/missing", testReadOnlyToken, "")
	require.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))

	// The envelope has exactly the documented fields, with the optional ones omitted when empty
	res := decodeJSON(t, w, http.StatusNotFound)
	assert.Equal(t, CodeNotFound, res["code"])
	assert.Equal(t, "host 'missing' is not registered", res["message"])
	assert.Equal(t, w.Header().Get("X-Request-ID"), res["requestId"])
	assert.NotContains(t, res, "retryable")
	assert.NotContains(t, res, "details")
	assert.Len(t, res, 3)
}

func TestInternalErrors(t *testing.T) {
	t.Run("provider error is a generic 500 and is logged", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(components.HostDetails{}, errors.New("database exploded: secret-dsn"))

		w := ts.do(t, http.MethodGet, "/api/v1/hosts/h1", testReadOnlyToken, "")
		e := decodeError(t, w, http.StatusInternalServerError, CodeInternal)
		assert.NotContains(t, e.Message, "secret-dsn")
		assert.Contains(t, ts.logs.String(), "database exploded")
	})

	t.Run("deadline exceeded is a retryable 504", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(components.HostDetails{}, wrappedDeadlineExceeded())

		w := ts.do(t, http.MethodGet, "/api/v1/hosts/h1", testReadOnlyToken, "")
		e := decodeError(t, w, http.StatusGatewayTimeout, CodeTimeout)
		assert.True(t, e.Retryable)
	})

	t.Run("panic in a handler is recovered", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false), nil)
		ts.backend.snapshotFn = func(components.HostDetails, protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			panic("boom")
		}

		w := ts.do(t, http.MethodGet, "/api/v1/hosts/h1", testReadOnlyToken, "")
		decodeError(t, w, http.StatusInternalServerError, CodeInternal)
		assert.Contains(t, ts.logs.String(), "Panic in management API handler")
	})
}

func TestBackendErrorMapping(t *testing.T) {
	tests := []struct {
		err       error
		status    int
		code      string
		retryable bool
	}{
		{err: ErrNotApplicable, status: http.StatusNotFound, code: CodeNotApplicable},
		{err: ErrHostReattached, status: http.StatusConflict, code: CodeHostReattached, retryable: true},
		{err: ErrHostUnavailable, status: http.StatusServiceUnavailable, code: CodeHostUnavailable, retryable: true},
		{err: errors.Join(errors.New("wrapped"), ErrHostUnavailable), status: http.StatusServiceUnavailable, code: CodeHostUnavailable, retryable: true},
	}
	for _, tc := range tests {
		t.Run(tc.code, func(t *testing.T) {
			e := backendError(tc.err)
			require.NotNil(t, e)
			assert.Equal(t, tc.status, e.status)
			assert.Equal(t, tc.code, e.Code)
			assert.Equal(t, tc.retryable, e.Retryable)
		})
	}
	assert.Nil(t, backendError(errors.New("other")))

	t.Run("runtimes not applicable in the local topology", func(t *testing.T) {
		ts := newTestServer(t)
		ts.backend.runtimesErr = ErrNotApplicable
		w := ts.do(t, http.MethodGet, "/api/v1/runtimes", testReadOnlyToken, "")
		decodeError(t, w, http.StatusNotFound, CodeNotApplicable)
	})
}

func TestFromProtocolError(t *testing.T) {
	tests := []struct {
		code      protocol.ErrorCode
		status    int
		apiCode   string
		retryable bool
	}{
		{code: protocol.ErrCodeHostReattached, status: http.StatusConflict, apiCode: CodeHostReattached, retryable: true},
		{code: protocol.ErrCodeHostUnavailable, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeHostMismatch, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeTransportFailure, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeHostDraining, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeRetryLater, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeOverloaded, status: http.StatusServiceUnavailable, apiCode: CodeHostUnavailable, retryable: true},
		{code: protocol.ErrCodeDeadlineExceeded, status: http.StatusGatewayTimeout, apiCode: CodeTimeout, retryable: true},
		{code: protocol.ErrCodeInternal, status: http.StatusInternalServerError, apiCode: CodeInternal},
	}
	for _, tc := range tests {
		t.Run(string(tc.code), func(t *testing.T) {
			// The host's error reaches the handler through a backend, which maps it with FromProtocolError
			ts := newTestServer(t)
			ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "A"), nil)
			ts.backend.snapshotFn = func(components.HostDetails, protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
				return protocol.HostSnapshotResponse{}, FromProtocolError(protocol.NewError(tc.code, "the host failed"))
			}

			w := ts.do(t, http.MethodGet, "/api/v1/hosts/h1/activations", testReadOnlyToken, "")
			e := decodeError(t, w, tc.status, tc.apiCode)
			assert.Equal(t, tc.retryable, e.Retryable)
		})
	}

	// A busy host is distinct from an unreachable one, so a backend doesn't look for the host elsewhere
	err := FromProtocolError(protocol.NewError(protocol.ErrCodeOverloaded, "busy"))
	require.ErrorIs(t, err, ErrHostBusy)
	require.NotErrorIs(t, err, ErrHostUnavailable)

	// An error that isn't a protocol error is returned as-is
	other := errors.New("other")
	assert.Same(t, other, FromProtocolError(other))
}
