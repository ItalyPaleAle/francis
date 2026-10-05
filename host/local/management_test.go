package local

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components/sqlite"
	"github.com/italypaleale/francis/internal/management"
	"github.com/italypaleale/francis/internal/testutil"
)

// Compile-time interface assertion
var _ management.Backend = (*managementBackend)(nil)

func TestHostLocalManagementAPI(t *testing.T) {
	readOnlyToken := strings.Repeat("r", 32)
	managementToken := strings.Repeat("m", 32)

	// Reserve a free TCP port for the management listener
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	bind := l.Addr().String()
	err = l.Close()
	require.NoError(t, err)

	h, err := NewHost(
		WithAddress(localFreeUDPAddr(t)),
		WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: testutil.SQLiteConnString(t)}),
		WithRuntimePSKs(localTestRuntimePSK),
		WithLogger(slog.New(slog.DiscardHandler)),
		WithManagementAPI(ManagementOptions{
			Bind:             bind,
			ReadOnlyTokens:   []string{readOnlyToken},
			ManagementTokens: []string{managementToken},
		}),
	)
	require.NoError(t, err)
	err = h.RegisterActor("S", func(actorID string, service *actor.Service) actor.Actor {
		return smokeActor{}
	})
	require.NoError(t, err)
	runLocalHost(t, h)

	_, err = h.Service().Invoke(t.Context(), "S", "x", "echo", "hi")
	require.NoError(t, err)

	get := func(t *testing.T, path string, token string) (int, map[string]any) {
		t.Helper()

		req, rErr := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+bind+path, nil)
		require.NoError(t, rErr)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		res, rErr := http.DefaultClient.Do(req)
		require.NoError(t, rErr)
		defer res.Body.Close()

		var body map[string]any
		rErr = json.NewDecoder(res.Body).Decode(&body)
		require.NoError(t, rErr)

		return res.StatusCode, body
	}

	// The listener starts alongside the host's other services, so wait for it
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		conn, dErr := net.Dial("tcp", bind)
		if assert.NoError(c, dErr) {
			_ = conn.Close()
		}
	}, 10*time.Second, 50*time.Millisecond)

	t.Run("requires a token", func(t *testing.T) {
		status, _ := get(t, "/api/v1/hosts", "")
		assert.Equal(t, http.StatusUnauthorized, status)
	})

	t.Run("lists the host", func(t *testing.T) {
		status, body := get(t, "/api/v1/hosts", readOnlyToken)
		require.Equal(t, http.StatusOK, status)
		items, _ := body["items"].([]any)
		require.Len(t, items, 1)
		item, _ := items[0].(map[string]any)
		assert.Equal(t, h.HostID(), item["hostId"])
	})

	t.Run("lists the activations of the host in-process", func(t *testing.T) {
		status, body := get(t, "/api/v1/hosts/"+h.HostID()+"/activations", readOnlyToken)
		require.Equal(t, http.StatusOK, status)
		items, _ := body["items"].([]any)
		require.Len(t, items, 1)
		item, _ := items[0].(map[string]any)
		assert.Equal(t, "S", item["actorType"])
		assert.Equal(t, "x", item["actorId"])
	})

	t.Run("runtimes are not applicable", func(t *testing.T) {
		status, body := get(t, "/api/v1/runtimes", readOnlyToken)
		assert.Equal(t, http.StatusNotFound, status)
		assert.Equal(t, "notApplicable", body["code"])
	})
}

func TestHostLocalManagementAPIInvalidConfig(t *testing.T) {
	_, err := NewHost(
		WithAddress(localFreeUDPAddr(t)),
		WithSQLiteProvider(sqlite.SQLiteProviderOptions{ConnectionString: testutil.SQLiteConnString(t)}),
		WithRuntimePSKs(localTestRuntimePSK),
		WithLogger(slog.New(slog.DiscardHandler)),
		WithManagementAPI(ManagementOptions{
			ReadOnlyTokens: []string{"too-short"},
		}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, "management API")
}
