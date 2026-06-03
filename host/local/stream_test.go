package local

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/actorcore"
	"github.com/italypaleale/francis/internal/peerauth"
	"github.com/italypaleale/francis/internal/ref"
)

// newStreamTestHost builds a minimal host with the actor core started, for streaming tests
func newStreamTestHost(t *testing.T) *Host {
	t.Helper()

	log := slog.New(slog.DiscardHandler)
	clock := clocktesting.NewFakeClock(time.Now())

	host := &Host{
		hostID:   "h1",
		log:      log,
		clock:    clock,
		peerAuth: &peerauth.PeerAuthenticationSharedKey{Key: "0123456789abcdef"},
	}
	host.service = actor.NewService(host)
	host.core = actorcore.NewManager(actorcore.Options{
		Service: host.service,
		Logger:  log,
		Clock:   clock,
	})
	host.core.ActorsConfig = map[string]components.ActorHostType{
		"T": {IdleTimeout: time.Minute},
		"O": {IdleTimeout: time.Minute},
	}
	host.core.ActorFactories = map[string]actor.Factory{
		"T": func(actorID string, service *actor.Service) actor.Actor { return echoActor{} },
		"O": func(actorID string, service *actor.Service) actor.Actor { return objectOnlyActor{} },
	}
	host.core.Start()
	t.Cleanup(host.core.Close)
	return host
}

// activate registers an already-active actor instance on the host
func activate(t *testing.T, host *Host, actorType string, actorID string, instance actor.Actor) {
	t.Helper()
	actorRef := ref.NewActorRef(actorType, actorID)
	act := actorcore.NewActiveActor(actorRef, instance, time.Minute, host.core.IdleProcessor, host.clock)
	host.core.Actors.Set(actorRef.String(), act)
}

// TestInvokeStreamSameHost streams to a locally-active actor and reads the response back
func TestInvokeStreamSameHost(t *testing.T) {
	host := newStreamTestHost(t)
	activate(t, host, "T", "s1", echoActor{})

	ct, resp, err := host.Service().InvokeStream(t.Context(), "T", "s1", "echo", "text/plain", strings.NewReader("hi"))
	require.NoError(t, err)
	defer resp.Close()

	assert.Equal(t, "text/plain", ct)
	got, err := io.ReadAll(resp)
	require.NoError(t, err)
	assert.Equal(t, "stream:hi", string(got))
}

// TestHandleStreamRequest drives the streaming HTTP handler end-to-end
func TestHandleStreamRequest(t *testing.T) {
	const sharedKey = "0123456789abcdef"

	// newRequest builds an authorized active-only stream request
	newRequest := func(actorType, actorID, body string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/v1/invoke-stream/"+actorType+"/"+actorID+"/echo", strings.NewReader(body))
		req.Header.Set(headerXHostID, "h1")
		req.Header.Set(headerContentType, "application/test")
		req.Header.Set(headerActiveOnly, "1")
		require.NoError(t, (&peerauth.PeerAuthenticationSharedKey{Key: sharedKey}).UpdateRequest(req))
		return req
	}

	t.Run("streams the response from an active actor", func(t *testing.T) {
		host := newStreamTestHost(t)
		activate(t, host, "T", "s1", echoActor{})

		rec := httptest.NewRecorder()
		host.getServerHandler().ServeHTTP(rec, newRequest("T", "s1", "ping"))

		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "application/test", rec.Header().Get(headerContentType))
		assert.Equal(t, "stream:ping", rec.Body.String())
	})

	t.Run("active-only rejects an actor that is not active", func(t *testing.T) {
		host := newStreamTestHost(t)

		rec := httptest.NewRecorder()
		host.getServerHandler().ServeHTTP(rec, newRequest("T", "inactive", "ping"))

		require.Equal(t, http.StatusConflict, rec.Code)
		var apiErr apiError
		require.NoError(t, msgpack.Unmarshal(rec.Body.Bytes(), &apiErr))
		assert.Equal(t, errApiActorNotActive.Code, apiErr.Code)
	})

	t.Run("actor without InvokeStream reports mode unsupported", func(t *testing.T) {
		host := newStreamTestHost(t)
		activate(t, host, "O", "o1", objectOnlyActor{})

		rec := httptest.NewRecorder()
		host.getServerHandler().ServeHTTP(rec, newRequest("O", "o1", "ping"))

		require.Equal(t, http.StatusBadRequest, rec.Code)
		var apiErr apiError
		require.NoError(t, msgpack.Unmarshal(rec.Body.Bytes(), &apiErr))
		assert.Equal(t, errApiInvokeModeUnsupported.Code, apiErr.Code)
	})
}
