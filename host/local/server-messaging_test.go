package local

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// echoActor is a minimal actor that implements Invoke for the handler tests
type echoActor struct{}

func (echoActor) Invoke(_ context.Context, _ string, _ actor.Envelope) (any, error) {
	return "ok", nil
}

// TestHandleMessageRequestActiveOnly verifies the X-Active-Only header is enforced by the owning host
func TestHandleMessageRequestActiveOnly(t *testing.T) {
	const sharedKey = "0123456789abcdef"
	log := slog.New(slog.DiscardHandler)
	clock := clocktesting.NewFakeClock(time.Now())

	newHost := func() *Host {
		host := &Host{
			hostID:   "h1",
			log:      log,
			clock:    clock,
			peerAuth: &peerauth.PeerAuthenticationSharedKey{Key: sharedKey},
		}
		host.service = actor.NewService(host)
		host.core = actorcore.NewManager(actorcore.Options{
			Service: host.service,
			Logger:  log,
			Clock:   clock,
		})
		host.core.ActorsConfig = map[string]components.ActorHostType{
			"T": {IdleTimeout: time.Minute},
		}
		host.core.ActorFactories = map[string]actor.Factory{
			"T": func(actorID string, service *actor.Service) actor.Actor { return echoActor{} },
		}
		host.core.Start()
		return host
	}

	// newRequest builds an authorized active-only invocation request for actor "T"/actorID
	newRequest := func(actorID string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/v1/invoke/T/"+actorID+"/echo", nil)
		req.Header.Set(headerXHostID, "h1")
		req.Header.Set(headerActiveOnly, "1")
		require.NoError(t, (&peerauth.PeerAuthenticationSharedKey{Key: sharedKey}).UpdateRequest(req))
		return req
	}

	t.Run("active-only rejects an actor that is not active", func(t *testing.T) {
		host := newHost()
		defer host.core.Close()

		rec := httptest.NewRecorder()
		host.getServerHandler().ServeHTTP(rec, newRequest("inactive"))

		// The owning host reports the actor as not active rather than activating it
		require.Equal(t, http.StatusConflict, rec.Code)
		var apiErr apiError
		require.NoError(t, msgpack.Unmarshal(rec.Body.Bytes(), &apiErr))
		assert.Equal(t, errApiActorNotActive.Code, apiErr.Code)

		// The actor must not have been activated
		_, exists := host.core.Actors.Get(ref.NewActorRef("T", "inactive").String())
		assert.False(t, exists, "active-only invocation must not activate the actor")
	})

	t.Run("active-only invokes an actor that is already active", func(t *testing.T) {
		host := newHost()
		defer host.core.Close()

		// Pre-activate the actor
		actorRef := ref.NewActorRef("T", "active1")
		activeAct := actorcore.NewActiveActor(actorRef, echoActor{}, time.Minute, host.core.IdleProcessor, clock)
		host.core.Actors.Set(actorRef.String(), activeAct)

		rec := httptest.NewRecorder()
		host.getServerHandler().ServeHTTP(rec, newRequest("active1"))

		require.Equal(t, http.StatusOK, rec.Code)
		var out string
		require.NoError(t, msgpack.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "ok", out)
	})
}
