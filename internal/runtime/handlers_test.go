package runtime

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/italypaleale/go-kit/ttlcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/components/standalone"
	"github.com/italypaleale/francis/internal/ca"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

func TestHandleRenewCert(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.30:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	t.Run("issues a certificate for the connection's host", func(t *testing.T) {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		resp := dispatchReq(t, rt, c, protocol.KindRenewCert, protocol.RenewCertRequest{WorkloadPubKey: pub})
		require.Equal(t, protocol.KindRenewCertResponse, resp.Kind)

		var out protocol.RenewCertResponse
		require.NoError(t, resp.DecodePayload(&out))
		require.NotEmpty(t, out.WorkloadCertDER)
		require.NotEmpty(t, out.CABundlePEM)

		// The issued certificate chains to the cluster CA and carries this host's SPIFFE identity
		leaf, err := x509.ParseCertificate(out.WorkloadCertDER)
		require.NoError(t, err)
		pool := ca.NewCertPool(rt.cas)
		_, err = leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
		require.NoError(t, err)
		id, err := ca.SPIFFEIDFromCert(leaf)
		require.NoError(t, err)
		assert.Equal(t, "/host/"+hostID, id.Path)
	})

	t.Run("rejects a request missing the public key", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindRenewCert, protocol.RenewCertRequest{})
		requireError(t, resp, protocol.ErrCodeBadRequest)
	})
}

// testRuntimePSK derives the test cluster CA, and testHostPSK is the host bootstrap key the test runtimes accept
var (
	testRuntimePSK = []byte("runtime-test-psk-0123456789abcdef")
	testHostPSK    = []byte("host-test-psk-0123456789abcdef")
)

// baseRuntimeOpts are the options every test runtime needs: a runtime PSK for the CA and a host bootstrap method
func baseRuntimeOpts() []RuntimeOption {
	return []RuntimeOption{
		WithRuntimePSKs(testRuntimePSK),
		WithHostBootstrapPSK(testHostPSK),
	}
}

// newTestRuntime returns a Runtime backed by an in-memory provider, ready for dispatch-level tests
func newTestRuntime(t *testing.T, opts ...RuntimeOption) (*Runtime, *standalone.StandaloneMemory) {
	t.Helper()

	prov, err := standalone.NewStandaloneMemory(slog.New(slog.DiscardHandler), standalone.StandaloneMemoryOptions{}, components.ProviderConfig{
		HostHealthCheckDeadline:   20 * time.Second,
		AlarmsLeaseDuration:       20 * time.Second,
		AlarmsFetchAheadInterval:  2500 * time.Millisecond,
		AlarmsFetchAheadBatchSize: 25,
	})
	require.NoError(t, err)

	all := append([]RuntimeOption{WithBind("127.0.0.1:0")}, baseRuntimeOpts()...)
	all = append(all, opts...)
	rt, err := NewRuntime(prov, all...)
	require.NoError(t, err)
	require.NoError(t, prov.Init(t.Context()))

	return rt, prov
}

// registerTestHost registers a host with the provider and returns its ID
func registerTestHost(t *testing.T, prov *standalone.StandaloneMemory, address string, actorTypes ...string) string {
	t.Helper()

	ats := make([]components.ActorHostType, len(actorTypes))
	for i, at := range actorTypes {
		ats[i] = components.ActorHostType{ActorType: at, IdleTimeout: time.Minute}
	}
	res, err := prov.RegisterHost(t.Context(), components.RegisterHostReq{Address: address, ActorTypes: ats})
	require.NoError(t, err)
	return res.HostID
}

// dispatchReq encodes a request and runs it through the runtime dispatcher
func dispatchReq(t *testing.T, rt *Runtime, c *hostConn, kind string, payload any) *protocol.Envelope {
	t.Helper()

	env, err := protocol.NewRequest(kind, payload)
	require.NoError(t, err)
	return rt.dispatch(t.Context(), c, env)
}

// requireError asserts the response is a protocol error with the given code
func requireError(t *testing.T, resp *protocol.Envelope, code protocol.ErrorCode) {
	t.Helper()

	perr, ok := resp.AsError()
	require.True(t, ok, "expected an error response, got kind %q", resp.Kind)
	assert.Equal(t, code, perr.Code)
}

func TestHandlerDoesNotLeakProviderErrorDetail(t *testing.T) {
	// Verifies that an internal provider failure is reported to the client with a generic message, not the raw error
	provider := components_mocks.NewMockActorProvider(t)

	// A provider error whose detail must never reach the client
	secret := errors.New("dial tcp 10.0.0.5:5432: connect with user=admin password=hunter2")
	provider.EXPECT().
		SetState(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(secret)

	rt, err := NewRuntime(provider, append([]RuntimeOption{WithBind("127.0.0.1:0")}, baseRuntimeOpts()...)...)
	require.NoError(t, err)

	c := &hostConn{
		hostID:    "h1",
		sessionID: "s1",
	}
	req, err := protocol.NewRequest(protocol.KindSetState, protocol.SetStateRequest{
		ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		Data:     []byte("data"),
	})
	require.NoError(t, err)

	resp := rt.handleSetState(t.Context(), c, req)

	perr, ok := resp.AsError()
	require.True(t, ok)
	assert.Equal(t, protocol.ErrCodeInternal, perr.Code)
	assert.Equal(t, "failed to set state", perr.Message, "the client message must be generic")
	assert.NotContains(t, perr.Message, "password", "the provider error detail must not be echoed to the client")
	assert.NotContains(t, perr.Message, "10.0.0.5", "the provider error detail must not be echoed to the client")
}

func TestHandleHealthCheck(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.1:1", "T")

	t.Run("persists health for a registered host", func(t *testing.T) {
		c := &hostConn{hostID: hostID, sessionID: "s1"}
		resp := dispatchReq(t, rt, c, protocol.KindHealthCheck, protocol.HealthCheckRequest{})
		assert.Equal(t, protocol.KindHealthCheckResponse, resp.Kind)
	})

	t.Run("reports unregistered for an unknown host", func(t *testing.T) {
		c := &hostConn{hostID: "00000000-0000-4000-8000-000000000000", sessionID: "s1"}
		resp := dispatchReq(t, rt, c, protocol.KindHealthCheck, protocol.HealthCheckRequest{})
		requireError(t, resp, protocol.ErrCodeHostUnregistered)
	})

	t.Run("updates supported actor types when provided", func(t *testing.T) {
		c := &hostConn{hostID: hostID, sessionID: "s1"}
		resp := dispatchReq(t, rt, c, protocol.KindHealthCheck, protocol.HealthCheckRequest{
			ActorTypes: []protocol.ActorHostType{{ActorType: "T2", IdleTimeoutMs: 60000}},
		})
		assert.Equal(t, protocol.KindHealthCheckResponse, resp.Kind)
		stored := c.actorTypes.Load()
		require.NotNil(t, stored)
		assert.Equal(t, []protocol.ActorHostType{
			{ActorType: "T2", IdleTimeoutMs: 60000},
		}, *stored)
	})
}

func TestHandleLookupActor(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.2:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	t.Run("resolves placement for a supported actor type", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
		require.Equal(t, protocol.KindLookupActorResponse, resp.Kind)

		var out protocol.LookupActorResponse
		require.NoError(t, resp.DecodePayload(&out))
		assert.Equal(t, hostID, out.HostID)
		assert.Equal(t, "10.0.0.2:1", out.Address)
	})

	t.Run("returns no host for an unsupported actor type", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "Unsupported", ActorID: "a1"})
		requireError(t, resp, protocol.ErrCodeNoHost)
	})

	t.Run("active-only lookup for an inactive actor is not active and not retryable", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "never-activated", ActiveOnly: true})
		perr, ok := resp.AsError()
		require.True(t, ok)
		assert.Equal(t, protocol.ErrCodeActorNotActive, perr.Code)
		assert.False(t, perr.Retryable())
	})

	t.Run("missing actor identity is a bad request", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T"})
		requireError(t, resp, protocol.ErrCodeBadRequest)
	})
}

func TestHandleLookupActorCache(t *testing.T) {
	rt, prov := newTestRuntime(t)
	rt.placementCache = ttlcache.NewCache[string, *cachedPlacement](&ttlcache.CacheOptions{MaxTTL: rt.placementCacheTTL()})
	defer rt.placementCache.Stop()

	hostID := registerTestHost(t, prov, "10.0.0.3:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	// First lookup allocates and caches the placement
	resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
	require.Equal(t, protocol.KindLookupActorResponse, resp.Kind)

	// Remove the host from the provider
	// The cache should still serve the prior placement
	err := prov.UnregisterHost(t.Context(), hostID)
	require.NoError(t, err)

	cachedResp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
	require.Equal(t, protocol.KindLookupActorResponse, cachedResp.Kind)
	var out protocol.LookupActorResponse
	require.NoError(t, cachedResp.DecodePayload(&out))
	assert.Equal(t, hostID, out.HostID, "cache should serve the prior placement")

	// SkipCache bypasses the cache and hits the provider, which no longer has a host
	freshResp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1", SkipCache: true})
	requireError(t, freshResp, protocol.ErrCodeNoHost)
}

func TestHandleLookupActorSkipCacheRefreshesCache(t *testing.T) {
	rt, prov := newTestRuntime(t)
	rt.placementCache = ttlcache.NewCache[string, *cachedPlacement](&ttlcache.CacheOptions{MaxTTL: rt.placementCacheTTL()})
	defer rt.placementCache.Stop()

	hostID := registerTestHost(t, prov, "10.0.0.9:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	// A SkipCache lookup bypasses the (empty) cache but must still populate it with the fresh placement
	skipResp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1", SkipCache: true})
	require.Equal(t, protocol.KindLookupActorResponse, skipResp.Kind)

	// Remove the host from the provider
	// A subsequent cached lookup should still serve the entry written above
	require.NoError(t, prov.UnregisterHost(t.Context(), hostID))

	cachedResp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
	require.Equal(t, protocol.KindLookupActorResponse, cachedResp.Kind)
	var out protocol.LookupActorResponse
	require.NoError(t, cachedResp.DecodePayload(&out))
	assert.Equal(t, hostID, out.HostID, "SkipCache lookup should have refreshed the cache")
}

func TestHandleLookupActorDrainingHost(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.4:1", "T")

	// Track the target host as draining in the host manager
	target := &hostConn{hostID: hostID, sessionID: "s1"}
	target.setDraining()
	rt.hosts.Register(target)

	caller := &hostConn{hostID: "caller", sessionID: "s2"}
	resp := dispatchReq(t, rt, caller, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})

	perr, ok := resp.AsError()
	require.True(t, ok)
	assert.Equal(t, protocol.ErrCodeRetryLater, perr.Code)
	assert.True(t, perr.Retryable())
	d, hasRetry := perr.RetryAfter()
	require.True(t, hasRetry)
	assert.Positive(t, d)
}

func TestHandleUnregisterKeepsHostRegistered(t *testing.T) {
	rt, prov := newTestRuntime(t)
	c := connectTestHost(t, rt, prov, "10.0.0.20:1", protocol.ActorHostType{ActorType: "T"})

	// Activate an actor on the host so we can prove its placement survives the unregister
	aref := ref.NewActorRef("T", "a1")
	_, err := prov.LookupActor(t.Context(), aref, components.LookupActorOpts{})
	require.NoError(t, err)

	resp := dispatchReq(t, rt, c, protocol.KindUnregisterHost, protocol.UnregisterHostRequest{})
	require.Equal(t, protocol.KindUnregisterHostResponse, resp.Kind)

	// The host is marked draining but must stay registered with its actor still active, so existing actors keep serving until the host is completely done
	assert.True(t, c.IsDraining())
	_, err = prov.LookupActor(t.Context(), aref, components.LookupActorOpts{ActiveOnly: true})
	require.NoError(t, err, "the active actor must still be placed after a graceful unregister")
}

func TestHandleRemoveActor(t *testing.T) {
	rt, prov := newTestRuntime(t)
	rt.placementCache = ttlcache.NewCache[string, *cachedPlacement](&ttlcache.CacheOptions{MaxTTL: rt.placementCacheTTL()})
	defer rt.placementCache.Stop()

	hostID := registerTestHost(t, prov, "10.0.0.10:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	t.Run("removes an active actor and clears its cached placement", func(t *testing.T) {
		// A lookup activates the actor and caches its placement
		resp := dispatchReq(t, rt, c, protocol.KindLookupActor, protocol.LookupActorRequest{ActorType: "T", ActorID: "a1"})
		require.Equal(t, protocol.KindLookupActorResponse, resp.Kind)
		_, cached := rt.placementCache.Get(ref.NewActorRef("T", "a1").String())
		require.True(t, cached, "lookup should have cached the placement")

		// Removing the actor acknowledges and drops the cache entry
		removeResp := dispatchReq(t, rt, c, protocol.KindRemoveActor, protocol.RemoveActorRequest{ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"}})
		assert.Equal(t, protocol.KindRemoveActorResponse, removeResp.Kind)
		_, stillCached := rt.placementCache.Get(ref.NewActorRef("T", "a1").String())
		assert.False(t, stillCached, "remove should have cleared the cached placement")
	})

	t.Run("reports not active for an actor that is not active", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindRemoveActor, protocol.RemoveActorRequest{ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "never-activated"}})
		requireError(t, resp, protocol.ErrCodeActorNotActive)
	})

	t.Run("missing actor identity is a bad request", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindRemoveActor, protocol.RemoveActorRequest{ActorRef: protocol.ActorRef{ActorType: "T"}})
		requireError(t, resp, protocol.ErrCodeBadRequest)
	})
}

func TestHandleState(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.5:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	data := []byte("actor-state")

	t.Run("set then get round-trips the state", func(t *testing.T) {
		setResp := dispatchReq(t, rt, c, protocol.KindSetState, protocol.SetStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
			Data:     data,
		})
		assert.Equal(t, protocol.KindSetStateResponse, setResp.Kind)

		getResp := dispatchReq(t, rt, c, protocol.KindGetState, protocol.GetStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		})
		require.Equal(t, protocol.KindGetStateResponse, getResp.Kind)
		var out protocol.GetStateResponse
		require.NoError(t, getResp.DecodePayload(&out))
		assert.Equal(t, data, out.Data)
	})

	t.Run("get for missing state reports not found", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindGetState, protocol.GetStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "missing"},
		})
		requireError(t, resp, protocol.ErrCodeStateNotFound)
	})

	t.Run("delete removes the state", func(t *testing.T) {
		delResp := dispatchReq(t, rt, c, protocol.KindDeleteState, protocol.DeleteStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		})
		assert.Equal(t, protocol.KindDeleteStateResponse, delResp.Kind)

		getResp := dispatchReq(t, rt, c, protocol.KindGetState, protocol.GetStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		})
		requireError(t, getResp, protocol.ErrCodeStateNotFound)

		// Deleting again reports not found
		delAgain := dispatchReq(t, rt, c, protocol.KindDeleteState, protocol.DeleteStateRequest{
			ActorRef: protocol.ActorRef{ActorType: "T", ActorID: "a1"},
		})
		requireError(t, delAgain, protocol.ErrCodeStateNotFound)
	})
}

func TestHandleAlarm(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.6:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	const dueMs = int64(1_700_000_000_000)
	aref := protocol.AlarmRef{ActorType: "T", ActorID: "a1", Name: "wake"}

	t.Run("set then get round-trips the alarm", func(t *testing.T) {
		setResp := dispatchReq(t, rt, c, protocol.KindSetAlarm, protocol.SetAlarmRequest{
			AlarmRef:        aref,
			AlarmProperties: protocol.AlarmProperties{DueTimeUnixMs: dueMs, Interval: "PT1M", Data: []byte("d")},
		})
		assert.Equal(t, protocol.KindSetAlarmResponse, setResp.Kind)

		getResp := dispatchReq(t, rt, c, protocol.KindGetAlarm, protocol.GetAlarmRequest{AlarmRef: aref})
		require.Equal(t, protocol.KindGetAlarmResponse, getResp.Kind)
		var out protocol.GetAlarmResponse
		require.NoError(t, getResp.DecodePayload(&out))
		assert.Equal(t, dueMs, out.DueTimeUnixMs)
		assert.Equal(t, "PT1M", out.Interval)
		assert.Equal(t, []byte("d"), out.Data)
	})

	t.Run("get for missing alarm reports not found", func(t *testing.T) {
		resp := dispatchReq(t, rt, c, protocol.KindGetAlarm, protocol.GetAlarmRequest{
			AlarmRef: protocol.AlarmRef{ActorType: "T", ActorID: "a1", Name: "missing"},
		})
		requireError(t, resp, protocol.ErrCodeAlarmNotFound)
	})

	t.Run("delete removes the alarm", func(t *testing.T) {
		delResp := dispatchReq(t, rt, c, protocol.KindDeleteAlarm, protocol.DeleteAlarmRequest{AlarmRef: aref})
		assert.Equal(t, protocol.KindDeleteAlarmResponse, delResp.Kind)

		delAgain := dispatchReq(t, rt, c, protocol.KindDeleteAlarm, protocol.DeleteAlarmRequest{AlarmRef: aref})
		requireError(t, delAgain, protocol.ErrCodeAlarmNotFound)
	})
}

func TestDispatchUnknownKind(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.7:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	env := protocol.NewEnvelope("totally.unknown", nil)
	resp := rt.dispatch(t.Context(), c, env)
	requireError(t, resp, protocol.ErrCodeBadRequest)
}

func TestDispatchSupersededSession(t *testing.T) {
	rt, prov := newTestRuntime(t)
	hostID := registerTestHost(t, prov, "10.0.0.8:1", "T")
	c := &hostConn{hostID: hostID, sessionID: "s1"}

	// A request carrying a different session ID is rejected
	env, err := protocol.NewRequest(protocol.KindHealthCheck, protocol.HealthCheckRequest{})
	require.NoError(t, err)
	env.SessionID = "stale-session"
	resp := rt.dispatch(t.Context(), c, env)
	requireError(t, resp, protocol.ErrCodeSessionSuperseded)
}
