package workflow

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

// newTestRegistry builds the registry singleton over the fake host, which is what every registry test drives
func newTestRegistry(t *testing.T, host *fakeHost) *registryActor {
	t.Helper()

	svc := actor.NewService(host)
	r, ok := newRegistryActor("registry", "singleton", svc).(*registryActor)
	require.True(t, ok, "the registry factory should build a registry actor")
	return r
}

// registerWith asks the registry whether a fingerprint is the one recorded for a version
func registerWith(t *testing.T, r *registryActor, version int, fingerprint string) registerResponse {
	t.Helper()

	res, err := r.Invoke(t.Context(), methodRegister, &payloadEnvelope{value: registerRequest{Version: version, Fingerprint: fingerprint}})
	require.NoError(t, err)

	resp, ok := res.(registerResponse)
	require.True(t, ok, "register should answer with a registerResponse")
	return resp
}

// recordedVersions returns the versions the registry holds, in the order it holds them
func recordedVersions(t *testing.T, r *registryActor) []registryEntry {
	t.Helper()

	res, err := r.Invoke(t.Context(), methodDefinitions, nil)
	require.NoError(t, err)

	resp, ok := res.(definitionsResponse)
	require.True(t, ok, "definitions should answer with a definitionsResponse")
	return resp.Entries
}

func TestTheRegistryRecordsTheFirstGraphItSeesForAVersion(t *testing.T) {
	host := newFakeHost()
	r := newTestRegistry(t, host)

	first := registerWith(t, r, 1, "fingerprint-a")
	assert.True(t, first.OK)
	assert.Equal(t, "fingerprint-a", first.Fingerprint)
	assert.NotZero(t, first.FirstSeenAt)

	// An entry is never overwritten, so the first deployment of a version defines it and the same host keeps agreeing
	again := registerWith(t, r, 1, "fingerprint-a")
	assert.True(t, again.OK)
	assert.Equal(t, first.FirstSeenAt, again.FirstSeenAt)

	// A host whose graph disagrees is told so, and is told what was recorded, so the conflict can name both fingerprints
	conflicting := registerWith(t, r, 1, "fingerprint-b")
	assert.False(t, conflicting.OK)
	assert.Equal(t, "fingerprint-a", conflicting.Fingerprint)
	assert.Equal(t, first.FirstSeenAt, conflicting.FirstSeenAt)

	// A different version is a separate entry, which is how a version bump deploys alongside the one it replaces
	next := registerWith(t, r, 2, "fingerprint-b")
	assert.True(t, next.OK)
	assert.Len(t, recordedVersions(t, r), 2)
}

func TestTheRegistryForgetsOnlyTheVersionItIsAskedTo(t *testing.T) {
	host := newFakeHost()
	r := newTestRegistry(t, host)

	registerWith(t, r, 1, "fingerprint-a")
	registerWith(t, r, 2, "fingerprint-b")

	_, err := r.Invoke(t.Context(), methodForget, &payloadEnvelope{value: forgetRequest{Version: 1}})
	require.NoError(t, err)

	entries := recordedVersions(t, r)
	require.Len(t, entries, 1)
	assert.Equal(t, 2, entries[0].Version)

	// Forgetting a version that is not there changes nothing, so an interrupted reset is safe to repeat
	_, err = r.Invoke(t.Context(), methodForget, &payloadEnvelope{value: forgetRequest{Version: 1}})
	require.NoError(t, err)
	assert.Len(t, recordedVersions(t, r), 1)

	// The version is free again, so a corrected graph can claim the number
	assert.True(t, registerWith(t, r, 1, "fingerprint-c").OK)
}

func TestTheRegistryRejectsWhatItCannotAnswer(t *testing.T) {
	host := newFakeHost()
	r := newTestRegistry(t, host)

	t.Run("a method it does not know", func(t *testing.T) {
		_, err := r.Invoke(t.Context(), "whatever", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown workflow registry method")
	})

	t.Run("a payload it cannot decode", func(t *testing.T) {
		// A payload that cannot be decoded fails the same way on every attempt, so retrying it would only waste attempts
		_, err := r.Invoke(t.Context(), methodRegister, &payloadEnvelope{value: json.RawMessage(`"not a request"`)})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)

		_, err = r.Invoke(t.Context(), methodForget, &payloadEnvelope{value: json.RawMessage(`"not a request"`)})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	})

	t.Run("an empty registry", func(t *testing.T) {
		assert.Empty(t, recordedVersions(t, r), "a registry nothing has registered with holds nothing")
	})
}

func TestAHostAsksTheRegistryAtMostOncePerVersion(t *testing.T) {
	host := newFakeHost()
	wf, err := New("cached-check", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// The check is the one synchronous call the engine makes from a turn, so it is cached for the life of the process
	for range 3 {
		ok, sErr := wf.serveVersion(t.Context(), svc, wf.def.version)
		require.NoError(t, sErr)
		assert.True(t, ok)
	}

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 1)
}

func TestAHostDeclinesAVersionItsOwnCodeDoesNotDefine(t *testing.T) {
	host := newFakeHost()
	wf, err := New("other-version", WithVersion(2), WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// Only the version this host's code defines can be checked against this host's fingerprint, so another version is declined without consulting the registry
	ok, err := wf.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	assert.False(t, ok)

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Empty(t, host.invokes, "a version mismatch is decided locally")
}

func TestARegistryLookupThatFailedIsNotCached(t *testing.T) {
	host := newFakeHost()
	host.registryErr = errors.New("the registry is unreachable")

	wf, err := New("transient-check", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// A timeout or a storage blip must not take the version out of service on this host until it restarts
	_, err = wf.serveVersion(t.Context(), svc, wf.def.version)
	require.Error(t, err)

	host.registryErr = nil
	served, err := wf.serveVersion(t.Context(), svc, wf.def.version)
	require.NoError(t, err)
	assert.True(t, served, "the version is servable once the registry answers")

	// The answer it did get is cached, so the retry costs one extra call and no more
	served, err = wf.serveVersion(t.Context(), svc, wf.def.version)
	require.NoError(t, err)
	assert.True(t, served)

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 2, "the failed lookup is retried once and the answer is then cached")
}

func TestADeclineIsCachedLikeAnyOtherAnswer(t *testing.T) {
	host := newFakeHost()
	host.registryResponse = registerResponse{OK: false, Fingerprint: "someone-elses-graph"}

	wf, err := New("cached-decline", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// A conflict is an answer the registry gave, so it is cached: the graph this host runs cannot change without restarting it
	for range 3 {
		served, sErr := wf.serveVersion(t.Context(), svc, wf.def.version)
		require.NoError(t, sErr)
		assert.False(t, served)
	}

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 1)
}
