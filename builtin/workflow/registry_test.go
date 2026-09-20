package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
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
	assert.True(t, first.Found)
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

func TestTheRegistryChecksKnownVersionsWithoutCreatingUnknownOnes(t *testing.T) {
	host := newFakeHost()
	r := newTestRegistry(t, host)

	res, err := r.Peek(t.Context(), methodCheck, &payloadEnvelope{value: registerRequest{Version: 1, Fingerprint: "fingerprint-a"}})
	require.NoError(t, err)
	unknown, ok := res.(registerResponse)
	require.True(t, ok)
	assert.False(t, unknown.Found)
	assert.Empty(t, recordedVersions(t, r))

	registerWith(t, r, 1, "fingerprint-a")
	res, err = r.Peek(t.Context(), methodCheck, &payloadEnvelope{value: registerRequest{Version: 1, Fingerprint: "fingerprint-a"}})
	require.NoError(t, err)
	known, ok := res.(registerResponse)
	require.True(t, ok)
	assert.True(t, known.Found)
	assert.True(t, known.OK)
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

		_, err = r.Peek(t.Context(), "whatever", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown workflow registry peek method")
	})

	t.Run("a payload it cannot decode", func(t *testing.T) {
		// A payload that cannot be decoded fails the same way on every attempt, so retrying it would only waste attempts
		_, err := r.Invoke(t.Context(), methodRegister, &payloadEnvelope{value: json.RawMessage(`"not a request"`)})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)

		_, err = r.Peek(t.Context(), methodCheck, &payloadEnvelope{value: json.RawMessage(`"not a request"`)})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)

		_, err = r.Invoke(t.Context(), methodForget, &payloadEnvelope{value: json.RawMessage(`"not a request"`)})
		require.ErrorIs(t, err, actor.ErrJobPermanentFailure)
	})

	t.Run("an empty registry", func(t *testing.T) {
		assert.Empty(t, recordedVersions(t, r), "a registry nothing has registered with holds nothing")
	})
}

func TestAHostChecksTheRegistryBeforeServingEachJob(t *testing.T) {
	host := newFakeHost()
	wf, err := New("cached-check", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// Each check observes a possible ForgetVersion reset, so live hosts cannot keep conflicting authority after another graph claims the version
	for range 3 {
		ok, sErr := wf.serveVersion(t.Context(), svc, wf.def.version)
		require.NoError(t, sErr)
		assert.True(t, ok)
	}

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 3)
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

func TestARegistryLookupRecoversAfterATransientFailure(t *testing.T) {
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

	// A later job checks again because registry resets must take effect without restarting this host
	served, err = wf.serveVersion(t.Context(), svc, wf.def.version)
	require.NoError(t, err)
	assert.True(t, served)

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 3)
}

func TestARegistryCheckFallsBackDuringARollingDeployment(t *testing.T) {
	host := newFakeHost()
	host.registryPeekErr = actorcore.ErrActorMethodUnsupported
	wf, err := New("rolling-check", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	served, err := wf.serveVersion(t.Context(), actor.NewService(host), wf.def.version)
	require.NoError(t, err)
	assert.True(t, served)

	host.mu.Lock()
	defer host.mu.Unlock()
	require.Len(t, host.invokes, 2)
	assert.Contains(t, host.invokes[0], methodCheck)
	assert.Contains(t, host.invokes[1], methodRegister)
}

func TestAHostRechecksARegistryDecline(t *testing.T) {
	host := newFakeHost()
	host.registryResponse = registerResponse{Found: true, OK: false, Fingerprint: "someone-elses-graph"}

	wf, err := New("cached-decline", WithSteps(Step("a", WithRun(noopRun))))
	require.NoError(t, err)

	svc := actor.NewService(host)

	// A conflict is checked again because an operator may reset the erroneous registry entry while this host remains online
	for range 3 {
		served, sErr := wf.serveVersion(t.Context(), svc, wf.def.version)
		require.NoError(t, sErr)
		assert.False(t, served)
	}

	host.mu.Lock()
	defer host.mu.Unlock()
	assert.Len(t, host.invokes, 3)
}

// identityHost models the registry actor's exclusive invocation lock and can pause the first workflow journal write
type identityHost struct {
	*registryRoutingHost

	registryMu   sync.RWMutex
	writeOnce    sync.Once
	workflowType string
	beforeWrite  bool
	writeReached chan struct{}
	writeRelease chan struct{}
}

func newIdentityHost(t *testing.T, wf *Workflow) *identityHost {
	t.Helper()
	h := &identityHost{registryRoutingHost: &registryRoutingHost{fakeHost: newFakeHost()}, workflowType: builtinActorType(wf.baseType)}
	svc := actor.NewService(h)
	r, ok := newRegistryActor(wf.registryType(), "singleton", svc).(*registryActor)
	require.True(t, ok)
	h.registry = r
	return h
}

func (h *identityHost) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	if method == methodRegister || method == methodForget || method == methodDefinitions {
		h.registryMu.Lock()
		defer h.registryMu.Unlock()
	}
	return h.registryRoutingHost.Invoke(ctx, actorType, actorID, method, data, opts...)
}

func (h *identityHost) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	if method == methodCheck {
		h.registryMu.RLock()
		defer h.registryMu.RUnlock()
	}
	return h.registryRoutingHost.Peek(ctx, actorType, actorID, method, data, opts...)
}

func (h *identityHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	// The barrier does not hold the storage lock, so the registry can observe either side of the journal's publication
	block := func() {
		if actorType != h.workflowType || h.writeReached == nil {
			return
		}
		h.writeOnce.Do(func() {
			close(h.writeReached)
			select {
			case <-h.writeRelease:
			case <-ctx.Done():
			}
		})
	}
	if h.beforeWrite {
		block()
	}
	err := h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
	if !h.beforeWrite {
		block()
	}
	return err
}

// jobsFor selects the recorded deliveries for one built-in actor family

// jobsFor selects the recorded deliveries for one built-in actor family
func jobsFor(h *fakeHost, bareType string, method string) []actor.JobInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	var jobs []actor.JobInfo
	for _, job := range h.jobs {
		if job.ActorType == builtinActorType(bareType) && job.Method == method {
			jobs = append(jobs, job)
		}
	}
	return jobs
}

func TestForgetSerializesWithJournalPublication(t *testing.T) {
	for _, beforeWrite := range []bool{true, false} {
		name := "journal wins"
		if beforeWrite {
			name = "forget wins"
		}
		t.Run(name, func(t *testing.T) {
			// Capture a generation-bound public start before a corrected deployment attempts to reset the same numeric version
			old, err := New("identity-race", WithSteps(Step("old", WithRun(noopRun))))
			require.NoError(t, err)
			corrected, err := New("identity-race", WithSteps(Step("corrected", WithRun(noopRun))))
			require.NoError(t, err)
			h := newIdentityHost(t, old)
			h.beforeWrite = beforeWrite
			h.writeReached = make(chan struct{})
			h.writeRelease = make(chan struct{})
			defer close(h.writeRelease)
			svc := actor.NewService(h)
			_, created, err := old.Service(svc).Start(t.Context(), nil, WithInstanceID("inst-1"))
			require.NoError(t, err)
			require.True(t, created)
			p, ok := reportedPayload(t, h.fakeHost, methodStart).(startPayload)
			require.True(t, ok)
			require.NotZero(t, p.RegistryGeneration)
			require.Equal(t, old.def.fingerprint, p.DefinitionFingerprint)

			// Pause exactly before or after persistence, without depending on scheduler timing
			o := newRoutedOrchestrator(t, old, "inst-1", svc)
			result := make(chan error, 1)
			go func() { result <- o.Job(t.Context(), methodStart, &payloadEnvelope{value: p}) }()
			select {
			case <-h.writeReached:
			case <-time.After(5 * time.Second):
				t.Fatal("start did not reach the journal barrier")
			}
			if beforeWrite {
				err = corrected.Service(svc).ForgetVersion(t.Context(), 1)
				require.NoError(t, err)
			} else {
				// Bypass the service's advisory listing to exercise the registry's own in-lock guard
				_, err = h.Invoke(t.Context(), builtinActorType(old.registryType()), "singleton", methodForget, forgetRequest{Version: 1, ReplacementFingerprint: corrected.def.fingerprint})
				require.ErrorIs(t, err, ErrVersionInUse)
			}
			h.writeRelease <- struct{}{}
			err = <-result
			require.NoError(t, err)

			// Exactly one ordering may dispatch work, and neither may rewrite the start's original graph identity
			st := readJournal(t, h.fakeHost, old, "inst-1")
			assert.Equal(t, p.RegistryGeneration, st.RegistryGeneration)
			assert.Equal(t, p.DefinitionFingerprint, st.DefinitionFingerprint)
			assert.Equal(t, beforeWrite, st.RegistryRejected)
			assert.Equal(t, !beforeWrite, st.RegistryConfirmed)
			if beforeWrite {
				assert.Equal(t, StatusFailed, st.Status)
				assert.Empty(t, jobsFor(h.fakeHost, old.workerType(""), methodRun))
			} else {
				assert.Equal(t, StatusRunning, st.Status)
				assert.Len(t, jobsFor(h.fakeHost, old.workerType(""), methodRun), 1)
			}
		})
	}
}

func TestRegistryGenerationsNeverReclaimForgottenAuthorization(t *testing.T) {
	h := newFakeHost()
	r := newTestRegistry(t, h)
	legacy := registryState{Versions: []registryEntry{{Version: 1, Fingerprint: "old"}}}
	err := r.client.SetState(t.Context(), legacy, nil)
	require.NoError(t, err)
	identity := registerWith(t, r, 1, "old")
	require.Positive(t, identity.Generation, "legacy entries acquire a generation before authorizing new starts")
	err = r.forget(t.Context(), &payloadEnvelope{value: forgetRequest{Version: 1}})
	require.NoError(t, err)
	replacement := registerWith(t, r, 1, "old")
	require.Greater(t, replacement.Generation, identity.Generation, "even reinstalling the same graph cannot revive an old start")
	res, err := r.register(t.Context(), &payloadEnvelope{value: registerRequest{Version: 1, Fingerprint: "old", Generation: identity.Generation}})
	require.NoError(t, err)
	assert.False(t, res.(registerResponse).OK) //nolint:forcetypeassert
}

func TestStartConfirmationRetriesBeforeDispatch(t *testing.T) {
	wf, err := New("confirm-retry", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	o := newTestOrchestrator(t, wf, h, "inst-1")
	p := startPayload{Version: 1, DefinitionFingerprint: wf.def.fingerprint, RegistryGeneration: 1}
	h.registryErr = errors.New("registry unavailable")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: p})
	require.Error(t, err)
	st := readJournal(t, h, wf, "inst-1")
	require.False(t, st.RegistryConfirmed)
	require.Empty(t, jobsFor(h, wf.workerType(""), methodRun))

	// The next delivery retries the durable confirmation before any task can leave the orchestrator
	h.registryErr = nil
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: p})
	require.NoError(t, err)
	st = readJournal(t, h, wf, "inst-1")
	require.True(t, st.RegistryConfirmed)
	require.Len(t, jobsFor(h, wf.workerType(""), methodRun), 1)
	h.registryErr = errors.New("registry unavailable again")
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 1}})
	require.NoError(t, err)
	assert.Equal(t, StatusCompleted, readJournal(t, h, wf, "inst-1").Status)
}

func TestTerminalRecoveryConfirmsBeforeReporting(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "job retry", true: "deadline retry"}[deadline], func(t *testing.T) {
			wf, err := New("terminal-confirm", WithSteps(Step("work", WithRun(noopRun))))
			require.NoError(t, err)
			h := newFakeHost()
			o := newTestOrchestrator(t, wf, h, "inst-1")
			st := instanceState{Workflow: wf.name, Version: 1, DefinitionFingerprint: wf.def.fingerprint, RegistryGeneration: 1, Status: StatusCompleted, CompletedAt: time.Now()}
			err = o.persist(t.Context(), &st, time.Now())
			require.NoError(t, err)
			h.registryResponse = registerResponse{Found: true, OK: false, Generation: 2}
			if deadline {
				err = o.runDeadline(t.Context())
				require.NoError(t, err)
			} else {
				p := startPayload{Version: 1, DefinitionFingerprint: wf.def.fingerprint, RegistryGeneration: 1}
				err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: p})
				require.NoError(t, err)
			}
			result := readJournal(t, h, wf, "inst-1")
			assert.True(t, result.RegistryRejected)
			assert.False(t, result.RegistryConfirmed)
			assert.Equal(t, StatusFailed, result.Status)
		})
	}
}

func TestTerminalDeliveriesRejectMismatchedGraph(t *testing.T) {
	for _, version := range []int{1, 2} {
		t.Run(map[int]string{1: "fingerprint", 2: "version"}[version], func(t *testing.T) {
			original, err := New("terminal-fence", WithSteps(Step("work", WithRun(noopRun), WithCompensate(noopCompensate))))
			require.NoError(t, err)
			other, err := New("terminal-fence", WithVersion(version), WithSteps(Step("different", WithRun(noopRun))))
			require.NoError(t, err)
			h := newFakeHost()
			o := newTestOrchestrator(t, original, h, "inst-1")
			err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
			require.NoError(t, err)
			err = o.Job(t.Context(), methodCancel, nil)
			require.NoError(t, err)
			st := readJournal(t, h, original, "inst-1")
			require.True(t, st.Status.IsTerminal())
			report := reportPayload{Step: "work", Attempt: 1}
			require.True(t, isLateAbandonedSuccess(&st, &event{kind: evDone, report: &report}))
			before := append([]byte(nil), h.state[key(builtinActorType(original.baseType), "inst-1")]...)
			wrong := newTestOrchestrator(t, other, h, "inst-1")
			require.ErrorIs(t, wrong.Job(t.Context(), methodDone, &payloadEnvelope{value: report}), actor.ErrJobRejected)
			assert.Equal(t, before, h.state[key(builtinActorType(original.baseType), "inst-1")])
			assert.Empty(t, jobsFor(h, original.undoType(""), methodCompensate))
		})
	}
}

func TestWorkerUsesLocalGraphIdentity(t *testing.T) {
	wf, err := New("worker-identity", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	h.registryErr = errors.New("registry unavailable")
	w := newTestWorker(t, wf, h, false)
	p := runPayloadFor(wf, "work")
	p.DefinitionFingerprint = "other graph"
	p.RegistryGeneration = 4
	require.ErrorIs(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}), actor.ErrJobRejected)
	p.DefinitionFingerprint = wf.def.fingerprint
	err = w.Job(t.Context(), methodRun, &payloadEnvelope{value: p})
	require.NoError(t, err)
	assert.Empty(t, h.invokes, "new task delivery does not depend on the registry singleton")
	p.DefinitionFingerprint = ""
	require.ErrorContains(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}), "registry unavailable")
}

func TestLegacyJournalBindsToTheRegisteredGraph(t *testing.T) {
	wf, err := New("legacy-identity", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	o := newTestOrchestrator(t, wf, h, "inst-1")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)

	// Remove fields absent from old journals and reactivate so recovery reads those exact persisted bytes
	st := readJournal(t, h, wf, "inst-1")
	st.DefinitionFingerprint = ""
	st.RegistryGeneration = 0
	st.RegistryConfirmed = false
	st.Timeout = 0
	st.UnknownVersion = ""
	err = h.SetState(t.Context(), builtinActorType(wf.baseType), "inst-1", st, nil)
	require.NoError(t, err)
	o = newTestOrchestrator(t, wf, h, "inst-1")
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "work", Attempt: 1}})
	require.NoError(t, err)
	st = readJournal(t, h, wf, "inst-1")
	assert.Equal(t, wf.def.fingerprint, st.DefinitionFingerprint)
	assert.Positive(t, st.RegistryGeneration)
	assert.True(t, st.RegistryConfirmed)
	assert.Equal(t, wf.def.timeout, st.Timeout)
	assert.Equal(t, wf.def.unknownVersion, st.UnknownVersion)
	assert.Equal(t, StatusCompleted, st.Status)
}

func TestDefinitionConflictsRemainObservable(t *testing.T) {
	var logs bytes.Buffer
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		err := provider.Shutdown(context.Background())
		require.NoError(t, err)
	})
	wf, err := New("identity-metrics", WithLogger(slog.New(slog.NewTextHandler(&logs, nil))), WithMeter(provider.Meter("identity")), WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	h.registryResponse = registerResponse{Found: true, OK: false, Fingerprint: "registered-graph", Generation: 2}
	_, err = wf.authorizeDefinition(t.Context(), actor.NewService(h))
	require.ErrorIs(t, err, ErrDefinitionConflict)
	require.Equal(t, int64(1), int64MetricTotal(t, reader, "francis.workflow.definition.conflicts"))

	// A local worker fence reports the same deployment conflict without asking the registry again
	w := newTestWorker(t, wf, h, false)
	p := runPayloadFor(wf, "work")
	p.DefinitionFingerprint = "registered-graph"
	p.RegistryGeneration = 2
	require.ErrorIs(t, w.Job(t.Context(), methodRun, &payloadEnvelope{value: p}), actor.ErrJobRejected)
	require.Equal(t, int64(2), int64MetricTotal(t, reader, "francis.workflow.definition.conflicts"))
	assert.Len(t, h.invokes, 1)
	assert.Contains(t, logs.String(), "version=1")
	assert.NotContains(t, logs.String(), "registered-graph")
	assert.NotContains(t, logs.String(), "Fingerprint")
}

func TestChildStartCarriesAuthorizedIdentity(t *testing.T) {
	child, err := New("identity-child", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	parent, err := New("identity-parent", WithSteps(Child("child", WithDefinition(child))))
	require.NoError(t, err)
	h := newFakeHost()
	o := newTestOrchestrator(t, parent, h, "inst-1")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	p, ok := reportedPayload(t, h, methodStart).(startPayload)
	require.True(t, ok)
	assert.Equal(t, child.def.fingerprint, p.DefinitionFingerprint)
	assert.Positive(t, p.RegistryGeneration)
	assert.Equal(t, child.def.version, p.Version)
}

// registryRoutingHost routes synchronous registry and purge calls while retaining the existing fake host's deterministic storage
type registryRoutingHost struct {
	*fakeHost

	registry  *registryActor
	workflows map[string]*Workflow
}

func (h *registryRoutingHost) Invoke(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	// Route registry calls through the real registry implementation so forget and cache behavior share one durable state
	if method == methodRegister || method == methodForget || method == methodDefinitions {
		result, err := h.registry.Invoke(ctx, method, &payloadEnvelope{value: data})
		return &fakeEnvelope{value: result}, err
	}

	// Route purge calls through the real orchestrator to exercise the service's sweep decisions
	if method == methodPurge {
		wf := h.workflows[actorType]
		obj := newOrchestrator(wf, actorID, actor.NewService(h))
		orchestrator, ok := obj.(*orchestrator)
		if !ok {
			return nil, errors.New("workflow factory did not return an orchestrator")
		}
		result, err := orchestrator.purge(ctx)
		return &fakeEnvelope{value: result}, err
	}
	return h.fakeHost.Invoke(ctx, actorType, actorID, method, data, opts...)
}

func newRoutedOrchestrator(t *testing.T, wf *Workflow, actorID string, svc *actor.Service) *orchestrator {
	t.Helper()
	obj := newOrchestrator(wf, actorID, svc)
	orchestrator, ok := obj.(*orchestrator)
	require.True(t, ok)
	return orchestrator
}

func (h *registryRoutingHost) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	if method == methodCheck {
		result, err := h.registry.Peek(ctx, method, &payloadEnvelope{value: data})
		return &fakeEnvelope{value: result}, err
	}
	return h.fakeHost.Peek(ctx, actorType, actorID, method, data, opts...)
}

func (h *registryRoutingHost) ListStates(ctx context.Context, actorType string, opts *actor.ListStatesOpts) (actor.StateList, error) {
	// Supply the filtering that the baseline fake host omits so the actual service listing and sweep can run
	h.mu.Lock()
	defer h.mu.Unlock()
	page := actor.StateList{}
	filter := opts.WorkflowLabels()
	for stateKey, encoded := range h.state {
		if !strings.HasPrefix(stateKey, actorType+"/") {
			continue
		}
		var st instanceState
		err := msgpack.Unmarshal(encoded, &st)
		if err != nil {
			return page, err
		}
		if filter != nil && filter.Status != "" && filter.Status != string(st.Status) {
			continue
		}
		if filter != nil && filter.Version != 0 && filter.Version != st.Version {
			continue
		}
		id := strings.TrimPrefix(stateKey, actorType+"/")
		if id <= opts.After {
			continue
		}
		page.States = append(page.States, actor.StateInfo{ActorID: id, Data: &fakeEnvelope{value: st}})
	}
	sort.Slice(page.States, func(i, j int) bool { return page.States[i].ActorID < page.States[j].ActorID })
	if opts.Limit > 0 && len(page.States) > opts.Limit {
		page.HasMore = true
		page.States = page.States[:opts.Limit]
	}
	return page, nil
}

func TestForgetVersionInvalidatesExistingDecisions(t *testing.T) {
	// Establish one approved graph and one rejected graph before the operator resets the unused version
	host := &registryRoutingHost{fakeHost: newFakeHost()}
	host.registry = newTestRegistry(t, host.fakeHost)
	svc := actor.NewService(host)
	old, err := New("cache", WithSteps(Step("old", WithRun(noopRun))))
	require.NoError(t, err)
	corrected, err := New("cache", WithSteps(Step("corrected", WithRun(noopRun))))
	require.NoError(t, err)
	served, err := old.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.True(t, served)
	served, err = corrected.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.False(t, served)

	// Reset through the public service and let a new process claim the corrected graph under the same version
	require.NoError(t, corrected.Service(svc).ForgetVersion(t.Context(), 1))
	restarted, err := New("cache", WithSteps(Step("corrected", WithRun(noopRun))))
	require.NoError(t, err)
	served, err = restarted.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	require.True(t, served)

	// Existing processes must converge on the new registry entry instead of retaining conflicting approvals or stale rejections
	served, err = old.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	assert.False(t, served, "the old graph remains approved after another graph claims its version")
	served, err = corrected.serveVersion(t.Context(), svc, 1)
	require.NoError(t, err)
	assert.True(t, served, "the corrected graph remains rejected after the registry reset")
}

func TestFingerprintDistinguishesCommaContainingReferences(t *testing.T) {
	for _, option := range []string{"inputFrom", "skipOnFailure"} {
		t.Run(option, func(t *testing.T) {
			// Keep the full graph identical while changing whether a reference names one comma-containing step or two separate steps
			build := func(references []string) *definition {
				steps := []StepSpec{
					Step("a", WithRun(noopRun)),
					Step("b", WithRun(noopRun)),
					Step("a,b", WithRun(noopRun)),
				}
				if option == "inputFrom" {
					steps = append(steps, Step("consumer", WithRun(noopRun), WithInputFrom(references...)))
				} else {
					steps = append([]StepSpec{Step("producer", WithRun(noopRun), WithSkipOnFailure(references...))}, steps...)
				}
				return testDefinition(t, "fingerprint", WithSteps(steps...))
			}
			oneReference := build([]string{"a,b"})
			twoReferences := build([]string{"a", "b"})
			assert.NotEqual(t, oneReference.fingerprint, twoReferences.fingerprint, "different valid reference lists have identical fingerprints")
		})
	}
}
