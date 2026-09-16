package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/italypaleale/francis/actor"
)

// identityHost models the registry actor's exclusive invocation lock and can pause the first workflow journal write
type identityHost struct {
	*reviewAPIHost

	registryMu   sync.RWMutex
	writeOnce    sync.Once
	workflowType string
	beforeWrite  bool
	writeReached chan struct{}
	writeRelease chan struct{}
}

func newIdentityHost(t *testing.T, wf *Workflow) *identityHost {
	t.Helper()
	h := &identityHost{reviewAPIHost: &reviewAPIHost{fakeHost: newFakeHost()}, workflowType: builtinActorType(wf.baseType)}
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
	return h.reviewAPIHost.Invoke(ctx, actorType, actorID, method, data, opts...)
}

func (h *identityHost) Peek(ctx context.Context, actorType string, actorID string, method string, data any, opts ...actor.InvokeOption) (actor.Envelope, error) {
	if method == methodCheck {
		h.registryMu.RLock()
		defer h.registryMu.RUnlock()
	}
	return h.reviewAPIHost.Peek(ctx, actorType, actorID, method, data, opts...)
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

func TestHardeningForgetSerializesWithJournalPublication(t *testing.T) {
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
			o := newReviewOrchestrator(t, old, "inst-1", svc)
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

func TestHardeningRegistryGenerationsNeverReclaimForgottenAuthorization(t *testing.T) {
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

func TestHardeningStartConfirmationRetriesBeforeDispatch(t *testing.T) {
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

func TestHardeningTerminalRecoveryConfirmsBeforeReporting(t *testing.T) {
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

func TestHardeningTerminalDeliveriesRejectMismatchedGraph(t *testing.T) {
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

func TestHardeningWorkerUsesLocalGraphIdentity(t *testing.T) {
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

func TestHardeningLegacyJournalBindsToTheRegisteredGraph(t *testing.T) {
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

func TestHardeningDefinitionConflictsRemainObservable(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		err := provider.Shutdown(context.Background())
		require.NoError(t, err)
	})
	wf, err := New("identity-metrics", WithMeter(provider.Meter("identity")), WithSteps(Step("work", WithRun(noopRun))))
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
}

func TestHardeningChildStartCarriesAuthorizedIdentity(t *testing.T) {
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

func TestHardeningUnknownHostUsesJournaledDeadlinePolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		policy  UnknownVersionPolicy
		timeout time.Duration
		status  Status
		want    Status
	}{
		{name: "park survives a host configured to fail", policy: ParkUnknownVersion, timeout: time.Minute, status: StatusRunning, want: StatusRunning},
		{name: "original timeout prevents an early failure", policy: FailUnknownVersion, timeout: time.Hour, status: StatusRunning, want: StatusRunning},
		{name: "original failure policy survives a host configured to park", policy: FailUnknownVersion, timeout: time.Minute, status: StatusRunning, want: StatusFailed},
		{name: "legacy unknown policy parks conservatively", status: StatusRunning, want: StatusRunning},
		{name: "suspended instance stays paused", policy: FailUnknownVersion, timeout: time.Minute, status: StatusSuspended, want: StatusSuspended},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostPolicy := FailUnknownVersion
			if tc.want == StatusFailed {
				hostPolicy = ParkUnknownVersion
			}
			wf, err := New("unknown-policy", WithVersion(2), WithTimeout(time.Minute), WithUnknownVersionPolicy(hostPolicy), WithSteps(Step("replacement", WithRun(noopRun))))
			require.NoError(t, err)
			h := newFakeHost()
			o := newTestOrchestrator(t, wf, h, "inst-1")
			st := instanceState{Workflow: wf.name, Version: 1, Status: tc.status, StartedAt: time.Now().Add(-2 * time.Minute), Timeout: tc.timeout, UnknownVersion: tc.policy}
			err = o.persist(t.Context(), &st, time.Now())
			require.NoError(t, err)
			err = o.runDeadline(t.Context())
			require.NoError(t, err)
			result := readJournal(t, h, wf, "inst-1")
			assert.Equal(t, tc.want, result.Status)
			assert.Equal(t, tc.timeout, result.Timeout)
			assert.Equal(t, tc.policy, result.UnknownVersion)
		})
	}
}

func TestHardeningUnknownVersionChildReportsFailureBeforeDroppingDeadline(t *testing.T) {
	wf, err := New("unknown-child", WithVersion(2), WithSteps(Step("replacement", WithRun(noopRun))))
	require.NoError(t, err)
	h := newFakeHost()
	o := newTestOrchestrator(t, wf, h, "child-1")
	st := instanceState{
		Workflow: wf.name, Version: 1, Status: StatusRunning,
		DefinitionFingerprint: "original-child-graph", RegistryGeneration: 1,
		StartedAt: time.Now().Add(-2 * time.Minute), Timeout: time.Minute, UnknownVersion: FailUnknownVersion,
		Parent: childOf("parent-1"),
	}
	err = o.persist(t.Context(), &st, time.Now())
	require.NoError(t, err)
	err = o.client.SetAlarm(t.Context(), alarmDeadline, deadlineAlarmProperties(time.Now()))
	require.NoError(t, err)

	// An unavailable parent transport keeps the terminal journal and recovery alarm until the failure report can be delivered
	h.failDispatch = true
	err = o.runDeadline(t.Context())
	require.Error(t, err)
	result := readJournal(t, h, wf, "child-1")
	assert.Equal(t, StatusFailed, result.Status)
	assert.True(t, result.RegistryConfirmed)
	assert.False(t, result.Reported)
	_, armed := alarmDue(t, h, wf, "child-1")
	assert.True(t, armed)

	h.failDispatch = false
	err = o.runDeadline(t.Context())
	require.NoError(t, err)
	result = readJournal(t, h, wf, "child-1")
	assert.True(t, result.Reported)
	assert.Equal(t, []string{methodDone}, reportsToParent(t, h, "parent-1"))
	_, armed = alarmDue(t, h, wf, "child-1")
	assert.False(t, armed)
}
