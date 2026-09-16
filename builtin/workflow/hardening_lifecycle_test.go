package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/actor"
)

// deliverLifecycleJob advances one durable delivery and releases its live idempotency key as the runtime would
func deliverLifecycleJob(t *testing.T, host *fakeHost, wf *Workflow, instanceID string, method string, o *orchestrator) {
	t.Helper()
	jobID := host.jobIDFor(builtinActorType(wf.baseType), instanceID, method)
	require.NotEmpty(t, jobID)
	err := o.Job(t.Context(), method, &payloadEnvelope{value: host.jobPayloads[jobID]})
	require.NoError(t, err)
	host.mu.Lock()
	host.removeJobLocked(jobID)
	host.mu.Unlock()
}

func TestHardeningSweepRetainsDescendantsUntilTheirRootTerminates(t *testing.T) {
	leafWF, err := New("hardening-leaf", WithSteps(Step("effect", WithRun(noopRun), WithCompensate(noopCompensate))))
	require.NoError(t, err)
	middleWF, err := New("hardening-middle", WithSteps(Child("leaf", WithDefinition(leafWF))))
	require.NoError(t, err)
	rootWF, err := New("hardening-root", WithSteps(Child("middle", WithDefinition(middleWF)), WaitForEvent("approval")))
	require.NoError(t, err)
	host := newFakeHost()
	root := newTestOrchestrator(t, rootWF, host, "root")
	middleID := workerActorID("root", "middle", 0)
	leafID := workerActorID(middleID, "leaf", 0)
	middle := newTestOrchestrator(t, middleWF, host, middleID)
	leaf := newTestOrchestrator(t, leafWF, host, leafID)

	// Complete both descendant workflows while the root remains open for a later external decision
	err = root.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	deliverLifecycleJob(t, host, middleWF, middleID, methodStart, middle)
	deliverLifecycleJob(t, host, leafWF, leafID, methodStart, leaf)
	err = leaf.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)
	deliverLifecycleJob(t, host, middleWF, middleID, methodDone, middle)
	deliverLifecycleJob(t, host, rootWF, "root", methodDone, root)
	require.Equal(t, StatusRunning, readJournal(t, host, rootWF, "root").Status)
	require.Equal(t, StatusCompleted, readJournal(t, host, middleWF, middleID).Status)
	leafState := readJournal(t, host, leafWF, leafID)
	leafState.CompletedAt = time.Now().Add(-3 * defaultRetention)
	err = leaf.client.SetState(t.Context(), leafState, nil)
	require.NoError(t, err)

	// The leaf sweep must follow its completed parent to the still-active root before deciding it can purge
	sweepHost := &reviewAPIHost{fakeHost: host, workflows: map[string]*Workflow{builtinActorType(leafWF.baseType): leafWF}}
	removed, err := leafWF.Service(actor.NewService(sweepHost)).PurgeTerminated(t.Context())
	require.NoError(t, err)
	require.Zero(t, removed)
	require.Equal(t, StatusCompleted, readJournal(t, host, leafWF, leafID).Status)

	// Retained descendants must still be able to undo their effects when the root is subsequently cancelled
	err = root.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "approval withdrawn"}})
	require.NoError(t, err)
	deliverLifecycleJob(t, host, middleWF, middleID, methodUnwind, middle)
	deliverLifecycleJob(t, host, leafWF, leafID, methodUnwind, leaf)
	require.NotEmpty(t, host.jobIDFor(builtinActorType(leafWF.undoType("")), workerActorID(leafID, "effect", 0), methodCompensate))
	err = leaf.Job(t.Context(), methodCompensated, &payloadEnvelope{value: compReportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)
	deliverLifecycleJob(t, host, middleWF, middleID, methodCompensated, middle)
	deliverLifecycleJob(t, host, rootWF, "root", methodCompensated, root)
	rootState := readJournal(t, host, rootWF, "root")
	require.Equal(t, StatusCancelled, rootState.Status)
	require.Equal(t, CompensationCompleted, rootState.Compensation)

	// Once the root terminates, the same retained chain no longer prevents collection
	active, err := leaf.parentStillRunning(t.Context(), leafState.Parent)
	require.NoError(t, err)
	require.False(t, active)
}

func TestHardeningPurgeRefusesCyclicAncestry(t *testing.T) {
	host := newFakeHost()
	wf, err := New("hardening-cycle", WithSteps(Step("effect", WithRun(noopRun))))
	require.NoError(t, err)
	st := instanceState{Status: StatusCompleted, Parent: &parentRef{Workflow: wf.name, InstanceID: "cycle"}}
	err = host.SetState(t.Context(), builtinActorType(wf.baseType), "cycle", st, nil)
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "cycle")
	_, err = o.purge(t.Context())
	require.ErrorContains(t, err, "ancestry contains a cycle")
	require.Equal(t, StatusCompleted, readJournal(t, host, wf, "cycle").Status)
}

type hardeningDeleteStateHost struct {
	*fakeHost
	deleteErr error
}

func (h *hardeningDeleteStateHost) DeleteState(ctx context.Context, actorType string, actorID string) error {
	if h.deleteErr != nil {
		return h.deleteErr
	}
	return h.fakeHost.DeleteState(ctx, actorType, actorID)
}

func TestHardeningPurgeRetriesAFailedStateDeletion(t *testing.T) {
	host := &hardeningDeleteStateHost{fakeHost: newFakeHost()}
	wf, err := New("hardening-purge-retry", WithSteps(Step("effect", WithRun(noopRun))))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
	require.NoError(t, err)

	// A provider failure must leave both the durable journal and the activation's retryable snapshot intact
	host.deleteErr = errors.New("injected deletion failure")
	_, err = o.purge(t.Context())
	require.ErrorIs(t, err, host.deleteErr)
	require.Equal(t, StatusCompleted, readJournal(t, host.fakeHost, wf, "instance").Status)
	host.deleteErr = nil
	result, err := o.purge(t.Context())
	require.NoError(t, err)
	require.Equal(t, purgeResult{Found: true}, result)
	var st instanceState
	err = host.GetState(t.Context(), builtinActorType(wf.baseType), "instance", &st)
	require.ErrorIs(t, err, actor.ErrStateNotFound)
}

type hardeningUndoCleanupHost struct {
	*fakeHost
	failActorType string
}

func (h *hardeningUndoCleanupHost) ListJobs(ctx context.Context, actorType string, actorID string) ([]actor.JobInfo, error) {
	if actorType == h.failActorType {
		return nil, errors.New("injected undo cleanup failure")
	}
	return h.fakeHost.ListJobs(ctx, actorType, actorID)
}

func TestHardeningCompensationTimeoutRemovesQueuedUndo(t *testing.T) {
	for _, failCleanup := range []bool{false, true} {
		t.Run(map[bool]string{false: "cleanup succeeds", true: "cleanup retries"}[failCleanup], func(t *testing.T) {
			host := &hardeningUndoCleanupHost{fakeHost: newFakeHost()}
			wf, err := New("hardening-undo-timeout", WithTimeout(time.Hour), WithSteps(
				Step("effect", WithRun(noopRun), WithCompensate(noopCompensate)),
				WaitForEvent("approval"),
			))
			require.NoError(t, err)
			o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
			err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
			require.NoError(t, err)
			err = o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "effect", Index: 0, Attempt: 1}})
			require.NoError(t, err)
			err = o.Job(t.Context(), methodCancel, &payloadEnvelope{value: reasonPayload{Reason: "stop"}})
			require.NoError(t, err)
			undoJob := host.jobIDFor(builtinActorType(wf.undoType("")), workerActorID("instance", "effect", 0), methodCompensate)
			require.NotEmpty(t, undoJob)

			// Exhaust the compensation budget before the queued undo can enter its handler
			st, err := o.client.GetState(t.Context())
			require.NoError(t, err)
			st.StartedAt = time.Now().Add(-2 * time.Hour)
			st.DeadlineAt = instanceDeadline(&st, wf.def)
			err = o.client.SetState(t.Context(), st, nil)
			require.NoError(t, err)
			if failCleanup {
				host.failActorType = builtinActorType(wf.undoType(""))
				err = o.Alarm(t.Context(), alarmDeadline, nil)
				require.NoError(t, err)
				require.Equal(t, StatusCompensating, readJournal(t, host.fakeHost, wf, "instance").Status)
				host.failActorType = ""
			}

			// Publishing abandonment requires queued undo removal, including after a transient cleanup failure
			err = o.Alarm(t.Context(), alarmDeadline, nil)
			require.NoError(t, err)
			st = readJournal(t, host.fakeHost, wf, "instance")
			require.True(t, st.Status.IsTerminal())
			require.Equal(t, CompensationFailed, st.Compensation)
			_, err = host.GetJob(t.Context(), undoJob)
			require.ErrorIs(t, err, actor.ErrJobNotFound)
		})
	}
}

type hardeningRecoveryHost struct {
	*fakeHost
	failAlarm bool
	failRead  bool
	failState bool
	failRetry bool
}

func (h *hardeningRecoveryHost) SetAlarm(ctx context.Context, actorType string, actorID string, name string, props actor.AlarmProperties) error {
	if h.failAlarm {
		return errors.New("injected alarm write failure")
	}
	return h.fakeHost.SetAlarm(ctx, actorType, actorID, name, props)
}

func (h *hardeningRecoveryHost) SetState(ctx context.Context, actorType string, actorID string, state any, opts *actor.SetStateOpts) error {
	if h.failState {
		return errors.New("injected state write failure")
	}
	return h.fakeHost.SetState(ctx, actorType, actorID, state, opts)
}

func (h *hardeningRecoveryHost) GetState(ctx context.Context, actorType string, actorID string, into any) error {
	if h.failRead {
		return errors.New("injected state read failure")
	}
	return h.fakeHost.GetState(ctx, actorType, actorID, into)
}

func (h *hardeningRecoveryHost) RetryJob(ctx context.Context, jobID string) (string, error) {
	if h.failRetry {
		return "", errors.New("injected job replay failure")
	}
	return h.fakeHost.RetryJob(ctx, jobID)
}

func TestHardeningRecurringDeadlineOutlivesTransientFailures(t *testing.T) {
	for _, failure := range []string{"journal read", "journal write", "alarm replacement"} {
		t.Run(failure, func(t *testing.T) {
			host := &hardeningRecoveryHost{fakeHost: newFakeHost()}
			wf, err := New("hardening-recurring", WithTimeout(time.Minute), WithSteps(WaitForEvent("approval")))
			require.NoError(t, err)
			o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
			err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
			require.NoError(t, err)
			props, err := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.NoError(t, err)
			require.Equal(t, "PT5S", props.Interval)
			err = props.Validate()
			require.NoError(t, err)
			delivery := &payloadEnvelope{value: props.Data}
			o = newReviewOrchestrator(t, wf, "instance", actor.NewService(host))

			// Fail more occurrences than the ordinary alarm retry limit while preventing any replacement write from repairing the row
			host.failAlarm = true
			host.failRead = failure == "journal read"
			host.failState = failure == "journal write"
			for range orchestratorMaxAttempts + 5 {
				err = o.Alarm(t.Context(), alarmDeadline, delivery)
				require.NoError(t, err, "a recurring occurrence must complete so the runtime reschedules it")
				remaining, readErr := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
				require.NoError(t, readErr)
				require.Equal(t, "PT5S", remaining.Interval)
				require.Equal(t, StatusRunning, readJournal(t, host.fakeHost, wf, "instance").Status)
			}

			// Recovery of the dependencies allows a later occurrence to enforce the original instance timeout and remove the recurrence
			host.failAlarm = false
			host.failRead = false
			host.failState = false
			st, err := o.client.GetState(t.Context())
			require.NoError(t, err)
			st.StartedAt = time.Now().Add(-2 * time.Minute)
			st.DeadlineAt = instanceDeadline(&st, wf.def)
			err = o.client.SetState(t.Context(), st, nil)
			require.NoError(t, err)
			err = o.Alarm(t.Context(), alarmDeadline, delivery)
			require.NoError(t, err)
			require.Equal(t, StatusFailed, readJournal(t, host.fakeHost, wf, "instance").Status)
			_, err = host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.ErrorIs(t, err, actor.ErrAlarmNotFound)
		})
	}
}

func TestHardeningLegacyDeadlineMigratesBeforeAJournalFailure(t *testing.T) {
	host := &hardeningRecoveryHost{fakeHost: newFakeHost()}
	wf, err := New("hardening-legacy-alarm", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newReviewOrchestrator(t, wf, "instance", actor.NewService(host))
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)

	// An alarm created by an older release must gain its fallback before a journal write can prevent a turn
	err = host.SetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline, actor.AlarmProperties{DueTime: time.Now()})
	require.NoError(t, err)
	host.failState = true
	err = o.Alarm(t.Context(), alarmDeadline, nil)
	require.NoError(t, err)
	props, err := host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
	require.NoError(t, err)
	require.Equal(t, "PT5S", props.Interval)
	require.Equal(t, deadlinePayload{Recurring: true}, props.Data)
}

func TestHardeningRecurringDeadlineStopsForInactiveJournals(t *testing.T) {
	for _, status := range []Status{"", StatusCompleted, StatusSuspended} {
		t.Run(string(status), func(t *testing.T) {
			host := newFakeHost()
			wf, err := New("hardening-inactive-alarm", WithSteps(WaitForEvent("approval")))
			require.NoError(t, err)
			o := newTestOrchestrator(t, wf, host, "instance")
			err = o.client.SetState(t.Context(), instanceState{Status: status}, nil)
			require.NoError(t, err)
			props := deadlineAlarmProperties(time.Now())
			err = o.client.SetAlarm(t.Context(), alarmDeadline, props)
			require.NoError(t, err)

			// A recurrence must stop explicitly when no active journal needs its deadline
			err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: props.Data})
			require.NoError(t, err)
			_, err = host.GetAlarm(t.Context(), builtinActorType(wf.baseType), "instance", alarmDeadline)
			require.ErrorIs(t, err, actor.ErrAlarmNotFound)
		})
	}
}

func TestHardeningDeadLetterRecoveryPreservesOriginalEvents(t *testing.T) {
	cases := []struct {
		method  string
		payload any
		status  Status
	}{
		{methodStart, startPayload{Version: 1, Input: json.RawMessage(`{"order":42}`), CreatedAt: time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)}, StatusRunning},
		{methodEvent, eventPayload{Name: "approval", Payload: json.RawMessage(`{"approved":true}`)}, StatusCompleted},
		{methodCancel, reasonPayload{Reason: "customer withdrew order"}, StatusCancelled},
		{methodSuspend, reasonPayload{Reason: "waiting for operator"}, StatusSuspended},
		{methodResume, nil, StatusRunning},
	}
	for _, tc := range cases {
		for _, callbackFails := range []bool{false, true} {
			name := tc.method
			if callbackFails {
				name += "/callback failure"
			}
			t.Run(name, func(t *testing.T) {
				host := &hardeningRecoveryHost{fakeHost: newFakeHost()}
				wf, err := New("hardening-event-recovery", WithSteps(WaitForEvent("approval")))
				require.NoError(t, err)
				svc := actor.NewService(host)
				o := newReviewOrchestrator(t, wf, "instance", svc)
				if tc.method != methodStart {
					err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
					require.NoError(t, err)
				}
				if tc.method == methodResume {
					err = o.Job(t.Context(), methodSuspend, &payloadEnvelope{value: reasonPayload{Reason: "paused"}})
					require.NoError(t, err)
				}

				// Exhaust a durable request before it can alter the journal and invoke the same best-effort hook as the runtime
				jobID, _, err := o.client.Dispatch(t.Context(), tc.method, tc.payload)
				require.NoError(t, err)
				host.deadLetter(jobID, "transient delivery failure")
				host.failRetry = callbackFails
				err = o.JobFailed(t.Context(), jobID, tc.method, nil, errors.New("transient delivery failure"))
				if callbackFails {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}

				// A new activation's recurring deadline can repair a failed callback from the retained record alone
				host.failRetry = false
				o = newReviewOrchestrator(t, wf, "instance", svc)
				err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
				require.NoError(t, err)
				recovered := host.jobIDFor(builtinActorType(wf.baseType), "instance", tc.method)
				require.NotEmpty(t, recovered)
				require.NotEqual(t, jobID, recovered)
				require.Equal(t, tc.payload, host.jobPayloads[recovered])
				deliverLifecycleJob(t, host.fakeHost, wf, "instance", tc.method, o)
				st := readJournal(t, host.fakeHost, wf, "instance")
				require.Equal(t, tc.status, st.Status)
				if tc.method == methodEvent {
					require.JSONEq(t, `{"approved":true}`, string(st.Output))
				}
				if tc.method == methodStart {
					require.JSONEq(t, `{"order":42}`, string(st.Input))
				}
			})
		}
	}
}

func TestHardeningDeadlineRecoversAnEventWhoseCallbackNeverRan(t *testing.T) {
	host := newFakeHost()
	wf, err := New("hardening-missed-hook", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	payload := eventPayload{Name: "approval", Payload: json.RawMessage(`"accepted"`)}
	jobID, _, err := o.client.Dispatch(t.Context(), methodEvent, payload)
	require.NoError(t, err)
	host.deadLetter(jobID, "transient delivery failure")

	// The durable alarm is sufficient to repair a dead letter even when no failure callback was delivered
	err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
	require.NoError(t, err)
	deliverLifecycleJob(t, host, wf, "instance", methodEvent, o)
	st := readJournal(t, host, wf, "instance")
	require.Equal(t, StatusCompleted, st.Status)
	require.JSONEq(t, `"accepted"`, string(st.Output))
}

func TestHardeningPermanentDeliveryFailuresRemainInspectable(t *testing.T) {
	host := newFakeHost()
	wf, err := New("hardening-permanent-event", WithSteps(WaitForEvent("approval")))
	require.NoError(t, err)
	o := newTestOrchestrator(t, wf, host, "instance")
	err = o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}})
	require.NoError(t, err)
	jobID, _, err := o.client.Dispatch(t.Context(), methodEvent, "malformed payload")
	require.NoError(t, err)
	host.deadLetter(jobID, actor.ErrJobPermanentFailure.Error())

	// Retrying a malformed payload cannot succeed and must not create an endless recovery loop
	err = o.JobFailed(t.Context(), jobID, methodEvent, nil, actor.ErrJobPermanentFailure)
	require.NoError(t, err)
	err = o.Alarm(t.Context(), alarmDeadline, &payloadEnvelope{value: deadlinePayload{Recurring: true}})
	require.NoError(t, err)
	job, err := host.GetJob(t.Context(), jobID)
	require.NoError(t, err)
	require.Equal(t, actor.JobStatusDeadLettered, job.Status)
	require.Equal(t, StatusRunning, readJournal(t, host, wf, "instance").Status)
}
