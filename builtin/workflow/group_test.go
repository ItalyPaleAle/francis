package workflow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAGroupMembersOwnRetryPolicyDecidesItsTask(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "member-retries", WithSteps(
		Parallel("notify",
			Step("email", WithRun(noopRun), WithMaxAttempts(1)),
			Step("sms", WithRun(noopRun), WithMaxAttempts(3), WithRetryBackoff(10*time.Second, time.Minute)),
		),
	))

	st := startJournal(t, def, now)
	reportFailure(t, st, def, "notify", 0, "the mail server is down", true, now)
	reportFailure(t, st, def, "notify", 1, "the carrier is down", true, now)

	// A report names the group, so the member that ran the task is what the attempt budget has to be read from
	notify := st.step("notify")
	email := notify.task(0)
	assert.True(t, email.Done, "the member's budget of one attempt is spent")
	assert.Equal(t, "the mail server is down", email.Error)

	sms := notify.task(1)
	assert.False(t, sms.Done, "the member's budget of three attempts leaves two")
	assert.Equal(t, 2, sms.Attempts)
	assert.Equal(t, now.Add(10*time.Second), sms.RetryAt, "the backoff is the member's own, not the group's default")
}

func TestAGroupMembersOwnCompensationPolicyDecidesItsUndo(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "member-comp-retries", WithSteps(
		Parallel("book",
			Step("hotel", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(1)),
			Step("flight", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateMaxAttempts(3), WithCompensateBackoff(20*time.Second, time.Minute)),
		),
		Step("boom", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "book", 0, "hotel-1", now)
	reportSuccess(t, st, def, "book", 1, "flight-1", now)
	advance(st, def, "inst-1", now)
	reportFailure(t, st, def, "boom", 0, "induced failure", false, now)
	advance(st, def, "inst-1", now)
	require.Equal(t, StepCompensating, stepStatus(t, st, "book"))

	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "book", Index: 0, Attempt: 1, Error: "cannot cancel", Retryable: true}}, now)
	apply(st, def, &event{kind: evCompensated, comp: &compReportPayload{Step: "book", Index: 1, Attempt: 1, Error: "cannot cancel", Retryable: true}}, now)

	book := st.step("book")
	assert.True(t, book.task(0).Comp.Done, "the member's budget of one attempt is spent")

	flight := book.task(1).Comp
	assert.False(t, flight.Done)
	assert.Equal(t, 2, flight.Attempts)
	assert.Equal(t, now.Add(20*time.Second), flight.RetryAt, "the backoff is the member's own, not the group's default")
}

func TestAGroupMembersOwnDependenciesReachItsHandler(t *testing.T) {
	host := newFakeHost()
	wf, err := New("member-inputs", WithSteps(
		Step("quote", WithRun(noopRun)),
		Step("plan", WithRun(noopRun)),
		Parallel("book",
			Step("hotel", WithRun(noopRun), WithInputFrom("quote")),
			Step("flight", WithRun(noopRun)),
		),
	))
	require.NoError(t, err)

	o := newTestOrchestrator(t, wf, host, "inst-1")
	require.NoError(t, o.Job(t.Context(), methodStart, &payloadEnvelope{value: startPayload{Version: 1}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "quote", Index: 0, Attempt: 1, Output: json.RawMessage(`"q-1"`)}}))
	require.NoError(t, o.Job(t.Context(), methodDone, &payloadEnvelope{value: reportPayload{Step: "plan", Index: 0, Attempt: 1, Output: json.RawMessage(`"p-1"`)}}))

	workerType := builtinActorType(wf.workerType(""))
	payloadFor := func(index int) runPayload {
		t.Helper()

		jobID := host.jobIDFor(workerType, workerActorID("inst-1", "book", index), methodRun)
		require.NotEmpty(t, jobID, "member %d should be dispatched", index)

		host.mu.Lock()
		defer host.mu.Unlock()
		p, ok := host.jobPayloads[jobID].(runPayload)
		require.True(t, ok)
		return p
	}

	// A member declares the data its own handler was written against, and only that member's task gets it
	hotel := payloadFor(0)
	assert.Contains(t, hotel.Outputs, "quote")
	assert.Contains(t, hotel.Outputs, "plan", "every member still reads the preceding step")

	flight := payloadFor(1)
	assert.NotContains(t, flight.Outputs, "quote", "a member's dependency is not the group's")
	assert.Contains(t, flight.Outputs, "plan")
}

func TestAParallelGroupTakesItsOwnOptions(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "group-options", WithSteps(
		Parallel("notify",
			Step("email", WithRun(noopRun)),
			Step("sms", WithRun(noopRun)),
		).With(WithFailurePolicy(TolerateFailures)),
		Step("after", WithRun(noopRun)),
	))

	st := startJournal(t, def, now)
	reportSuccess(t, st, def, "notify", 0, "email-ok", now)
	reportFailure(t, st, def, "notify", 1, "the carrier is down", false, now)
	advance(st, def, "inst-1", now)

	// Without a way to set the group's own options this policy could not be declared for a static group at all
	assert.Equal(t, StepCompleted, stepStatus(t, st, "notify"))
	assert.Equal(t, StepRunning, stepStatus(t, st, "after"))
}

func TestGroupOptionsSurviveOnTheSpec(t *testing.T) {
	// With applies to the spec a constructor already built, so it reaches the graph exactly as an option passed to Step would
	def := testDefinition(t, "group-with", WithSteps(
		Step("plan", WithRun(noopRun)),
		Parallel("notify", Step("email", WithRun(noopRun))).
			With(WithOptional(), WithInputFrom("plan"), WithStepTimeout(time.Minute)),
	))

	notify := def.byName["notify"]
	assert.True(t, notify.optional)
	assert.Equal(t, []string{"plan"}, notify.inputFrom)
	assert.Equal(t, time.Minute, notify.stepTimeout)
}

func TestMemberCompensateOnFailure(t *testing.T) {
	now := time.Now()
	def := testDefinition(t, "member-policy", WithSteps(Parallel("group",
		Step("a", WithRun(noopRun), WithCompensate(noopCompensate), WithCompensateOnFailure()),
	)))
	st := startJournal(t, def, now)
	reportFailure(t, st, def, "group", 0, "partial effect", false, now)
	advance(st, def, "inst-1", now)
	assert.NotNil(t, st.step("group").Tasks[0].Comp)
	t.Logf("status=%s compensation=%s", st.Status, st.Compensation)
}
