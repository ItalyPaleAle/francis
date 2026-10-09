package management

import (
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
)

// orchRef is the orchestrator actor of instance i1 of workflow wf
var orchRef = ref.NewActorRef(workflow.OrchestratorActorType("wf"), "i1")

// journal returns a stored journal with the given status, encoded with the journal's wire field names
func journal(t *testing.T, status workflow.Status, extra map[string]any) []byte {
	t.Helper()
	st := map[string]any{
		"workflow":              "wf",
		"version":               2,
		"definitionFingerprint": "fp-2",
		"status":                string(status),
		"input":                 []byte(`{"order":42}`),
		"createdAt":             testNow.Add(-time.Hour),
		"startedAt":             testNow.Add(-time.Hour + time.Second),
		"steps": []map[string]any{
			{"name": "charge", "kind": "task", "status": "completed", "remaining": 0, "tasks": []map[string]any{{"index": 0, "done": true}}},
			{"name": "children", "kind": "child", "status": "running", "remaining": 1, "tasks": []map[string]any{{"index": 0, "childId": "c1", "childType": "workflow.sub"}}},
		},
	}
	maps.Copy(st, extra)
	return encodeMsgpack(t, st)
}

// noDeadJobs makes the provider report no dead-lettered jobs for the instance
func (ts *testServer) noDeadJobs() {
	ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{
		ActorType: orchRef.ActorType,
		ActorID:   orchRef.ActorID,
		Status:    components.JobStatusDeadLettered,
		Limit:     components.DefaultManagementListLimit,
	}).Return(components.QueryJobsRes{}, nil).Maybe()
}

func TestGetInstance(t *testing.T) {
	for _, token := range []string{testReadOnlyToken, testManagementToken} {
		t.Run("journal with data, token "+tokenSuffix(token), func(t *testing.T) {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusCompleted, map[string]any{
				"output":      []byte(`{"ok":true}`),
				"completedAt": testNow,
			}), nil)
			ts.provider.EXPECT().QueryJobs(mock.Anything, mock.Anything).Return(components.QueryJobsRes{Jobs: []components.JobInfo{
				{JobID: "dj", ActorType: orchRef.ActorType, ActorID: "i1", Method: "turn", Status: components.JobStatusDeadLettered, DueTime: testNow, Attempts: 5},
			}}, nil)

			res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", token, ""), http.StatusOK)
			assert.Equal(t, "wf", res["workflow"])
			assert.Equal(t, "i1", res["instanceId"])
			assert.Equal(t, "completed", res["status"])
			assert.InDelta(t, 2, res["version"], 0)
			assert.Equal(t, "fp-2", res["definitionFingerprint"])
			assert.Equal(t, true, res["hasOutput"])
			assert.Equal(t, true, res["eventHistory"])
			assert.Equal(t, testNow.Format(time.RFC3339), res["completedAt"])

			// Both token kinds hold workflows:data:read, so the data is included
			assert.Equal(t, map[string]any{"order": float64(42)}, res["input"])
			assert.Equal(t, map[string]any{"ok": true}, res["output"])
			assert.NotContains(t, res, "dataRedacted")

			steps := arr(t, res["steps"])
			require.Len(t, steps, 2)
			assert.Equal(t, "charge", obj(t, steps[0])["name"])
			assert.InDelta(t, 1, obj(t, steps[0])["taskCount"], 0)
			assert.Equal(t, []any{map[string]any{"workflow": "sub", "instanceId": "c1"}}, obj(t, steps[1])["children"])

			dead := arr(t, res["deadJobs"])
			require.Len(t, dead, 1)
			assert.Equal(t, "dj", obj(t, dead[0])["jobId"])
			assert.Equal(t, map[string]any{"workflow": "wf", "role": "orchestrator", "instanceId": "i1"}, obj(t, dead[0])["workflow"])

			// Reading the data is audited, without the data itself
			recs := ts.logs.auditRecords(t, "workflowInstance.readData")
			require.Len(t, recs, 1)
			assert.Equal(t, tokenSuffix(token), recs[0]["tokenSuffix"])
			assert.Equal(t, "wf", recs[0]["workflow"])
			assert.Equal(t, "i1", recs[0]["instanceId"])
			assert.NotContains(t, ts.logs.String(), "order")
		})
	}

	t.Run("stored JSON null output", func(t *testing.T) {
		ts := newTestServer(t)
		ts.noDeadJobs()
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusCompleted, map[string]any{"output": []byte("null")}), nil)

		w := ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", testReadOnlyToken, "")
		res := decodeJSON(t, w, http.StatusOK)
		assert.Equal(t, true, res["hasOutput"])
		assert.Contains(t, res, "output")
		assert.Nil(t, res["output"])
	})

	t.Run("instance without output", func(t *testing.T) {
		ts := newTestServer(t)
		ts.noDeadJobs()
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusRunning, nil), nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, false, res["hasOutput"])
		assert.NotContains(t, res, "output")
	})

	t.Run("pending instance takes its data from the start job", func(t *testing.T) {
		ts := newTestServer(t)
		ts.noDeadJobs()
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(encodeMsgpack(t, map[string]any{
			"pendingStart": map[string]any{"version": 3, "workflow": "wf", "createdAt": testNow},
		}), nil)
		ts.provider.EXPECT().GetAlarm(mock.Anything, ref.NewAlarmRef(orchRef.ActorType, "i1", workflow.StartJobName)).Return(components.GetAlarmRes{
			AlarmProperties: ref.AlarmProperties{Data: encodeMsgpack(t, map[string]any{
				"input":                 []byte(`"hello"`),
				"version":               3,
				"definitionFingerprint": "fp-3",
			})},
		}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, "pending", res["status"])
		assert.InDelta(t, 3, res["version"], 0)
		assert.Equal(t, "fp-3", res["definitionFingerprint"])
		assert.Equal(t, "hello", res["input"])
		assert.Equal(t, testNow.Format(time.RFC3339), res["createdAt"])
	})

	t.Run("pending instance without a live start job", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(encodeMsgpack(t, map[string]any{
			"pendingStart": map[string]any{"version": 1},
		}), nil)
		ts.provider.EXPECT().GetAlarm(mock.Anything, mock.Anything).Return(components.GetAlarmRes{}, components.ErrNoAlarm)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("unknown instance", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(nil, components.ErrNoState)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("invalid workflow name", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/workflows/a.b/instances/i1", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})
}

func TestListEvents(t *testing.T) {
	t.Run("event history disabled", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusRunning, map[string]any{"noEventHistory": true}), nil)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1/events", testReadOnlyToken, ""), http.StatusNotFound, CodeEventHistoryDisabled)
	})

	t.Run("lists decoded events", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusRunning, nil), nil)
		idx := 0
		ts.provider.EXPECT().ListWorkflowEvents(mock.Anything, components.ListWorkflowEventsReq{
			ActorType: orchRef.ActorType,
			ActorID:   "i1",
			AfterSeq:  4,
			Limit:     2,
		}).Return(components.ListWorkflowEventsRes{
			Events: []components.WorkflowEvent{
				{Seq: 5, Time: testNow, Kind: workflow.EventKindTaskDispatched, Data: encodeMsgpack(t, map[string]any{"timeSource": "dispatch", "step": "charge", "taskIndex": idx, "attempt": 1})},
				{Seq: 6, Time: testNow, Kind: workflow.EventKindChildStarted, Data: encodeMsgpack(t, map[string]any{"child": map[string]any{"workflow": "sub", "instanceId": "c1"}})},
			},
			HasMore: true,
		}, nil)

		cursor := encodeCursor(eventsCursor{After: 4})
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances/i1/events?limit=2&cursor="+cursor, testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		first := obj(t, items[0])
		assert.InDelta(t, 5, first["seq"], 0)
		assert.Equal(t, "task_dispatched", first["kind"])
		assert.InDelta(t, 0, first["taskIndex"], 0)
		assert.Equal(t, map[string]any{"workflow": "sub", "instanceId": "c1"}, obj(t, items[1])["child"])

		var next eventsCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, int64(6), next.After)
	})
}

func TestControlInstance(t *testing.T) {
	const base = "/api/v1/workflows/wf/instances/i1/"

	invalid := []struct {
		name   string
		action string
		body   string
	}{
		{name: "cancel without a body", action: "cancel", body: ""},
		{name: "cancel without a reason", action: "cancel", body: `{}`},
		{name: "cancel with a blank reason", action: "cancel", body: `{"reason":"   "}`},
		{name: "resume with a reason", action: "resume", body: `{"reason":"because"}`},
		{name: "suspend with a reason too long", action: "suspend", body: `{"reason":"` + strings.Repeat("r", maxReasonLength+1) + `"}`},
		{name: "unknown body field", action: "suspend", body: `{"reason":"x","force":true}`},
		{name: "invalid workflow name", action: "suspend", body: `{}`},
	}
	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			// The mock provider has no expectations, so validation must happen before any read
			ts := newTestServer(t)
			target := base + tc.action
			if tc.name == "invalid workflow name" {
				target = "/api/v1/workflows/a.b/instances/i1/suspend"
			}
			decodeError(t, ts.do(t, http.MethodPost, target, testManagementToken, tc.body), http.StatusBadRequest, CodeBadRequest)
			assert.Empty(t, ts.backend.dispatches)

			recs := ts.logs.auditRecords(t, "workflowInstance."+tc.action)
			require.Len(t, recs, 1)
			assert.Equal(t, "failed", recs[0]["result"])
		})
	}

	noEffect := []struct {
		action string
		status workflow.Status
	}{
		{action: "cancel", status: workflow.StatusCompleted},
		{action: "cancel", status: workflow.StatusFailed},
		{action: "cancel", status: workflow.StatusCancelled},
		{action: "cancel", status: workflow.StatusCompensating},
		{action: "suspend", status: workflow.StatusCompleted},
		{action: "resume", status: workflow.StatusCancelled},
	}
	for _, tc := range noEffect {
		t.Run(tc.action+" has no effect on a "+string(tc.status)+" instance", func(t *testing.T) {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, tc.status, nil), nil)

			body := `{"reason":"stop"}`
			if tc.action == "resume" {
				body = ""
			}
			res := decodeJSON(t, ts.do(t, http.MethodPost, base+tc.action, testManagementToken, body), http.StatusOK)
			assert.Equal(t, true, res["noEffect"])
			assert.Equal(t, false, res["coalesced"])
			assert.Equal(t, string(tc.status), res["status"])
			assert.Equal(t, tc.action, res["action"])
			assert.Empty(t, ts.backend.dispatches)
		})
	}

	dispatch := []struct {
		action  workflow.ControlAction
		status  workflow.Status
		body    string
		reason  string
		created bool
	}{
		{action: workflow.ControlCancel, status: workflow.StatusRunning, body: `{"reason":"customer asked"}`, reason: "customer asked", created: true},
		{action: workflow.ControlSuspend, status: workflow.StatusRunning, body: `{"reason":"investigating"}`, reason: "investigating", created: false},
		{action: workflow.ControlSuspend, status: workflow.StatusCompensating, body: ``, created: true},
		{action: workflow.ControlResume, status: workflow.StatusSuspended, body: `{}`, created: true},
		{action: workflow.ControlCancel, status: workflow.StatusPending, body: `{"reason":"x"}`, reason: "x", created: false},
	}
	for _, tc := range dispatch {
		t.Run(string(tc.action)+" dispatches a control job for a "+string(tc.status)+" instance", func(t *testing.T) {
			ts := newTestServer(t)
			if tc.status == workflow.StatusPending {
				ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(encodeMsgpack(t, map[string]any{"pendingStart": map[string]any{"version": 1}}), nil)
				ts.provider.EXPECT().GetAlarm(mock.Anything, mock.Anything).Return(components.GetAlarmRes{}, nil)
			} else {
				ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, tc.status, nil), nil)
			}
			ts.backend.dispatchFn = func(ref.AlarmRef, components.SetAlarmReq) (bool, error) {
				return tc.created, nil
			}

			res := decodeJSON(t, ts.do(t, http.MethodPost, base+string(tc.action), testManagementToken, tc.body), http.StatusAccepted)
			assert.Equal(t, false, res["noEffect"])
			assert.Equal(t, !tc.created, res["coalesced"])
			assert.Equal(t, string(tc.status), res["status"])

			// The job is exactly the one WorkflowService would dispatch
			method, key, data, err := workflow.ControlJob(tc.action, tc.reason)
			require.NoError(t, err)
			require.Len(t, ts.backend.dispatches, 1)
			d := ts.backend.dispatches[0]
			assert.Equal(t, ref.NewAlarmRef(orchRef.ActorType, "i1", key), d.Ref)
			assert.Equal(t, method, d.Req.JobMethod)
			assert.Equal(t, components.AlarmKindJob, d.Req.Kind)
			assert.Equal(t, data, d.Req.Data)
			assert.True(t, d.Req.DueTime.Equal(testNow))
			assert.True(t, d.Req.RejectIfClusterLocked, "the provider must re-check the exclusive-access lease with the insert")

			if tc.reason != "" {
				var payload map[string]any
				err := msgpack.Unmarshal(d.Req.Data, &payload)
				require.NoError(t, err)
				assert.Equal(t, tc.reason, payload["reason"])
			}

			recs := ts.logs.auditRecords(t, "workflowInstance."+string(tc.action))
			require.Len(t, recs, 1)
			assert.Equal(t, "accepted", recs[0]["result"])
			assert.Equal(t, tc.reason, recs[0]["reason"])
			assert.Equal(t, "wf", recs[0]["workflow"])
			assert.Equal(t, "i1", recs[0]["instanceId"])
		})
	}

	t.Run("refused while an exclusive-access lease is held", func(t *testing.T) {
		// The provider refuses the dispatch atomically, and the lease is never read
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusRunning, nil), nil)
		ts.backend.dispatchFn = func(ref.AlarmRef, components.SetAlarmReq) (bool, error) {
			return false, fmt.Errorf("failed to dispatch job: %w", components.ErrClusterLocked)
		}

		e := decodeError(t, ts.do(t, http.MethodPost, base+"suspend", testManagementToken, ""), http.StatusConflict, CodeExclusiveLeaseHeld)
		assert.True(t, e.Retryable)
		assert.Empty(t, e.Details)
	})

	t.Run("dispatch failure", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(journal(t, workflow.StatusRunning, nil), nil)
		ts.backend.dispatchFn = func(ref.AlarmRef, components.SetAlarmReq) (bool, error) {
			return false, ErrHostUnavailable
		}

		decodeError(t, ts.do(t, http.MethodPost, base+"suspend", testManagementToken, ""), http.StatusServiceUnavailable, CodeHostUnavailable)
	})

	t.Run("unknown instance", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, orchRef).Return(nil, components.ErrNoState)

		decodeError(t, ts.do(t, http.MethodPost, base+"cancel", testManagementToken, `{"reason":"x"}`), http.StatusNotFound, CodeNotFound)
		assert.Empty(t, ts.backend.dispatches)
	})
}

func TestListInstances(t *testing.T) {
	t.Run("invalid filters", func(t *testing.T) {
		ts := newTestServer(t)
		for _, q := range []string{"status=bogus", "version=0", "version=x", "createdFrom=yesterday", "createdTo=2026-01-01"} {
			decodeError(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances?"+q, testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
		}
	})

	t.Run("lists instances from their labels", func(t *testing.T) {
		ts := newTestServer(t)
		created := components.FormatWorkflowCreated(testNow)
		ts.provider.EXPECT().ListStates(mock.Anything, components.ListStatesReq{
			ActorType:      orchRef.ActorType,
			WorkflowLabels: &components.WorkflowLabels{Status: "running", Version: 2},
			CreatedFrom:    testNow.Add(-time.Hour),
			Limit:          components.DefaultManagementListLimit,
		}).Return(components.ListStatesRes{States: []components.ActorStateInfo{
			{ActorID: "i1", WorkflowLabels: &components.WorkflowLabels{Status: "running", Version: 2, Created: created}},
			{ActorID: "i2"},
		}}, nil)

		from := testNow.Add(-time.Hour).Format(time.RFC3339)
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances?status=running&version=2&createdFrom="+from, testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		assert.Equal(t, "running", obj(t, items[0])["status"])
		assert.Equal(t, testNow.Format(time.RFC3339), obj(t, items[0])["createdAt"])
		assert.Equal(t, "unknown", obj(t, items[1])["status"])
	})

	t.Run("lists several statuses one after another", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().ListStates(mock.Anything, components.ListStatesReq{
			ActorType:      orchRef.ActorType,
			WorkflowLabels: &components.WorkflowLabels{Status: "running", Parent: "p"},
			Limit:          components.DefaultManagementListLimit,
		}).Return(components.ListStatesRes{States: []components.ActorStateInfo{
			{ActorID: "i2", WorkflowLabels: &components.WorkflowLabels{Status: "running"}},
		}}, nil)
		ts.provider.EXPECT().ListStates(mock.Anything, components.ListStatesReq{
			ActorType:      orchRef.ActorType,
			WorkflowLabels: &components.WorkflowLabels{Status: "failed", Parent: "p"},
			Limit:          components.DefaultManagementListLimit - 1,
		}).Return(components.ListStatesRes{States: []components.ActorStateInfo{
			{ActorID: "i1", WorkflowLabels: &components.WorkflowLabels{Status: "failed"}},
		}, HasMore: true}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/workflows/wf/instances?status=failed&status=running&parent=p", testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		assert.Equal(t, "i2", obj(t, items[0])["instanceId"])
		assert.Equal(t, "i1", obj(t, items[1])["instanceId"])

		var next instancesCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, instancesCursor{Group: 1, After: "i1"}, next)
	})
}

// TestInstanceStatuses checks the statuses listed by hand in isInstanceStatus and countWorkflowInstances against every Status the workflow package declares
func TestInstanceStatuses(t *testing.T) {
	declared := declaredConstants(t, filepath.Join("..", "..", "builtin", "workflow"), "Status")

	// Guard against the parsing silently finding nothing, which would make the checks pass vacuously
	require.Contains(t, declared, string(workflow.StatusPending))
	require.Contains(t, declared, string(workflow.StatusCancelled))

	t.Run("every declared status is a valid filter", func(t *testing.T) {
		for _, s := range declared {
			assert.True(t, isInstanceStatus(workflow.Status(s)), "status %s", s)
		}
		assert.False(t, isInstanceStatus("unknown"))
		assert.False(t, isInstanceStatus(""))
	})

	t.Run("the summary counts every declared status", func(t *testing.T) {
		// Every status has one instance, so each status the summary queries appears in its counts
		prov := components_mocks.NewMockActorProvider(t)
		prov.EXPECT().CountStates(mock.Anything, mock.Anything).Return(1, nil)

		counts, err := countWorkflowInstances(t.Context(), prov, workflow.OrchestratorActorType("wf"))
		require.NoError(t, err)
		assert.ElementsMatch(t, declared, slices.Collect(maps.Keys(counts)))
	})
}
