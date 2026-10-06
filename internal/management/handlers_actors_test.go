package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/italypaleale/francis/builtin/workflow"
	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/protocol"
)

func TestGetActorState(t *testing.T) {
	aRef := ref.NewActorRef("counter", "c1")
	state := mp(b(0x82), fixstr("n"), mp(b(0xcf), be64(1<<60)), fixstr("name"), fixstr("x"))

	for _, accept := range []string{"", "application/msgpack", "application/json", "*/*", "application/msgpack;q=0"} {
		t.Run("raw bytes with Accept "+accept, func(t *testing.T) {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil)

			w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "", "Accept", accept)
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "application/msgpack", w.Header().Get("Content-Type"))
			assert.Equal(t, strconv.Itoa(len(state)), w.Header().Get("Content-Length"))
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			assert.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
			assert.Equal(t, state, w.Body.Bytes())
		})
	}

	t.Run("stored bytes are never decoded", func(t *testing.T) {
		for _, data := range [][]byte{{0xc1, 0x00}, {0xdb, 0xff, 0xff, 0xff, 0xf0}} {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(data, nil)
			w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "")
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, data, w.Body.Bytes())
		}
	})

	t.Run("no state", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(nil, components.ErrNoState)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("invalid path", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/a%2Fb", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("a workflow's actor types also require the data scope", func(t *testing.T) {
		orchestrator := workflow.OrchestratorActorType("orders")
		ts := newTestServer(t)

		// Read-only tokens hold workflows:data:read too, so they read the state
		ts.provider.EXPECT().GetState(mock.Anything, ref.NewActorRef(orchestrator, "i1")).Return(state, nil).Once()
		w := ts.do(t, http.MethodGet, "/api/v1/actor-states/"+orchestrator+"/i1", testReadOnlyToken, "")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, state, w.Body.Bytes())

		// getState calls the handler as a caller that holds actors:state:read but not workflows:data:read, which no configured token is
		getState := func(actorType string, actorID string) (*httptest.ResponseRecorder, *apiError) {
			r := httptest.NewRequest(http.MethodGet, "/api/v1/actor-states/"+actorType+"/"+actorID, nil)
			r.SetPathValue("type", actorType)
			r.SetPathValue("id", actorID)
			r = r.WithContext(context.WithValue(r.Context(), callerCtxKey{}, &Caller{scopes: map[Scope]struct{}{ScopeActorsStateRead: {}}}))
			w := httptest.NewRecorder()
			return w, ts.srv.handleGetActorState(w, r)
		}

		// The state of a workflow's actor is refused before it is read
		_, apiErr := getState(orchestrator, "i1")
		require.NotNil(t, apiErr)
		assert.Equal(t, http.StatusForbidden, apiErr.status)
		assert.Equal(t, CodeForbidden, apiErr.Code)

		// The state of other actors is still readable
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil).Once()
		w, apiErr = getState("counter", "c1")
		require.Nil(t, apiErr)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}

func TestListActorStates(t *testing.T) {
	t.Run("requires the type", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states?type=a/b", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("lists IDs and labels without state", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().ListStates(mock.Anything, components.ListStatesReq{ActorType: "T", After: "a0", Limit: 2}).Return(components.ListStatesRes{
			States: []components.ActorStateInfo{
				{ActorID: "a1"},
				{ActorID: "a2", WorkflowLabels: &components.WorkflowLabels{Status: "running", Version: 2}},
			},
			HasMore: true,
		}, nil)

		cursor := encodeCursor(actorStatesCursor{After: "a0"})
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/actor-states?type=T&limit=2&cursor="+cursor, testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		assert.Equal(t, map[string]any{"actorId": "a1"}, items[0])
		assert.Equal(t, map[string]any{"actorId": "a2", "workflowLabels": map[string]any{"status": "running", "version": float64(2)}}, items[1])

		var next actorStatesCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, "a2", next.After)
	})
}

func TestDeactivateActor(t *testing.T) {
	aRef := ref.NewActorRef("counter", "c1")

	t.Run("actor that is not active", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().LookupActor(mock.Anything, aRef, components.LookupActorOpts{ActiveOnly: true}).Return(components.LookupActorRes{}, components.ErrNoActor)

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""), http.StatusOK)
		assert.Equal(t, map[string]any{"actorType": "counter", "actorId": "c1", "notActive": true}, res)
		assert.Empty(t, ts.backend.deactivates)

		recs := ts.logs.auditRecords(t, "actor.deactivate")
		require.Len(t, recs, 1)
		assert.Equal(t, "accepted", recs[0]["result"])
		assert.Equal(t, "counter", recs[0]["actorType"])
		assert.Equal(t, "c1", recs[0]["actorId"])
	})

	t.Run("host went away since the lookup", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().LookupActor(mock.Anything, aRef, mock.Anything).Return(components.LookupActorRes{HostID: "h1"}, nil)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(components.HostDetails{}, components.ErrHostUnregistered)

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""), http.StatusOK)
		assert.Equal(t, true, res["notActive"])
		assert.Equal(t, "h1", res["hostId"])
		assert.Empty(t, ts.backend.deactivates)
	})

	t.Run("deactivates an active actor", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().LookupActor(mock.Anything, aRef, mock.Anything).Return(components.LookupActorRes{HostID: "h1"}, nil)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "counter"), nil)

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""), http.StatusOK)
		assert.Equal(t, false, res["notActive"])
		assert.Equal(t, "h1", res["hostId"])
		assert.Equal(t, []string{"h1/counter/c1"}, ts.backend.deactivates)
	})

	t.Run("host reports the actor was not active", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().LookupActor(mock.Anything, aRef, mock.Anything).Return(components.LookupActorRes{HostID: "h1"}, nil)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "counter"), nil)
		ts.backend.deactivateFn = func(components.HostDetails, string, string) (bool, error) {
			return true, nil
		}

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""), http.StatusOK)
		assert.Equal(t, true, res["notActive"])
	})

	t.Run("host reattached", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().LookupActor(mock.Anything, aRef, mock.Anything).Return(components.LookupActorRes{HostID: "h1"}, nil)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "counter"), nil)
		ts.backend.deactivateFn = func(components.HostDetails, string, string) (bool, error) {
			return false, ErrHostReattached
		}

		e := decodeError(t, ts.do(t, http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""), http.StatusConflict, CodeHostReattached)
		assert.True(t, e.Retryable)

		recs := ts.logs.auditRecords(t, "actor.deactivate")
		require.Len(t, recs, 1)
		assert.Equal(t, CodeHostReattached, recs[0]["errorCode"])
		assert.InDelta(t, http.StatusConflict, recs[0]["status"], 0)
	})
}

func TestListJobs(t *testing.T) {
	t.Run("invalid status", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs?status=bogus", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("id without type", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs?id=x", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("invalid cursor", func(t *testing.T) {
		ts := newTestServer(t)

		// Job cursors must hold a UUID, so one that does not is rejected before reaching the provider
		cursor := encodeCursor(map[string]string{"a": "not-a-job-id"})
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs?cursor="+cursor, testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("passes the cursor to the provider", func(t *testing.T) {
		ts := newTestServer(t)
		after := uuid.NewV7()
		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{After: after, Limit: components.DefaultManagementListLimit}).
			Return(components.QueryJobsRes{}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/jobs?cursor="+encodeCursor(jobsCursor{After: after}), testReadOnlyToken, ""), http.StatusOK)
		assert.Empty(t, arr(t, res["items"]))
		assert.NotContains(t, res, "nextCursor")
	})

	t.Run("fails when the provider returns a job ID that is not a UUID", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{Limit: components.DefaultManagementListLimit}).
			Return(components.QueryJobsRes{Jobs: []components.JobInfo{{JobID: "not-a-uuid", ActorType: "T", ActorID: "i"}}, HasMore: true}, nil)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs", testReadOnlyToken, ""), http.StatusInternalServerError, CodeInternal)
	})

	t.Run("lists jobs", func(t *testing.T) {
		ts := newTestServer(t)
		lastID := uuid.NewV7()
		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{ActorType: "T", ActorID: "i", Status: components.JobStatusDeadLettered, Limit: components.DefaultManagementListLimit}).
			Return(components.QueryJobsRes{Jobs: []components.JobInfo{
				{JobID: "j1", ActorType: "T", ActorID: "i", Method: "m", Status: components.JobStatusDeadLettered, DueTime: testNow, Attempts: 3, LastError: "boom", EndedAt: testNow},
				{JobID: lastID.String(), ActorType: "T", ActorID: "i", Method: "m", Status: components.JobStatusPending, DueTime: testNow, Attempts: 1, LastError: "hidden"},
			}, HasMore: true}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/jobs?type=T&id=i&status=dead", testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		dead := obj(t, items[0])
		assert.InDelta(t, 3, dead["attempts"], 0)
		assert.Equal(t, "boom", dead["lastError"])
		assert.NotContains(t, dead, "createdAt")
		assert.NotContains(t, dead, "workflow")
		pending := obj(t, items[1])
		assert.NotContains(t, pending, "attempts")
		assert.NotContains(t, pending, "lastError")
		assert.NotContains(t, pending, "endedAt")

		var next jobsCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, lastID, next.After)
	})

	t.Run("lists several statuses one after another", func(t *testing.T) {
		ts := newTestServer(t)
		pendingID := uuid.NewV7()
		deadID := uuid.NewV7()
		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{Status: components.JobStatusPending, Limit: 2}).
			Return(components.QueryJobsRes{Jobs: []components.JobInfo{{JobID: pendingID.String(), Status: components.JobStatusPending}}}, nil)
		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{Status: components.JobStatusDeadLettered, Limit: 1}).
			Return(components.QueryJobsRes{Jobs: []components.JobInfo{{JobID: deadID.String(), Status: components.JobStatusDeadLettered}}, HasMore: true}, nil)

		// The statuses are listed in their lifecycle order, whatever order the request gives them in
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/jobs?status=dead&status=pending&limit=2", testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		assert.Equal(t, "pending", obj(t, items[0])["status"])
		assert.Equal(t, "dead", obj(t, items[1])["status"])

		// The cursor continues in the second status
		var next jobsCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, jobsCursor{Group: 1, After: deadID}, next)

		ts.provider.EXPECT().QueryJobs(mock.Anything, components.QueryJobsReq{Status: components.JobStatusDeadLettered, After: deadID, Limit: 2}).
			Return(components.QueryJobsRes{}, nil)
		res = decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/jobs?status=dead&status=pending&limit=2&cursor="+str(t, res["nextCursor"]), testReadOnlyToken, ""), http.StatusOK)
		assert.Empty(t, arr(t, res["items"]))
		assert.NotContains(t, res, "nextCursor")
	})

	t.Run("lists several types and IDs as combinations", func(t *testing.T) {
		ts := newTestServer(t)
		for _, req := range []components.QueryJobsReq{
			{ActorType: "A", ActorID: "x", Limit: components.DefaultManagementListLimit},
			{ActorType: "A", ActorID: "y", Limit: components.DefaultManagementListLimit},
			{ActorType: "B", ActorID: "x", Limit: components.DefaultManagementListLimit},
			{ActorType: "B", ActorID: "y", Limit: components.DefaultManagementListLimit},
		} {
			ts.provider.EXPECT().QueryJobs(mock.Anything, req).Return(components.QueryJobsRes{}, nil)
		}

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/jobs?type=B&type=A&id=y&id=x", testReadOnlyToken, ""), http.StatusOK)
		assert.Empty(t, arr(t, res["items"]))
		assert.NotContains(t, res, "nextCursor")
	})

	t.Run("an id needs a type", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs?id=x&id=y&status=dead", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("a cursor past the statuses is invalid", func(t *testing.T) {
		ts := newTestServer(t)
		cursor := encodeCursor(jobsCursor{Group: 1})
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs?status=dead&cursor="+cursor, testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("unknown job", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetJob(mock.Anything, "nope").Return(components.JobInfo{}, components.ErrNoJob)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/jobs/nope", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})
}

func TestNewWorkflowLink(t *testing.T) {
	prefix := workflow.ActorTypePrefix
	idx := func(i int) *int {
		return &i
	}

	tests := []struct {
		name      string
		actorType string
		actorID   string
		want      *workflowLinkJSON
	}{
		{name: "not a workflow actor", actorType: "counter", actorID: "c1"},
		{name: "orchestrator", actorType: prefix + "order", actorID: "i1", want: &workflowLinkJSON{Workflow: "order", Role: "orchestrator", InstanceID: "i1"}},
		{name: "worker", actorType: prefix + "order.worker", actorID: "i1|charge|0", want: &workflowLinkJSON{Workflow: "order", Role: "worker", InstanceID: "i1", Step: "charge", TaskIndex: idx(0)}},
		{name: "worker with an instance ID containing the separator", actorType: prefix + "order.worker.gpu", actorID: "a|b|charge|12", want: &workflowLinkJSON{Workflow: "order", Role: "worker", Capability: "gpu", InstanceID: "a|b", Step: "charge", TaskIndex: idx(12)}},
		{name: "worker with a malformed ID", actorType: prefix + "order.worker", actorID: "i1|charge|x", want: &workflowLinkJSON{Workflow: "order", Role: "worker"}},
		{name: "worker with a short ID", actorType: prefix + "order.worker", actorID: "i1", want: &workflowLinkJSON{Workflow: "order", Role: "worker"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, newWorkflowLink(tc.actorType, tc.actorID))
		})
	}
}

// encodeMsgpack encodes a value for a test fixture
func encodeMsgpack(t *testing.T, v any) []byte {
	t.Helper()
	data, err := msgpack.Marshal(v)
	require.NoError(t, err)
	return data
}

func TestHostActivations(t *testing.T) {
	// The host holds one activation of A and three of C, and pages through each type with the index of the next activation as its cursor
	held := map[string][]string{"A": {"a1"}, "C": {"c1", "c2", "c3"}}
	newServer := func(t *testing.T) (*testServer, *[]protocol.HostSnapshotRequest) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "A", "B", "C"), nil)

		var reqs []protocol.HostSnapshotRequest
		ts.backend.snapshotFn = func(_ components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			reqs = append(reqs, req)
			ids := held[req.ActorType]
			res := protocol.HostSnapshotResponse{HostID: "h1", ObservedAtUnixMs: testNow.UnixMilli(), ActiveCount: len(ids)}
			if req.SkipActivations {
				return res, nil
			}

			start, _ := strconv.Atoi(req.After)
			end := min(start+req.Limit, len(ids))
			for _, id := range ids[start:end] {
				res.Activations = append(res.Activations, protocol.ActivationInfo{ActorType: req.ActorType, ActorID: id})
			}
			if end < len(ids) {
				res.Next = strconv.Itoa(end)
			}
			return res, nil
		}
		return ts, &reqs
	}
	ids := func(t *testing.T, res map[string]any) []string {
		items := arr(t, res["items"])
		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, str(t, obj(t, item)["actorId"]))
		}
		return out
	}

	t.Run("lists several types one after another, counting all of them", func(t *testing.T) {
		ts, _ := newServer(t)
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts/h1/activations?type=C&type=A&limit=2", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, []string{"a1", "c1"}, ids(t, res))
		assert.InDelta(t, 4, res["activeCount"], 0)

		var next hostActivationsCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, hostActivationsCursor{Group: 1, After: "1"}, next)
	})

	t.Run("a later page counts the types it doesn't list", func(t *testing.T) {
		ts, reqs := newServer(t)
		cursor := encodeCursor(hostActivationsCursor{Group: 1, After: "1"})
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts/h1/activations?type=C&type=A&limit=2&cursor="+cursor, testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, []string{"c2", "c3"}, ids(t, res))
		assert.InDelta(t, 4, res["activeCount"], 0)
		assert.NotContains(t, res, "nextCursor")
		assert.Contains(t, *reqs, protocol.HostSnapshotRequest{ActorType: "A", SkipActivations: true})
	})

	t.Run("a single type takes one request", func(t *testing.T) {
		ts, reqs := newServer(t)
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts/h1/activations?type=C", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, []string{"c1", "c2", "c3"}, ids(t, res))
		assert.InDelta(t, 3, res["activeCount"], 0)
		assert.Len(t, *reqs, 1)
	})
}

func TestListActivationsSeveralTypes(t *testing.T) {
	// Each simulated host holds activations of several types, and pages each type the way actorcore.Manager.Snapshot does
	held := map[string]map[string][]string{
		"h1": {"A": {"a1"}, "B": {"b1", "b2"}},
		"h2": {"B": {"b3"}},
		"h3": {"A": {"a2", "a3"}, "C": {"c1"}},
	}
	hosts := []components.HostDetails{testHost("h1", false, "A", "B"), testHost("h2", false, "B"), testHost("h3", false, "A", "C")}

	setup := func(t *testing.T, concurrency int, failing ...string) (*testServer, *atomic.Int32) {
		ts := newTestServer(t)
		ts.srv.fanOutConcurrency = concurrency
		ts.provider.EXPECT().ListHostDetails(mock.Anything, mock.Anything).Return(components.ListHostDetailsRes{Hosts: hosts}, nil)
		var calls atomic.Int32
		ts.backend.snapshotFn = func(h components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			calls.Add(1)
			if slices.Contains(failing, h.HostID) {
				return protocol.HostSnapshotResponse{}, ErrHostUnavailable
			}
			res := protocol.HostSnapshotResponse{HostID: h.HostID, ObservedAtUnixMs: testNow.UnixMilli()}
			for _, id := range held[h.HostID][req.ActorType] {
				key := req.ActorType + "/" + id
				if req.After != "" && key <= req.After {
					continue
				}
				if len(res.Activations) == req.Limit {
					last := res.Activations[len(res.Activations)-1]
					res.Next = last.ActorType + "/" + last.ActorID
					break
				}
				res.Activations = append(res.Activations, protocol.ActivationInfo{ActorType: req.ActorType, ActorID: id, ActivatedAtUnixMs: testNow.UnixMilli()})
			}
			return res, nil
		}
		return ts, &calls
	}

	// collect follows the cursors to the last page, returning the activations as host/type/ID
	collect := func(t *testing.T, ts *testServer, query string, limit int) []string {
		var (
			all    []string
			cursor string
		)
		for range 20 {
			target := "/api/v1/activations?" + query + "&limit=" + strconv.Itoa(limit)
			if cursor != "" {
				target += "&cursor=" + cursor
			}
			res := decodeJSON(t, ts.do(t, http.MethodGet, target, testReadOnlyToken, ""), http.StatusOK)
			for _, it := range arr(t, res["items"]) {
				item := obj(t, it)
				all = append(all, str(t, item["hostId"])+"/"+str(t, item["actorType"])+"/"+str(t, item["actorId"]))
			}
			cursor, _ = res["nextCursor"].(string)
			if cursor == "" {
				return all
			}
		}
		require.Fail(t, "pagination does not terminate")
		return nil
	}

	for _, tc := range [][2]int{{1, 1}, {1, 2}, {2, 2}, {16, 2}, {2, 3}, {16, 5}, {16, 100}} {
		concurrency, limit := tc[0], tc[1]
		t.Run("pages across hosts and types with concurrency "+strconv.Itoa(concurrency)+" and limit "+strconv.Itoa(limit), func(t *testing.T) {
			ts, _ := setup(t, concurrency)
			all := collect(t, ts, "type=B&type=A", limit)
			assert.Equal(t, []string{"h1/A/a1", "h1/B/b1", "h1/B/b2", "h2/B/b3", "h3/A/a2", "h3/A/a3"}, all)
		})
	}

	t.Run("several hosts", func(t *testing.T) {
		ts, _ := setup(t, 16)
		all := collect(t, ts, "type=A&host=h3&host=h1", 100)
		assert.Equal(t, []string{"h1/A/a1", "h3/A/a2", "h3/A/a3"}, all)
	})

	t.Run("reports a host that cannot be queried once", func(t *testing.T) {
		ts, calls := setup(t, 16, "h2")
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/activations?type=A&type=B", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, true, res["partial"])
		require.Len(t, arr(t, res["errors"]), 1)
		assert.EqualValues(t, 6, calls.Load())
	})

	t.Run("none of the hosts is registered", func(t *testing.T) {
		ts, _ := setup(t, 16)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/activations?host=x&host=y", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})
}

func TestListPlacementsSeveralValues(t *testing.T) {
	ts := newTestServer(t)
	ts.provider.EXPECT().ListPlacements(mock.Anything, components.ListPlacementsReq{ActorType: "A", HostID: "h1", Limit: 2}).
		Return(components.ListPlacementsRes{Placements: []components.PlacementInfo{{ActorType: "A", ActorID: "a1", HostID: "h1"}}}, nil)
	ts.provider.EXPECT().ListPlacements(mock.Anything, components.ListPlacementsReq{ActorType: "A", HostID: "h2", Limit: 1}).
		Return(components.ListPlacementsRes{Placements: []components.PlacementInfo{{ActorType: "A", ActorID: "a2", HostID: "h2"}}, HasMore: true}, nil)

	// The combinations are listed by type and then host, whatever order the request gives them in
	res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/placements?host=h2&host=h1&type=B&type=A&limit=2", testReadOnlyToken, ""), http.StatusOK)
	items := arr(t, res["items"])
	require.Len(t, items, 2)
	assert.Equal(t, "a1", obj(t, items[0])["actorId"])
	assert.Equal(t, "a2", obj(t, items[1])["actorId"])

	var next placementsCursor
	require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
	assert.Equal(t, placementsCursor{Group: 1, Type: "A", ID: "a2"}, next)
}

func TestListAlarmsSeveralValues(t *testing.T) {
	t.Run("an id needs a type", func(t *testing.T) {
		ts := newTestServer(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/alarms?id=x&id=y", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("lists the combinations by type and then ID", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().ListAlarms(mock.Anything, components.ListAlarmsReq{ActorType: "A", ActorID: "x", Limit: components.DefaultManagementListLimit}).
			Return(components.ListAlarmsRes{Alarms: []components.AlarmInfo{{ActorType: "A", ActorID: "x", Name: "n1"}}}, nil)
		ts.provider.EXPECT().ListAlarms(mock.Anything, components.ListAlarmsReq{ActorType: "A", ActorID: "y", Limit: components.DefaultManagementListLimit - 1}).
			Return(components.ListAlarmsRes{}, nil)
		ts.provider.EXPECT().ListAlarms(mock.Anything, components.ListAlarmsReq{ActorType: "B", ActorID: "x", Limit: components.DefaultManagementListLimit - 1}).
			Return(components.ListAlarmsRes{Alarms: []components.AlarmInfo{{ActorType: "B", ActorID: "x", Name: "n2"}}}, nil)
		ts.provider.EXPECT().ListAlarms(mock.Anything, components.ListAlarmsReq{ActorType: "B", ActorID: "y", Limit: components.DefaultManagementListLimit - 2}).
			Return(components.ListAlarmsRes{}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/alarms?type=B&type=A&id=y&id=x", testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 2)
		assert.Equal(t, "n1", obj(t, items[0])["name"])
		assert.Equal(t, "n2", obj(t, items[1])["name"])
		assert.NotContains(t, res, "nextCursor")
	})
}

func TestListActivations(t *testing.T) {
	// Each simulated host holds activations of type T with the listed IDs, and pages them the way actorcore.Manager.Snapshot does
	activations := map[string][]string{
		"h1": {"a1", "a2"},
		"h2": {"b1", "b2", "b3"},
		"h3": {"c1"},
	}
	hosts := []components.HostDetails{testHost("h1", false, "T"), testHost("h2", false, "T"), testHost("h3", false, "T")}

	setup := func(t *testing.T, concurrency int, failing ...string) *testServer {
		ts := newTestServer(t)
		ts.srv.fanOutConcurrency = concurrency
		ts.provider.EXPECT().ListHostDetails(mock.Anything, mock.Anything).Return(components.ListHostDetailsRes{Hosts: hosts}, nil)
		ts.backend.snapshotFn = func(h components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			if slices.Contains(failing, h.HostID) {
				return protocol.HostSnapshotResponse{}, ErrHostUnavailable
			}
			res := protocol.HostSnapshotResponse{HostID: h.HostID, ObservedAtUnixMs: testNow.UnixMilli(), ActiveCount: len(activations[h.HostID])}
			for _, id := range activations[h.HostID] {
				if req.After != "" && "T/"+id <= req.After {
					continue
				}
				if len(res.Activations) == req.Limit {
					res.Next = "T/" + res.Activations[len(res.Activations)-1].ActorID
					break
				}
				res.Activations = append(res.Activations, protocol.ActivationInfo{ActorType: "T", ActorID: id, ActivatedAtUnixMs: testNow.UnixMilli()})
			}
			return res, nil
		}
		return ts
	}

	// ids returns the IDs of the activations in a page, prefixed by their host
	ids := func(t *testing.T, res map[string]any) []string {
		items := arr(t, res["items"])
		out := make([]string, 0, len(items))
		for _, it := range items {
			item := obj(t, it)
			out = append(out, str(t, item["hostId"])+"/"+str(t, item["actorId"]))
		}
		return out
	}

	for _, tc := range [][2]int{{1, 1}, {1, 2}, {2, 2}, {16, 2}, {1, 3}, {2, 3}, {16, 3}, {16, 5}, {1, 6}, {16, 100}} {
		concurrency, limit := tc[0], tc[1]
		t.Run("pages across hosts with concurrency "+strconv.Itoa(concurrency)+" and limit "+strconv.Itoa(limit), func(t *testing.T) {
			ts := setup(t, concurrency)

			var (
				all    []string
				cursor string
				pages  int
			)
			for {
				target := "/api/v1/activations?limit=" + strconv.Itoa(limit)
				if cursor != "" {
					target += "&cursor=" + cursor
				}
				res := decodeJSON(t, ts.do(t, http.MethodGet, target, testReadOnlyToken, ""), http.StatusOK)
				assert.Equal(t, false, res["partial"])
				page := ids(t, res)
				assert.LessOrEqual(t, len(page), limit)
				all = append(all, page...)
				pages++
				require.Less(t, pages, 10, "pagination does not terminate")

				next, _ := res["nextCursor"].(string)
				if next == "" {
					break
				}
				cursor = next
			}
			assert.Equal(t, []string{"h1/a1", "h1/a2", "h2/b1", "h2/b2", "h2/b3", "h3/c1"}, all)
		})
	}

	t.Run("host that returns a short page with more activations", func(t *testing.T) {
		// The hosts cap their pages below the requested limit, as they do at protocol.MaxSnapshotPageSize, so a page ends early and continues from the same host
		ts := setup(t, 16)
		full := ts.backend.snapshotFn
		ts.backend.snapshotFn = func(h components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			req.Limit = min(req.Limit, 1)
			return full(h, req)
		}

		var (
			all    []string
			cursor string
		)
		for range 10 {
			target := "/api/v1/activations?limit=100"
			if cursor != "" {
				target += "&cursor=" + cursor
			}
			res := decodeJSON(t, ts.do(t, http.MethodGet, target, testReadOnlyToken, ""), http.StatusOK)
			all = append(all, ids(t, res)...)
			cursor, _ = res["nextCursor"].(string)
			if cursor == "" {
				break
			}
		}
		assert.Equal(t, []string{"h1/a1", "h1/a2", "h2/b1", "h2/b2", "h2/b3", "h3/c1"}, all)
	})

	t.Run("reports hosts that cannot be queried", func(t *testing.T) {
		ts := setup(t, 16, "h2")

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/activations", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, true, res["partial"])
		assert.Equal(t, []string{"h1/a1", "h1/a2", "h3/c1"}, ids(t, res))
		errs := arr(t, res["errors"])
		require.Len(t, errs, 1)
		assert.Equal(t, "h2", obj(t, errs[0])["hostId"])
		assert.Equal(t, CodeHostUnavailable, obj(t, errs[0])["code"])
	})

	t.Run("no host can be queried", func(t *testing.T) {
		ts := setup(t, 16, "h1", "h2", "h3")

		e := decodeError(t, ts.do(t, http.MethodGet, "/api/v1/activations", testReadOnlyToken, ""), http.StatusServiceUnavailable, CodeNoHostsReachable)
		assert.True(t, e.Retryable)
		assert.Len(t, e.Details["errors"], 3)
	})

	t.Run("unknown host filter", func(t *testing.T) {
		ts := setup(t, 16)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/activations?host=nope", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("host filter", func(t *testing.T) {
		ts := setup(t, 16)
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/activations?host=h2", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, []string{"h2/b1", "h2/b2", "h2/b3"}, ids(t, res))
		assert.NotContains(t, res, "nextCursor")
	})
}
