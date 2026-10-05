package management

import (
	"net/http"
	"slices"
	"strconv"
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

	t.Run("JSON rendering", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil)

		// Read-only tokens hold actors:state:read
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, "counter", res["actorType"])
		assert.Equal(t, "c1", res["actorId"])
		assert.InDelta(t, len(state), res["size"], 0)

		// Every number is rendered as a string by convention, which doesn't make the state lossy
		assert.Equal(t, false, res["lossy"])
		assert.Equal(t, map[string]any{"n": "1152921504606846976", "name": "x"}, res["state"])
	})

	t.Run("JSON rendering of a lossy value", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(mp(b(0x81), fixstr("data"), b(0xc4, 0x01, 0x01)), nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, true, res["lossy"])
		assert.Equal(t, map[string]any{"data": map[string]any{"$binary": "AQ=="}}, res["state"])
	})

	t.Run("JSON rendering preserves key order", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil)

		w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "", "Accept", "application/json")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Contains(t, w.Body.String(), `"state":{"n":"1152921504606846976","name":"x"}`)
	})

	for _, accept := range []string{"application/msgpack", "application/x-msgpack", "application/vnd.msgpack", "text/html, APPLICATION/MSGPACK;q=0.9"} {
		t.Run("raw bytes with Accept "+accept, func(t *testing.T) {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil)

			w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "", "Accept", accept)
			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "application/msgpack", w.Header().Get("Content-Type"))
			assert.Equal(t, strconv.Itoa(len(state)), w.Header().Get("Content-Length"))
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			assert.Equal(t, state, w.Body.Bytes())
		})
	}

	// Each Accept header either prefers the raw bytes or keeps the JSON rendering, depending on the qualities
	negotiation := []struct {
		accept  string
		msgpack bool
	}{
		{accept: "application/msgpack;q=0", msgpack: false},
		{accept: "application/msgpack; q=0.0", msgpack: false},
		{accept: "application/json, application/msgpack;q=0", msgpack: false},
		{accept: "application/json;q=0.9, application/msgpack;q=0.5", msgpack: false},
		{accept: "application/json;q=0.5, application/msgpack;q=0.9", msgpack: true},
		{accept: "application/json, application/msgpack", msgpack: true},
		{accept: "*/*, application/msgpack;q=0.8", msgpack: false},
		{accept: "application/*;q=0.8, application/msgpack;q=0.9", msgpack: true},
		{accept: "application/json;q=0.1, */*, application/msgpack;q=0.5", msgpack: true},
		{accept: "application/msgpack;q=0, application/x-msgpack", msgpack: true},
		{accept: "application/msgpack;q=2", msgpack: false},
		{accept: "application/msgpack;q=NaN", msgpack: false},
		{accept: "application/msgpack;q=high", msgpack: false},
		{accept: "*/*", msgpack: false},
		{accept: "", msgpack: false},
	}
	for _, tc := range negotiation {
		t.Run("negotiates Accept "+tc.accept, func(t *testing.T) {
			ts := newTestServer(t)
			ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(state, nil)

			w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "", "Accept", tc.accept)
			require.Equal(t, http.StatusOK, w.Code)
			if tc.msgpack {
				assert.Equal(t, "application/msgpack", w.Header().Get("Content-Type"))
			} else {
				assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
			}
		})
	}

	t.Run("raw bytes of a value that is not valid MessagePack", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(b(0xc1, 0x00), nil)

		w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, "", "Accept", "application/msgpack")
		require.Equal(t, http.StatusOK, w.Code)
		assert.Equal(t, b(0xc1, 0x00), w.Body.Bytes())
	})

	t.Run("state that is not valid MessagePack", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(b(0xc1, 0x00), nil)

		e := decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusUnprocessableEntity, CodeStateNotDecodable)
		assert.Contains(t, e.Message, "application/msgpack")
	})

	t.Run("state whose map keys collide in JSON", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, aRef).Return(mp(b(0x82, 0x01), fixstr("a"), fixstr("1"), fixstr("b")), nil)

		e := decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusUnprocessableEntity, CodeStateNotDecodable)
		assert.Contains(t, e.Message, `JSON name "1"`)
		assert.Contains(t, e.Message, "application/msgpack")
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
