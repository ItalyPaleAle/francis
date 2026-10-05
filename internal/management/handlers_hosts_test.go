package management

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

func TestListHosts(t *testing.T) {
	hosts := []components.HostDetails{
		testHost("h1", false, "A"),
		testHost("h2", true, "A", "B"),
		testHost("h3", false),
	}
	hosts[0].RuntimeID = "rt-registered"
	hosts[0].ActorTypes[0].ActiveCount = 3

	setup := func(t *testing.T) *testServer {
		ts := newTestServer(t)
		ts.backend.reachability["h1"] = HostReachability{Reachable: true, OwnerRuntimeID: "rt-live"}
		ts.backend.reachability["h2"] = HostReachability{Reachable: true}
		return ts
	}

	t.Run("lists every host with its state", func(t *testing.T) {
		ts := setup(t)
		ts.provider.EXPECT().ListHostDetails(mock.Anything, components.ListHostDetailsReq{Limit: components.DefaultManagementListLimit}).
			Return(components.ListHostDetailsRes{Hosts: hosts}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts", testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 3)
		assert.NotContains(t, res, "nextCursor")

		h1 := obj(t, items[0])
		assert.Equal(t, "h1", h1["hostId"])
		assert.Equal(t, HostStateConnected, h1["state"])
		assert.Equal(t, "rt-live", h1["ownerRuntimeId"])
		assert.InDelta(t, 3, h1["placementCount"], 0)
		assert.InDelta(t, 1, h1["actorTypeCount"], 0)
		assert.Equal(t, HostStateDraining, obj(t, items[1])["state"])
		assert.Equal(t, HostStateUnreachable, obj(t, items[2])["state"])
	})

	t.Run("filters by state and returns a cursor", func(t *testing.T) {
		ts := setup(t)
		ts.provider.EXPECT().ListHostDetails(mock.Anything, components.ListHostDetailsReq{After: "h0", Limit: 3}).
			Return(components.ListHostDetailsRes{Hosts: hosts, HasMore: true}, nil)

		cursor := encodeCursor(hostsCursor{After: "h0"})
		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts?state=unreachable&limit=3&cursor="+cursor, testReadOnlyToken, ""), http.StatusOK)
		items := arr(t, res["items"])
		require.Len(t, items, 1)
		assert.Equal(t, "h3", obj(t, items[0])["hostId"])

		var next hostsCursor
		require.Nil(t, decodeCursor(str(t, res["nextCursor"]), &next))
		assert.Equal(t, "h3", next.After)
	})

	t.Run("invalid state", func(t *testing.T) {
		ts := setup(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/hosts?state=bogus", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("invalid cursor", func(t *testing.T) {
		ts := setup(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/hosts?cursor=%21%21", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})

	t.Run("invalid limit", func(t *testing.T) {
		ts := setup(t)
		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/hosts?limit=1001", testReadOnlyToken, ""), http.StatusBadRequest, CodeBadRequest)
	})
}

func TestGetHost(t *testing.T) {
	t.Run("unknown host", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "nope").Return(components.HostDetails{}, components.ErrHostUnregistered)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/hosts/nope", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("host with its runtime report", func(t *testing.T) {
		ts := newTestServer(t)
		h := testHost("h1", false, "A", "B")
		h.ActorTypes[0].ConcurrencyLimit = 5
		h.ActorTypes[0].ActiveCount = 7
		ts.backend.reachability["h1"] = HostReachability{Reachable: true}
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(h, nil)

		var gotReq protocol.HostSnapshotRequest
		ts.backend.snapshotFn = func(_ components.HostDetails, req protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			gotReq = req
			return protocol.HostSnapshotResponse{
				HostID:           "h1",
				ObservedAtUnixMs: testNow.Add(-time.Second).UnixMilli(),
				ActiveCount:      7,
				CapacityGroups:   []protocol.CapacityGroupInfo{{Name: "g", Limit: 10, InUse: 2}},
			}, nil
		}

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts/h1", testReadOnlyToken, ""), http.StatusOK)
		assert.True(t, gotReq.SkipActivations)
		assert.Equal(t, "h1", res["hostId"])
		assert.Equal(t, HostStateConnected, res["state"])
		assert.Equal(t, testNow.Format(time.RFC3339), res["observedAt"])
		assert.NotContains(t, res, "runtimeError")

		types := arr(t, res["actorTypes"])
		require.Len(t, types, 2)
		limited := obj(t, obj(t, types[0])["placements"])
		assert.InDelta(t, 5, limited["limit"], 0)
		assert.InDelta(t, 0, limited["available"], 0, "available never goes negative")
		assert.Equal(t, false, limited["unlimited"])
		unlimited := obj(t, obj(t, types[1])["placements"])
		assert.Nil(t, unlimited["limit"])
		assert.Equal(t, true, unlimited["unlimited"])

		rt := obj(t, res["runtime"])
		assert.InDelta(t, 7, rt["activeCount"], 0)
		groups := arr(t, rt["capacityGroups"])
		require.Len(t, groups, 1)
		assert.Equal(t, []any{}, obj(t, groups[0])["actorTypes"])
	})

	t.Run("host that cannot be queried", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "A"), nil)
		ts.backend.snapshotFn = func(components.HostDetails, protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			return protocol.HostSnapshotResponse{}, ErrHostUnavailable
		}

		res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/hosts/h1", testReadOnlyToken, ""), http.StatusOK)
		assert.Equal(t, HostStateUnreachable, res["state"])
		assert.NotContains(t, res, "runtime")
		rtErr := obj(t, res["runtimeError"])
		assert.Equal(t, CodeHostUnavailable, rtErr["code"])
		assert.Equal(t, "h1", rtErr["hostId"])
	})
}

func TestListRuntimes(t *testing.T) {
	ts := newTestServer(t)
	n := 2
	ts.backend.runtimes = []RuntimeStatus{
		{RuntimeInfo: components.RuntimeInfo{RuntimeID: "rt-b"}, Error: "unreachable"},
		{RuntimeInfo: components.RuntimeInfo{RuntimeID: "rt-a"}, Self: true, ConnectedHosts: &n},
	}

	res := decodeJSON(t, ts.do(t, http.MethodGet, "/api/v1/runtimes", testReadOnlyToken, ""), http.StatusOK)
	items := arr(t, res["items"])
	require.Len(t, items, 2)
	a := obj(t, items[0])
	assert.Equal(t, "rt-a", a["runtimeId"])
	assert.Equal(t, true, a["self"])
	assert.InDelta(t, 2, a["connectedHosts"], 0)
	b := obj(t, items[1])
	assert.Nil(t, b["connectedHosts"])
	assert.Equal(t, "unreachable", b["error"])
}

func TestDrainHost(t *testing.T) {
	// markDraining expects the provider to be asked to mark h1 draining, reporting the given result
	markDraining := func(ts *testServer, force bool, res components.MarkHostDrainingRes, err error) {
		ts.provider.EXPECT().MarkHostDraining(mock.Anything, components.MarkHostDrainingReq{HostID: "h1", Force: force}).Return(res, err).Once()
	}

	t.Run("refuses to drain the last server without force", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "A", "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{LastServerOf: []string{"A"}}, nil)

		w := ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, `{"reason":"maintenance"}`)
		e := decodeError(t, w, http.StatusConflict, CodeLastServer)
		assert.Equal(t, []any{"A"}, e.Details["actorTypes"])
		assert.Empty(t, ts.backend.drains)

		// The failed action is audited with its error code
		recs := ts.logs.auditRecords(t, "host.drain")
		require.Len(t, recs, 1)
		assert.Equal(t, "failed", recs[0]["result"])
		assert.Equal(t, CodeLastServer, recs[0]["errorCode"])
		assert.Equal(t, "WARN", recs[0]["level"])
	})

	t.Run("drains the last server with force", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "A", "B"), nil)
		markDraining(ts, true, components.MarkHostDrainingRes{LastServerOf: []string{"A"}}, nil)

		w := ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, `{"force":true,"timeout":"45s","reason":"maintenance"}`)
		res := decodeJSON(t, w, http.StatusAccepted)
		assert.Equal(t, "h1", res["hostId"])
		assert.Equal(t, false, res["alreadyDraining"])
		assert.Equal(t, []any{"A"}, res["forcedActorTypes"])

		require.Len(t, ts.backend.drains, 1)
		assert.Equal(t, "h1", ts.backend.drains[0].HostID)
		assert.Equal(t, int64(45000), ts.backend.drains[0].Req.TimeoutMs)
		assert.Equal(t, "maintenance", ts.backend.drains[0].Req.Reason)

		recs := ts.logs.auditRecords(t, "host.drain")
		require.Len(t, recs, 1)
		assert.Equal(t, "accepted", recs[0]["result"])
		assert.Equal(t, "maintenance", recs[0]["reason"])
		assert.Equal(t, true, recs[0]["force"])
		assert.Equal(t, "h1", recs[0]["hostId"])
	})

	t.Run("drains a host whose types are served elsewhere", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, nil)

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusAccepted)
		assert.NotContains(t, res, "forcedActorTypes")
		require.Len(t, ts.backend.drains, 1)
		assert.Zero(t, ts.backend.drains[0].Req.TimeoutMs)
	})

	t.Run("repeating a drain is never refused", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", true, "A"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{AlreadyDraining: true}, nil)
		ts.backend.drainFn = func(components.HostDetails, protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
			return protocol.HostDrainResponse{AlreadyDraining: true}, nil
		}

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, "{}"), http.StatusAccepted)
		assert.Equal(t, true, res["alreadyDraining"])
	})

	t.Run("refused while an exclusive-access lease is held", func(t *testing.T) {
		// The provider refuses the mark atomically, and the lease is never read
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, fmt.Errorf("wrapped: %w", components.ErrClusterLocked))

		e := decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusConflict, CodeExclusiveLeaseHeld)
		assert.True(t, e.Retryable)
		assert.Empty(t, e.Details)
		assert.Empty(t, ts.backend.drains)
	})

	t.Run("unknown host", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "nope").Return(components.HostDetails{}, components.ErrHostUnregistered)

		decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/nope/drain", testManagementToken, ""), http.StatusNotFound, CodeNotFound)
	})

	t.Run("host unregistered while marking it draining", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, components.ErrHostUnregistered)

		decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusNotFound, CodeNotFound)
		assert.Empty(t, ts.backend.drains)
	})

	// failDrain makes the drain request itself fail, as when the host or its runtime could not be reached
	failDrain := func(ts *testServer) {
		ts.backend.drainFn = func(components.HostDetails, protocol.HostDrainRequest) (protocol.HostDrainResponse, error) {
			return protocol.HostDrainResponse{}, ErrHostUnavailable
		}
	}

	t.Run("a host that did not accept the drain is put back into service", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, nil)
		failDrain(ts)

		// The host answers that it is not draining, so the mark is removed
		ts.provider.EXPECT().ClearHostDraining(mock.Anything, "h1").Return(nil).Once()

		e := decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusServiceUnavailable, CodeHostUnavailable)
		assert.True(t, e.Retryable)
		assert.Equal(t, false, e.Details["hostDraining"])
	})

	t.Run("a drain whose acknowledgement was lost is accepted", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, nil)
		failDrain(ts)

		// The host answers that it is draining, so the drain took effect and its mark stays
		ts.backend.snapshotFn = func(host components.HostDetails, _ protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			return protocol.HostSnapshotResponse{HostID: host.HostID, Draining: true}, nil
		}

		res := decodeJSON(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusAccepted)
		assert.Equal(t, "h1", res["hostId"])
	})

	t.Run("a host that can't be asked keeps its mark", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, nil)
		failDrain(ts)

		// The host may have accepted the drain, so the mark is not removed
		ts.backend.snapshotFn = func(components.HostDetails, protocol.HostSnapshotRequest) (protocol.HostSnapshotResponse, error) {
			return protocol.HostSnapshotResponse{}, ErrHostUnavailable
		}

		e := decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusServiceUnavailable, CodeHostUnavailable)
		assert.True(t, e.Retryable)
		assert.Equal(t, true, e.Details["hostDraining"])
	})

	t.Run("a mark that can't be removed is reported", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetHostDetails(mock.Anything, "h1").Return(testHost("h1", false, "B"), nil)
		markDraining(ts, false, components.MarkHostDrainingRes{}, nil)
		failDrain(ts)
		ts.provider.EXPECT().ClearHostDraining(mock.Anything, "h1").Return(errors.New("database is down")).Once()

		e := decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, ""), http.StatusServiceUnavailable, CodeHostUnavailable)
		assert.Equal(t, true, e.Details["hostDraining"])
	})

	invalid := map[string]string{
		"invalid timeout":         `{"timeout":"soon"}`,
		"negative timeout":        `{"timeout":"-1s"}`,
		"zero timeout":            `{"timeout":"0s"}`,
		"timeout over the max":    `{"timeout":"5m1s"}`,
		"unknown body field":      `{"force":true,"bogus":1}`,
		"malformed body":          `{"force":`,
		"wrong field type":        `{"force":"yes"}`,
		"reason over the max len": `{"reason":"` + strings.Repeat("x", maxReasonLength+1) + `"}`,
	}
	for name, body := range invalid {
		t.Run(name, func(t *testing.T) {
			// The mock provider has no expectations, so validation must happen before any read
			ts := newTestServer(t)
			decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, body), http.StatusBadRequest, CodeBadRequest)
			assert.Empty(t, ts.backend.drains)

			recs := ts.logs.auditRecords(t, "host.drain")
			require.Len(t, recs, 1)
			assert.Equal(t, CodeBadRequest, recs[0]["errorCode"])
		})
	}

	t.Run("body too large", func(t *testing.T) {
		ts := newTestServer(t)
		body := `{"reason":"` + strings.Repeat("x", maxRequestBodySize) + `"}`
		decodeError(t, ts.do(t, http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, body), http.StatusRequestEntityTooLarge, CodePayloadTooLarge)
	})
}
