package management

import (
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
)

func TestAuditActorStateRead(t *testing.T) {
	for _, token := range []string{testReadOnlyToken, testManagementToken} {
		t.Run(tokenSuffix(token), func(t *testing.T) {
			ts := newTestServer(t)
			secret := "super-secret-state-value"
			ts.provider.EXPECT().GetState(mock.Anything, ref.NewActorRef("counter", "c1")).Return(fixstr(secret[:20]), nil)

			w := ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", token, "")
			require.Equal(t, http.StatusOK, w.Code)

			recs := ts.logs.auditRecords(t, "actorState.read")
			require.Len(t, recs, 1)
			rec := recs[0]
			assert.Equal(t, "INFO", rec["level"])
			assert.Equal(t, "Management API sensitive read", rec["msg"])
			assert.Equal(t, tokenSuffix(token), rec["tokenSuffix"])
			assert.Len(t, rec["tokenSuffix"], tokenSuffixLength)
			assert.Equal(t, "counter", rec["actorType"])
			assert.Equal(t, "c1", rec["actorId"])
			assert.Equal(t, "192.0.2.10:40000", rec["remoteAddr"])
			assert.Equal(t, w.Header().Get("X-Request-Id"), rec["requestId"])

			// Neither the token nor the state is ever logged
			logs := ts.logs.String()
			assert.NotContains(t, logs, token)
			assert.NotContains(t, logs, token[:len(token)-tokenSuffixLength])
			assert.NotContains(t, logs, secret[:20])
		})
	}

	t.Run("read is audited even when there is no state", func(t *testing.T) {
		ts := newTestServer(t)
		ts.provider.EXPECT().GetState(mock.Anything, mock.Anything).Return(nil, components.ErrNoState)

		decodeError(t, ts.do(t, http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""), http.StatusNotFound, CodeNotFound)
		assert.Len(t, ts.logs.auditRecords(t, "actorState.read"), 1)
	})
}

func TestAuditNeverLogsTokens(t *testing.T) {
	ts := newTestServer(t)
	ts.noLease()
	ts.provider.EXPECT().GetState(mock.Anything, mock.Anything).Return(fixstr("x"), nil).Maybe()
	ts.provider.EXPECT().LookupActor(mock.Anything, mock.Anything, mock.Anything).Return(components.LookupActorRes{}, components.ErrNoActor).Maybe()
	ts.provider.EXPECT().GetHostDetails(mock.Anything, mock.Anything).Return(components.HostDetails{}, components.ErrHostUnregistered).Maybe()

	// Exercise successful, failed, unauthorized and forbidden requests with both tokens
	requests := []struct {
		method string
		path   string
		token  string
		body   string
	}{
		{http.MethodGet, "/api/v1/actor-states/counter/c1", testReadOnlyToken, ""},
		{http.MethodGet, "/api/v1/actor-states/counter/c1", testManagementToken, ""},
		{http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testManagementToken, ""},
		{http.MethodPost, "/api/v1/actors/counter/c1/deactivate", testReadOnlyToken, ""},
		{http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, `{"reason":"r"}`},
		{http.MethodPost, "/api/v1/hosts/h1/drain", testManagementToken, `{"timeout":"bad"}`},
		{http.MethodPost, "/api/v1/workflows/wf/instances/i1/cancel", testManagementToken, `{}`},
		{http.MethodGet, "/api/v1/hosts?token=" + testManagementToken, "", ""},
	}
	for _, rq := range requests {
		ts.do(t, rq.method, rq.path, rq.token, rq.body)
	}

	logs := ts.logs.String()
	require.NotEmpty(t, logs)
	assert.NotContains(t, logs, testReadOnlyToken)
	assert.NotContains(t, logs, testManagementToken)

	// Every audit record identifies the caller by the token suffix only
	var audits int
	for _, rec := range ts.logs.records(t) {
		if rec["audit"] != "management" {
			continue
		}
		audits++
		suffix, _ := rec["tokenSuffix"].(string)
		assert.Contains(t, []string{tokenSuffix(testReadOnlyToken), tokenSuffix(testManagementToken)}, suffix, "record: %v", rec)
	}
	assert.Equal(t, 6, audits, "reads and actions that passed authorization are audited")
}

func TestAuditTruncatesAnOversizedReason(t *testing.T) {
	ts := newTestServer(t)

	// The reason is rejected for being too long, and the rejection is still audited
	reason := strings.Repeat("é", maxReasonLength)
	body := `{"reason":"` + reason + `"}`
	decodeError(t, ts.do(t, http.MethodPost, "/api/v1/workflows/order/instances/o1/cancel", testManagementToken, body), http.StatusBadRequest, CodeBadRequest)

	recs := ts.logs.auditRecords(t, "workflowInstance.cancel")
	require.Len(t, recs, 1)
	logged, ok := recs[0]["reason"].(string)
	require.True(t, ok)
	assert.LessOrEqual(t, len(logged), maxReasonLength)
	assert.True(t, utf8.ValidString(logged), "the cut must not split a character")
	assert.Equal(t, true, recs[0]["reasonTruncated"])
}
