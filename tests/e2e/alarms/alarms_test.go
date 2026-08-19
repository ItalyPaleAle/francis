//go:build e2e

package alarms_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const alarmTimeout = 30 * time.Second

type alarmState struct {
	Activated      bool   `json:"activated"`
	ActivationHost string `json:"activationHost"`
	AlarmCount     int    `json:"alarmCount"`
	LastAlarmName  string `json:"lastAlarmName"`
	LastAlarmData  string `json:"lastAlarmData"`
	LastAlarmHost  string `json:"lastAlarmHost"`
}

func TestAlarmDelivery(t *testing.T) {
	baseURL := strings.TrimRight(os.Getenv("E2E_ALARMS_URL"), "/")
	require.NotEmpty(t, baseURL, "E2E_ALARMS_URL must point at the alarms test app")
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	t.Run("active actor", func(t *testing.T) {
		actorID := fmt.Sprintf("active-%d", time.Now().UnixNano())

		// Activate and persist the actor before scheduling its alarm
		activateURL := baseURL + "/actors/" + actorID + "/activate"
		var activated alarmState
		postJSON(t, client, activateURL, nil, http.StatusOK, &activated)
		require.True(t, activated.Activated)
		require.NotEmpty(t, activated.ActivationHost)

		// A later one-shot alarm must be delivered to the already active actor
		alarmName := "active-alarm"
		alarmData := "active-payload"
		scheduleAlarm(t, client, baseURL, actorID, alarmName, alarmData)
		state := waitForAlarm(t, client, baseURL, actorID)
		assert.True(t, state.Activated)
		assert.GreaterOrEqual(t, state.AlarmCount, 1)
		assert.Equal(t, alarmName, state.LastAlarmName)
		assert.Equal(t, alarmData, state.LastAlarmData)
		assert.NotEmpty(t, state.LastAlarmHost)
	})

	t.Run("inactive actor", func(t *testing.T) {
		actorID := fmt.Sprintf("inactive-%d", time.Now().UnixNano())

		// A missing state confirms no invocation has activated this actor before scheduling
		_, found, err := fetchState(client, baseURL, actorID)
		require.NoError(t, err)
		require.False(t, found)

		// Alarm dispatch must place and activate the actor without an invocation
		const alarmName = "inactive-alarm"
		const alarmData = "inactive-payload"
		scheduleAlarm(t, client, baseURL, actorID, alarmName, alarmData)
		state := waitForAlarm(t, client, baseURL, actorID)
		assert.False(t, state.Activated, "the actor should have been activated by its alarm rather than an invocation")
		assert.GreaterOrEqual(t, state.AlarmCount, 1)
		assert.Equal(t, alarmName, state.LastAlarmName)
		assert.Equal(t, alarmData, state.LastAlarmData)
		assert.NotEmpty(t, state.LastAlarmHost)
	})
}

func scheduleAlarm(t *testing.T, client *http.Client, baseURL string, actorID string, alarmName string, data string) {
	t.Helper()

	url := baseURL + "/actors/" + actorID + "/alarms/" + alarmName
	body := map[string]any{
		"delayMillis": 1500,
		"data":        data,
	}
	postJSON(t, client, url, body, http.StatusAccepted, nil)
}

func waitForAlarm(t *testing.T, client *http.Client, baseURL string, actorID string) alarmState {
	t.Helper()

	var state alarmState
	var lastErr error
	received := assert.Eventually(t, func() bool {
		var found bool
		state, found, lastErr = fetchState(client, baseURL, actorID)
		return lastErr == nil && found && state.AlarmCount > 0
	}, alarmTimeout, 250*time.Millisecond)
	require.True(t, received)
	require.NoError(t, lastErr)
	return state
}

func fetchState(client *http.Client, baseURL string, actorID string) (alarmState, bool, error) {
	// The URL is supplied by the E2E runner and deliberately targets its local port-forward
	// #nosec G704 -- The E2E runner controls the destination
	response, err := client.Get(baseURL + "/actors/" + actorID + "/state")
	if err != nil {
		return alarmState{}, false, err
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusNotFound {
		return alarmState{}, false, nil
	}
	if response.StatusCode != http.StatusOK {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return alarmState{}, false, err
		}
		return alarmState{}, false, fmt.Errorf("state request returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	var state alarmState
	err = json.NewDecoder(response.Body).Decode(&state)
	if err != nil {
		return alarmState{}, false, err
	}
	return state, true, nil
}

func postJSON(t *testing.T, client *http.Client, url string, body any, expectedStatus int, result any) {
	t.Helper()

	var payload io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		payload = bytes.NewReader(encoded)
	}

	// The URL is supplied by the E2E runner and deliberately targets its local port-forward
	// #nosec G704 -- The E2E runner controls the destination
	request, err := http.NewRequest(http.MethodPost, url, payload)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	// #nosec G704 -- The request destination was validated by the E2E runner
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, expectedStatus, response.StatusCode, "unexpected response: %s", strings.TrimSpace(string(responseBody)))
	if result != nil {
		err = json.Unmarshal(responseBody, result)
		require.NoError(t, err)
	}
}
