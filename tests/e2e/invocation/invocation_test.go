//go:build e2e

package invocation_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	concurrentInvocations = 24
	invocationReplicas    = 3
)

type invocationResult struct {
	ActorID    string `json:"actorId"`
	Count      int    `json:"count"`
	ExecutedBy string `json:"executedBy"`
	ServedBy   string `json:"servedBy"`
}

func TestActorInvocation(t *testing.T) {
	baseURLs := replicaURLs(t)
	client := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	// Send one call through each application replica while requiring all of them to reach the same durable actor
	actorID := fmt.Sprintf("sequential-%d", time.Now().UnixNano())
	sequentialServers := make(map[string]struct{}, len(baseURLs))
	var sequentialOwner string
	for index, baseURL := range baseURLs {
		expected := index + 1
		result := increment(t, client, baseURL, actorID)
		assert.Equal(t, expected, result.Count)
		assert.Equal(t, actorID, result.ActorID)
		assert.NotEmpty(t, result.ExecutedBy)
		assert.NotEmpty(t, result.ServedBy)
		if sequentialOwner == "" {
			sequentialOwner = result.ExecutedBy
		}
		assert.Equal(t, sequentialOwner, result.ExecutedBy, "one active actor must have one owner")
		sequentialServers[result.ServedBy] = struct{}{}
	}
	require.Len(t, sequentialServers, len(baseURLs), "every application replica must serve an invocation")

	// Distribute concurrent calls across every replica to prove turn-based serialization and shared state across all routing paths
	concurrentActorID := fmt.Sprintf("concurrent-%d", time.Now().UnixNano())
	results := make([]invocationResult, concurrentInvocations)
	errs := make([]error, concurrentInvocations)
	var waitGroup sync.WaitGroup
	for i := range concurrentInvocations {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			baseURL := baseURLs[index%len(baseURLs)]
			results[index], errs[index] = incrementResult(client, baseURL, concurrentActorID)
		}(i)
	}
	waitGroup.Wait()

	counts := make([]int, concurrentInvocations)
	concurrentServers := make(map[string]struct{}, len(baseURLs))
	var concurrentOwner string
	for i := range results {
		require.NoError(t, errs[i])
		counts[i] = results[i].Count
		assert.NotEmpty(t, results[i].ExecutedBy)
		assert.NotEmpty(t, results[i].ServedBy)
		if concurrentOwner == "" {
			concurrentOwner = results[i].ExecutedBy
		}
		assert.Equal(t, concurrentOwner, results[i].ExecutedBy, "one active actor must have one owner")
		concurrentServers[results[i].ServedBy] = struct{}{}
	}
	require.Len(t, concurrentServers, len(baseURLs), "every application replica must serve a concurrent invocation")
	sort.Ints(counts)
	for i, count := range counts {
		assert.Equal(t, i+1, count)
	}
}

func replicaURLs(t *testing.T) []string {
	t.Helper()

	values := strings.Split(os.Getenv("E2E_INVOCATION_URLS"), ",")
	urls := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimRight(strings.TrimSpace(value), "/")
		if value != "" {
			urls = append(urls, value)
		}
	}
	require.Len(t, urls, invocationReplicas, "E2E_INVOCATION_URLS must contain one endpoint for each application replica")
	return urls
}

func increment(t *testing.T, client *http.Client, baseURL string, actorID string) invocationResult {
	t.Helper()
	result, err := incrementResult(client, baseURL, actorID)
	require.NoError(t, err)
	return result
}

func incrementResult(client *http.Client, baseURL string, actorID string) (invocationResult, error) {
	// The URL is supplied by the E2E runner and deliberately targets its local port-forward
	// #nosec G704 -- The E2E runner controls the destination
	request, err := http.NewRequest(http.MethodPost, baseURL+"/actors/"+actorID+"/increment", nil)
	if err != nil {
		return invocationResult{}, err
	}
	// #nosec G704 -- The request destination was validated by the E2E runner
	response, err := client.Do(request)
	if err != nil {
		return invocationResult{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return invocationResult{}, readErr
		}
		return invocationResult{}, fmt.Errorf("increment returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	var result invocationResult
	err = json.NewDecoder(response.Body).Decode(&result)
	if err != nil {
		return invocationResult{}, err
	}
	return result, nil
}
