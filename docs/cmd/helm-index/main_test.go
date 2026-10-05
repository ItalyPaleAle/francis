package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testRepository = "owner/charts/demo"
	testToken      = "test-token"
)

// fakeChart is a chart version served by fakeRegistry
type fakeChart struct {
	configMediaType string
	metadata        map[string]any
	layerDigest     string
	created         string
}

// fakeRegistry serves the subset of the OCI distribution API that helm-index uses, with ghcr-style anonymous token auth
type fakeRegistry struct {
	t      *testing.T
	server *httptest.Server
	charts map[string]fakeChart
	pages  [][]string

	// Number of manifest requests that fail with a 503 before the registry recovers
	failManifests atomic.Int32
}

func newFakeRegistry(t *testing.T, charts map[string]fakeChart, pages [][]string) *fakeRegistry {
	t.Helper()

	r := &fakeRegistry{t: t, charts: charts, pages: pages}
	r.server = httptest.NewServer(http.HandlerFunc(r.serveHTTP))
	t.Cleanup(r.server.Close)
	return r
}

func (r *fakeRegistry) serveHTTP(w http.ResponseWriter, req *http.Request) {
	// Hand out a token only for anonymous pull access to the test repository, like ghcr does
	if req.URL.Path == "/token" {
		if req.URL.Query().Get("scope") != "repository:"+testRepository+":pull" || req.URL.Query().Get("service") != "fake" {
			http.Error(w, `{"errors":[{"code":"DENIED"}]}`, http.StatusForbidden)
			return
		}
		writeJSON(w, map[string]string{"token": testToken})
		return
	}

	// Every registry request needs the token
	if req.Header.Get("Authorization") != "Bearer "+testToken {
		w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="%s/token",service="fake",scope="repository:%s:pull"`, r.server.URL, testRepository))
		http.Error(w, `{"errors":[{"code":"UNAUTHORIZED"}]}`, http.StatusUnauthorized)
		return
	}

	rest, ok := strings.CutPrefix(req.URL.Path, "/v2/"+testRepository+"/")
	if !ok {
		http.NotFound(w, req)
		return
	}

	switch {
	case rest == "tags/list":
		// Pages are numbered by the "last" parameter, which holds the index of the page to serve
		page := 0
		if req.URL.Query().Get("last") != "" {
			_, _ = fmt.Sscanf(req.URL.Query().Get("last"), "%d", &page)
		}
		if page+1 < len(r.pages) {
			w.Header().Set("Link", fmt.Sprintf(`</v2/%s/tags/list?last=%d&n=1000>; rel="next"`, testRepository, page+1))
		}
		writeJSON(w, map[string]any{"name": testRepository, "tags": r.pages[page]})

	case strings.HasPrefix(rest, "manifests/"):
		if r.failManifests.Add(-1) >= 0 {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		if req.Header.Get("Accept") != ociManifestMediaType {
			r.t.Errorf("manifest requested with Accept %q", req.Header.Get("Accept"))
		}

		tag := strings.TrimPrefix(rest, "manifests/")
		chart, ok := r.charts[tag]
		if !ok {
			http.NotFound(w, req)
			return
		}
		manifest := map[string]any{
			"schemaVersion": 2,
			"config":        map[string]string{"mediaType": chart.configMediaType, "digest": "sha256:config-" + tag},
			"layers": []map[string]string{
				{"mediaType": "application/vnd.cncf.helm.chart.provenance.v1.prov", "digest": "sha256:prov-" + tag},
				{"mediaType": helmChartMediaType, "digest": chart.layerDigest},
			},
		}
		if chart.created != "" {
			manifest["annotations"] = map[string]string{createdAnnotation: chart.created}
		}
		writeJSON(w, manifest)

	case strings.HasPrefix(rest, "blobs/sha256:config-"):
		tag := strings.TrimPrefix(rest, "blobs/sha256:config-")
		chart, ok := r.charts[tag]
		if !ok {
			http.NotFound(w, req)
			return
		}
		writeJSON(w, chart.metadata)

	default:
		http.NotFound(w, req)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func demoMetadata(version string) map[string]any {
	return map[string]any{
		"apiVersion":  "v2",
		"name":        "demo",
		"version":     version,
		"appVersion":  version,
		"description": "Demo chart",
		"keywords":    []any{"demo"},
	}
}

func TestBuildIndex(t *testing.T) {
	charts := map[string]fakeChart{
		"0.1.0": {
			configMediaType: helmConfigMediaType,
			metadata:        demoMetadata("0.1.0"),
			layerDigest:     "sha256:aaaa",
			created:         "2026-01-01T00:00:00Z",
		},
		"0.2.0-rc.1": {
			configMediaType: helmConfigMediaType,
			metadata:        demoMetadata("0.2.0-rc.1"),
			layerDigest:     "sha256:bbbb",
			created:         "2026-02-01T00:00:00Z",
		},
		// A tag that looks like a version but holds a container image
		"0.3.0": {
			configMediaType: "application/vnd.oci.image.config.v1+json",
			metadata:        map[string]any{},
			layerDigest:     "sha256:cccc",
		},
	}
	pages := [][]string{
		{"0.1.0", "sha256-0123456789abcdef"},
		{"0.2.0-rc.1", "0.3.0"},
	}
	registry := newFakeRegistry(t, charts, pages)

	// The first manifest request fails, which the client must retry
	registry.failManifests.Store(1)

	client, err := newRegistryClient(registry.server.URL, testRepository)
	if err != nil {
		t.Fatalf("newRegistryClient: %v", err)
	}
	client.retryDelay = time.Millisecond

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.FixedZone("PST", -8*3600))
	index, err := buildIndex(t.Context(), client, now)
	if err != nil {
		t.Fatalf("buildIndex: %v", err)
	}

	host := strings.TrimPrefix(registry.server.URL, "http://")
	expectRC := demoMetadata("0.2.0-rc.1")
	expectRC["urls"] = []string{"oci://" + host + "/" + testRepository + ":0.2.0-rc.1"}
	expectRC["digest"] = "bbbb"
	expectRC["created"] = "2026-02-01T00:00:00Z"
	expectStable := demoMetadata("0.1.0")
	expectStable["urls"] = []string{"oci://" + host + "/" + testRepository + ":0.1.0"}
	expectStable["digest"] = "aaaa"
	expectStable["created"] = "2026-01-01T00:00:00Z"

	expect := &indexFile{
		APIVersion: "v1",
		Entries: map[string][]map[string]any{
			"demo": {expectRC, expectStable},
		},
		Generated: "2026-03-01T20:00:00Z",
	}
	if !reflect.DeepEqual(index, expect) {
		got, _ := json.MarshalIndent(index, "", "  ")
		want, _ := json.MarshalIndent(expect, "", "  ")
		t.Errorf("unexpected index\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestRun(t *testing.T) {
	t.Run("writes the index", func(t *testing.T) {
		charts := map[string]fakeChart{
			"1.0.0": {
				configMediaType: helmConfigMediaType,
				metadata:        demoMetadata("1.0.0"),
				layerDigest:     "sha256:dddd",
				created:         "2026-01-01T00:00:00Z",
			},
		}
		registry := newFakeRegistry(t, charts, [][]string{{"1.0.0"}})

		out := filepath.Join(t.TempDir(), "charts", "index.yaml")
		err := run(t.Context(), registry.server.URL, testRepository, out)
		if err != nil {
			t.Fatalf("run: %v", err)
		}

		data, err := os.ReadFile(out)
		if err != nil {
			t.Fatalf("failed to read the index: %v", err)
		}
		var index indexFile
		err = json.Unmarshal(data, &index)
		if err != nil {
			t.Fatalf("index is not valid JSON: %v", err)
		}
		if len(index.Entries["demo"]) != 1 || index.Entries["demo"][0]["version"] != "1.0.0" {
			t.Errorf("unexpected entries: %v", index.Entries)
		}
	})

	t.Run("refuses to write an empty index", func(t *testing.T) {
		registry := newFakeRegistry(t, map[string]fakeChart{}, [][]string{{"sha256-0123456789abcdef"}})

		out := filepath.Join(t.TempDir(), "index.yaml")
		err := run(t.Context(), registry.server.URL, testRepository, out)
		if err == nil || !strings.Contains(err.Error(), "no charts found") {
			t.Fatalf("expected a no charts error, got %v", err)
		}
		_, err = os.Stat(out)
		if !os.IsNotExist(err) {
			t.Errorf("expected no index to be written, got %v", err)
		}
	})

	t.Run("fails when the registry keeps failing", func(t *testing.T) {
		registry := newFakeRegistry(t, map[string]fakeChart{}, [][]string{{"1.0.0"}})
		registry.failManifests.Store(maxAttempts)

		client, err := newRegistryClient(registry.server.URL, testRepository)
		if err != nil {
			t.Fatalf("newRegistryClient: %v", err)
		}
		client.retryDelay = time.Millisecond

		_, err = buildIndex(t.Context(), client, time.Now())
		if err == nil || !strings.Contains(err.Error(), "status 503") {
			t.Fatalf("expected a 503 error, got %v", err)
		}
	})
}
