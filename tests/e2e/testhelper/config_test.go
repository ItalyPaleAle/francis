package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiscoverTestsUsesDefinitionMarkers(t *testing.T) {
	// Create one marked E2E test and one infrastructure folder with its own unit test
	rootDir := t.TempDir()
	writeTestFile(t, rootDir, "tests/e2e/invocation/test.json", `{"replicas":3}`)
	writeTestFile(t, rootDir, "tests/e2e/invocation/app/main.go", "package main\n")
	writeTestFile(t, rootDir, "tests/e2e/invocation/invocation_test.go", "package invocation_test\n")
	writeTestFile(t, rootDir, "tests/e2e/testhelper/config_test.go", "package main\n")

	// Discover only the explicitly marked folder and preserve its metadata
	tests, err := discoverTests(rootDir)
	if err != nil {
		t.Fatalf("discoverTests returned an error: %v", err)
	}
	if len(tests) != 1 {
		t.Fatalf("discoverTests returned %d tests, want 1", len(tests))
	}
	want := testDefinition{Name: "invocation", Replicas: 3}
	if tests[0] != want {
		t.Fatalf("discoverTests returned %+v, want %+v", tests[0], want)
	}
}

func TestDiscoverTestsRejectsUnknownSettings(t *testing.T) {
	// Create a marked test with a misspelled setting that would otherwise be ignored by encoding/json
	rootDir := t.TempDir()
	writeTestFile(t, rootDir, "tests/e2e/invocation/test.json", `{"replicas":3,"replica":2}`)
	writeTestFile(t, rootDir, "tests/e2e/invocation/app/main.go", "package main\n")
	writeTestFile(t, rootDir, "tests/e2e/invocation/invocation_test.go", "package invocation_test\n")

	// Require an actionable decode failure before the suite can touch a cluster
	_, err := discoverTests(rootDir)
	if err == nil {
		t.Fatal("discoverTests returned no error for an unknown setting")
	}
	if !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("discoverTests returned %q, want an unknown field error", err)
	}
}

func writeTestFile(t *testing.T, rootDir string, relativePath string, contents string) {
	t.Helper()

	// Create parent directories so each test describes only the files relevant to its scenario
	path := filepath.Join(rootDir, filepath.FromSlash(relativePath))
	err := os.MkdirAll(filepath.Dir(path), 0o755)
	if err != nil {
		t.Fatalf("failed to create test directory: %v", err)
	}

	// Write the fixture with ordinary repository permissions
	err = os.WriteFile(path, []byte(contents), 0o600)
	if err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
}
