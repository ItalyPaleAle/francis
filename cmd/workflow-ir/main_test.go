package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/builtin/workflow"
)

// sampleIR returns the canonical IR of a small definition
func sampleIR(t *testing.T) []byte {
	t.Helper()

	run := func(context.Context, workflow.Task) (any, error) {
		return nil, nil
	}
	wf, err := workflow.New("sample", workflow.WithVersion(4), workflow.WithSteps(
		workflow.Step("first", workflow.WithRun(run)),
		workflow.WaitForEvent("approval"),
	))
	require.NoError(t, err)

	data, err := wf.CanonicalIR()
	require.NoError(t, err)
	return data
}

// runCommand runs the command with the given arguments and standard input, returning its exit code and both output streams
func runCommand(args []string, stdin []byte) (code int, stdout string, stderr string) {
	var out, errOut bytes.Buffer
	code = run(args, bytes.NewReader(stdin), &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestRunRendersEveryInputEncoding(t *testing.T) {
	data := sampleIR(t)
	wrapped := base64.StdEncoding.EncodeToString(data)
	wrapped = wrapped[:10] + "\n" + wrapped[10:] + "\n"

	tests := []struct {
		name  string
		args  []string
		stdin []byte
	}{
		{name: "raw bytes", stdin: data},
		{name: "hex", stdin: []byte(hex.EncodeToString(data) + "\n")},
		{name: "wrapped base64", stdin: []byte(wrapped)},
		{name: "url-safe base64 without padding", stdin: []byte(base64.RawURLEncoding.EncodeToString(data))},
		{name: "explicit hex", args: []string{"-encoding", "hex", "-"}, stdin: []byte(hex.EncodeToString(data))},
		{name: "explicit binary", args: []string{"-encoding=binary"}, stdin: data},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(tc.args, tc.stdin)
			require.Equal(t, 0, code, stderr)
			assert.True(t, strings.HasPrefix(stdout, "# Workflow `sample`, version 4\n"))
			assert.Empty(t, stderr)
		})
	}
}

func TestRunReadsAFileAndRendersMermaid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sample.ir")
	err := os.WriteFile(path, sampleIR(t), 0o600)
	require.NoError(t, err)

	code, stdout, stderr := runCommand([]string{"-format", "mermaid", path}, nil)
	require.Equal(t, 0, code, stderr)
	assert.True(t, strings.HasPrefix(stdout, "flowchart TD\n"))
	assert.Contains(t, stdout, `n1>"approval<br/>waits for event approval"]`)
}

func TestRunReportsErrors(t *testing.T) {
	data := sampleIR(t)

	tests := []struct {
		name     string
		args     []string
		stdin    []byte
		code     int
		contains string
	}{
		{name: "empty input", stdin: nil, code: 1, contains: "the input is empty"},
		{name: "unrecognized text", stdin: []byte("this is not an IR!"), code: 1, contains: "neither raw IR bytes nor hex or base64"},
		{name: "invalid IR", args: []string{"-encoding", "binary"}, stdin: []byte{0x91, 0xc0}, code: 1, contains: "failed to decode the workflow IR"},
		{name: "invalid hex", args: []string{"-encoding", "hex"}, stdin: []byte("zz"), code: 1, contains: "failed to decode the input as hex"},
		{name: "unknown encoding", args: []string{"-encoding", "yaml"}, stdin: data, code: 1, contains: `unknown encoding "yaml"`},
		{name: "unknown format", args: []string{"-format", "html"}, stdin: data, code: 1, contains: `unknown IR format "html"`},
		{name: "missing file", args: []string{filepath.Join(t.TempDir(), "missing.ir")}, code: 1, contains: "failed to read"},
		{name: "too many files", args: []string{"a.ir", "b.ir"}, code: 2, contains: "expected at most one file"},
		{name: "unknown flag", args: []string{"-nope"}, code: 2, contains: "flag provided but not defined"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := runCommand(tc.args, tc.stdin)
			assert.Equal(t, tc.code, code)
			assert.Empty(t, stdout)
			assert.Contains(t, stderr, tc.contains)
		})
	}
}

func TestRunPrintsHelp(t *testing.T) {
	code, stdout, stderr := runCommand([]string{"-h"}, nil)
	assert.Equal(t, 0, code)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Usage: workflow-ir [flags] [file]")
}
