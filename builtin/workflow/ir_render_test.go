package workflow

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderIRMermaid(t *testing.T) {
	child, err := New("charge", WithSteps(Step("authorize", WithRun(noopRun))))
	require.NoError(t, err)
	wf, err := New("checkout",
		WithOutput("ship"),
		WithSteps(
			Step("reserve", WithRun(noopRun), WithCompensate(noopCompensate)),
			WaitForEvent("approval", WithSkipIf("reserve", false)),
			Child("payment", WithDefinition(child)),
			Parallel("notify",
				Step("email", WithRun(noopRun)),
				Step("sms", WithRun(noopRun)),
			).With(WithOptional()),
			ForEach("pack", WithItemsFrom("reserve"), WithRun(noopRun), WithMaxParallel(2), WithRequiredCapability("gpu")),
			Loop("poll",
				Step("check", WithRun(noopRun)),
			).With(WithUntil("check", true), WithMaxIterations(5)),
			Step("ship", WithRun(noopRun), WithInputFrom("reserve")),
		),
	)
	require.NoError(t, err)

	data, err := wf.CanonicalIR()
	require.NoError(t, err)
	out, err := RenderIR(data, IRFormatMermaid)
	require.NoError(t, err)

	expected := `flowchart TD
    wfStart(["start"])
    n0["reserve<br/>↺ compensable"]
    n1>"approval<br/>waits for event approval"]
    n2[["payment<br/>child charge v1<br/>↺ compensable"]]
    subgraph n3["notify (parallel)"]
        n3_0["email"]
        n3_1["sms"]
    end
    n4[/"pack<br/>for each item of reserve<br/>at most 2 in flight<br/>requires gpu"\]
    subgraph n6_loop["loop poll"]
        n5["check"]
        n6{"poll<br/>until check = true<br/>at most 5 iterations"}
    end
    n7["ship"]
    wfEnd(["end"])
    wfStart --> n0
    n0 --> n1
    n1 --> n2
    n2 --> n3
    n3 --> n4
    n4 --> n5
    n5 --> n6
    n6 -->|"done"| n7
    n7 --> wfEnd
    n6 -->|"repeat"| n5
    n0 -.->|"skip if false"| n1
    n0 -.->|"items"| n4
    n0 -.->|"input"| n7
    n7 -.->|"output"| wfEnd
    classDef optional stroke-dasharray: 5 5
    class n3 optional
`
	assert.Equal(t, expected, out)
}

func TestRenderIRMarkdown(t *testing.T) {
	wf := newIRSampleWorkflow(t)

	data, err := wf.CanonicalIR()
	require.NoError(t, err)
	out, err := RenderIR(data, IRFormatMarkdown)
	require.NoError(t, err)

	// The identity matches what the registry records for this definition
	assert.Contains(t, out, "# Workflow `order`, version 3\n")
	assert.Contains(t, out, "| Fingerprint | `"+wf.def.fingerprint+"` |\n")
	assert.Contains(t, out, "| IR format version | 2 |\n")

	// The diagram is embedded, so the document renders on its own
	assert.Contains(t, out, "## Graph\n\n```mermaid\nflowchart TD\n")

	// Workflow-wide settings show resolved values, including the defaults the declaration left out
	assert.Contains(t, out, "| Instance timeout | 2h0m0s |\n")
	assert.Contains(t, out, "| Retention of cancelled instances | 24h0m0s |\n")
	assert.Contains(t, out, "| Instance output | the output of `ship` |\n")
	assert.Contains(t, out, "| Max journal size | 1048576 bytes (1 MiB) |\n")
	assert.Contains(t, out, "| Compensation failure policy | `abort` |\n")

	// Each step's table holds the settings that govern its kind, with defaults resolved
	assert.Contains(t, out, "\n### 1. `validate` (step)\n")
	assert.Contains(t, out, "| Attempt timeout | 1m0s |\n")
	assert.Contains(t, out, "| Skip on failure | `approval` |\n")
	assert.Contains(t, out, "| Event name | `approved` |\n")
	assert.Contains(t, out, "| Skip if | when `needs-review` outputs false |\n")
	assert.Contains(t, out, "| Child workflow | `charge`, version 2 |\n")
	assert.Contains(t, out, "| Max tasks in flight | 4 |\n")
	assert.Contains(t, out, "| Required capability | `gpu` |\n")
	assert.Contains(t, out, "| Until | when `check` outputs true |\n")
	assert.Contains(t, out, "| Input from | the preceding step, plus `list-items`, `validate` |\n")

	// A group's members are nested under it and read what the group reads
	assert.Contains(t, out, "\n#### 5.2. `sms` (step, member of `notify`)\n")
	assert.Contains(t, out, "| Input from | what its group receives, plus `validate` |\n")

	// Loop body steps are top-level steps, and say which loop repeats them
	assert.Contains(t, out, "\n### 9. `check` (step)\n")
	assert.Contains(t, out, "| Repeated by | loop `poll`, as step 1 of 2 of its body |\n")

	// A wait step has no task, so no task policy is printed for it
	approval := markdownSection(out, "### 3. `approval` (wait)")
	assert.NotContains(t, approval, "Attempts")
	assert.Contains(t, approval, "| Event timeout | 24h0m0s |\n")
}

func TestRenderIRDiffShowsWhatChanged(t *testing.T) {
	render := func(attempts int) []string {
		wf, err := New("diff", WithSteps(
			Step("first", WithRun(noopRun)),
			Step("second", WithRun(noopRun), WithMaxAttempts(attempts)),
		))
		require.NoError(t, err)
		data, err := wf.CanonicalIR()
		require.NoError(t, err)
		out, err := RenderIR(data, IRFormatMarkdown)
		require.NoError(t, err)
		return strings.Split(out, "\n")
	}

	// Rendering is deterministic, so the same definition always produces the same document
	first := render(5)
	second := render(5)
	assert.Equal(t, first, second)

	// Changing one setting changes exactly the fingerprint and the row of that setting
	before := render(3)
	after := render(5)
	var changed []string
	for _, line := range after {
		if !slices.Contains(before, line) {
			changed = append(changed, line)
		}
	}
	require.Len(t, changed, 2)
	assert.True(t, strings.HasPrefix(changed[0], "| Fingerprint | `"))
	assert.Equal(t, "| Attempts | 5 |", changed[1])
}

func TestRenderIREscapesNames(t *testing.T) {
	name := "say \"hi\" <i>#1 `now`"
	wf, err := New("escape", WithSteps(
		Step(name, WithRun(noopRun), WithRequiredCapability("a|b")),
	))
	require.NoError(t, err)
	data, err := wf.CanonicalIR()
	require.NoError(t, err)

	// Mermaid labels use numeric entities for everything that could end the label or start markup
	mermaid, err := RenderIR(data, IRFormatMermaid)
	require.NoError(t, err)
	assert.Contains(t, mermaid, `n0["say #34;hi#34; #60;i#62;#35;1 #96;now#96;<br/>requires a#124;b"]`)

	// Markdown code spans use a fence longer than the backticks inside them, padded when the value ends with one, and table cells escape pipes
	markdown, err := RenderIR(data, IRFormatMarkdown)
	require.NoError(t, err)
	assert.Contains(t, markdown, "### 1. `` say \"hi\" <i>#1 `now` `` (step)\n")
	assert.Contains(t, markdown, "| Required capability | `a\\|b` |\n")
}

func TestRenderIRRejectsUnknownFormat(t *testing.T) {
	wf, err := New("format", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	data, err := wf.CanonicalIR()
	require.NoError(t, err)

	_, err = RenderIR(data, IRFormat("html"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown IR format "html"`)
}

func TestRenderIRRejectsInvalidIR(t *testing.T) {
	_, err := RenderIR([]byte("not an IR"), IRFormatMarkdown)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to decode the workflow IR")
}

// markdownSection returns the text of one section, from its heading up to the next heading of any level
func markdownSection(doc string, heading string) string {
	start := strings.Index(doc, "\n"+heading+"\n")
	if start < 0 {
		return ""
	}
	section, _, _ := strings.Cut(doc[start+1+len(heading):], "\n#")
	return section
}
