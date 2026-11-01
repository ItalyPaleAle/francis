package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// newIRSampleWorkflow builds a definition that uses every node kind and most step options, so IR tests cover every field of the canonical schema
func newIRSampleWorkflow(t *testing.T) *Workflow {
	t.Helper()

	child, err := New("charge", WithVersion(2), WithSteps(
		Step("authorize", WithRun(noopRun), WithCompensate(noopCompensate)),
	))
	require.NoError(t, err)

	wf, err := New("order",
		WithVersion(3),
		WithOutput("ship"),
		WithTimeout(2*time.Hour),
		WithRetention(RetentionPolicy{Completed: time.Hour, Failed: 48 * time.Hour}),
		WithCompensationFailurePolicy(AbortUnwinding),
		WithSteps(
			Step("validate", WithRun(noopRun), WithAttemptTimeout(time.Minute)),
			Step("needs-review", WithRun(noopRun), WithSkipOnFailure("approval")),
			WaitForEvent("approval", WithEventName("approved"), WithEventTimeout(24*time.Hour), WithSkipIf("needs-review", false)),
			Child("payment", WithDefinition(child), WithMaxAttempts(1)),
			Parallel("notify",
				Step("email", WithRun(noopRun)),
				Step("sms", WithRun(noopRun), WithCompensate(noopCompensate), WithInputFrom("validate")),
			).With(WithFailurePolicy(TolerateFailures), WithOptional()),
			Step("list-items", WithRun(noopRun)),
			ForEach("pack", WithItemsFrom("list-items"), WithRun(noopRun), WithMaxParallel(4), WithRequiredCapability("gpu"), WithCompensateOnFailure()),
			ForEach("charge-each", WithItemsFrom("list-items"), WithChild(child), WithFailurePolicy(CollectFailures)),
			Loop("poll",
				Step("check", WithRun(noopRun)),
				Step("pause", WithRun(noopRun), WithRetryBackoff(time.Second, 5*time.Second)),
			).With(WithUntil("check", true), WithMaxIterations(20)),
			Step("ship", WithRun(noopRun), WithInputFrom("validate", "list-items"), WithCompensate(noopCompensate), WithCompensateTimeout(time.Minute)),
		),
	)
	require.NoError(t, err)
	return wf
}

func TestCanonicalIRDecodesToTheSameIR(t *testing.T) {
	wf := newIRSampleWorkflow(t)

	data, err := wf.CanonicalIR()
	require.NoError(t, err)

	decoded, err := decodeCanonicalIR(data)
	require.NoError(t, err)

	// The decoded IR is exactly what was lowered, apart from the one workflow-wide setting the IR deliberately leaves out
	expected := *wf.def.definitionIR
	expected.noEventHistory = false
	assert.Equal(t, &expected, decoded)

	// The decoder's field counts match what the encoder writes
	assert.Len(t, canonicalDefinition(decoded), canonicalDefinitionFields)
	assert.Len(t, canonicalStep(decoded.steps[0]), canonicalStepFields)
}

func TestCanonicalIRExcludesEventHistoryOptOut(t *testing.T) {
	with, err := New("history", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	without, err := New("history", WithoutEventHistory(), WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)

	withBytes, err := with.CanonicalIR()
	require.NoError(t, err)
	withoutBytes, err := without.CanonicalIR()
	require.NoError(t, err)

	// Opting out of event history changes what is recorded about a run rather than how it runs, so it is not part of the identity
	assert.Equal(t, withBytes, withoutBytes)
}

func TestDecodeCanonicalIRRejectsInvalidInput(t *testing.T) {
	wf, err := New("invalid", WithSteps(Step("only", WithRun(noopRun))))
	require.NoError(t, err)
	valid, err := wf.CanonicalIR()
	require.NoError(t, err)

	// encodeWith re-encodes the definition array after letting the test change it
	encodeWith := func(change func(fields []any) []any) []byte {
		fields := change(canonicalDefinition(wf.def.definitionIR))
		enc, encErr := msgpack.Marshal(fields)
		require.NoError(t, encErr)
		return enc
	}

	tests := []struct {
		name     string
		data     []byte
		contains string
	}{
		{
			name:     "empty",
			data:     nil,
			contains: "failed to decode the workflow IR",
		},
		{
			name:     "not an array",
			data:     []byte{0xa3, 'a', 'b', 'c'},
			contains: "failed to decode the workflow IR",
		},
		{
			name:     "truncated",
			data:     valid[:len(valid)-3],
			contains: "failed to decode the workflow IR",
		},
		{
			name: "wrong magic",
			data: encodeWith(func(fields []any) []any {
				fields[0] = "something.else"
				return fields
			}),
			contains: `the magic is "something.else"`,
		},
		{
			name: "newer format version",
			data: encodeWith(func(fields []any) []any {
				fields[1] = int64(definitionIRVersion + 1)
				return fields
			}),
			contains: "unsupported IR format version 3",
		},
		{
			name: "extra definition field",
			data: encodeWith(func(fields []any) []any {
				return append(fields, "extra")
			}),
			contains: "the definition has 17 fields",
		},
		{
			name: "missing step field",
			data: encodeWith(func(fields []any) []any {
				step := canonicalStep(wf.def.steps[0])
				fields[4] = []any{step[:len(step)-1]}
				return fields
			}),
			contains: "step at index 0: the step has 31 fields",
		},
		{
			name: "integer not encoded as 64 bits",
			data: encodeWith(func(fields []any) []any {
				fields[3] = int8(1)
				return fields
			}),
			contains: "not in canonical form",
		},
		{
			name:     "trailing bytes",
			data:     append(append([]byte{}, valid...), 0xc0),
			contains: "not in canonical form",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := decodeCanonicalIR(tc.data)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.contains)
		})
	}
}
