package workflow

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

func TestGoDeclarationsCompileToCanonicalIR(t *testing.T) {
	otherRun := func(ctx context.Context, task Task) (any, error) {
		return "other implementation", nil
	}
	otherCompensate := func(ctx context.Context, compensation Compensation) error {
		return nil
	}

	implicit, err := New("canonical",
		WithSteps(
			Step("source", WithRun(noopRun), WithCompensate(noopCompensate)),
			ForEach("work", WithItemsFrom("source"), WithRun(noopRun)),
			WaitForEvent("approval"),
		),
	)
	require.NoError(t, err)

	explicit, err := New("canonical",
		WithVersion(defaultVersion),
		WithTimeout(defaultTimeout),
		WithRetention(RetentionPolicy{Completed: defaultRetention, Failed: defaultRetention, Cancelled: defaultRetention}),
		WithMaxInputSize(defaultMaxInputSize),
		WithMaxOutputSize(defaultMaxOutputSize),
		WithMaxJournalSize(defaultMaxJournalSize),
		WithMaxDepth(defaultMaxDepth),
		WithUnknownVersionPolicy(ParkUnknownVersion),
		WithCompensationFailurePolicy(ContinueUnwinding),
		WithSteps(
			Step("source", WithRun(otherRun), WithCompensate(otherCompensate), WithMaxAttempts(defaultMaxAttempts), WithRetryBackoff(defaultRetryInitial, defaultRetryMax), WithCompensateMaxAttempts(defaultCompMaxAttempts), WithCompensateBackoff(defaultCompInitial, defaultCompMax)),
			ForEach("work", WithItemsFrom("source"), WithRun(otherRun), WithMaxAttempts(defaultMaxAttempts), WithRetryBackoff(defaultRetryInitial, defaultRetryMax), WithCompensateMaxAttempts(defaultCompMaxAttempts), WithCompensateBackoff(defaultCompInitial, defaultCompMax), WithFailurePolicy(FailFast)),
			WaitForEvent("approval", WithEventName("approval")),
		),
	)
	require.NoError(t, err)

	implicitBytes, err := implicit.def.canonicalBytes()
	require.NoError(t, err)
	explicitBytes, err := explicit.def.canonicalBytes()
	require.NoError(t, err)

	// Equivalent declarations lower to identical bytes even when defaults are explicit and handler implementations differ
	assert.Equal(t, implicitBytes, explicitBytes)
	assert.Equal(t, implicit.def.fingerprint, explicit.def.fingerprint)
	assert.True(t, bytes.Contains(implicitBytes, []byte(canonicalIRMagic)))
	assert.False(t, bytes.Contains(implicitBytes, []byte("implementation")))

	// Executable Go values are linked beside the canonical IR and remain available to workers
	assert.NotNil(t, implicit.def.bindings["source"].run)
	assert.NotNil(t, implicit.def.bindings["source"].compensate)
	assert.Nil(t, implicit.def.bindings["approval"].run)
}

func TestIRFormatVersionContributesToTheFingerprint(t *testing.T) {
	first, err := New("format", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)
	second, err := New("format", WithSteps(Step("work", WithRun(noopRun))))
	require.NoError(t, err)

	second.def.formatVersion++
	err = second.def.setFingerprint()
	require.NoError(t, err)

	assert.NotEqual(t, first.def.fingerprint, second.def.fingerprint)
}

func TestIRMessagePackTypesHaveStableBytes(t *testing.T) {
	tests := []canonicalField{
		{name: "string", value: "value", expectedHex: "a576616c7565"},
		{name: "int64", value: int64(256), expectedHex: "d30000000000000100"},
		{name: "true", value: true, expectedHex: "c3"},
		{name: "false", value: false, expectedHex: "c2"},
		{name: "nil", value: nil, expectedHex: "c0"},
		{name: "empty strings", value: []string{}, expectedHex: "90"},
		{name: "strings", value: []string{"a", "bc"}, expectedHex: "92a161a26263"},
		{name: "empty values", value: []any{}, expectedHex: "90"},
		{name: "values", value: []any{"a", int64(1)}, expectedHex: "92a161d30000000000000001"},
	}

	assertCanonicalFields(t, tests)
}

func TestStepIRFormatVersionTwoHasStableBytes(t *testing.T) {
	step := &stepDef{
		name:                "task",
		kind:                KindChild,
		hasRun:              true,
		hasCompensate:       false,
		itemsFrom:           "items",
		inputFrom:           []string{"first", "second"},
		maxAttempts:         1,
		retryInitial:        2,
		retryMax:            3,
		compMaxAttempt:      4,
		compInitial:         5,
		compMax:             6,
		attemptTimeout:      7,
		eventTimeout:        8,
		eventName:           "event",
		optional:            true,
		skipOnFailure:       []string{"failed"},
		skipIfStep:          "condition",
		skipIfValue:         true,
		hasSkipIf:           true,
		failurePolicy:       CollectFailures,
		maxParallel:         9,
		compensateOnFailure: true,
		capability:          "gpu",
		body:                []string{"body"},
		untilStep:           "done",
		untilValue:          true,
		hasUntil:            true,
		maxIterations:       10,
		compensateTimeout:   11,
		members:             []*stepDef{},
		child:               &childDefinitionIR{name: "child", version: 2},
	}
	schema := canonicalStep(step)
	fields := []canonicalField{
		{name: "name", value: schema[0], expectedHex: "a47461736b"},
		{name: "kind", value: schema[1], expectedHex: "a56368696c64"},
		{name: "has run", value: schema[2], expectedHex: "c3"},
		{name: "has compensate", value: schema[3], expectedHex: "c2"},
		{name: "members", value: schema[4], expectedHex: "90"},
		{name: "child", value: schema[5], expectedHex: "92a56368696c64d30000000000000002"},
		{name: "items from", value: schema[6], expectedHex: "a56974656d73"},
		{name: "input from", value: schema[7], expectedHex: "92a56669727374a67365636f6e64"},
		{name: "max attempts", value: schema[8], expectedHex: "d30000000000000001"},
		{name: "retry initial", value: schema[9], expectedHex: "d30000000000000002"},
		{name: "retry max", value: schema[10], expectedHex: "d30000000000000003"},
		{name: "compensation max attempts", value: schema[11], expectedHex: "d30000000000000004"},
		{name: "compensation initial", value: schema[12], expectedHex: "d30000000000000005"},
		{name: "compensation max", value: schema[13], expectedHex: "d30000000000000006"},
		{name: "attempt timeout", value: schema[14], expectedHex: "d30000000000000007"},
		{name: "event timeout", value: schema[15], expectedHex: "d30000000000000008"},
		{name: "event name", value: schema[16], expectedHex: "a56576656e74"},
		{name: "optional", value: schema[17], expectedHex: "c3"},
		{name: "skip on failure", value: schema[18], expectedHex: "91a66661696c6564"},
		{name: "skip if step", value: schema[19], expectedHex: "a9636f6e646974696f6e"},
		{name: "skip if value", value: schema[20], expectedHex: "c3"},
		{name: "has skip if", value: schema[21], expectedHex: "c3"},
		{name: "failure policy", value: schema[22], expectedHex: "b0636f6c6c6563742d6661696c75726573"},
		{name: "max parallel", value: schema[23], expectedHex: "d30000000000000009"},
		{name: "compensate on failure", value: schema[24], expectedHex: "c3"},
		{name: "capability", value: schema[25], expectedHex: "a3677075"},
		{name: "body", value: schema[26], expectedHex: "91a4626f6479"},
		{name: "until step", value: schema[27], expectedHex: "a4646f6e65"},
		{name: "until value", value: schema[28], expectedHex: "c3"},
		{name: "has until", value: schema[29], expectedHex: "c3"},
		{name: "max iterations", value: schema[30], expectedHex: "d3000000000000000a"},
		{name: "compensation timeout", value: schema[31], expectedHex: "d3000000000000000b"},
	}
	expectedFields := assertCanonicalFields(t, fields)
	assertCanonicalHex(t, "step", "dc0020"+expectedFields, schema)
	assertCanonicalHex(t, "steps", "91dc0020"+expectedFields, canonicalSteps([]*stepDef{step}))
}

func TestDefinitionIRFormatVersionTwoHasStableBytes(t *testing.T) {
	ir := &definitionIR{
		version:                   2,
		timeout:                   3,
		retention:                 RetentionPolicy{Completed: 4, Failed: 5, Cancelled: 6},
		outputStep:                "output",
		maxInputSize:              7,
		maxOutputSize:             8,
		maxJournalSize:            9,
		maxDepth:                  10,
		unknownVersion:            FailUnknownVersion,
		compensationFailurePolicy: AbortUnwinding,
		formatVersion:             definitionIRVersion,
		name:                      "workflow",
		steps:                     []*stepDef{},
	}
	schema := canonicalDefinition(ir)
	fields := []canonicalField{
		{name: "magic", value: schema[0], expectedHex: "b36672616e6369732e776f726b666c6f772e6972"},
		{name: "format version", value: schema[1], expectedHex: "d30000000000000002"},
		{name: "name", value: schema[2], expectedHex: "a8776f726b666c6f77"},
		{name: "version", value: schema[3], expectedHex: "d30000000000000002"},
		{name: "steps", value: schema[4], expectedHex: "90"},
		{name: "timeout", value: schema[5], expectedHex: "d30000000000000003"},
		{name: "completed retention", value: schema[6], expectedHex: "d30000000000000004"},
		{name: "failed retention", value: schema[7], expectedHex: "d30000000000000005"},
		{name: "cancelled retention", value: schema[8], expectedHex: "d30000000000000006"},
		{name: "output step", value: schema[9], expectedHex: "a66f7574707574"},
		{name: "max input size", value: schema[10], expectedHex: "d30000000000000007"},
		{name: "max output size", value: schema[11], expectedHex: "d30000000000000008"},
		{name: "max journal size", value: schema[12], expectedHex: "d30000000000000009"},
		{name: "max depth", value: schema[13], expectedHex: "d3000000000000000a"},
		{name: "unknown version", value: schema[14], expectedHex: "a46661696c"},
		{name: "compensation failure policy", value: schema[15], expectedHex: "a561626f7274"},
	}
	expectedFields := assertCanonicalFields(t, fields)
	assertCanonicalHex(t, "definition", "dc0010"+expectedFields, schema)

	encoded, err := ir.canonicalBytes()
	require.NoError(t, err)
	assert.Equal(t, "dc0010"+expectedFields, hex.EncodeToString(encoded))
}

func TestCanonicalIRNormalizesReferenceSets(t *testing.T) {
	first, err := New("references", WithSteps(
		Step("producer", WithRun(noopRun), WithSkipOnFailure("a", "b")),
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
		Step("consumer", WithRun(noopRun), WithInputFrom("a", "b")),
	))
	require.NoError(t, err)
	second, err := New("references", WithSteps(
		Step("producer", WithRun(noopRun), WithSkipOnFailure("b", "a", "a")),
		Step("a", WithRun(noopRun)),
		Step("b", WithRun(noopRun)),
		Step("consumer", WithRun(noopRun), WithInputFrom("b", "a", "b")),
	))
	require.NoError(t, err)

	// Reference order and duplicates do not affect execution, so they normalize to one identity
	assert.Equal(t, first.def.fingerprint, second.def.fingerprint)
}

type canonicalField struct {
	name        string
	value       any
	expectedHex string
}

func assertCanonicalFields(t *testing.T, fields []canonicalField) string {
	t.Helper()
	expected := make([]string, len(fields))
	for i, field := range fields {
		expected[i] = field.expectedHex
		t.Run(field.name, func(t *testing.T) {
			assertCanonicalHex(t, field.name, field.expectedHex, field.value)
		})
	}
	return strings.Join(expected, "")
}

func assertCanonicalHex(t *testing.T, name string, expected string, value any) {
	t.Helper()
	encoded, err := msgpack.Marshal(value)
	require.NoError(t, err)
	assert.Equal(t, expected, hex.EncodeToString(encoded), name)
}
