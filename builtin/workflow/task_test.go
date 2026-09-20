package workflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskEnvelopeExposesTheTaskIdentity(t *testing.T) {
	t.Run("a positional task reports its index", func(t *testing.T) {
		task := &taskEnvelope{p: &runPayload{
			InstanceID: "inst-1",
			Workflow:   "orders",
			Step:       "notify",
			Index:      2,
			Attempt:    3,
			Positional: true,
		}}

		assert.Equal(t, "inst-1", task.InstanceID())
		assert.Equal(t, "orders", task.Workflow())
		assert.Equal(t, "notify", task.Step())
		assert.Equal(t, 2, task.Index())
		assert.Equal(t, 3, task.Attempt())
	})

	t.Run("a plain step reports no index", func(t *testing.T) {
		// A plain step has no siblings to be positioned among, so its handler sees -1 rather than the journal's own index
		task := &taskEnvelope{p: &runPayload{Step: "plan", Index: 0}}
		assert.Equal(t, -1, task.Index())
	})
}

func TestTaskDecodeReportsWhatTheStepMayNotRead(t *testing.T) {
	task := &taskEnvelope{p: &runPayload{
		Outputs: map[string]json.RawMessage{
			"present": json.RawMessage(`{"id":"abc"}`),
			"skipped": nil,
			"empty":   nil,
		},
		Skipped: []string{"skipped"},
	}}

	t.Run("an output the step was given decodes", func(t *testing.T) {
		var out struct {
			ID string `json:"id"`
		}
		require.NoError(t, task.DecodeOutput("present", &out))
		assert.Equal(t, "abc", out.ID)
	})

	t.Run("a skipped step reports ErrStepSkipped", func(t *testing.T) {
		// A skipped step and a step that returned nothing both have no output, so the error is the only thing that tells them apart
		var out string
		err := task.DecodeOutput("skipped", &out)
		require.ErrorIs(t, err, ErrStepSkipped)
		assert.Contains(t, err.Error(), `"skipped"`)
	})

	t.Run("a step the task was not given reports ErrStepNotFound", func(t *testing.T) {
		var out string
		err := task.DecodeOutput("elsewhere", &out)
		require.ErrorIs(t, err, ErrStepNotFound)
		assert.Contains(t, err.Error(), "WithInputFrom")
	})

	t.Run("a step that returned nothing leaves the target alone", func(t *testing.T) {
		out := "untouched"
		require.NoError(t, task.DecodeOutput("empty", &out))
		assert.Equal(t, "untouched", out)
	})

	t.Run("an output that does not fit the target reports the decode error", func(t *testing.T) {
		var out int
		err := task.DecodeOutput("present", &out)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrStepNotFound)
		require.NotErrorIs(t, err, ErrStepSkipped)
	})
}

func TestTaskDecodeIsANoOpForAnAbsentPayload(t *testing.T) {
	// Every one of these is absent for some legitimate kind of task, so decoding one has to leave the caller's value as it was rather than fail
	task := &taskEnvelope{p: &runPayload{}}

	input := "untouched"
	require.NoError(t, task.DecodeInput(&input))
	assert.Equal(t, "untouched", input)

	item := "untouched"
	require.NoError(t, task.DecodeItem(&item))
	assert.Equal(t, "untouched", item)

	result := "untouched"
	require.NoError(t, task.DecodeResult(&result))
	assert.Equal(t, "untouched", result)

	assert.Empty(t, task.Cause())
}

func TestTaskDecodeSurfacesAMalformedPayload(t *testing.T) {
	// The payloads are written by the engine, so a decode failure here means the handler asked for the wrong type and should see why
	task := &taskEnvelope{p: &runPayload{
		Input:  json.RawMessage(`{"count":1}`),
		Item:   json.RawMessage(`"an item"`),
		Result: json.RawMessage(`[1,2,3]`),
	}}

	var wrongInput []string
	err := task.DecodeInput(&wrongInput)
	require.Error(t, err)

	var wrongItem int
	err = task.DecodeItem(&wrongItem)
	require.Error(t, err)

	var wrongResult string
	err = task.DecodeResult(&wrongResult)
	require.Error(t, err)
}

func TestCompensationCarriesTheResultAndTheCause(t *testing.T) {
	// A compensation identifies the effect to undo from the output the forward task produced, so that output has to reach it alongside the reason for the unwind
	comp := &taskEnvelope{
		p: &runPayload{
			Step:   "book",
			Result: json.RawMessage(`{"reservation":"r-77"}`),
			Cause:  "payment declined",
		},
		undo: true,
	}

	var res struct {
		Reservation string `json:"reservation"`
	}
	require.NoError(t, comp.DecodeResult(&res))
	assert.Equal(t, "r-77", res.Reservation)
	assert.Equal(t, "payment declined", comp.Cause())

	// The concrete envelope has to satisfy the narrower Task interface too, since a compensation reads its own identity the same way a forward task does
	var task Task = comp
	assert.Equal(t, "book", task.Step())
}
