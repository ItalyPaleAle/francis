package workflow

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

func TestSuspendRecordUsesTheEventTimeoutWireName(t *testing.T) {
	rec := suspendRecord{RemainingEventTimeout: time.Minute}

	enc, err := msgpack.Marshal(rec)
	require.NoError(t, err)
	assert.True(t, bytes.Contains(enc, []byte("remainingEventTimeout")))
	assert.False(t, bytes.Contains(enc, []byte("remainingStepTimeout")))
}

func TestAJournalEncodesWhenItIsHandedOverAsAValue(t *testing.T) {
	// The actor client takes the state by value, so a marshaler the value's method set does not carry would leave the journal unwritable
	newState := func() instanceState {
		return instanceState{
			Version:   1,
			Status:    StatusRunning,
			CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
			StartedAt: time.Now().UTC().Truncate(time.Millisecond),
			Steps: []stepRecord{
				{Name: "one", Kind: KindStep, Status: StepRunning, Remaining: 1},
			},
		}
	}

	t.Run("without a cached encoding", func(t *testing.T) {
		st := newState()

		enc, err := msgpack.Marshal(st)
		require.NoError(t, err)

		var got instanceState
		err = msgpack.Unmarshal(enc, &got)
		require.NoError(t, err)
		assert.Equal(t, st.Status, got.Status)
		assert.Equal(t, st.Version, got.Version)
		require.Len(t, got.Steps, 1)
		assert.Equal(t, "one", got.Steps[0].Name)
	})

	t.Run("reusing the encoding the size check produced", func(t *testing.T) {
		st := newState()

		size, err := journalSize(&st)
		require.NoError(t, err)
		require.Positive(t, size)
		require.NotEmpty(t, st.encoded)

		enc, err := msgpack.Marshal(st)
		require.NoError(t, err)
		assert.Equal(t, st.encoded, enc, "the write should reuse the bytes the size check measured")
	})

	t.Run("through a pointer", func(t *testing.T) {
		st := newState()

		enc, err := msgpack.Marshal(&st)
		require.NoError(t, err)

		var got instanceState
		err = msgpack.Unmarshal(enc, &got)
		require.NoError(t, err)
		assert.Equal(t, st.Status, got.Status)
	})
}

func TestJournalSizeUsesTheProviderEncoding(t *testing.T) {
	st := &instanceState{
		Workflow: "wire-size",
		Version:  1,
		Status:   StatusRunning,
		Steps:    []stepRecord{{Name: "work", Kind: KindStep, Status: StepRunning, Tasks: []taskRecord{{Index: 0, Attempts: 1}}}},
	}

	size, err := journalSize(st)
	require.NoError(t, err)
	encoded, err := msgpack.Marshal(*st)
	require.NoError(t, err)
	assert.Len(t, encoded, size)
	assert.Equal(t, st.encoded, encoded)

	wf, err := New("wire-size", WithSteps(WaitForEvent("ready")))
	require.NoError(t, err)
	host := newFakeHost()
	o := newTestOrchestrator(t, wf, host, "instance-1")
	st.Status = StatusFailed
	err = o.persist(t.Context(), st, time.Now())
	require.NoError(t, err)
	stored := readJournal(t, host, wf, "instance-1")
	assert.Equal(t, StatusFailed, stored.Status)
}
