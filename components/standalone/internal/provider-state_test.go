package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/italypaleale/francis/components"
)

// seqs returns the sequence numbers of events, in order
func seqs(events []components.WorkflowEvent) []int64 {
	res := make([]int64, len(events))
	for i, ev := range events {
		res[i] = ev.Seq
	}
	return res
}

// eventsWithSeqs builds events with the given sequence numbers, whose data names the batch
func eventsWithSeqs(batch string, seqNums ...int64) []components.WorkflowEvent {
	res := make([]components.WorkflowEvent, len(seqNums))
	for i, s := range seqNums {
		res[i] = components.WorkflowEvent{Seq: s, Kind: "k", Data: []byte(batch)}
	}
	return res
}

func TestMergeWorkflowEvents(t *testing.T) {
	key := NewActorKey("T", "a")

	t.Run("appending keeps a held snapshot unchanged", func(t *testing.T) {
		changes := NewChanges()
		defer changes.Release()

		// Leave spare capacity, so the append can reuse the backing array
		current := make([]components.WorkflowEvent, 0, 16)
		current = append(current, eventsWithSeqs("first", 1, 2, 3)...)
		snapshot := current

		merged, changed := mergeWorkflowEvents(key, current, eventsWithSeqs("second", 4, 5), changes)
		require.True(t, changed)
		assert.Equal(t, []int64{1, 2, 3, 4, 5}, seqs(merged))
		assert.Equal(t, []int64{1, 2, 3}, seqs(snapshot), "a reader's snapshot must not change")
		require.Len(t, changes.WorkflowEvents.Insert, 1)
		assert.Equal(t, []int64{4, 5}, seqs(changes.WorkflowEvents.Insert[0].Events))
	})

	t.Run("a retried batch only adds what is new", func(t *testing.T) {
		changes := NewChanges()
		defer changes.Release()

		current := eventsWithSeqs("first", 1, 2, 3)
		merged, changed := mergeWorkflowEvents(key, current, eventsWithSeqs("retry", 2, 3, 4), changes)
		require.True(t, changed)
		assert.Equal(t, []int64{1, 2, 3, 4}, seqs(merged))
		assert.Equal(t, "first", string(merged[1].Data), "a stored event is kept as it was")

		// Nothing new leaves the history as it was
		merged, changed = mergeWorkflowEvents(key, merged, eventsWithSeqs("again", 2, 3), changes)
		assert.False(t, changed)
		assert.Equal(t, []int64{1, 2, 3, 4}, seqs(merged))
	})

	t.Run("a gap is filled in order without touching the current slice", func(t *testing.T) {
		changes := NewChanges()
		defer changes.Release()

		current := eventsWithSeqs("first", 1, 2, 5)
		merged, changed := mergeWorkflowEvents(key, current, eventsWithSeqs("fill", 4, 3, 6), changes)
		require.True(t, changed)
		assert.Equal(t, []int64{1, 2, 3, 4, 5, 6}, seqs(merged))
		assert.Equal(t, []int64{1, 2, 5}, seqs(current))
	})

	t.Run("a sequence number repeated within a batch keeps its first occurrence", func(t *testing.T) {
		changes := NewChanges()
		defer changes.Release()

		batch := append(eventsWithSeqs("one", 2, 3), eventsWithSeqs("two", 2)...)
		merged, _ := mergeWorkflowEvents(key, eventsWithSeqs("first", 1), batch, changes)
		assert.Equal(t, []int64{1, 2, 3}, seqs(merged))
		assert.Equal(t, "one", string(merged[1].Data))
	})

	t.Run("a history that starts over replaces the previous one", func(t *testing.T) {
		changes := NewChanges()
		defer changes.Release()

		merged, changed := mergeWorkflowEvents(key, eventsWithSeqs("old", 1, 2, 3), eventsWithSeqs("new", 1, 2), changes)
		require.True(t, changed)
		assert.Equal(t, []int64{1, 2}, seqs(merged))
		assert.Equal(t, "new", string(merged[0].Data))
		assert.Equal(t, []ActorKey{key}, changes.WorkflowEvents.Reset)
	})
}
