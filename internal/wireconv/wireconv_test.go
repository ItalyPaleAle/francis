package wireconv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/italypaleale/francis/components"
)

func TestWorkflowLabelsRoundTrip(t *testing.T) {
	labels := &components.WorkflowLabels{Status: "running", Version: 3, Parent: "p1", Created: components.FormatWorkflowCreated(time.Now())}
	assert.Equal(t, labels, WorkflowLabelsFromWire(WorkflowLabelsToWire(labels)))

	// Absent labels stay absent in both directions
	assert.Nil(t, WorkflowLabelsToWire(nil))
	assert.Nil(t, WorkflowLabelsFromWire(nil))
}

func TestWorkflowEventsRoundTrip(t *testing.T) {
	events := []components.WorkflowEvent{
		{Seq: 1, Time: time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC), Kind: "instance_started", Data: []byte("a")},
		{Seq: 2, Time: time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC), Kind: "step_started"},
	}
	assert.Equal(t, events, WorkflowEventsFromWire(WorkflowEventsToWire(events)))

	// No events travel as none
	assert.Nil(t, WorkflowEventsToWire(nil))
	assert.Nil(t, WorkflowEventsFromWire(nil))
}
