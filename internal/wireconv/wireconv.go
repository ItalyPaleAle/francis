// Package wireconv converts the workflow engine's labels and events between their provider shape and their wire shape
// The remote host converts them to the wire and the runtime converts them back, so both directions live together here while the protocol package stays independent of the provider interface
package wireconv

import (
	"time"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/protocol"
)

// WorkflowLabelsToWire restates the workflow engine's labels in the wire's own shape, or returns nil when there are none
func WorkflowLabelsToWire(labels *components.WorkflowLabels) *protocol.WorkflowLabels {
	if labels == nil {
		return nil
	}

	return &protocol.WorkflowLabels{
		Status:  labels.Status,
		Version: labels.Version,
		Parent:  labels.Parent,
		Created: labels.Created,
	}
}

// WorkflowLabelsFromWire reads the workflow engine's labels back off the wire, or returns nil when there are none
func WorkflowLabelsFromWire(labels *protocol.WorkflowLabels) *components.WorkflowLabels {
	if labels == nil {
		return nil
	}

	return &components.WorkflowLabels{
		Status:  labels.Status,
		Version: labels.Version,
		Parent:  labels.Parent,
		Created: labels.Created,
	}
}

// WorkflowEventsToWire restates the workflow events appended with a state write in the wire's own shape
func WorkflowEventsToWire(events []components.WorkflowEvent) []protocol.WorkflowEvent {
	if len(events) == 0 {
		return nil
	}

	out := make([]protocol.WorkflowEvent, len(events))
	for i, ev := range events {
		out[i] = protocol.WorkflowEvent{
			Seq:          ev.Seq,
			TimeUnixNano: ev.Time.UnixNano(),
			Kind:         ev.Kind,
			Data:         ev.Data,
		}
	}
	return out
}

// WorkflowEventsFromWire reads the workflow events appended with a state write back off the wire
func WorkflowEventsFromWire(events []protocol.WorkflowEvent) []components.WorkflowEvent {
	if len(events) == 0 {
		return nil
	}

	out := make([]components.WorkflowEvent, len(events))
	for i, ev := range events {
		out[i] = components.WorkflowEvent{
			Seq:  ev.Seq,
			Time: time.Unix(0, ev.TimeUnixNano).UTC(),
			Kind: ev.Kind,
			Data: ev.Data,
		}
	}
	return out
}
