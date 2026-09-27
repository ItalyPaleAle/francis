//go:build unit

package workflow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const describeValueLimit = 200

// Describe returns a multi-line, human-readable rendering of the journal, meant to be printed while debugging
// Values such as inputs and outputs are shortened, so a journal with a wide fan-out stays readable
func (st *instanceState) Describe() string {
	var b strings.Builder

	// The header says which instance this is and where it stands
	fmt.Fprintf(&b, "Workflow %q version %d: %s", st.Workflow, st.Version, describeStatus(st.Status))
	if st.Status == StatusCompensating && st.TerminalStatus != "" {
		fmt.Fprintf(&b, ", will end as %s", st.TerminalStatus)
	}
	b.WriteString("\n")
	describeLine(&b, "  ", "Cause", st.Cause)
	describeLine(&b, "  ", "Compensation", string(st.Compensation))
	describeLine(&b, "  ", "Input", describeValue(st.Input))
	describeLine(&b, "  ", "Output", describeValue(st.Output))

	// Timestamps and the deadline explain how long the instance has been at it and when it gives up
	describeLine(&b, "  ", "Created", describeTime(st.CreatedAt))
	describeLine(&b, "  ", "Started", describeTime(st.StartedAt))
	describeLine(&b, "  ", "Completed", describeTime(st.CompletedAt))
	describeLine(&b, "  ", "Deadline", describeTime(st.DeadlineAt))
	if st.Timeout > 0 {
		describeLine(&b, "  ", "Timeout", st.Timeout.String())
	}
	if st.Suspended != nil {
		describeLine(&b, "  ", "Suspended", fmt.Sprintf("since %s, resumes as %s with %s left on the timeout, reason %q", describeTime(st.Suspended.At), st.Suspended.ResumeTo, st.Suspended.RemainingTimeout, st.Suspended.Reason))
	}

	// A child names its parent, and whether the parent has been told how it ended
	if st.Parent != nil {
		parent := fmt.Sprintf("workflow %q instance %q, step %q task %d, depth %d", st.Parent.Workflow, st.Parent.InstanceID, st.Parent.Step, st.Parent.Index, st.Parent.Depth)
		if st.Parent.UnwoundBy > 0 {
			parent += fmt.Sprintf(", asked to undo itself by compensation attempt %d", st.Parent.UnwoundBy)
		}
		if st.Reported {
			parent += ", outcome reported"
		}
		describeLine(&b, "  ", "Parent", parent)
	}
	if st.Reopened {
		describeLine(&b, "  ", "Reopened", "yes, after it had already terminated")
	}

	// The compensation stack is printed in the order it unwinds, which is the reverse of the order it was pushed
	if len(st.Stack) > 0 {
		unwind := make([]string, len(st.Stack))
		for i, name := range st.Stack {
			unwind[len(st.Stack)-1-i] = name
		}
		describeLine(&b, "  ", "Compensation stack", strings.Join(unwind, ", ")+" (unwinds in this order)")
	}
	if len(st.EventNames) > 0 {
		describeLine(&b, "  ", "Accepts events", strings.Join(st.EventNames, ", "))
	}

	// The definition identity is what decides which hosts may drive this journal
	definition := st.DefinitionFingerprint
	if len(definition) > 12 {
		definition = definition[:12]
	}
	if st.RegistryGeneration > 0 {
		definition += fmt.Sprintf(", registry generation %d", st.RegistryGeneration)
		switch {
		case st.RegistryConfirmed:
			definition += " (confirmed)"
		case st.RegistryRejected:
			definition += " (rejected)"
		default:
			definition += " (not confirmed yet)"
		}
	}
	describeLine(&b, "  ", "Definition", strings.TrimPrefix(definition, ", "))

	// Every step follows, with its tasks indented beneath it
	b.WriteString("Steps:\n")
	if len(st.Steps) == 0 {
		b.WriteString("  (none)\n")
	}
	for i := range st.Steps {
		st.Steps[i].describe(&b, st.Steps[i].Name == st.Cursor && st.Cursor != "")
	}

	return strings.TrimSuffix(b.String(), "\n")
}

// describe writes one step and its tasks
func (sr *stepRecord) describe(b *strings.Builder, current bool) {
	fmt.Fprintf(b, "  %s (%s): %s", sr.Name, sr.Kind, sr.Status)
	if sr.Status == StepRunning && len(sr.Tasks) > 0 {
		fmt.Fprintf(b, ", %d of %d tasks left", sr.Remaining, len(sr.Tasks))
	}
	if sr.Iteration > 0 {
		fmt.Fprintf(b, ", iteration %d", sr.Iteration)
	}
	if current {
		b.WriteString(" <- current")
	}
	b.WriteString("\n")

	describeLine(b, "    ", "Error", sr.Error)
	describeLine(b, "    ", "Started", describeTime(sr.StartedAt))
	describeLine(b, "    ", "Completed", describeTime(sr.CompletedAt))
	describeLine(b, "    ", "Event", describeValue(sr.Event))
	describeLine(b, "    ", "Output", describeValue(sr.Output))

	for i := range sr.Tasks {
		sr.Tasks[i].describe(b)
	}
}

// describe writes one task, with its details on the lines beneath it
func (tr *taskRecord) describe(b *strings.Builder) {
	fmt.Fprintf(b, "    task %d: %s\n", tr.Index, tr.describeOutcome())

	describeLine(b, "      ", "Item", describeValue(tr.Item))
	describeLine(b, "      ", "Output", describeValue(tr.Output))
	if tr.ChildID != "" {
		child := fmt.Sprintf("%s %q", tr.ChildType, tr.ChildID)
		if tr.ChildStatus != "" {
			child += fmt.Sprintf(", ended %s", tr.ChildStatus)
		}
		if tr.ChildCompensation != "" {
			child += fmt.Sprintf(", compensation %s", tr.ChildCompensation)
		}
		describeLine(b, "      ", "Child", child)
	}
	if tr.Comp != nil {
		describeLine(b, "      ", "Undo", tr.Comp.describeOutcome())
	}
	describeLine(b, "      ", "Completed", describeTime(tr.CompletedAt))
}

// describeOutcome summarizes where a task's attempts stand
func (tr *taskRecord) describeOutcome() string {
	switch {
	case tr.Done && tr.Abandoned:
		return "abandoned: " + tr.Error
	case tr.Done && tr.Error != "":
		return fmt.Sprintf("failed on attempt %d: %s", tr.Attempts, tr.Error)
	case tr.Done:
		return fmt.Sprintf("succeeded on attempt %d", tr.Attempts)
	}

	out := fmt.Sprintf("attempt %d dispatched", tr.Attempts)
	if tr.DispatchedAttempt < tr.Attempts {
		out = fmt.Sprintf("attempt %d, dispatch not recorded yet", tr.Attempts)
	}
	if !tr.RetryAt.IsZero() {
		out += ", not before " + describeTime(tr.RetryAt)
	}
	if tr.LastError != "" {
		out += ", last error: " + tr.LastError
	}
	return out
}

// describeOutcome summarizes where a task's compensation stands
func (c *compRecord) describeOutcome() string {
	switch {
	case c.Done && c.Error != "":
		return fmt.Sprintf("failed on attempt %d: %s", c.Attempts, c.Error)
	case c.Done:
		return fmt.Sprintf("succeeded on attempt %d", c.Attempts)
	}

	out := fmt.Sprintf("attempt %d dispatched", c.Attempts)
	if c.DispatchedAttempt < c.Attempts {
		out = fmt.Sprintf("attempt %d, dispatch not recorded yet", c.Attempts)
	}
	if !c.RetryAt.IsZero() {
		out += ", not before " + describeTime(c.RetryAt)
	}
	if c.LastError != "" {
		out += ", last error: " + c.LastError
	}
	return out
}

// describeStatus names an instance status, including the empty one a journal has before its start job ran
func describeStatus(s Status) string {
	if s == "" {
		return "not started"
	}
	return string(s)
}

// describeLine writes one labeled line, and nothing at all for an empty value
func describeLine(b *strings.Builder, indent string, label string, value string) {
	if value == "" {
		return
	}
	fmt.Fprintf(b, "%s%s: %s\n", indent, label, value)
}

// describeTime renders a timestamp in UTC with millisecond precision, and the zero time as empty
func describeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

// describeValue renders a JSON value on one line, shortened when it is longer than describeValueLimit
func describeValue(v json.RawMessage) string {
	if len(v) == 0 {
		return ""
	}

	// Compacting keeps an indented value on one line, and a value that is not valid JSON is printed as it is
	var compact bytes.Buffer
	err := json.Compact(&compact, v)
	out := compact.Bytes()
	if err != nil {
		out = v
	}
	if len(out) <= describeValueLimit {
		return string(out)
	}

	// Cut on a character boundary so a multi-byte character is never split
	cut := describeValueLimit
	for cut > 0 && !utf8.RuneStart(out[cut]) {
		cut--
	}
	return fmt.Sprintf("%s... (%d bytes)", out[:cut], len(v))
}
