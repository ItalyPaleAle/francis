package workflow

import (
	"encoding/json"
	"fmt"
	"time"
)

// buildRunPayload assembles what one task needs to run, from the journal, in memory
// Building a payload is orchestration; performing the work it describes is a step, and the engine never ships the whole journal to a worker, so a step's data dependencies stay explicit and auditable from the definition alone (§5.3)
func (o *orchestrator) buildRunPayload(st *instanceState, sr *stepRecord, d *stepDef, member *stepDef, tr *taskRecord, now time.Time) (runPayload, error) {
	outputs, skipped := o.upstreamOutputs(st, sr, d)

	p := runPayload{
		InstanceID:       o.instanceID,
		Workflow:         o.def.name,
		Version:          st.Version,
		Step:             sr.Name,
		Index:            tr.Index,
		Positional:       isPositional(d),
		Attempt:          tr.Attempts,
		Input:            st.Input,
		Item:             tr.Item,
		Outputs:          outputs,
		Skipped:          skipped,
		OrchestratorType: o.wf.baseType,
		MaxOutputSize:    o.def.maxOutputSize,
		TraceParent:      st.TraceParent,
	}

	// A member of a parallel group runs its own handler, so the worker is told which one rather than inferring it from the position
	if member != d {
		p.Handler = member.name
	}

	_ = now
	return p, nil
}

// upstreamOutputs collects the outputs a step's tasks may read: the output of the immediately preceding step, and those of the steps named with WithInputFrom
func (o *orchestrator) upstreamOutputs(st *instanceState, sr *stepRecord, d *stepDef) (map[string]json.RawMessage, []string) {
	outputs := map[string]json.RawMessage{}
	var skipped []string

	add := func(name string) {
		other := st.step(name)
		if other == nil {
			return
		}
		if other.Status == StepSkipped {
			skipped = append(skipped, name)
			outputs[name] = nil
			return
		}
		outputs[name] = stepOutput(other, o.def.byName[name])
	}

	prev := o.precedingStepName(sr.Name)
	if prev != "" {
		add(prev)
	}
	for _, name := range d.inputFrom {
		add(name)
	}

	if len(outputs) == 0 {
		return nil, nil
	}
	return outputs, skipped
}

// precedingStepName returns the top-level step declared immediately before a step, or an empty string for the first one
func (o *orchestrator) precedingStepName(name string) string {
	pos, ok := o.def.order[name]
	if !ok || pos == 0 {
		return ""
	}
	return o.def.steps[pos-1].name
}

// childInput returns what a child instance receives as its workflow input
// A child of a fan-out gets its item, which is what makes "one child per element" read the way it looks; any other child gets the output of the preceding step, falling back to the parent's own input when there is none
func (o *orchestrator) childInput(st *instanceState, sr *stepRecord, d *stepDef, tr *taskRecord) (json.RawMessage, error) {
	if d.kind == KindForEach {
		return tr.Item, nil
	}

	prev := o.precedingStepName(sr.Name)
	if prev != "" {
		other := st.step(prev)
		if other != nil && other.Status != StepSkipped {
			out := stepOutput(other, o.def.byName[prev])
			if len(out) > 0 {
				return out, nil
			}
		}
	}

	return st.Input, nil
}

// isPositional reports whether a step's tasks have siblings to be positioned among, which is what makes an index meaningful to a handler
func isPositional(d *stepDef) bool {
	switch d.kind {
	case KindParallel, KindForEach:
		return true
	default:
		return false
	}
}

// isoInterval renders a duration as the ISO8601 form the job scheduler takes
func isoInterval(d time.Duration) string {
	return fmt.Sprintf("PT%dS", int(d.Round(time.Second).Seconds()))
}
