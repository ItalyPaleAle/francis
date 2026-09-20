package workflow

import (
	"encoding/json"
)

// buildRunPayload assembles what one task needs to run, from the journal, in memory
// Building a payload is orchestration, while performing the work it describes is a step, and the engine never ships the whole journal to a worker, so a step's data dependencies stay explicit and auditable from the definition alone
func (o *orchestrator) buildRunPayload(st *instanceState, sr *stepRecord, d *stepDef, member *stepDef, tr *taskRecord) runPayload {
	outputs, skipped := o.upstreamOutputs(st, sr, d, member)

	p := runPayload{
		InstanceID:            o.instanceID,
		Workflow:              o.def.name,
		Version:               st.Version,
		DefinitionFingerprint: st.DefinitionFingerprint,
		RegistryGeneration:    st.RegistryGeneration,
		Step:                  sr.Name,
		Index:                 tr.Index,
		Positional:            d.isPositional(),
		Attempt:               tr.Attempts,
		Input:                 st.Input,
		Item:                  tr.Item,
		Outputs:               outputs,
		Skipped:               skipped,
		OrchestratorType:      o.wf.baseType,
		MaxOutputSize:         o.def.maxOutputSize,
		TraceParent:           st.TraceParent,
	}

	// A member of a parallel group runs its own handler, so the worker is told which one rather than inferring it from the position
	if member != d {
		p.Handler = member.name
	}

	return p
}

// upstreamOutputs collects the outputs a task may read: the output of the immediately preceding step, and those of the steps named with WithInputFrom
// A group's member declares its own dependencies alongside the group's, and a task gets both, since the member is what the handler was written against
func (o *orchestrator) upstreamOutputs(st *instanceState, sr *stepRecord, d *stepDef, member *stepDef) (map[string]json.RawMessage, []string) {
	outputs := map[string]json.RawMessage{}
	var skipped []string

	add := func(name string) {
		// The group's dependencies and its member's can name the same step, and a task reads each output once
		_, seen := outputs[name]
		if seen {
			return
		}

		other := st.step(name)
		if other == nil {
			return
		}
		if other.Status == StepSkipped {
			skipped = append(skipped, name)
			outputs[name] = nil
			return
		}
		outputs[name] = o.def.byName[name].stepOutput(other)
	}

	prev := o.precedingStepName(sr.Name)
	// Fan-out tasks already receive their individual item, so shipping the source array implicitly would multiply its size by the fan-out width
	if prev != "" && (d.kind != KindForEach || prev != d.itemsFrom) {
		add(prev)
	}
	for _, name := range d.inputFrom {
		add(name)
	}
	if member != d {
		for _, name := range member.inputFrom {
			add(name)
		}
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
// A child of a fan-out gets its item, so "one child per element" reads the way it looks
// Any other child gets the output of the preceding step, falling back to the parent's own input when there is none
func (o *orchestrator) childInput(st *instanceState, sr *stepRecord, d *stepDef, tr *taskRecord) json.RawMessage {
	if d.kind == KindForEach {
		return tr.Item
	}

	prev := o.precedingStepName(sr.Name)
	if prev != "" {
		other := st.step(prev)
		if other != nil && other.Status != StepSkipped {
			out := o.def.byName[prev].stepOutput(other)
			if len(out) > 0 {
				return out
			}
		}
	}

	return st.Input
}

// isPositional reports whether a step's tasks have siblings to be positioned among, so an index means something to a handler
func (d *stepDef) isPositional() bool {
	switch d.kind {
	case KindParallel, KindForEach:
		return true
	default:
		return false
	}
}
