package workflow

import (
	"go.opentelemetry.io/otel/metric"
)

// engineMetrics holds the OpenTelemetry instruments the workflow engine records
// A no-op meter yields no-op instruments, so the engine records without nil checks
type engineMetrics struct {
	// instancesStarted counts instances that began running
	instancesStarted metric.Int64Counter
	// instancesTerminated counts instances that reached a terminal status, tagged by that status
	instancesTerminated metric.Int64Counter
	// instancesRunning tracks how many instances are currently running on this host's view
	instancesRunning metric.Int64UpDownCounter
	// instanceDuration records how long an instance took from start to termination, in seconds
	instanceDuration metric.Float64Histogram
	// stepDuration records how long a step took, tagged by step name and outcome, in seconds
	stepDuration metric.Float64Histogram
	// taskAttempts counts attempts made, tagged by step name and whether the attempt failed
	taskAttempts metric.Int64Counter
	// transportFailures counts attempts that failed because a report could not be delivered, rather than because the handler failed
	transportFailures metric.Int64Counter
	// compensationsRun and compensationsFailed count the unwind's work
	compensationsRun    metric.Int64Counter
	compensationsFailed metric.Int64Counter
	// instancesSuspended counts suspensions
	instancesSuspended metric.Int64Counter
	// childrenStarted counts child instances started from a parent
	childrenStarted metric.Int64Counter
	// instancesPurged counts instances removed by an explicit purge or the sweep
	instancesPurged metric.Int64Counter
	// turnDuration records how long a Workflow turn took, in seconds
	// It should sit in single-digit milliseconds, and a regression is the signal that something has been inlined onto the orchestrator that should be a step (§13.4)
	turnDuration metric.Float64Histogram
	// duplicateEvents counts turns that re-applied an already-recorded event, which is the direct measure of how often ordering invariant 2 is doing its job
	duplicateEvents metric.Int64Counter
	// definitionConflicts counts jobs declined because this host's graph does not match the one registered for the version
	definitionConflicts metric.Int64Counter
}

// newEngineMetrics creates the engine's instruments from the meter
func newEngineMetrics(meter metric.Meter) (*engineMetrics, error) {
	m := &engineMetrics{}

	var err error
	m.instancesStarted, err = meter.Int64Counter("francis.workflow.instances.started",
		metric.WithDescription("Number of workflow instances started"))
	if err != nil {
		return nil, err
	}

	m.instancesTerminated, err = meter.Int64Counter("francis.workflow.instances.terminated",
		metric.WithDescription("Number of workflow instances that reached a terminal status"))
	if err != nil {
		return nil, err
	}

	m.instancesRunning, err = meter.Int64UpDownCounter("francis.workflow.instances.running",
		metric.WithDescription("Number of workflow instances currently running"))
	if err != nil {
		return nil, err
	}

	m.instanceDuration, err = meter.Float64Histogram("francis.workflow.instance.duration",
		metric.WithDescription("Duration of a workflow instance from start to termination"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.stepDuration, err = meter.Float64Histogram("francis.workflow.step.duration",
		metric.WithDescription("Duration of a workflow step"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.taskAttempts, err = meter.Int64Counter("francis.workflow.task.attempts",
		metric.WithDescription("Number of task attempts made"))
	if err != nil {
		return nil, err
	}

	m.transportFailures, err = meter.Int64Counter("francis.workflow.task.transport_failures",
		metric.WithDescription("Number of attempts that failed because the report could not be delivered"))
	if err != nil {
		return nil, err
	}

	m.compensationsRun, err = meter.Int64Counter("francis.workflow.compensations.run",
		metric.WithDescription("Number of compensations run"))
	if err != nil {
		return nil, err
	}

	m.compensationsFailed, err = meter.Int64Counter("francis.workflow.compensations.failed",
		metric.WithDescription("Number of compensations that failed for good"))
	if err != nil {
		return nil, err
	}

	m.instancesSuspended, err = meter.Int64Counter("francis.workflow.instances.suspended",
		metric.WithDescription("Number of times an instance was suspended"))
	if err != nil {
		return nil, err
	}

	m.childrenStarted, err = meter.Int64Counter("francis.workflow.children.started",
		metric.WithDescription("Number of child instances started"))
	if err != nil {
		return nil, err
	}

	m.instancesPurged, err = meter.Int64Counter("francis.workflow.instances.purged",
		metric.WithDescription("Number of instances purged"))
	if err != nil {
		return nil, err
	}

	m.turnDuration, err = meter.Float64Histogram("francis.workflow.turn.duration",
		metric.WithDescription("Duration of a Workflow orchestrator turn"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}

	m.duplicateEvents, err = meter.Int64Counter("francis.workflow.turns.duplicate_events",
		metric.WithDescription("Number of turns that re-applied an already-recorded event"))
	if err != nil {
		return nil, err
	}

	m.definitionConflicts, err = meter.Int64Counter("francis.workflow.definition.conflicts",
		metric.WithDescription("Number of jobs declined because this host's definition does not match the registered one"))
	if err != nil {
		return nil, err
	}

	return m, nil
}
