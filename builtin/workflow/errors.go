package workflow

import "errors"

var (
	// ErrInstanceNotFound is returned when no instance with the given ID exists, or its journal has passed its retention
	ErrInstanceNotFound = errors.New("workflow instance not found")
	// ErrInstanceActive is returned by Purge when the instance has not terminated yet
	ErrInstanceActive = errors.New("workflow instance is still active")
	// ErrInstanceTerminated is returned when an operation only a running instance accepts is called on one that has terminated
	ErrInstanceTerminated = errors.New("workflow instance has terminated")
	// ErrStepSkipped is returned by Task.DecodeOutput when the named step was skipped and therefore produced no output
	ErrStepSkipped = errors.New("step was skipped")
	// ErrStepNotFound is returned by Task.DecodeOutput when the named step is not one this task may read
	ErrStepNotFound = errors.New("step output is not available to this task")
	// ErrInputTooLarge is returned by Start when the encoded input exceeds WithMaxInputSize
	ErrInputTooLarge = errors.New("workflow input is too large")
	// ErrOutputTooLarge is returned to a handler's attempt when its encoded output exceeds WithMaxOutputSize
	ErrOutputTooLarge = errors.New("task output is too large")
	// ErrJournalTooLarge is the cause recorded when an instance is failed because its journal outgrew WithMaxJournalSize
	ErrJournalTooLarge = errors.New("workflow journal is too large")
	// ErrMaxDepthExceeded is returned when starting a child whose parent chain is deeper than WithMaxDepth
	ErrMaxDepthExceeded = errors.New("workflow child depth limit exceeded")
	// ErrNoSuchEvent is returned by RaiseEvent when the definition has no WaitForEvent step listening for the event
	ErrNoSuchEvent = errors.New("workflow has no step waiting for this event")
	// ErrDefinitionConflict is returned by the registry when a version is already recorded with a different fingerprint
	ErrDefinitionConflict = errors.New("workflow definition conflicts with the one registered for this version")
	// ErrVersionInUse is returned by ForgetVersion when instances of the version still exist
	ErrVersionInUse = errors.New("workflow version still has instances")

	// errWaitingForStart is returned by a turn whose event arrived before the start job created the journal
	// It is an ordinary error rather than a permanent one, so Francis retries the job and the control lands once the instance exists
	errWaitingForStart = errors.New("workflow instance has not started yet")

	// errVersionNotServed is returned by a turn whose durable input names a version this host does not serve
	// It never reaches a caller: a job is re-routed with actor.ErrJobRejected, and a deadline follows the definition's unknown-version policy instead
	errVersionNotServed = errors.New("workflow instance version is not served by this host")
)
