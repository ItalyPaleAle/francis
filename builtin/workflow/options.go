package workflow

import (
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/metric"
)

const (
	// defaultVersion is the definition version stamped on instances when WithVersion is not set
	defaultVersion = 1
	// defaultTimeout bounds an instance when WithTimeout is not set
	defaultTimeout = time.Hour
	// defaultConcurrency is the strict per-host task limit when WithConcurrency is not set
	defaultConcurrency = 1
	// defaultParkInterval is how long an instance whose version this host cannot serve waits before looking again for a host that can
	defaultParkInterval = 10 * time.Minute
	// defaultMaxDepth bounds the parent chain of child instances, to prevent infinite recursion
	defaultMaxDepth = 8
	// defaultMaxIterations bounds how many times a loop repeats its body when WithMaxIterations is not set
	defaultMaxIterations = 100
	// defaultAutoPurgeInterval is how often terminated instances past their retention are swept when no custom schedule is set
	defaultAutoPurgeInterval = 12 * time.Hour

	// defaultMaxAttempts is how many attempts a forward task gets before it is failed
	defaultMaxAttempts = 3
	// defaultRetryInitial and defaultRetryMax bound the doubling backoff between a forward task's attempts
	defaultRetryInitial = 2 * time.Second
	defaultRetryMax     = time.Minute
	// defaultCompMaxAttempts is how many attempts a compensation gets, higher than the forward policy because a failed rollback leaves the system inconsistent
	defaultCompMaxAttempts = 10
	// defaultCompInitial and defaultCompMax bound the doubling backoff between a compensation's attempts
	defaultCompInitial = 10 * time.Second
	defaultCompMax     = 10 * time.Minute

	// defaultMaxInputSize caps the workflow input, which is shipped in every task's payload
	// 64KB
	defaultMaxInputSize = 64 << 10
	// defaultMaxOutputSize caps a single task's output, as a ceiling against a misbehaving task rather than a per-task budget
	// 16KB
	defaultMaxOutputSize = 16 << 10
	// defaultMaxJournalSize caps the encoded journal, because every report rewrites the whole document
	// 1MB
	defaultMaxJournalSize = 1 << 20

	// defaultRetention is how long a terminated instance is kept when WithRetention names no duration for its status
	defaultRetention = 24 * time.Hour
)

// RetentionPolicy is how long a terminated instance's journal is kept, per terminal status
// A failed run is usually worth keeping longer than a successful one, which is why the durations are separate
type RetentionPolicy struct {
	Completed time.Duration
	Failed    time.Duration
	Cancelled time.Duration
}

// jobRetention is how long the engine's own jobs keep a record once they end, whether they completed or dead-lettered
// It matches the journal's own TTL backstop, so a task's record and the journal entry that accounts for it disappear together, and a dead-lettered job can never be left behind by an instance that expired without ever being purged
func (d *definition) jobRetention() time.Duration {
	longest := max(
		d.retention.forStatus(StatusCompleted),
		d.retention.forStatus(StatusFailed),
		d.retention.forStatus(StatusCancelled),
	)
	return 2 * longest
}

// forStatus returns the retention configured for a terminal status, falling back to the default for one the policy leaves unset
func (r RetentionPolicy) forStatus(s Status) time.Duration {
	var d time.Duration
	switch s {
	case StatusCompleted:
		d = r.Completed
	case StatusFailed:
		d = r.Failed
	case StatusCancelled:
		d = r.Cancelled
	}
	if d <= 0 {
		return defaultRetention
	}
	return d
}

// options holds the configuration of a workflow, built up from the functional Option values passed to New
type options struct {
	version                   int
	timeout                   time.Duration
	retention                 RetentionPolicy
	autoPurgeInterval         time.Duration
	autoPurgeIntervalSet      bool
	autoPurgeCron             string
	autoPurgeCronSet          bool
	concurrency               int
	compensateConcurrency     int
	capabilities              []string
	steps                     []StepSpec
	outputStep                string
	maxInputSize              int
	maxOutputSize             int
	maxJournalSize            int
	maxDepth                  int
	unknownVersion            UnknownVersionPolicy
	compensationFailurePolicy CompensationFailurePolicy
	logger                    *slog.Logger
	meter                     metric.Meter
}

// Option configures a workflow built with New
type Option func(*options)

// WithVersion sets the definition's version, which is stamped on every instance it starts
// Bump it for any change to the graph: the registry refuses a version whose fingerprint does not match the one first registered under that number
func WithVersion(v int) Option {
	return func(o *options) {
		o.version = v
	}
}

// WithTimeout bounds how long an instance may run before it is failed and unwound, defaulting to one hour
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
	}
}

// WithRetention sets how long a terminated instance's journal is kept, per terminal status
func WithRetention(p RetentionPolicy) Option {
	return func(o *options) {
		o.retention = p
	}
}

// WithAutoPurgeInterval sets how often the automatic sweep purges terminated instances past their retention, defaulting to 12 hours
// It is mutually exclusive with WithAutoPurgeCron
func WithAutoPurgeInterval(interval time.Duration) Option {
	return func(o *options) {
		o.autoPurgeInterval = interval
		o.autoPurgeIntervalSet = true
	}
}

// WithAutoPurgeCron sets the cron expression for the automatic sweep that purges terminated instances past their retention
// It is mutually exclusive with WithAutoPurgeInterval
func WithAutoPurgeCron(cronExpr string) Option {
	return func(o *options) {
		o.autoPurgeCron = cronExpr
		o.autoPurgeCronSet = true
	}
}

// WithConcurrency sets the strict maximum number of tasks this host runs at once across every worker queue of the workflow, defaulting to 1
// It is mirrored as the cluster-wide placement hint, so hosts are rarely handed more work than they can run
func WithConcurrency(n int) Option {
	return func(o *options) {
		o.concurrency = n
	}
}

// WithCompensateConcurrency sets the strict per-host budget of the undo queues, defaulting to the same number as WithConcurrency
// The undo types form their own capacity group so a slow unwind cannot starve forward work, or the other way around
func WithCompensateConcurrency(n int) Option {
	return func(o *options) {
		o.compensateConcurrency = n
	}
}

// WithCapability advertises a capability on this host, so steps that require it can run here
// It can be passed more than once, and a host that advertises none still serves every step with no requirement
func WithCapability(capability string) Option {
	return func(o *options) {
		o.capabilities = append(o.capabilities, capability)
	}
}

// WithSteps declares the workflow's graph, in the order the steps run
func WithSteps(steps ...StepSpec) Option {
	return func(o *options) {
		o.steps = append(o.steps, steps...)
	}
}

// WithOutput names the step whose output becomes the instance's output, which otherwise is the output of the last step
// It is what a parent reads back from a child instance
func WithOutput(step string) Option {
	return func(o *options) {
		o.outputStep = step
	}
}

// WithMaxInputSize caps the encoded workflow input, checked at Start, because the input is shipped in every task's payload
func WithMaxInputSize(n int) Option {
	return func(o *options) {
		o.maxInputSize = n
	}
}

// WithMaxOutputSize caps a single task's encoded output, checked on the worker before it reports, so the orchestrator never spends a turn serializing something unbounded
func WithMaxOutputSize(n int) Option {
	return func(o *options) {
		o.maxOutputSize = n
	}
}

// WithMaxJournalSize caps the encoded journal, checked before every state write
// Exceeding it fails the instance, which is a much better outcome than an instance that can no longer persist and therefore can no longer progress
func WithMaxJournalSize(n int) Option {
	return func(o *options) {
		o.maxJournalSize = n
	}
}

// WithMaxDepth bounds the parent chain of child instances, defaulting to 8
// It is the only thing that stops a definition that references itself
func WithMaxDepth(n int) Option {
	return func(o *options) {
		o.maxDepth = n
	}
}

// WithUnknownVersionPolicy decides what the deadline alarm does when it fires on a host that does not have the instance's version, defaulting to ParkUnknownVersion
func WithUnknownVersionPolicy(p UnknownVersionPolicy) Option {
	return func(o *options) {
		o.unknownVersion = p
	}
}

// WithCompensationFailurePolicy decides what a failing compensation costs the rest of the unwind, defaulting to ContinueUnwinding
func WithCompensationFailurePolicy(p CompensationFailurePolicy) Option {
	return func(o *options) {
		o.compensationFailurePolicy = p
	}
}

// WithLogger sets the logger the engine uses for instance and task lifecycle events
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		o.logger = l
	}
}

// WithMeter sets the OpenTelemetry meter the engine records its instruments on
// Without one the instruments are no-ops, so the engine records without nil checks either way
func WithMeter(m metric.Meter) Option {
	return func(o *options) {
		o.meter = m
	}
}
