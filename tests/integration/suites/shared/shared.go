//go:build integration

// Package shared holds small helpers reused across integration scenarios
package shared

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/italypaleale/francis/actor"
	"github.com/italypaleale/francis/internal/actorcore"
	timeutils "github.com/italypaleale/francis/internal/time"
	frameworkhost "github.com/italypaleale/francis/tests/integration/framework/process/host"
)

// CounterActorType is the registered type name for the counter actor
const CounterActorType = "counter"

// CounterState is the persisted state of the counter actor
type CounterState struct {
	N int64 `json:"n"`
}

// CounterResult is returned by the counter actor's increment method
type CounterResult struct {
	N int64 `json:"n"`
}

// CounterActor is a trivial actor that persists a monotonically increasing counter
// It exercises state read and write through whichever provider and runtime are in use
type CounterActor struct {
	client actor.Client[CounterState]
}

// NewCounterActor is the actor.Factory for CounterActor
func NewCounterActor(actorID string, service *actor.Service) actor.Actor {
	return &CounterActor{
		client: actor.NewActorClient[CounterState](CounterActorType, actorID, service),
	}
}

// Invoke handles the increment method by loading state, incrementing, persisting, and returning the new value
func (a *CounterActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	state, err := a.client.GetState(ctx)
	if err != nil {
		return nil, err
	}

	if method == "increment" {
		state.N++
	}

	err = a.client.SetState(ctx, state, nil)
	if err != nil {
		return nil, err
	}

	return CounterResult(state), nil
}

// CounterReg returns the registration for the counter actor with the given idle timeout
func CounterReg(idle time.Duration) frameworkhost.ActorReg {
	return frameworkhost.ActorReg{
		Type:    CounterActorType,
		Factory: NewCounterActor,
		Opts:    actorcore.RegisterActorOptions{IdleTimeout: idle},
	}
}

// ProbeActorType is the registered type name for the probe actor
const ProbeActorType = "probe"

// Probe method names understood by the probe actor's Invoke
const (
	// ProbeMethodIncrement loads, increments, and persists the counter, returning the new value
	ProbeMethodIncrement = "increment"
	// ProbeMethodGet returns the persisted state without mutating it
	ProbeMethodGet = "get"
	// ProbeMethodPing returns a constant without touching state, so it activates an actor cheaply
	ProbeMethodPing = "ping"
	// ProbeMethodHold occupies the actor's turn for a short time, so overlapping calls to the same actor would be observable if turn-based concurrency were violated
	ProbeMethodHold = "hold"
	// ProbeMethodArmAlarm schedules an alarm on the actor itself through its client, so alarm scheduling from inside an invocation can be exercised
	ProbeMethodArmAlarm = "arm-alarm"
	// ProbeMethodFail returns an error, so error propagation back to the caller can be exercised
	ProbeMethodFail = "fail"
)

// ProbeFailMessage is the error text the probe returns for ProbeMethodFail, so callers can assert it propagated
const ProbeFailMessage = "induced invoke failure"

// ProbeSelfAlarmName is the name of the alarm the probe actor schedules on itself for ProbeMethodArmAlarm
const ProbeSelfAlarmName = "self"

// ProbeState is the persisted state of the probe actor
type ProbeState struct {
	N int64 `json:"n"`
}

// ProbeActor is a versatile actor used by the alarm, invocation, and state scenarios
// It persists a counter like the counter actor, but also records alarm executions and per-actor invocation concurrency into the process-global ProbeObserver so tests can assert on behavior that happens in background goroutines
type ProbeActor struct {
	actorID string
	service *actor.Service
	client  actor.Client[ProbeState]
}

// NewProbeActor is the actor.Factory for ProbeActor
func NewProbeActor(actorID string, service *actor.Service) actor.Actor {
	return &ProbeActor{
		actorID: actorID,
		service: service,
		client:  actor.NewActorClient[ProbeState](ProbeActorType, actorID, service),
	}
}

// Invoke dispatches the probe methods used across scenarios
func (a *ProbeActor) Invoke(ctx context.Context, method string, _ actor.Envelope) (any, error) {
	// Record which host ran this invocation so placement and failover scenarios can observe where the actor lives
	ProbeObserver.recordInvokeHost(a.actorID, HostLabel(a.service))

	switch method {
	case ProbeMethodPing:
		return "pong", nil

	case ProbeMethodFail:
		// Return an error so the caller can assert it propagates back, including across a peer or runtime boundary
		return nil, errors.New(ProbeFailMessage)

	case ProbeMethodHold:
		// Track concurrent turns for this actor, then hold the turn briefly
		// With turn-based concurrency the observed maximum must never exceed one
		ProbeObserver.enterHold(a.actorID)
		defer ProbeObserver.exitHold(a.actorID)
		time.Sleep(40 * time.Millisecond)
		return nil, nil

	case ProbeMethodArmAlarm:
		// Schedule an alarm on this actor through its client, due immediately, so it fires shortly after this invocation returns
		err := a.client.SetAlarm(ctx, ProbeSelfAlarmName, actor.AlarmProperties{DueTime: time.Now()})
		if err != nil {
			return nil, err
		}
		return nil, nil

	case ProbeMethodIncrement, ProbeMethodGet:
		state, err := a.client.GetState(ctx)
		if err != nil {
			return nil, err
		}

		if method == ProbeMethodIncrement {
			state.N++
			err = a.client.SetState(ctx, state, nil)
			if err != nil {
				return nil, err
			}
		}

		return state, nil

	default:
		return nil, errors.New("unknown probe method: " + method)
	}
}

// Alarm records the execution into the observer and fails when the actor has a fault configured, so alarm retry and failure handling can be exercised end to end
func (a *ProbeActor) Alarm(_ context.Context, name string, data actor.Envelope) error {
	// Decode any associated data so the observer can record what the alarm carried
	var payload string
	if data != nil {
		_ = data.Decode(&payload)
	}

	// Record which host ran this alarm so cross-host and migration scenarios can observe where it executed
	ProbeObserver.recordAlarmHost(a.actorID, HostLabel(a.service))

	// Recording also decides whether this execution should fail, consuming one transient failure if configured
	fail := ProbeObserver.recordAlarm(a.actorID, name, payload)
	if fail {
		return errors.New("induced alarm failure")
	}

	// When asked, occupy the actor's turn for a moment so a test can detect any overlap between an alarm execution and a concurrent invocation
	// Alarm execution and invocation share the actor's turn-based lock, so the observed maximum must never exceed one
	if ProbeObserver.alarmHoldEnabled(a.actorID) {
		ProbeObserver.enterHold(a.actorID)
		defer ProbeObserver.exitHold(a.actorID)
		time.Sleep(40 * time.Millisecond)
	}
	return nil
}

// Job records the execution into the observer and fails when the actor has a job fault configured, so job retry, dead-letter, and permanent-failure handling can be exercised end to end
func (a *ProbeActor) Job(_ context.Context, method string, data actor.Envelope) error {
	// Decode any associated data so the observer can record what the job carried
	var payload string
	if data != nil {
		_ = data.Decode(&payload)
	}

	// Recording decides whether this execution should fail, and whether it should fail permanently
	fail, permanent := ProbeObserver.recordJob(a.actorID, method, payload)
	if permanent {
		// Wrap the sentinel so the engine dead-letters the job immediately, skipping the remaining retries
		return fmt.Errorf("induced permanent job failure: %w", actor.ErrJobPermanentFailure)
	} else if fail {
		return errors.New("induced job failure")
	}
	return nil
}

// JobFailed records that a job was dead-lettered, so dead-letter scenarios can observe the reaction hook
func (a *ProbeActor) JobFailed(_ context.Context, jobID string, method string, _ actor.Envelope, _ error) error {
	ProbeObserver.recordJobFailed(a.actorID, jobID, method)
	return nil
}

// Deactivate records that the actor was deactivated, so lifecycle scenarios can observe idle and halt-driven deactivation
func (a *ProbeActor) Deactivate(_ context.Context) error {
	ProbeObserver.recordDeactivate(a.actorID)
	return nil
}

// ProbeStreamContentType is the content type the probe sets on a streamed response, so callers can assert it round-tripped
const ProbeStreamContentType = "application/x-probe-echo"

// InvokeStream echoes the streamed request body back, prefixed with the method name, so streaming round-trips can be exercised end to end
func (a *ProbeActor) InvokeStream(_ context.Context, method string, _ string, body io.Reader, w actor.StreamResponseWriter) error {
	// Read the whole request body, which streams in over the transport
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	// Set the content type before the first write, then echo the method and the body back as the response
	w.SetContentType(ProbeStreamContentType)
	_, err = w.Write([]byte(method + ":"))
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

// ProbeReg returns the registration for the probe actor with the given options
func ProbeReg(opts actorcore.RegisterActorOptions) frameworkhost.ActorReg {
	return frameworkhost.ActorReg{
		Type:    ProbeActorType,
		Factory: NewProbeActor,
		Opts:    opts,
	}
}

// AlarmFire is a single recorded execution of a probe actor's alarm
type AlarmFire struct {
	// Name is the alarm name passed to the Alarm method
	Name string
	// Data is the decoded string payload, empty when the alarm carried no data
	Data string
	// Failed reports whether this execution returned an induced failure
	Failed bool
}

// ProbeObserver is the process-global registry the probe actor writes to
// Integration scenarios run the host in the same process as the test, so background alarm executions and concurrent invocations can be observed here rather than through the provider
// Scenarios must use unique actor IDs so their observations do not collide, since the suite shares one process across all scenarios
// hostLabels maps a host's actor service to a stable label, so the probe can record which host ran an invocation or alarm
// The factory receives the same *actor.Service that the host exposes, so a test can label each host's service and then read back where an actor was placed
var hostLabels sync.Map

// SetHostLabel associates a label with a host's actor service, for placement and failover assertions
func SetHostLabel(svc *actor.Service, label string) {
	hostLabels.Store(svc, label)
}

// HostLabel returns the label registered for a host's actor service, or an empty string if none was set
func HostLabel(svc *actor.Service) string {
	v, ok := hostLabels.Load(svc)
	if !ok {
		return ""
	}
	vStr, _ := v.(string)
	return vStr
}

var ProbeObserver = &probeObserver{
	fires:         map[string][]AlarmFire{},
	faults:        map[string]int{},
	holdNow:       map[string]int{},
	holdMax:       map[string]int{},
	alarmHold:     map[string]bool{},
	invokeHost:    map[string]string{},
	alarmHost:     map[string]string{},
	deactivations: map[string]int{},
	jobFires:      map[string][]JobFire{},
	jobFaults:     map[string]int{},
	jobPermanent:  map[string]bool{},
	jobFailed:     map[string]int{},
}

// JobFire is a single recorded execution of a probe actor's job
type JobFire struct {
	// Method is the job method passed to the Job method
	Method string
	// Data is the decoded string payload, empty when the job carried no data
	Data string
	// Failed reports whether this execution returned an induced failure
	Failed bool
}

type probeObserver struct {
	mu sync.Mutex
	// fires records every alarm execution keyed by actor ID, in order
	fires map[string][]AlarmFire
	// faults holds the remaining induced failures keyed by actor ID: a positive value fails that many more executions, a negative value fails every execution
	faults map[string]int
	// holdNow and holdMax track current and peak concurrent hold invocations keyed by actor ID
	holdNow map[string]int
	holdMax map[string]int
	// alarmHold marks actors whose alarm execution should also occupy the hold gauge, so alarm-versus-invocation overlap can be detected
	alarmHold map[string]bool
	// globalNow and globalMax track current and peak concurrent hold turns across all actors, so cross-actor parallelism can be measured
	globalNow int
	globalMax int
	// invokeHost and alarmHost record the label of the host that last ran an invocation or alarm for an actor
	invokeHost map[string]string
	alarmHost  map[string]string
	// deactivations counts how many times an actor's Deactivate hook has run, keyed by actor ID
	deactivations map[string]int
	// jobFires records every job execution keyed by actor ID, in order
	jobFires map[string][]JobFire
	// jobFaults holds the remaining induced job failures keyed by actor ID: positive fails that many more executions, negative fails every execution
	jobFaults map[string]int
	// jobPermanent marks actors whose next job execution returns ErrJobPermanentFailure, consumed once
	jobPermanent map[string]bool
	// jobFailed counts how many times an actor's JobFailed hook has run, keyed by actor ID
	jobFailed map[string]int
}

// SetJobFault configures induced job failures for an actor before its job runs
// A positive count fails that many executions before succeeding, modelling a transient fault, while a negative count fails every execution, modelling a persistent fault
func (o *probeObserver) SetJobFault(actorID string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.jobFaults[actorID] = count
}

// SetJobPermanentFailure marks an actor so its next job execution returns ErrJobPermanentFailure, dead-lettering it immediately
func (o *probeObserver) SetJobPermanentFailure(actorID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.jobPermanent[actorID] = true
}

// recordJob appends an execution for the actor and reports whether it should fail and whether that failure is permanent
func (o *probeObserver) recordJob(actorID string, method string, data string) (fail bool, permanent bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// A permanent failure is consumed once and skips the transient fault logic
	if o.jobPermanent[actorID] {
		permanent = true
		delete(o.jobPermanent, actorID)
	} else {
		remaining, ok := o.jobFaults[actorID]
		switch {
		case ok && remaining < 0:
			// Persistent fault: always fail
			fail = true
		case ok && remaining > 0:
			// Transient fault: fail and consume one
			fail = true
			o.jobFaults[actorID] = remaining - 1
		}
	}

	o.jobFires[actorID] = append(o.jobFires[actorID], JobFire{
		Method: method,
		Data:   data,
		Failed: fail || permanent,
	})
	return fail, permanent
}

// JobFires returns a copy of the recorded job executions for an actor
func (o *probeObserver) JobFires(actorID string) []JobFire {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]JobFire, len(o.jobFires[actorID]))
	copy(out, o.jobFires[actorID])
	return out
}

// JobCount returns how many times an actor's job has executed
func (o *probeObserver) JobCount(actorID string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.jobFires[actorID])
}

// recordJobFailed notes that an actor's JobFailed hook ran
func (o *probeObserver) recordJobFailed(actorID string, _ string, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.jobFailed[actorID]++
}

// JobFailedCount returns how many times an actor's JobFailed hook has run
func (o *probeObserver) JobFailedCount(actorID string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.jobFailed[actorID]
}

// recordDeactivate notes that an actor's Deactivate hook ran
func (o *probeObserver) recordDeactivate(actorID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.deactivations[actorID]++
}

// DeactivateCount returns how many times an actor has been deactivated
func (o *probeObserver) DeactivateCount(actorID string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.deactivations[actorID]
}

// recordInvokeHost notes the host that ran an invocation for an actor
func (o *probeObserver) recordInvokeHost(actorID string, label string) {
	if label == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.invokeHost[actorID] = label
}

// LastInvokeHost returns the label of the host that most recently ran an invocation for an actor
func (o *probeObserver) LastInvokeHost(actorID string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.invokeHost[actorID]
}

// recordAlarmHost notes the host that ran an alarm for an actor
func (o *probeObserver) recordAlarmHost(actorID string, label string) {
	if label == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.alarmHost[actorID] = label
}

// LastAlarmHost returns the label of the host that most recently ran an alarm for an actor
func (o *probeObserver) LastAlarmHost(actorID string) string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.alarmHost[actorID]
}

// SetAlarmFault configures induced alarm failures for an actor before its alarm is triggered
// A positive count fails that many executions before succeeding, modelling a transient fault, while a negative count fails every execution, modelling a persistent fault
func (o *probeObserver) SetAlarmFault(actorID string, count int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.faults[actorID] = count
}

// recordAlarm appends an execution for the actor and reports whether it should fail, consuming one transient failure when configured
func (o *probeObserver) recordAlarm(actorID string, name string, data string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Decide failure from the configured fault, consuming one transient failure
	fail := false
	remaining, ok := o.faults[actorID]
	switch {
	case ok && remaining < 0:
		// Persistent fault: always fail
		fail = true
	case ok && remaining > 0:
		// Transient fault: fail and consume one
		fail = true
		o.faults[actorID] = remaining - 1
	}

	o.fires[actorID] = append(o.fires[actorID], AlarmFire{Name: name, Data: data, Failed: fail})
	return fail
}

// AlarmFires returns a copy of the recorded executions for an actor
func (o *probeObserver) AlarmFires(actorID string) []AlarmFire {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]AlarmFire, len(o.fires[actorID]))
	copy(out, o.fires[actorID])
	return out
}

// AlarmCount returns how many times an actor's alarm has executed
func (o *probeObserver) AlarmCount(actorID string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.fires[actorID])
}

// enterHold records the start of a hold turn, updating both the per-actor and the global peak concurrency
func (o *probeObserver) enterHold(actorID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.holdNow[actorID]++
	if o.holdNow[actorID] > o.holdMax[actorID] {
		o.holdMax[actorID] = o.holdNow[actorID]
	}
	o.globalNow++
	if o.globalNow > o.globalMax {
		o.globalMax = o.globalNow
	}
}

// exitHold records the end of a hold turn
func (o *probeObserver) exitHold(actorID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.holdNow[actorID]--
	o.globalNow--
}

// MaxHoldConcurrency returns the peak number of hold turns that ran at once for an actor
func (o *probeObserver) MaxHoldConcurrency(actorID string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.holdMax[actorID]
}

// ResetGlobalConcurrency clears the global hold gauge so a test can measure cross-actor parallelism in isolation
// The global gauge is process-wide and otherwise monotonic, so a test that asserts on it must reset first
func (o *probeObserver) ResetGlobalConcurrency() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.globalNow = 0
	o.globalMax = 0
}

// MaxGlobalConcurrency returns the peak number of hold turns that ran at once across all actors
func (o *probeObserver) MaxGlobalConcurrency() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.globalMax
}

// SetAlarmHold marks an actor so its alarm execution also occupies the hold gauge, letting a test detect overlap between an alarm and concurrent invocations
func (o *probeObserver) SetAlarmHold(actorID string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.alarmHold[actorID] = true
}

// alarmHoldEnabled reports whether an actor's alarm execution should occupy the hold gauge
func (o *probeObserver) alarmHoldEnabled(actorID string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.alarmHold[actorID]
}

// ISOInterval renders a Go duration as the ISO8601 string Francis expects for an alarm's repetition interval
// Alarm intervals constructed directly in Go bypass the JSON normalization the public API performs, so scenarios use this to produce a value NextExecution can parse
func ISOInterval(d time.Duration) string {
	return timeutils.Duration{Time: d}.String()
}
