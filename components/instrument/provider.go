// Package instrument provides an instrumented decorator for actor providers
//
// WrapProvider wraps a components.ActorProvider so every provider method call (health checks, lease renewals, lookups, and so on) emits an OpenTelemetry span, with optional duration logging
// Spans are always emitted and are no-ops until the application configures an OpenTelemetry provider
//
// Statement-level SQL instrumentation (spans and query logs for individual statements) lives in github.com/italypaleale/go-sql-utils/instrument and its driver-specific subpackages, which the providers apply automatically when they open the database connection themselves
package instrument

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/italypaleale/francis/components"
	"github.com/italypaleale/francis/internal/ref"
	"github.com/italypaleale/francis/internal/tracing"
)

// providerWrapper decorates a components.ActorProvider, emitting one span per method call and, optionally, logging method durations
// Unlike the statement-level instrumentation, this works with any provider regardless of how its database connection was created
type providerWrapper struct {
	base          components.ActorProvider
	log           *slog.Logger
	cfg           components.OperationLogConfig
	logConfigured bool
}

type operationDisposition uint8

const (
	operationSuccess operationDisposition = iota
	operationExpected
	operationWarning
	operationFailure
)

// WrapProvider returns an ActorProvider that emits a span for every provider method call, with the duration of each call logged according to cfg
// The returned provider delegates every method to p, and Close still only closes the resources p owns
// Wrapping an already-wrapped provider is a no-op, so callers can wrap defensively
func WrapProvider(p components.ActorProvider, log *slog.Logger, cfg components.OperationLogConfig) components.ActorProvider {
	_, ok := p.(*providerWrapper)
	if ok {
		return p
	}

	return &providerWrapper{
		base:          p,
		log:           log,
		cfg:           cfg,
		logConfigured: log != nil && (cfg.Enabled || cfg.SlowThreshold > 0),
	}
}

// UnwrapProvider returns the provider wrapped by WrapProvider, reporting whether p was wrapped at all
// A p that was never wrapped is returned as-is with ok = false
func UnwrapProvider(p components.ActorProvider) (components.ActorProvider, bool) {
	w, ok := p.(*providerWrapper)
	if !ok {
		return p, false
	}

	return w.base, true
}

// beginOp starts the span and starts the optional log clock only when operation logging is configured
//
//nolint:spancheck // the span is ended by finishOp, which callers invoke via defer
func (w *providerWrapper) beginOp(ctx context.Context, method string) (context.Context, trace.Span, time.Time) {
	spanCtx, span := tracing.Start(ctx, "provider."+method,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(tracing.ProviderMethod(method)),
	)

	if !w.logConfigured {
		return spanCtx, span, time.Time{}
	}

	return spanCtx, span, time.Now()
}

// finishOp applies the centralized outcome policy to the span and optional operation log
func (w *providerWrapper) finishOp(ctx context.Context, span trace.Span, method string, start time.Time, err error) {
	disposition := classifyOperation(method, err)
	if disposition == operationWarning || disposition == operationFailure {
		tracing.End(span, err)
	} else {
		span.End()
	}

	if !w.logConfigured {
		return
	}

	dur := time.Since(start)
	var (
		level   slog.Level
		message string
	)
	switch {
	case disposition == operationWarning:
		level = slog.LevelWarn
		message = "Provider operation warning"
	case disposition == operationFailure:
		level = slog.LevelError
		message = "Provider operation failed"
	case w.cfg.SlowThreshold > 0 && dur >= w.cfg.SlowThreshold:
		level = slog.LevelWarn
		message = "Slow provider operation"
	case w.cfg.Enabled:
		level = slog.LevelDebug
		message = "Executed provider operation"
	default:
		return
	}

	if !w.log.Enabled(ctx, level) {
		return
	}

	attrs := make([]slog.Attr, 0, 3)
	attrs = append(attrs,
		slog.String("method", method),
		slog.Duration("duration", dur),
	)
	if disposition == operationWarning || disposition == operationFailure {
		attrs = append(attrs, slog.Any("error", err))
	}

	w.log.LogAttrs(ctx, level, message, attrs...)
}

// classifyOperation maps provider domain outcomes to one consistent span and log policy
func classifyOperation(method string, err error) operationDisposition {
	if err == nil {
		return operationSuccess
	}

	switch method {
	case "LookupActor":
		if errors.Is(err, components.ErrNoHost) {
			return operationWarning
		} else if errors.Is(err, components.ErrNoActor) {
			return operationExpected
		}
	case "Restore":
		if errors.Is(err, components.ErrHostsConnected) {
			return operationWarning
		}
	case "UnregisterHost":
		if errors.Is(err, components.ErrHostUnregistered) {
			return operationExpected
		}
	case "RemoveActor":
		if errors.Is(err, components.ErrNoActor) {
			return operationExpected
		}
	case "GetAlarm", "DeleteAlarm", "DeadLetterAlarm", "ReleaseAlarmLease", "GetLeasedAlarm", "UpdateLeasedAlarm", "DeleteLeasedAlarm":
		if errors.Is(err, components.ErrNoAlarm) {
			return operationExpected
		}
	case "GetJob", "CancelJob", "GetDeadJob", "DeleteDeadJob", "RetryDeadJob":
		if errors.Is(err, components.ErrNoJob) {
			return operationExpected
		}
	case "GetState", "DeleteState":
		if errors.Is(err, components.ErrNoState) {
			return operationExpected
		}
	case "AcquireExclusiveLease":
		if errors.Is(err, components.ErrExclusiveHeld) {
			return operationExpected
		}
	}

	return operationFailure
}

// Init implements components.ActorProvider
func (w *providerWrapper) Init(ctx context.Context) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "Init")
	err = w.base.Init(spanCtx)
	w.finishOp(spanCtx, span, "Init", start, err)

	return err
}

// Run implements components.ActorProvider
// Run is deliberately not traced: it blocks until shutdown, so a span would last the process lifetime
func (w *providerWrapper) Run(ctx context.Context) error {
	return w.base.Run(ctx)
}

// Close implements components.ActorProvider
func (w *providerWrapper) Close() (err error) {
	ctx, span, start := w.beginOp(context.Background(), "Close")
	err = w.base.Close()
	w.finishOp(ctx, span, "Close", start, err)

	return err
}

// RegisterHost implements components.ActorProvider
func (w *providerWrapper) RegisterHost(ctx context.Context, req components.RegisterHostReq) (res components.RegisterHostRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "RegisterHost")
	res, err = w.base.RegisterHost(spanCtx, req)
	w.finishOp(spanCtx, span, "RegisterHost", start, err)

	return res, err
}

// UpdateActorHost implements components.ActorProvider
func (w *providerWrapper) UpdateActorHost(ctx context.Context, hostID string, req components.UpdateActorHostReq) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "UpdateActorHost")
	err = w.base.UpdateActorHost(spanCtx, hostID, req)
	w.finishOp(spanCtx, span, "UpdateActorHost", start, err)

	return err
}

// UnregisterHost implements components.ActorProvider
func (w *providerWrapper) UnregisterHost(ctx context.Context, hostID string) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "UnregisterHost")
	err = w.base.UnregisterHost(spanCtx, hostID)
	w.finishOp(spanCtx, span, "UnregisterHost", start, err)

	return err
}

// ListHosts implements components.ActorProvider
func (w *providerWrapper) ListHosts(ctx context.Context) (res []components.HostInfo, err error) {
	spanCtx, span, start := w.beginOp(ctx, "ListHosts")
	res, err = w.base.ListHosts(spanCtx)
	w.finishOp(spanCtx, span, "ListHosts", start, err)

	return res, err
}

// LookupActor implements components.ActorProvider
func (w *providerWrapper) LookupActor(ctx context.Context, actorRef ref.ActorRef, opts components.LookupActorOpts) (res components.LookupActorRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "LookupActor")
	res, err = w.base.LookupActor(spanCtx, actorRef, opts)
	w.finishOp(spanCtx, span, "LookupActor", start, err)

	return res, err
}

// RemoveActor implements components.ActorProvider
func (w *providerWrapper) RemoveActor(ctx context.Context, actorRef ref.ActorRef) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "RemoveActor")
	err = w.base.RemoveActor(spanCtx, actorRef)
	w.finishOp(spanCtx, span, "RemoveActor", start, err)

	return err
}

// GetAlarm implements components.ActorProvider
func (w *providerWrapper) GetAlarm(ctx context.Context, alarmRef ref.AlarmRef) (res components.GetAlarmRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "GetAlarm")
	res, err = w.base.GetAlarm(spanCtx, alarmRef)
	w.finishOp(spanCtx, span, "GetAlarm", start, err)

	return res, err
}

// SetAlarm implements components.ActorProvider
func (w *providerWrapper) SetAlarm(ctx context.Context, alarmRef ref.AlarmRef, req components.SetAlarmReq) (res *ref.AlarmLease, err error) {
	spanCtx, span, start := w.beginOp(ctx, "SetAlarm")
	res, err = w.base.SetAlarm(spanCtx, alarmRef, req)
	w.finishOp(spanCtx, span, "SetAlarm", start, err)

	return res, err
}

// DeleteAlarm implements components.ActorProvider
func (w *providerWrapper) DeleteAlarm(ctx context.Context, alarmRef ref.AlarmRef) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "DeleteAlarm")
	err = w.base.DeleteAlarm(spanCtx, alarmRef)
	w.finishOp(spanCtx, span, "DeleteAlarm", start, err)

	return err
}

// DispatchJob implements components.ActorProvider
func (w *providerWrapper) DispatchJob(ctx context.Context, alarmRef ref.AlarmRef, req components.SetAlarmReq) (jobID string, err error) {
	spanCtx, span, start := w.beginOp(ctx, "DispatchJob")
	jobID, err = w.base.DispatchJob(spanCtx, alarmRef, req)
	w.finishOp(spanCtx, span, "DispatchJob", start, err)

	return jobID, err
}

// DeadLetterAlarm implements components.ActorProvider
func (w *providerWrapper) DeadLetterAlarm(ctx context.Context, lease *ref.AlarmLease, req components.DeadLetterAlarmReq) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "DeadLetterAlarm")
	err = w.base.DeadLetterAlarm(spanCtx, lease, req)
	w.finishOp(spanCtx, span, "DeadLetterAlarm", start, err)

	return err
}

// GetJob implements components.ActorProvider
func (w *providerWrapper) GetJob(ctx context.Context, jobID string) (res components.JobInfo, err error) {
	spanCtx, span, start := w.beginOp(ctx, "GetJob")
	res, err = w.base.GetJob(spanCtx, jobID)
	w.finishOp(spanCtx, span, "GetJob", start, err)

	return res, err
}

// ListJobs implements components.ActorProvider
func (w *providerWrapper) ListJobs(ctx context.Context, actorType string, actorID string) (res []components.JobInfo, err error) {
	spanCtx, span, start := w.beginOp(ctx, "ListJobs")
	res, err = w.base.ListJobs(spanCtx, actorType, actorID)
	w.finishOp(spanCtx, span, "ListJobs", start, err)

	return res, err
}

// CancelJob implements components.ActorProvider
func (w *providerWrapper) CancelJob(ctx context.Context, actorType string, actorID string, jobID string) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "CancelJob")
	err = w.base.CancelJob(spanCtx, actorType, actorID, jobID)
	w.finishOp(spanCtx, span, "CancelJob", start, err)

	return err
}

// GetDeadJob implements components.ActorProvider
func (w *providerWrapper) GetDeadJob(ctx context.Context, jobID string) (res components.GetDeadJobRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "GetDeadJob")
	res, err = w.base.GetDeadJob(spanCtx, jobID)
	w.finishOp(spanCtx, span, "GetDeadJob", start, err)

	return res, err
}

// DeleteDeadJob implements components.ActorProvider
func (w *providerWrapper) DeleteDeadJob(ctx context.Context, jobID string) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "DeleteDeadJob")
	err = w.base.DeleteDeadJob(spanCtx, jobID)
	w.finishOp(spanCtx, span, "DeleteDeadJob", start, err)

	return err
}

// RetryDeadJob implements components.ActorProvider
func (w *providerWrapper) RetryDeadJob(ctx context.Context, jobID string) (newJobID string, err error) {
	spanCtx, span, start := w.beginOp(ctx, "RetryDeadJob")
	newJobID, err = w.base.RetryDeadJob(spanCtx, jobID)
	w.finishOp(spanCtx, span, "RetryDeadJob", start, err)

	return newJobID, err
}

// FetchAndLeaseUpcomingAlarms implements components.ActorProvider
func (w *providerWrapper) FetchAndLeaseUpcomingAlarms(ctx context.Context, req components.FetchAndLeaseUpcomingAlarmsReq) (res []*ref.AlarmLease, err error) {
	spanCtx, span, start := w.beginOp(ctx, "FetchAndLeaseUpcomingAlarms")
	res, err = w.base.FetchAndLeaseUpcomingAlarms(spanCtx, req)
	w.finishOp(spanCtx, span, "FetchAndLeaseUpcomingAlarms", start, err)

	return res, err
}

// RenewAlarmLeases implements components.ActorProvider
func (w *providerWrapper) RenewAlarmLeases(ctx context.Context, req components.RenewAlarmLeasesReq) (res components.RenewAlarmLeasesRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "RenewAlarmLeases")
	res, err = w.base.RenewAlarmLeases(spanCtx, req)
	w.finishOp(spanCtx, span, "RenewAlarmLeases", start, err)

	return res, err
}

// ReleaseAlarmLease implements components.ActorProvider
func (w *providerWrapper) ReleaseAlarmLease(ctx context.Context, lease *ref.AlarmLease) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "ReleaseAlarmLease")
	err = w.base.ReleaseAlarmLease(spanCtx, lease)
	w.finishOp(spanCtx, span, "ReleaseAlarmLease", start, err)

	return err
}

// GetLeasedAlarm implements components.ActorProvider
func (w *providerWrapper) GetLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (res components.GetLeasedAlarmRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "GetLeasedAlarm")
	res, err = w.base.GetLeasedAlarm(spanCtx, lease)
	w.finishOp(spanCtx, span, "GetLeasedAlarm", start, err)

	return res, err
}

// UpdateLeasedAlarm implements components.ActorProvider
func (w *providerWrapper) UpdateLeasedAlarm(ctx context.Context, lease *ref.AlarmLease, req components.UpdateLeasedAlarmReq) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "UpdateLeasedAlarm")
	err = w.base.UpdateLeasedAlarm(spanCtx, lease, req)
	w.finishOp(spanCtx, span, "UpdateLeasedAlarm", start, err)

	return err
}

// DeleteLeasedAlarm implements components.ActorProvider
func (w *providerWrapper) DeleteLeasedAlarm(ctx context.Context, lease *ref.AlarmLease) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "DeleteLeasedAlarm")
	err = w.base.DeleteLeasedAlarm(spanCtx, lease)
	w.finishOp(spanCtx, span, "DeleteLeasedAlarm", start, err)

	return err
}

// GetState implements components.ActorProvider
func (w *providerWrapper) GetState(ctx context.Context, actorRef ref.ActorRef) (res []byte, err error) {
	spanCtx, span, start := w.beginOp(ctx, "GetState")
	res, err = w.base.GetState(spanCtx, actorRef)
	w.finishOp(spanCtx, span, "GetState", start, err)

	return res, err
}

// SetState implements components.ActorProvider
func (w *providerWrapper) SetState(ctx context.Context, actorRef ref.ActorRef, data []byte, opts components.SetStateOpts) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "SetState")
	err = w.base.SetState(spanCtx, actorRef, data, opts)
	w.finishOp(spanCtx, span, "SetState", start, err)

	return err
}

// DeleteState implements components.ActorProvider
func (w *providerWrapper) DeleteState(ctx context.Context, actorRef ref.ActorRef) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "DeleteState")
	err = w.base.DeleteState(spanCtx, actorRef)
	w.finishOp(spanCtx, span, "DeleteState", start, err)

	return err
}

// ListStates implements components.ActorProvider
func (w *providerWrapper) ListStates(ctx context.Context, req components.ListStatesReq) (res components.ListStatesRes, err error) {
	spanCtx, span, start := w.beginOp(ctx, "ListStates")
	res, err = w.base.ListStates(spanCtx, req)
	w.finishOp(spanCtx, span, "ListStates", start, err)

	return res, err
}

// Backup implements components.ActorProvider
func (w *providerWrapper) Backup(ctx context.Context, writer io.Writer) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "Backup")
	err = w.base.Backup(spanCtx, writer)
	w.finishOp(spanCtx, span, "Backup", start, err)

	return err
}

// Restore implements components.ActorProvider
func (w *providerWrapper) Restore(ctx context.Context, reader io.Reader) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "Restore")
	err = w.base.Restore(spanCtx, reader)
	w.finishOp(spanCtx, span, "Restore", start, err)

	return err
}

// HealthCheckPolicy implements components.ActorProvider
// This is a pure configuration read with no database access, so it is not traced
func (w *providerWrapper) HealthCheckPolicy() *components.HealthCheckPolicy {
	return w.base.HealthCheckPolicy()
}

// RenewLeaseInterval implements components.ActorProvider
// This is a pure configuration read with no database access, so it is not traced
func (w *providerWrapper) RenewLeaseInterval() time.Duration {
	return w.base.RenewLeaseInterval()
}

// AcquireExclusiveLease implements components.ActorProvider
func (w *providerWrapper) AcquireExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error) {
	spanCtx, span, start := w.beginOp(ctx, "AcquireExclusiveLease")
	expiresAt, err = w.base.AcquireExclusiveLease(spanCtx, owner, ttl)
	w.finishOp(spanCtx, span, "AcquireExclusiveLease", start, err)

	return expiresAt, err
}

// RenewExclusiveLease implements components.ActorProvider
func (w *providerWrapper) RenewExclusiveLease(ctx context.Context, owner string, ttl time.Duration) (expiresAt time.Time, err error) {
	spanCtx, span, start := w.beginOp(ctx, "RenewExclusiveLease")
	expiresAt, err = w.base.RenewExclusiveLease(spanCtx, owner, ttl)
	w.finishOp(spanCtx, span, "RenewExclusiveLease", start, err)

	return expiresAt, err
}

// ReleaseExclusiveLease implements components.ActorProvider
func (w *providerWrapper) ReleaseExclusiveLease(ctx context.Context, owner string) (err error) {
	spanCtx, span, start := w.beginOp(ctx, "ReleaseExclusiveLease")
	err = w.base.ReleaseExclusiveLease(spanCtx, owner)
	w.finishOp(spanCtx, span, "ReleaseExclusiveLease", start, err)

	return err
}
