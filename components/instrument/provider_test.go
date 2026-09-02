package instrument

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"

	"github.com/italypaleale/francis/components"
	components_mocks "github.com/italypaleale/francis/internal/mocks/components"
	"github.com/italypaleale/francis/internal/ref"
)

var _ components.ActorProvider = (*providerWrapper)(nil)

func TestWrapProviderDelegatesAndSpans(t *testing.T) {
	sr := setupSpanRecorder(t)

	actorRef := ref.NewActorRef("testType", "actor-1")
	want := []byte("state-data")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		GetState(mock.Anything, actorRef).
		Return(want, nil)

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	got, err := p.GetState(t.Context(), actorRef)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	span := spansByName(t, sr)["provider.GetState"]
	require.NotNil(t, span, "expected a provider.GetState span")
	assert.NotEqual(t, codes.Error, span.Status().Code)

	method, ok := spanAttr(span, "francis.provider.method")
	require.True(t, ok)
	assert.Equal(t, "GetState", method)
}

func TestWrapProviderReturnsImmediateAlarmLease(t *testing.T) {
	sr := setupSpanRecorder(t)
	alarmRef := ref.NewAlarmRef("testType", "actor-1", "wake")
	dueTime := time.Now().Add(time.Second)
	req := components.SetAlarmReq{
		DueTime:        dueTime,
		Kind:           components.AlarmKindAlarm,
		LeaseImmediate: []string{"host-1"},
	}
	want := ref.NewAlarmLease(alarmRef, "alarm-id", dueTime, "lease-id")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		SetAlarm(mock.Anything, alarmRef, req).
		Return(want, nil)

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	got, err := p.SetAlarm(t.Context(), alarmRef, req)
	require.NoError(t, err)
	assert.Same(t, want, got)

	span := spansByName(t, sr)["provider.SetAlarm"]
	require.NotNil(t, span)
	assert.NotEqual(t, codes.Error, span.Status().Code)
}

func TestWrapProviderReturnsImmediateJobLease(t *testing.T) {
	sr := setupSpanRecorder(t)
	jobRef := ref.NewAlarmRef("testType", "actor-1", "job-key")
	dueTime := time.Now().Add(time.Second)
	req := components.SetAlarmReq{
		DueTime:        dueTime,
		Kind:           components.AlarmKindJob,
		JobMethod:      "process",
		LeaseImmediate: []string{"host-1"},
	}
	want := ref.NewAlarmLease(jobRef, "job-id", dueTime, "lease-id")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		DispatchJob(mock.Anything, jobRef, req).
		Return("job-id", want, nil)

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	jobID, got, err := p.DispatchJob(t.Context(), jobRef, req)
	require.NoError(t, err)
	assert.Equal(t, "job-id", jobID)
	assert.Same(t, want, got)

	span := spansByName(t, sr)["provider.DispatchJob"]
	require.NotNil(t, span)
	assert.NotEqual(t, codes.Error, span.Status().Code)
}

func TestWrapProviderRecordsErrors(t *testing.T) {
	sr := setupSpanRecorder(t)

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		UpdateActorHost(mock.Anything, "host-1", mock.Anything).
		Return(errors.New("database is locked (5) (SQLITE_BUSY)"))

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	err := p.UpdateActorHost(t.Context(), "host-1", components.UpdateActorHostReq{UpdateLastHealthCheck: true})
	require.Error(t, err)

	span := spansByName(t, sr)["provider.UpdateActorHost"]
	require.NotNil(t, span)
	assert.Equal(t, codes.Error, span.Status().Code)
	assert.Contains(t, span.Status().Description, "SQLITE_BUSY")
}

func TestWrapProviderBenignErrorsAreNotFailures(t *testing.T) {
	sr := setupSpanRecorder(t)

	actorRef := ref.NewActorRef("testType", "actor-1")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		GetState(mock.Anything, actorRef).
		Return(nil, components.ErrNoState)

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	_, err := p.GetState(t.Context(), actorRef)
	require.ErrorIs(t, err, components.ErrNoState)

	// A missing state is an expected outcome, so the span must not be marked failed
	span := spansByName(t, sr)["provider.GetState"]
	require.NotNil(t, span)
	assert.NotEqual(t, codes.Error, span.Status().Code)
}

func TestWrapProviderIsIdempotent(t *testing.T) {
	sr := setupSpanRecorder(t)

	actorRef := ref.NewActorRef("testType", "actor-1")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		GetState(mock.Anything, actorRef).
		Return(nil, components.ErrNoState)

	// Wrapping twice must not double-wrap: the inner provider sees exactly one call, and exactly one span is emitted
	p := WrapProvider(WrapProvider(base, nil, components.OperationLogConfig{}), nil, components.OperationLogConfig{})

	unwrapped, ok := UnwrapProvider(p)
	require.True(t, ok, "provider should be wrapped")
	assert.Same(t, base, unwrapped)

	_, err := p.GetState(t.Context(), actorRef)
	require.ErrorIs(t, err, components.ErrNoState)

	count := 0
	for _, s := range sr.Ended() {
		if s.Name() == "provider.GetState" {
			count++
		}
	}
	assert.Equal(t, 1, count, "double-wrapping must not emit duplicate spans")
}

func TestUnwrapProviderOnUnwrapped(t *testing.T) {
	base := components_mocks.NewMockActorProvider(t)

	unwrapped, ok := UnwrapProvider(base)
	assert.False(t, ok)
	assert.Same(t, base, unwrapped)
}

func TestWrapProviderRunIsNotTraced(t *testing.T) {
	sr := setupSpanRecorder(t)

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		Run(mock.Anything).
		Return(nil)

	p := WrapProvider(base, nil, components.OperationLogConfig{})
	err := p.Run(t.Context())
	require.NoError(t, err)

	assert.Empty(t, sr.Ended(), "Run must not emit spans")
}

func TestWrapProviderLogsDurations(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()

	actorRef := ref.NewActorRef("testType", "actor-1")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		GetState(mock.Anything, actorRef).
		Return(nil, components.ErrNoState)

	p := WrapProvider(base, slog.New(handler), components.OperationLogConfig{
		Enabled:       true,
		SlowThreshold: time.Hour,
	})

	_, err := p.GetState(t.Context(), actorRef)
	require.ErrorIs(t, err, components.ErrNoState)

	// The operation is logged at Debug, without the benign error attached
	require.Len(t, handler.records, 1)
	r := handler.records[0]
	assert.Equal(t, slog.LevelDebug, r.Level)
	assert.Equal(t, "Executed provider operation", r.Message)
	for _, v := range attrStrings(r) {
		assert.NotContains(t, v, "no state found")
	}
}

func TestWrapProviderSlowOperationWarns(t *testing.T) {
	setupSpanRecorder(t)

	handler := newCaptureHandler()

	base := components_mocks.NewMockActorProvider(t)
	// Keep the operation comfortably above the threshold across platforms with coarse clock resolution
	base.EXPECT().
		RenewAlarmLeases(mock.Anything, mock.Anything).
		After(10*time.Millisecond).
		Return(components.RenewAlarmLeasesRes{}, nil)

	p := WrapProvider(base, slog.New(handler), components.OperationLogConfig{
		SlowThreshold: time.Millisecond,
	})

	_, err := p.RenewAlarmLeases(t.Context(), components.RenewAlarmLeasesReq{})
	require.NoError(t, err)

	require.Len(t, handler.records, 1)
	r := handler.records[0]
	assert.Equal(t, slog.LevelWarn, r.Level)
	assert.Equal(t, "Slow provider operation", r.Message)

	var hasMethod bool
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "method" && a.Value.String() == "RenewAlarmLeases" {
			hasMethod = true
		}
		return true
	})
	assert.True(t, hasMethod)
}

func TestWrapProviderWarningOutcomeMarksSpanAndWarns(t *testing.T) {
	sr := setupSpanRecorder(t)
	handler := newCaptureHandler()
	actorRef := ref.NewActorRef("testType", "actor-1")

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		LookupActor(mock.Anything, actorRef, components.LookupActorOpts{}).
		Return(components.LookupActorRes{}, components.ErrNoHost)

	p := WrapProvider(base, slog.New(handler), components.OperationLogConfig{
		SlowThreshold: time.Hour,
	})
	_, err := p.LookupActor(t.Context(), actorRef, components.LookupActorOpts{})
	require.ErrorIs(t, err, components.ErrNoHost)

	span := spansByName(t, sr)["provider.LookupActor"]
	require.NotNil(t, span)
	assert.Equal(t, codes.Error, span.Status().Code)

	require.Len(t, handler.records, 1)
	record := handler.records[0]
	assert.Equal(t, slog.LevelWarn, record.Level)
	assert.Equal(t, "Provider operation warning", record.Message)
	assert.Contains(t, attrStrings(record), components.ErrNoHost.Error())
}

func TestWrapProviderRenewalLeaseLossFailsAndLogsError(t *testing.T) {
	sr := setupSpanRecorder(t)
	handler := newCaptureHandler()

	base := components_mocks.NewMockActorProvider(t)
	base.EXPECT().
		RenewExclusiveLease(mock.Anything, "admin", time.Minute).
		Return(time.Time{}, components.ErrExclusiveHeld)

	p := WrapProvider(base, slog.New(handler), components.OperationLogConfig{Enabled: true})
	_, err := p.RenewExclusiveLease(t.Context(), "admin", time.Minute)
	require.ErrorIs(t, err, components.ErrExclusiveHeld)

	span := spansByName(t, sr)["provider.RenewExclusiveLease"]
	require.NotNil(t, span)
	assert.Equal(t, codes.Error, span.Status().Code)

	require.Len(t, handler.records, 1)
	record := handler.records[0]
	assert.Equal(t, slog.LevelError, record.Level)
	assert.Equal(t, "Provider operation failed", record.Message)
	assert.Contains(t, attrStrings(record), components.ErrExclusiveHeld.Error())
}

func TestWrapProviderSkipsLogClockWhenLoggingIsDisabled(t *testing.T) {
	setupSpanRecorder(t)
	base := components_mocks.NewMockActorProvider(t)
	p := WrapProvider(base, slog.New(slog.DiscardHandler), components.OperationLogConfig{})
	wrapper, ok := p.(*providerWrapper)
	require.True(t, ok)

	ctx, span, start := wrapper.beginOp(t.Context(), "GetState")
	assert.True(t, start.IsZero())
	wrapper.finishOp(ctx, span, "GetState", start, nil)
}

func TestClassifyOperation(t *testing.T) {
	tests := []struct {
		name   string
		method string
		err    error
		want   operationDisposition
	}{
		{name: "success", method: "Init", want: operationSuccess},
		{name: "expected state absence", method: "GetState", err: components.ErrNoState, want: operationExpected},
		{name: "expected actor absence", method: "LookupActor", err: components.ErrNoActor, want: operationExpected},
		{name: "expected acquisition contention", method: "AcquireExclusiveLease", err: components.ErrExclusiveHeld, want: operationExpected},
		{name: "placement warning", method: "LookupActor", err: components.ErrNoHost, want: operationWarning},
		{name: "restore warning", method: "Restore", err: components.ErrHostsConnected, want: operationWarning},
		{name: "renewal lease loss", method: "RenewExclusiveLease", err: components.ErrExclusiveHeld, want: operationFailure},
		{name: "unexpected failure", method: "GetState", err: errors.New("database unavailable"), want: operationFailure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, classifyOperation(test.method, test.err))
		})
	}
}
