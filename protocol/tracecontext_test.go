package protocol

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextRoundTrip(t *testing.T) {
	// Install a W3C trace context propagator for the duration of the test, restoring the previous one after
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prev)
	})

	// Build a context carrying a known remote span context
	traceID, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("0123456789abcdef")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	// Inject into a request envelope, then round-trip it through msgpack as it would travel on the wire
	env := NewEnvelope(KindLookupActor, nil)
	InjectTraceContext(ctx, env)
	require.NotEmpty(t, env.TraceContext, "trace context should be injected when a span is active")

	encoded, err := Marshal(env)
	require.NoError(t, err)
	decoded := &Envelope{}
	err = Unmarshal(encoded, decoded)
	require.NoError(t, err)

	// Extract on the receiving side and confirm the span context survived the trip
	got := trace.SpanContextFromContext(ExtractTraceContext(context.Background(), decoded))
	assert.Equal(t, sc.TraceID(), got.TraceID())
	assert.Equal(t, sc.SpanID(), got.SpanID())
	assert.True(t, got.IsRemote())
	assert.True(t, got.IsSampled())
}

func TestTraceContextNoopWithoutSpan(t *testing.T) {
	// Install a real propagator so the no-op is due to the absent span, not an absent propagator
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTextMapPropagator(prev)
	})

	// With no active span, nothing is written to the envelope so the wire stays unchanged
	env := NewEnvelope(KindLookupActor, nil)
	InjectTraceContext(context.Background(), env)
	assert.Empty(t, env.TraceContext)

	// Extracting from an envelope carrying no trace context yields no valid span context
	ctx := ExtractTraceContext(context.Background(), env)
	assert.False(t, trace.SpanContextFromContext(ctx).IsValid())
}
