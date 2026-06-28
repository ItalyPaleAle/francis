package protocol

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// InjectTraceContext serializes the trace context from ctx into the envelope using the globally configured propagator
// It is a no-op when no propagator is configured or no span is active, so messages stay clean unless tracing is enabled
// Only requests carry trace context, since a response is matched to its request by the stream it arrives on
func InjectTraceContext(ctx context.Context, e *Envelope) {
	// Inject into a map carrier so the composite propagator can add traceparent, tracestate, and baggage entries
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Leave the field unset when nothing was propagated so the wire stays unchanged with tracing disabled
	if len(carrier) == 0 {
		return
	}

	e.TraceContext = carrier
}

// ExtractTraceContext returns a context carrying the remote span context encoded in the inbound envelope
// It returns ctx unchanged when the envelope carries no trace context
func ExtractTraceContext(ctx context.Context, e *Envelope) context.Context {
	if len(e.TraceContext) == 0 {
		return ctx
	}

	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(e.TraceContext))
}
