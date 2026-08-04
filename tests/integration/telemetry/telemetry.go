//go:build integration

// Package telemetry captures spans for integration scenarios that explicitly enable collection
package telemetry

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	captureEnabled atomic.Bool
	recorder       *tracetest.SpanRecorder
)

type captureSampler struct{}

func (captureSampler) ShouldSample(sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if captureEnabled.Load() {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}

	return sdktrace.SamplingResult{Decision: sdktrace.Drop}
}

func (captureSampler) Description() string {
	return "FrancisIntegrationCaptureSampler"
}

// Install binds package-level tracers to a dynamically sampled provider while leaving collection disabled by default
func Install(t *testing.T) {
	t.Helper()

	// Install once for the test binary because OpenTelemetry proxy tracers bind to the first concrete provider
	recorder = tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(captureSampler{}),
		sdktrace.WithSpanProcessor(recorder),
	)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		captureEnabled.Store(false)
		otel.SetTracerProvider(previous)
		shutdownErr := provider.Shutdown(context.Background())
		assert.NoError(t, shutdownErr)
	})
}

// StartCapture enables sampling and returns the first recorder index owned by the caller
func StartCapture() int {
	if recorder == nil {
		panic("integration telemetry is not installed")
	}

	offset := len(recorder.Ended())
	captureEnabled.Store(true)
	return offset
}

// StopCapture disables sampling for scenarios that do not assert telemetry
func StopCapture() {
	captureEnabled.Store(false)
}

// EndedSpanNames returns the names recorded at or after offset
func EndedSpanNames(offset int) map[string]bool {
	if recorder == nil {
		panic("integration telemetry is not installed")
	}

	spans := recorder.Ended()
	if offset > len(spans) {
		panic("integration telemetry offset is out of range")
	}

	names := make(map[string]bool, len(spans)-offset)
	for _, span := range spans[offset:] {
		names[span.Name()] = true
	}
	return names
}
