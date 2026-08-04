package instrument

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	testSpanRecorder = tracetest.NewSpanRecorder()
	testTracerOnce   sync.Once
)

type spanRecorderView struct {
	start int
}

func (v *spanRecorderView) Ended() []sdktrace.ReadOnlySpan {
	ended := testSpanRecorder.Ended()
	return ended[v.start:]
}

// setupSpanRecorder installs one provider for the package and returns a test-local view of its spans
func setupSpanRecorder(t *testing.T) *spanRecorderView {
	t.Helper()

	testTracerOnce.Do(func() {
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(testSpanRecorder))
		otel.SetTracerProvider(tp)
	})

	return &spanRecorderView{
		start: len(testSpanRecorder.Ended()),
	}
}

// spansByName indexes the recorded spans by name
func spansByName(t *testing.T, sr *spanRecorderView) map[string]sdktrace.ReadOnlySpan {
	t.Helper()

	byName := make(map[string]sdktrace.ReadOnlySpan)
	for _, s := range sr.Ended() {
		byName[s.Name()] = s
	}

	return byName
}

// spanAttr returns the value of the span attribute with the given key, reporting whether it was present
func spanAttr(span sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, attr := range span.Attributes() {
		if string(attr.Key) == key {
			return attr.Value.AsString(), true
		}
	}

	return "", false
}

// captureHandler is a slog.Handler that collects records in memory for assertions
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
	level   slog.Level
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{
		level: slog.LevelDebug,
	}
}

func (h *captureHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

// attrStrings returns all attribute values of the record as strings, for "must not contain parameter values" assertions
func attrStrings(r slog.Record) []string {
	vals := make([]string, 0, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		vals = append(vals, a.Value.String())
		return true
	})

	return vals
}
